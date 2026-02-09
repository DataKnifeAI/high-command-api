import json
import logging
import psycopg2
from psycopg2 import pool
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Connection pool defaults (poller does 264+ planet saves + 44 campaign saves per cycle)
DEFAULT_POOL_MIN_CONN = 2
DEFAULT_POOL_MAX_CONN = 50


class _PooledConnection:
    """Wrapper that returns connection to pool on close() instead of closing it."""

    def __init__(self, conn, pool_instance):
        self._conn = conn
        self._pool = pool_instance

    def close(self):
        try:
            self._conn.rollback()
        except Exception:
            pass
        self._pool.putconn(self._conn)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)


class Database:
    """PostgreSQL database manager for Hell Divers 2 API data with connection pooling"""

    def __init__(self, database_url: Optional[str] = None):
        """
        Initialize database connection pool

        Args:
            database_url: PostgreSQL connection string (postgresql://user:pass@host:port/db)
                         If None, will try to get from DATABASE_URL env var
        """
        if database_url is None:
            import os
            database_url = os.getenv("DATABASE_URL", "")

        if not database_url:
            raise ValueError("DATABASE_URL must be provided")

        self.database_url = database_url
        self._initialized = False
        self._pool: Optional[pool.ThreadedConnectionPool] = None

    def _get_pool(self) -> pool.ThreadedConnectionPool:
        """Get or create the connection pool (lazy init)"""
        if self._pool is None:
            import os
            maxconn = DEFAULT_POOL_MAX_CONN
            if (env_max := os.getenv("POOL_MAX_CONN")) is not None:
                try:
                    maxconn = int(env_max)
                except ValueError:
                    pass
            self._pool = pool.ThreadedConnectionPool(
                minconn=DEFAULT_POOL_MIN_CONN,
                maxconn=maxconn,
                dsn=self.database_url,
            )
            logger.info("Database connection pool initialized")
        return self._pool

    def _get_connection(self):
        """Get a database connection from the pool (returns to pool on close)."""
        conn = self._get_pool().getconn()
        return _PooledConnection(conn, self._get_pool())

    def close_pool(self):
        """Close the connection pool. Call on application shutdown."""
        if self._pool is not None:
            self._pool.closeall()
            self._pool = None
            logger.info("Database connection pool closed")

    @staticmethod
    def _parse_expiration_time(expiration_time: str) -> Optional[datetime]:
        """Parse ISO 8601 expiration time string and return as UTC datetime.
        
        Args:
            expiration_time: ISO 8601 formatted string (e.g., "2025-10-26T12:00:00Z")
            
        Returns:
            Timezone-aware datetime in UTC, or None if parsing fails
        """
        try:
            # Parse ISO 8601 format, normalize 'Z' to UTC offset
            dt = datetime.fromisoformat(expiration_time.replace('Z', '+00:00'))
            # Ensure result is timezone-aware (UTC)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except (ValueError, AttributeError):
            return None

    def _init_db(self):
        """Initialize database schema (lazy - called on first use)"""
        conn = None
        try:
            conn = self._get_connection()
            cursor = conn.cursor()

            # War Status Table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS war_status (
                    id SERIAL PRIMARY KEY,
                    data JSONB NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Statistics Table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS statistics (
                    id SERIAL PRIMARY KEY,
                    data JSONB NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Planet Status Table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS planet_status (
                    id SERIAL PRIMARY KEY,
                    planet_index INTEGER UNIQUE NOT NULL,
                    data JSONB NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Campaigns Table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS campaigns (
                    id SERIAL PRIMARY KEY,
                    campaign_id BIGINT UNIQUE NOT NULL,
                    planet_index INTEGER,
                    status TEXT,
                    data JSONB NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Assignments Table (Major Orders)
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS assignments (
                    id SERIAL PRIMARY KEY,
                    assignment_id BIGINT UNIQUE NOT NULL,
                    data JSONB NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Dispatches Table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS dispatches (
                    id SERIAL PRIMARY KEY,
                    dispatch_id BIGINT UNIQUE NOT NULL,
                    data JSONB NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Planet Events Table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS planet_events (
                    id SERIAL PRIMARY KEY,
                    event_id BIGINT UNIQUE NOT NULL,
                    planet_index INTEGER,
                    event_type TEXT,
                    data JSONB NOT NULL,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # System Status Table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS system_status (
                    id SERIAL PRIMARY KEY,
                    key TEXT UNIQUE NOT NULL,
                    value TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Create indexes for frequently queried columns
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_war_status_timestamp ON war_status(timestamp)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_statistics_timestamp ON statistics(timestamp)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_planet_status_index ON planet_status(planet_index)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_campaigns_timestamp ON campaigns(timestamp)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_assignments_timestamp ON assignments(timestamp)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_dispatches_timestamp ON dispatches(timestamp)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_planet_events_index ON planet_events(planet_index)"
            )

            conn.commit()
            conn.close()
        except psycopg2.OperationalError:
            # Database connection failed - this is OK during tests/imports
            # Schema will be created when first actual operation happens
            pass
        except Exception:
            # Any other error during init is also OK - will be handled on first use
            pass

    def save_war_status(self, data: Dict) -> bool:
        """Save war status to database"""
        try:
            self._init_db()  # Ensure schema exists (idempotent)
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO war_status (data) VALUES (%s)",
                    (json.dumps(data),)
                )
                conn.commit()
                return True
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to save war status: {e}")
            return False

    def save_statistics(self, data: Dict) -> bool:
        """Save statistics to database"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO statistics (data) VALUES (%s)",
                    (json.dumps(data),)
                )
                conn.commit()
                return True
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to save statistics: {e}")
            return False

    def save_planet_status(self, planet_index: int, data: Dict) -> bool:
        """Save or update planet status"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """INSERT INTO planet_status (planet_index, data) 
                       VALUES (%s, %s)
                       ON CONFLICT (planet_index) 
                       DO UPDATE SET data = EXCLUDED.data, timestamp = CURRENT_TIMESTAMP""",
                    (planet_index, json.dumps(data)),
                )
                conn.commit()
                return True
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to save planet status: {e}")
            return False

    def save_campaign(self, campaign_id: int, planet_index: int, data: Dict) -> bool:
        """Save campaign to database"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                # Check if campaign has expired
                expiration_time = data.get("expiresAt")
                status = "unknown"
                if expiration_time:
                    exp_dt = self._parse_expiration_time(expiration_time)
                    if exp_dt:
                        now = datetime.now(timezone.utc)
                        status = "active" if now < exp_dt else "expired"
                    else:
                        status = "active"  # Default to active if parsing fails
                else:
                    status = "active"

                cursor.execute(
                    """INSERT INTO campaigns (campaign_id, planet_index, status, data) 
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (campaign_id) 
                       DO UPDATE SET planet_index = EXCLUDED.planet_index, 
                                     status = EXCLUDED.status, 
                                     data = EXCLUDED.data, 
                                     timestamp = CURRENT_TIMESTAMP""",
                    (campaign_id, planet_index, status, json.dumps(data)),
                )
                conn.commit()
                return True
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to save campaign: {e}")
            return False

    def get_latest_war_status(self) -> Optional[Dict]:
        """Get the latest war status"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT data FROM war_status ORDER BY timestamp DESC LIMIT 1")
                result = cursor.fetchone()
                if result:
                    # JSONB returns as dict, convert to dict if needed
                    data = result[0]
                    if isinstance(data, dict):
                        return data
                    return json.loads(data) if isinstance(data, str) else data
                return None
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to get war status: {e}")
            return None

    def get_latest_statistics(self) -> Optional[Dict]:
        """Get the latest statistics"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT data FROM statistics ORDER BY timestamp DESC LIMIT 1"
                )
                result = cursor.fetchone()
                if result:
                    data = result[0]
                    if isinstance(data, dict):
                        return data
                    return json.loads(data) if isinstance(data, str) else data
                return None
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return None

    def get_planet_status(self, planet_index: int) -> Optional[Dict]:
        """Get planet status by index"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT data FROM planet_status WHERE planet_index = %s ORDER BY timestamp DESC LIMIT 1",
                    (planet_index,),
                )
                result = cursor.fetchone()
                if result:
                    data = result[0]
                    if isinstance(data, dict):
                        return data
                    return json.loads(data) if isinstance(data, str) else data
                return None
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to get planet status: {e}")
            return None

    def get_active_campaigns(self) -> List[Dict]:
        """Get all active campaigns"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT data FROM campaigns WHERE status = %s ORDER BY timestamp DESC",
                    ("active",),
                )
                results = cursor.fetchall()
                campaigns = []
                for row in results:
                    data = row[0]
                    if isinstance(data, dict):
                        campaigns.append(data)
                    else:
                        campaigns.append(json.loads(data) if isinstance(data, str) else data)
                
                # Filter campaigns to include only those not yet expired
                active_campaigns = []
                now = datetime.now(timezone.utc)
                for campaign in campaigns:
                    expiration_time = campaign.get("expiresAt")
                    if expiration_time:
                        exp_dt = self._parse_expiration_time(expiration_time)
                        if exp_dt is None or now < exp_dt:
                            active_campaigns.append(campaign)
                    else:
                        active_campaigns.append(campaign)
                
                return active_campaigns
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to get active campaigns: {e}")
            return []

    def get_assignment(self, limit: int = 10) -> List[Dict]:
        """Get assignments with optional limit"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT data FROM assignments ORDER BY timestamp DESC LIMIT %s",
                    (limit,),
                )
                results = cursor.fetchall()
                assignments = []
                for row in results:
                    data = row[0]
                    if isinstance(data, dict):
                        assignments.append(data)
                    else:
                        assignments.append(json.loads(data) if isinstance(data, str) else data)
                return assignments
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to get assignments: {e}")
            return []

    def get_latest_assignments(self, limit: int = 10) -> List[Dict]:
        """Get latest assignments (alias for get_assignment)"""
        return self.get_assignment(limit)

    def save_assignment(self, assignment_id: int, data: Dict) -> bool:
        """Save assignment to database"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """INSERT INTO assignments (assignment_id, data) 
                       VALUES (%s, %s)
                       ON CONFLICT (assignment_id) 
                       DO UPDATE SET data = EXCLUDED.data, timestamp = CURRENT_TIMESTAMP""",
                    (assignment_id, json.dumps(data)),
                )
                conn.commit()
                return True
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to save assignment: {e}")
            return False

    def save_dispatch(self, dispatch_id: int, data: Dict) -> bool:
        """Save dispatch to database"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """INSERT INTO dispatches (dispatch_id, data) 
                       VALUES (%s, %s)
                       ON CONFLICT (dispatch_id) 
                       DO UPDATE SET data = EXCLUDED.data, timestamp = CURRENT_TIMESTAMP""",
                    (dispatch_id, json.dumps(data)),
                )
                conn.commit()
                return True
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to save dispatch: {e}")
            return False

    def get_dispatches(self, limit: int = 10) -> List[Dict]:
        """Get dispatches with optional limit, sorted by published date (newest first)"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT data FROM dispatches ORDER BY timestamp DESC"
                )
                results = cursor.fetchall()
                # Parse and sort by published date from JSON data (newest first)
                dispatches = []
                for row in results:
                    data = row[0]
                    if isinstance(data, dict):
                        dispatches.append(data)
                    else:
                        dispatches.append(json.loads(data) if isinstance(data, str) else data)
                dispatches.sort(
                    key=lambda x: x.get("published", ""),
                    reverse=True
                )
                return dispatches[:limit]
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to get dispatches: {e}")
            return []

    def get_latest_dispatches(self, limit: int = 10) -> List[Dict]:
        """Get latest dispatches (alias for get_dispatches)"""
        return self.get_dispatches(limit)

    def save_assignments(self, data: List[Dict]) -> bool:
        """Save assignments (Major Orders) to database"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                for assignment in data:
                    assignment_id = assignment.get("id")
                    if assignment_id:
                        cursor.execute(
                            """INSERT INTO assignments (assignment_id, data) 
                               VALUES (%s, %s)
                               ON CONFLICT (assignment_id) 
                               DO UPDATE SET data = EXCLUDED.data, timestamp = CURRENT_TIMESTAMP""",
                            (assignment_id, json.dumps(assignment)),
                        )
                conn.commit()
                return True
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to save assignments: {e}")
            return False

    def save_dispatches(self, data: List[Dict]) -> bool:
        """Save dispatches (news/announcements) to database"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                for dispatch in data:
                    dispatch_id = dispatch.get("id")
                    if dispatch_id:
                        cursor.execute(
                            """INSERT INTO dispatches (dispatch_id, data) 
                               VALUES (%s, %s)
                               ON CONFLICT (dispatch_id) 
                               DO UPDATE SET data = EXCLUDED.data, timestamp = CURRENT_TIMESTAMP""",
                            (dispatch_id, json.dumps(dispatch)),
                        )
                conn.commit()
                return True
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to save dispatches: {e}")
            return False

    def save_planet_event(self, event_id: int, planet_index: int, event_type: str, data: Dict) -> bool:
        """Save planet event to database"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """INSERT INTO planet_events (event_id, planet_index, event_type, data) 
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (event_id) 
                       DO UPDATE SET planet_index = EXCLUDED.planet_index, 
                                     event_type = EXCLUDED.event_type, 
                                     data = EXCLUDED.data, 
                                     timestamp = CURRENT_TIMESTAMP""",
                    (event_id, planet_index, event_type, json.dumps(data)),
                )
                conn.commit()
                return True
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to save planet event: {e}")
            return False

    def save_planet_events(self, data: List[Dict]) -> bool:
        """Save planet events to database"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                for event in data:
                    event_id = event.get("id")
                    # Support both snake_case and camelCase for planet_index, explicit None checks
                    planet_index = event.get("planet_index") if "planet_index" in event else event.get("planetIndex")
                    event_type = event.get("event_type") if "event_type" in event else event.get("eventType", "unknown")
                    if event_id and planet_index:
                        cursor.execute(
                            """INSERT INTO planet_events (event_id, planet_index, event_type, data) 
                               VALUES (%s, %s, %s, %s)
                               ON CONFLICT (event_id) 
                               DO UPDATE SET planet_index = EXCLUDED.planet_index, 
                                             event_type = EXCLUDED.event_type, 
                                             data = EXCLUDED.data, 
                                             timestamp = CURRENT_TIMESTAMP""",
                            (event_id, planet_index, event_type, json.dumps(event)),
                        )
                conn.commit()
                return True
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to save planet events: {e}")
            return False

    def get_planet_events(
        self, planet_index: Optional[int] = None, limit: int = 10
    ) -> List[Dict]:
        """Get planet events with optional filtering"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                if planet_index:
                    cursor.execute(
                        "SELECT data FROM planet_events WHERE planet_index = %s ORDER BY timestamp DESC LIMIT %s",
                        (planet_index, limit),
                    )
                else:
                    cursor.execute(
                        "SELECT data FROM planet_events ORDER BY timestamp DESC LIMIT %s",
                        (limit,),
                    )
                results = cursor.fetchall()
                events = []
                for row in results:
                    data = row[0]
                    if isinstance(data, dict):
                        events.append(data)
                    else:
                        events.append(json.loads(data) if isinstance(data, str) else data)
                return events
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to get planet events: {e}")
            return []

    def get_latest_planet_events(self, limit: int = 10) -> List[Dict]:
        """Get latest planet events (alias for get_planet_events with no planet_index filter)"""
        return self.get_planet_events(limit=limit)

    def get_planet_status_history(self, planet_index: int, limit: int = 10) -> List[Dict]:
        """Get status history for a planet"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT data, timestamp FROM planet_status WHERE planet_index = %s ORDER BY timestamp DESC LIMIT %s",
                    (planet_index, limit),
                )
                results = cursor.fetchall()
                history = []
                for row in results:
                    data = row[0]
                    if isinstance(data, dict):
                        history.append({"data": data, "timestamp": row[1]})
                    else:
                        history.append({"data": json.loads(data) if isinstance(data, str) else data, "timestamp": row[1]})
                return history
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to get planet status history: {e}")
            return []

    def get_statistics_history(self, limit: int = 100) -> List[Dict]:
        """Get statistics history"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT data, timestamp FROM statistics ORDER BY timestamp DESC LIMIT %s",
                    (limit,),
                )
                results = cursor.fetchall()
                history = []
                for row in results:
                    data = row[0]
                    if isinstance(data, dict):
                        history.append({"data": data, "timestamp": row[1]})
                    else:
                        history.append({"data": json.loads(data) if isinstance(data, str) else data, "timestamp": row[1]})
                return history
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to get statistics history: {e}")
            return []

    def get_latest_planets_snapshot(self) -> Optional[List[Dict]]:
        """Get most recent cached snapshot of all planets

        Used as fallback when live API is unavailable.
        Returns all planet status records from the most recent collection cycle.
        """
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                # Get the most recent timestamp from planet_status
                cursor.execute(
                    "SELECT DISTINCT timestamp FROM planet_status ORDER BY timestamp DESC LIMIT 1"
                )
                result = cursor.fetchone()

                if not result:
                    return None

                latest_timestamp = result[0]

                # Get all planets from that timestamp
                cursor.execute(
                    "SELECT data FROM planet_status WHERE timestamp = %s ORDER BY planet_index ASC",
                    (latest_timestamp,),
                )
                results = cursor.fetchall()
                planets = []
                for row in results:
                    data = row[0]
                    if isinstance(data, dict):
                        planets.append(data)
                    else:
                        planets.append(json.loads(data) if isinstance(data, str) else data)
                return planets if planets else None
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to get latest planets snapshot: {e}")
            return None

    def get_latest_campaigns_snapshot(self) -> Optional[List[Dict]]:
        """Get most recent cached snapshot of all campaigns

        Used as fallback when live API is unavailable.
        Returns most recent campaign data for each campaign.
        """
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                # Get the most recent campaign data for each campaign_id
                cursor.execute(
                    """SELECT data FROM campaigns 
                       WHERE (campaign_id, timestamp) IN (
                           SELECT campaign_id, MAX(timestamp) FROM campaigns GROUP BY campaign_id
                       )
                       ORDER BY timestamp DESC"""
                )
                results = cursor.fetchall()
                campaigns = []
                for row in results:
                    data = row[0]
                    if isinstance(data, dict):
                        campaigns.append(data)
                    else:
                        campaigns.append(json.loads(data) if isinstance(data, str) else data)
                return campaigns if campaigns else None
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to get latest campaigns snapshot: {e}")
            return None

    def get_latest_factions_snapshot(self) -> Optional[List[Dict]]:
        """Get most recent cached snapshot of all factions

        Used as fallback when live API is unavailable.
        Factions are extracted from war status data.
        """
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT data FROM war_status ORDER BY timestamp DESC LIMIT 1")
                result = cursor.fetchone()

                if not result:
                    return None

                war_data = result[0]
                if isinstance(war_data, dict):
                    return war_data.get("factions", None)
                else:
                    war_dict = json.loads(war_data) if isinstance(war_data, str) else war_data
                    return war_dict.get("factions", None)
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to get latest factions snapshot: {e}")
            return None

    def get_latest_biomes_snapshot(self) -> Optional[List[Dict]]:
        """Get most recent cached snapshot of all biomes

        Used as fallback when live API is unavailable.
        Biomes are extracted from planet data.
        """
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                # Get the most recent timestamp from planet_status
                cursor.execute(
                    "SELECT DISTINCT timestamp FROM planet_status ORDER BY timestamp DESC LIMIT 1"
                )
                result = cursor.fetchone()

                if not result:
                    return None

                latest_timestamp = result[0]

                # Get all planets from that timestamp and extract unique biomes
                cursor.execute(
                    "SELECT data FROM planet_status WHERE timestamp = %s",
                    (latest_timestamp,),
                )
                results = cursor.fetchall()

                if not results:
                    return None

                # Extract unique biomes from planets
                biomes = {}
                for row in results:
                    planet_data = row[0]
                    if isinstance(planet_data, str):
                        planet_data = json.loads(planet_data)
                    elif not isinstance(planet_data, dict):
                        continue
                    
                    # Type guard: check if biome is a dict before accessing .get()
                    if "biome" in planet_data and isinstance(planet_data["biome"], dict):
                        biome_name = planet_data["biome"].get("name")
                        if biome_name and biome_name not in biomes:
                            biomes[biome_name] = planet_data["biome"]

                return list(biomes.values()) if biomes else None
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to get latest biomes snapshot: {e}")
            return None

    def update_system_status(self, key: str, value: str) -> bool:
        """Update system status"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute(
                    """INSERT INTO system_status (key, value) 
                       VALUES (%s, %s)
                       ON CONFLICT (key) 
                       DO UPDATE SET value = EXCLUDED.value, timestamp = CURRENT_TIMESTAMP""",
                    (key, value),
                )
                conn.commit()
                return True
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to update system status: {e}")
            return False

    def get_system_status(self, key: str) -> Optional[str]:
        """Get system status value"""
        try:
            conn = self._get_connection()
            try:
                cursor = conn.cursor()
                cursor.execute("SELECT value FROM system_status WHERE key = %s", (key,))
                result = cursor.fetchone()
                return result[0] if result else None
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Failed to get system status: {e}")
            return None

    def set_upstream_status(self, available: bool) -> bool:
        """Set upstream API availability status"""
        return self.update_system_status("upstream_api_available", "true" if available else "false")

    def get_upstream_status(self) -> bool:
        """Get upstream API availability status"""
        status = self.get_system_status("upstream_api_available")
        return status == "true" if status else False
