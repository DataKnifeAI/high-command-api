import json
import logging
import sqlite3
from datetime import datetime, timezone
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class Database:
    """SQLite database manager for Hell Divers 2 API data"""

    def __init__(self, db_path: str = "helldivers2.db"):
        self.db_path = db_path
        self._init_db()

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
        """Initialize database schema"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # War Status Table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS war_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Statistics Table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS statistics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    data TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Planet Status Table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS planet_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    planet_index INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Campaigns Table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS campaigns (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    campaign_id INTEGER UNIQUE NOT NULL,
                    planet_index INTEGER,
                    status TEXT,
                    data TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Assignments Table (Major Orders)
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS assignments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    assignment_id INTEGER UNIQUE NOT NULL,
                    data TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Dispatches Table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS dispatches (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dispatch_id INTEGER UNIQUE NOT NULL,
                    data TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # Planet Events Table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS planet_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id INTEGER UNIQUE NOT NULL,
                    planet_index INTEGER,
                    event_type TEXT,
                    data TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

            # System Status Table
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS system_status (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT UNIQUE NOT NULL,
                    value TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
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

    def save_war_status(self, data: Dict) -> bool:
        """Save war status to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO war_status (data) VALUES (?)", (json.dumps(data),)
                )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save war status: {e}")
            return False

    def save_statistics(self, data: Dict) -> bool:
        """Save statistics to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT INTO statistics (data) VALUES (?)", (json.dumps(data),)
                )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save statistics: {e}")
            return False

    def save_planet_status(self, planet_index: int, data: Dict) -> bool:
        """Save or update planet status"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO planet_status (planet_index, data) VALUES (?, ?)",
                    (planet_index, json.dumps(data)),
                )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save planet status: {e}")
            return False

    def save_campaign(self, campaign_id: int, planet_index: int, data: Dict) -> bool:
        """Save campaign to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
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
                    "INSERT OR REPLACE INTO campaigns (campaign_id, planet_index, status, data) VALUES (?, ?, ?, ?)",
                    (campaign_id, planet_index, status, json.dumps(data)),
                )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save campaign: {e}")
            return False

    def get_latest_war_status(self) -> Optional[Dict]:
        """Get the latest war status"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT data FROM war_status ORDER BY timestamp DESC LIMIT 1")
                result = cursor.fetchone()
                return json.loads(result[0]) if result else None
        except Exception as e:
            logger.error(f"Failed to get war status: {e}")
            return None

    def get_latest_statistics(self) -> Optional[Dict]:
        """Get the latest statistics"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT data FROM statistics ORDER BY timestamp DESC LIMIT 1"
                )
                result = cursor.fetchone()
                return json.loads(result[0]) if result else None
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return None

    def get_planet_status(self, planet_index: int) -> Optional[Dict]:
        """Get planet status by index"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT data FROM planet_status WHERE planet_index = ?",
                    (planet_index,),
                )
                result = cursor.fetchone()
                return json.loads(result[0]) if result else None
        except Exception as e:
            logger.error(f"Failed to get planet status: {e}")
            return None

    def get_active_campaigns(self) -> List[Dict]:
        """Get all active campaigns"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT data FROM campaigns WHERE status = ? ORDER BY timestamp DESC",
                    ("active",),
                )
                results = cursor.fetchall()
                campaigns = [json.loads(row[0]) for row in results]
                
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
        except Exception as e:
            logger.error(f"Failed to get active campaigns: {e}")
            return []

    def get_assignment(self, limit: int = 10) -> List[Dict]:
        """Get assignments with optional limit"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT data FROM assignments ORDER BY timestamp DESC LIMIT ?",
                    (limit,),
                )
                results = cursor.fetchall()
                return [json.loads(row[0]) for row in results]
        except Exception as e:
            logger.error(f"Failed to get assignments: {e}")
            return []

    def get_latest_assignments(self, limit: int = 10) -> List[Dict]:
        """Get latest assignments (alias for get_assignment)"""
        return self.get_assignment(limit)

    def save_assignment(self, assignment_id: int, data: Dict) -> bool:
        """Save assignment to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO assignments (assignment_id, data) VALUES (?, ?)",
                    (assignment_id, json.dumps(data)),
                )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save assignment: {e}")
            return False

    def save_dispatch(self, dispatch_id: int, data: Dict) -> bool:
        """Save dispatch to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO dispatches (dispatch_id, data) VALUES (?, ?)",
                    (dispatch_id, json.dumps(data)),
                )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save dispatch: {e}")
            return False

    def get_dispatches(self, limit: int = 10) -> List[Dict]:
        """Get dispatches with optional limit, sorted by published date (newest first)"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT data FROM dispatches ORDER BY timestamp DESC",
                    ()
                )
                results = cursor.fetchall()
                # Parse and sort by published date from JSON data (newest first)
                dispatches = [json.loads(row[0]) for row in results]
                dispatches.sort(
                    key=lambda x: x.get("published", ""),
                    reverse=True
                )
                return dispatches[:limit]
        except Exception as e:
            logger.error(f"Failed to get dispatches: {e}")
            return []

    def get_latest_dispatches(self, limit: int = 10) -> List[Dict]:
        """Get latest dispatches (alias for get_dispatches)"""
        return self.get_dispatches(limit)

    def save_assignments(self, data: List[Dict]) -> bool:
        """Save assignments (Major Orders) to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                for assignment in data:
                    assignment_id = assignment.get("id")
                    if assignment_id:
                        cursor.execute(
                            "INSERT OR REPLACE INTO assignments (assignment_id, data) VALUES (?, ?)",
                            (assignment_id, json.dumps(assignment)),
                        )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save assignments: {e}")
            return False

    def save_dispatches(self, data: List[Dict]) -> bool:
        """Save dispatches (news/announcements) to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                for dispatch in data:
                    dispatch_id = dispatch.get("id")
                    if dispatch_id:
                        cursor.execute(
                            "INSERT OR REPLACE INTO dispatches (dispatch_id, data) VALUES (?, ?)",
                            (dispatch_id, json.dumps(dispatch)),
                        )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save dispatches: {e}")
            return False

    def save_planet_event(self, event_id: int, planet_index: int, event_type: str, data: Dict) -> bool:
        """Save planet event to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO planet_events (event_id, planet_index, event_type, data) VALUES (?, ?, ?, ?)",
                    (event_id, planet_index, event_type, json.dumps(data)),
                )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save planet event: {e}")
            return False

    def save_planet_events(self, data: List[Dict]) -> bool:
        """Save planet events to database"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                for event in data:
                    event_id = event.get("id")
                    # Support both snake_case and camelCase for planet_index, explicit None checks
                    planet_index = event.get("planet_index") if "planet_index" in event else event.get("planetIndex")
                    event_type = event.get("event_type") if "event_type" in event else event.get("eventType", "unknown")
                    if event_id and planet_index:
                        cursor.execute(
                            "INSERT OR REPLACE INTO planet_events (event_id, planet_index, event_type, data) VALUES (?, ?, ?, ?)",
                            (event_id, planet_index, event_type, json.dumps(event)),
                        )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to save planet events: {e}")
            return False
    def get_planet_events(
        self, planet_index: Optional[int] = None, limit: int = 10
    ) -> List[Dict]:
        """Get planet events with optional filtering"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                if planet_index:
                    cursor.execute(
                        "SELECT data FROM planet_events WHERE planet_index = ? ORDER BY timestamp DESC LIMIT ?",
                        (planet_index, limit),
                    )
                else:
                    cursor.execute(
                        "SELECT data FROM planet_events ORDER BY timestamp DESC LIMIT ?",
                        (limit,),
                    )
                results = cursor.fetchall()
                return [json.loads(row[0]) for row in results]
        except Exception as e:
            logger.error(f"Failed to get planet events: {e}")
            return []

    def get_latest_planet_events(self, limit: int = 10) -> List[Dict]:
        """Get latest planet events (alias for get_planet_events with no planet_index filter)"""
        return self.get_planet_events(limit=limit)

    def get_planet_status_history(self, planet_index: int, limit: int = 10) -> List[Dict]:
        """Get status history for a planet"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT data, timestamp FROM planet_status WHERE planet_index = ? ORDER BY timestamp DESC LIMIT ?",
                    (planet_index, limit),
                )
                results = cursor.fetchall()
                return [{"data": json.loads(row[0]), "timestamp": row[1]} for row in results]
        except Exception as e:
            logger.error(f"Failed to get planet status history: {e}")
            return []

    def get_statistics_history(self, limit: int = 100) -> List[Dict]:
        """Get statistics history"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT data, timestamp FROM statistics ORDER BY timestamp DESC LIMIT ?",
                    (limit,),
                )
                results = cursor.fetchall()
                return [{"data": json.loads(row[0]), "timestamp": row[1]} for row in results]
        except Exception as e:
            logger.error(f"Failed to get statistics history: {e}")
            return []

    def get_latest_planets_snapshot(self) -> Optional[List[Dict]]:
        """Get most recent cached snapshot of all planets

        Used as fallback when live API is unavailable.
        Returns all planet status records from the most recent collection cycle.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
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
                    "SELECT data FROM planet_status WHERE timestamp = ? ORDER BY planet_index ASC",
                    (latest_timestamp,),
                )
                results = cursor.fetchall()
                return [json.loads(row[0]) for row in results] if results else None
        except Exception as e:
            logger.error(f"Failed to get latest planets snapshot: {e}")
            return None

    def get_latest_campaigns_snapshot(self) -> Optional[List[Dict]]:
        """Get most recent cached snapshot of all campaigns

        Used as fallback when live API is unavailable.
        Returns most recent campaign data for each campaign.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
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
                return [json.loads(row[0]) for row in results] if results else None
        except Exception as e:
            logger.error(f"Failed to get latest campaigns snapshot: {e}")
            return None

    def get_latest_factions_snapshot(self) -> Optional[List[Dict]]:
        """Get most recent cached snapshot of all factions

        Used as fallback when live API is unavailable.
        Factions are extracted from war status data.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT data FROM war_status ORDER BY timestamp DESC LIMIT 1")
                result = cursor.fetchone()

                if not result:
                    return None

                war_data = json.loads(result[0])
                return war_data.get("factions", None)
        except Exception as e:
            logger.error(f"Failed to get latest factions snapshot: {e}")
            return None

    def get_latest_biomes_snapshot(self) -> Optional[List[Dict]]:
        """Get most recent cached snapshot of all biomes

        Used as fallback when live API is unavailable.
        Biomes are extracted from planet data.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
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
                    "SELECT data FROM planet_status WHERE timestamp = ?",
                    (latest_timestamp,),
                )
                results = cursor.fetchall()

                if not results:
                    return None

                # Extract unique biomes from planets
                biomes = {}
                for row in results:
                    planet_data = json.loads(row[0])
                    # Type guard: check if biome is a dict before accessing .get()
                    if "biome" in planet_data and isinstance(planet_data["biome"], dict):
                        biome_name = planet_data["biome"].get("name")
                        if biome_name and biome_name not in biomes:
                            biomes[biome_name] = planet_data["biome"]

                return list(biomes.values()) if biomes else None
        except Exception as e:
            logger.error(f"Failed to get latest biomes snapshot: {e}")
            return None

    def update_system_status(self, key: str, value: str) -> bool:
        """Update system status"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "INSERT OR REPLACE INTO system_status (key, value) VALUES (?, ?)",
                    (key, value),
                )
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to update system status: {e}")
            return False

    def get_system_status(self, key: str) -> Optional[str]:
        """Get system status value"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT value FROM system_status WHERE key = ?", (key,))
                result = cursor.fetchone()
                return result[0] if result else None
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
