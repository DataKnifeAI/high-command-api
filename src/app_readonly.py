import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from src.config import Config
from src.database import Database
from src.scraper import HellDivers2Scraper
from src.claude_proxy import router as claude_router

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Initialize database and scraper (for fallback reads only)
database_url = Config.DATABASE_URL
if not database_url:
    raise ValueError("DATABASE_URL environment variable must be set")
db = Database(database_url)
scraper = HellDivers2Scraper()


# Lifespan context manager for startup/shutdown
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting Hell Divers 2 API (Read-Only Mode)")
    yield
    # Shutdown
    logger.info("Shutting down Hell Divers 2 API")
    scraper.close()
    db.close_pool()


# Initialize FastAPI app
app = FastAPI(
    title="Hell Divers 2 API",
    description="Read-only API for Hell Divers 2 game data",
    version="1.0.0",
    lifespan=lifespan,
)

# Claude proxy (when CLAUDE_API_KEY is set)
app.include_router(claude_router)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========================
# War Status Endpoints
# ========================


@app.get("/api/war/status", tags=["War"])
async def get_war_status():
    """Get current war status"""
    data = db.get_latest_war_status()
    if data:
        return data
    raise HTTPException(status_code=404, detail="No war status data available")


@app.post("/api/war/status/refresh", tags=["War"])
async def refresh_war_status():
    """Manually refresh war status (read-only mode - uses cache)"""
    data = db.get_latest_war_status()
    if data:
        return {"success": True, "data": data, "note": "read-only mode - returning cached data"}
    raise HTTPException(status_code=404, detail="No war status data available")


# ========================
# Campaign Endpoints
# ========================


@app.get("/api/campaigns", tags=["Campaigns"])
async def get_campaigns():
    """Get campaign information (with cache fallback)"""
    # Try live API first
    data = scraper.get_campaign_info()

    # Fallback to cache if live API fails
    if data is None:
        data = db.get_latest_campaigns_snapshot()

    if data is not None:
        return data
    raise HTTPException(
        status_code=503, detail="No campaign data available (live fetch failed and no cached data)"
    )


@app.get("/api/campaigns/active", tags=["Campaigns"])
async def get_active_campaigns():
    """Get active campaigns"""
    data = db.get_active_campaigns()
    if data:
        return data
    raise HTTPException(status_code=404, detail="No active campaigns")


# ========================
# Assignments Endpoints (Major Orders)
# ========================


@app.get("/api/assignments", tags=["Assignments"])
async def get_assignments(
    limit: int = Query(10, ge=1, le=100),
    sort: str = Query("newest", pattern="^(newest|oldest)$"),
    active_only: bool = Query(False),
):
    """Get current and recent assignments (Major Orders)

    Query Parameters:
    - limit: Number of assignments to return (1-100, default: 10)
    - sort: Sort order - 'newest' (default) or 'oldest'
    - active_only: If true, return only active assignments
    """
    data = db.get_latest_assignments(limit)
    if data:
        # Filter active only if requested
        if active_only:
            if isinstance(data, dict) and "data" in data:
                data["data"] = [a for a in data["data"] if not a.get("expired", False)]
            elif isinstance(data, list):
                data = [a for a in data if not a.get("expired", False)]

        # Apply sorting
        if sort == "oldest" and isinstance(data, dict) and "data" in data:
            data["data"] = list(reversed(data["data"]))
        elif sort == "oldest" and isinstance(data, list):
            data = list(reversed(data))

        return data
    raise HTTPException(status_code=404, detail="No assignments available")


@app.post("/api/assignments/refresh", tags=["Assignments"])
async def refresh_assignments():
    """Manually refresh assignments (read-only mode - uses cache)"""
    data = db.get_latest_assignments(10)
    if data:
        return {"success": True, "data": data, "note": "read-only mode - returning cached data"}
    raise HTTPException(status_code=404, detail="No assignments available")


# ========================
# Dispatches Endpoints (News/Announcements)
# ========================


@app.get("/api/dispatches", tags=["Dispatches"])
async def get_dispatches(
    limit: int = Query(10, ge=1, le=100),
    sort: str = Query("newest", pattern="^(newest|oldest)$"),
    search: str = Query(None, min_length=1, max_length=100),
):
    """Get current and recent dispatches (news and announcements)

    Query Parameters:
    - limit: Number of dispatches to return (1-100, default: 10)
    - sort: Sort order - 'newest' (default) or 'oldest'
    - search: Filter by text search in dispatch content
    """
    data = db.get_latest_dispatches(limit)
    if data:
        # Apply text search filter if provided
        if search:
            search_lower = search.lower()
            if isinstance(data, dict) and "data" in data:
                data["data"] = [d for d in data["data"] if search_lower in str(d).lower()]
            elif isinstance(data, list):
                data = [d for d in data if search_lower in str(d).lower()]

        # Apply sorting
        if sort == "oldest" and isinstance(data, dict) and "data" in data:
            data["data"] = list(reversed(data["data"]))
        elif sort == "oldest" and isinstance(data, list):
            data = list(reversed(data))

        return data
    raise HTTPException(status_code=404, detail="No dispatches available")


@app.post("/api/dispatches/refresh", tags=["Dispatches"])
async def refresh_dispatches():
    """Manually refresh dispatches (read-only mode - uses cache)"""
    data = db.get_latest_dispatches(10)
    if data:
        return {"success": True, "data": data, "note": "read-only mode - returning cached data"}
    raise HTTPException(status_code=404, detail="No dispatches available")


# ========================
# Planet Events Endpoints
# ========================


@app.get("/api/planet-events", tags=["Planets"])
async def get_planet_events(
    limit: int = Query(10, ge=1, le=100),
    sort: str = Query("newest", pattern="^(newest|oldest)$"),
    planet_index: int = Query(None, ge=0),
    event_type: str = Query(None, pattern="^(defense|offensive|both)$"),
):
    """Get current and recent planet events

    Query Parameters:
    - limit: Number of events to return (1-100, default: 10)
    - sort: Sort order - 'newest' (default) or 'oldest'
    - planet_index: Filter by specific planet index
    - event_type: Filter by event type - 'defense', 'offensive', or 'both' (default)
    """
    data = db.get_latest_planet_events(limit)
    if data:
        events = data.get("data") if isinstance(data, dict) else data
        if not isinstance(events, list):
            events = [data] if data else []

        # Filter by planet index if provided
        if planet_index is not None:
            events = [e for e in events if e.get("planet_index") == planet_index]

        # Filter by event type if provided
        if event_type and event_type != "both":
            events = [e for e in events if e.get("event_type", "").lower() == event_type.lower()]

        # Apply sorting
        if sort == "oldest":
            events = list(reversed(events))

        # Return in same format as original data
        if isinstance(data, dict):
            data["data"] = events
            return data
        return events if events else data

    raise HTTPException(status_code=404, detail="No planet events available")


@app.post("/api/planet-events/refresh", tags=["Planets"])
async def refresh_planet_events():
    """Manually refresh planet events (read-only mode - uses cache)"""
    data = db.get_latest_planet_events(10)
    if data:
        return {"success": True, "data": data, "note": "read-only mode - returning cached data"}
    raise HTTPException(status_code=404, detail="No planet events available")


# ========================
# Planet Endpoints
# ========================


@app.get("/api/planets", tags=["Planets"])
async def get_planets():
    """Get all planets (with cache fallback)"""
    # Try live API first
    data = scraper.get_planets()

    # Fallback to cache if live API fails
    if data is None:
        data = db.get_latest_planets_snapshot()

    if data is not None:
        return data
    raise HTTPException(
        status_code=503, detail="Failed to fetch planets and no cached data available"
    )


@app.get("/api/planets/{planet_index}", tags=["Planets"])
async def get_planet_status(planet_index: int):
    """Get status of a specific planet (with cache fallback)"""
    # Try cache first (poller updates this)
    data = db.get_planet_status(planet_index)

    # Fallback to live API if cache fails
    if data is None:
        data = scraper.get_planet_status(planet_index)

    if data is not None:
        return data
    raise HTTPException(
        status_code=503,
        detail=f"Failed to fetch planet {planet_index} and no cached data available",
    )


@app.get("/api/planets/{planet_index}/history", tags=["Planets"])
async def get_planet_history(planet_index: int, limit: int = Query(10, ge=1, le=100)):
    """Get status history for a planet"""
    data = db.get_planet_status_history(planet_index, limit)
    if data:
        return data
    raise HTTPException(status_code=404, detail=f"No history for planet {planet_index}")


# ========================
# Statistics Endpoints
# ========================


@app.get("/api/statistics", tags=["Statistics"])
async def get_statistics():
    """Get latest global statistics"""
    data = db.get_latest_statistics()
    if data:
        return data
    raise HTTPException(status_code=404, detail="No statistics available")


@app.get("/api/statistics/history", tags=["Statistics"])
async def get_statistics_history(limit: int = Query(100, ge=1, le=1000)):
    """Get statistics history"""
    # All statistics are stored with timestamps, but for API compatibility, return only the latest statistic in array format
    data = db.get_latest_statistics()
    if data:
        return [data]
    raise HTTPException(status_code=404, detail="No statistics history available")


@app.post("/api/statistics/refresh", tags=["Statistics"])
async def refresh_statistics():
    """Manually refresh statistics (read-only mode - uses cache)"""
    data = db.get_latest_statistics()
    if data:
        return {"success": True, "data": data, "note": "read-only mode - returning cached data"}
    raise HTTPException(status_code=404, detail="No statistics available")


# ========================
# Faction Endpoints
# ========================


@app.get("/api/factions", tags=["Factions"])
async def get_factions():
    """Get all factions (with cache fallback)"""
    # Try live API first
    data = scraper.get_factions()

    # Fallback to cache if live API fails
    if data is None:
        data = db.get_latest_factions_snapshot()

    if data is not None:
        return data
    raise HTTPException(
        status_code=503, detail="Failed to fetch factions and no cached data available"
    )


# ========================
# Biome Endpoints
# ========================


@app.get("/api/biomes", tags=["Biomes"])
async def get_biomes():
    """Get all biomes (with cache fallback)"""
    # Try live API first
    data = scraper.get_biomes()

    # Fallback to cache if live API fails
    if data is None:
        data = db.get_latest_biomes_snapshot()

    if data is not None:
        return data
    raise HTTPException(
        status_code=503, detail="Failed to fetch biomes and no cached data available"
    )


# ========================
# Health Check
# ========================


@app.get("/api/config", tags=["Config"])
async def get_config():
    """Public config for UI (e.g. whether Claude is available via backend)."""
    return {
        "claudeEnabled": bool(Config.CLAUDE_API_KEY),
    }


@app.get("/api/livez", tags=["Health"])
async def liveness_probe():
    """Lightweight liveness probe - no DB, for Kubernetes liveness checks.
    Returns 200 if the process is alive. Use /api/health for readiness (includes DB check).
    """
    return {"status": "ok"}


@app.get("/api/health", tags=["Health"])
async def health_check():
    """Health check endpoint - includes DB and upstream status (for readiness)."""
    # Returns local service status and upstream API status
    upstream_status = db.get_upstream_status()
    return {
        "status": "healthy",
        "mode": "read-only",
        "upstream_api": "online" if upstream_status else "offline",
    }


# ========================
# Root Endpoint
# ========================


@app.get("/", tags=["Root"])
async def root():
    """Root endpoint - API information"""
    return {
        "name": "Hell Divers 2 API",
        "version": "1.0.0",
        "mode": "read-only",
        "description": "Read-only API for Hell Divers 2 game data",
        "docs": "/docs",
        "redoc": "/redoc",
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=5000)
