#!/usr/bin/env python3
"""
High Command API - Unified Entry Point

Supports running as either:
- API server (read-only FastAPI application)
- Collector (background data poller)

Set MODE environment variable to:
- "api" or "API" - Run as API server (default)
- "collector" or "COLLECTOR" - Run as data collector/poller
"""

import os
import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def run_api():
    """Run the API server"""
    import uvicorn
    from src.app_readonly import app
    from src.config import Config
    
    port = int(os.getenv("PORT", "5000"))
    host = os.getenv("HOST", "0.0.0.0")
    
    logger.info(f"Starting High Command API server on {host}:{port}")
    uvicorn.run(app, host=host, port=port)


# Global collector reference for signal handler
_collector = None


def run_collector():
    """Run the data collector/poller"""
    import signal
    import time
    from src.database import Database
    from src.collector import DataCollector
    from src.config import Config
    
    global _collector
    
    logger.info("Starting High Command API Collector")
    
    # Initialize database
    db_path = Config.DATABASE_URL.replace("sqlite:///", "")
    if db_path.startswith("/"):
        db_path = db_path
    else:
        # Default to data directory in container
        db_path = f"/data/{db_path}"
    
    db = Database(db_path)
    logger.info(f"Database initialized at: {db_path}")
    
    # Initialize collector
    interval = Config.SCRAPE_INTERVAL
    _collector = DataCollector(db, interval=interval)
    
    def signal_handler(sig, frame):
        """Handle shutdown signals"""
        logger.info("Received shutdown signal, stopping collector...")
        if _collector:
            _collector.stop()
        sys.exit(0)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start collector
    try:
        _collector.start()
        logger.info(f"Collector started with {interval}s interval")
        
        # Keep running
        while _collector.is_running:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Collector error: {e}", exc_info=True)
    finally:
        if _collector:
            _collector.stop()
        logger.info("Collector stopped")


def main():
    """Main entry point"""
    mode = os.getenv("MODE", "api").upper()
    
    if mode in ("COLLECTOR", "POLLER"):
        run_collector()
    elif mode in ("API", "SERVER"):
        run_api()
    else:
        logger.error(f"Unknown MODE: {mode}. Use 'api' or 'collector'")
        sys.exit(1)


if __name__ == "__main__":
    main()
