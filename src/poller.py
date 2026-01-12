#!/usr/bin/env python3
"""
High Command API Poller Service

Separate service that polls the Hell Divers 2 API and updates the database.
Runs as a standalone process, separate from the API service.
"""

import logging
import signal
import sys
from src.database import Database
from src.collector import DataCollector
from src.config import Config

# Configure logging
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Global collector instance
collector = None


def signal_handler(sig, frame):
    """Handle shutdown signals"""
    logger.info("Received shutdown signal, stopping collector...")
    if collector:
        collector.stop()
    sys.exit(0)


def main():
    """Main entry point for poller service"""
    global collector
    
    logger.info("Starting High Command API Poller Service")
    
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
    collector = DataCollector(db, interval=interval)
    
    # Register signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Start collector
    try:
        collector.start()
        logger.info(f"Poller service started with {interval}s interval")
        
        # Keep running
        while collector.is_running:
            signal.pause()
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.error(f"Poller service error: {e}", exc_info=True)
    finally:
        if collector:
            collector.stop()
        logger.info("Poller service stopped")


if __name__ == "__main__":
    main()
