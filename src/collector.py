import logging
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from src.scraper import HellDivers2Scraper
from src.database import Database

logger = logging.getLogger(__name__)


class DataCollector:
    """Manages background data collection from Hell Divers 2 API"""

    def __init__(self, db: Database, interval: int = 300):
        self.db = db
        self.scraper = HellDivers2Scraper()
        self.scheduler = BackgroundScheduler()
        self.interval = interval
        self.is_running = False

    def start(self):
        """Start the data collection scheduler and run an initial pull so MCP/UI have data immediately."""
        if self.is_running:
            logger.warning("Data collector is already running")
            return

        self.scheduler.add_job(
            self.collect_all_data, "interval", seconds=self.interval, id="collect_data"
        )
        self.scheduler.start()
        self.is_running = True
        logger.info(f"Data collector started with {self.interval}s interval")

        # Initial pull so MCP and UI have data while the interval runs
        thread = threading.Thread(target=self.collect_all_data, daemon=True)
        thread.start()
        logger.info("Initial data collection started in background")

    def stop(self):
        """Stop the data collection scheduler"""
        if not self.is_running:
            return

        self.scheduler.shutdown()
        self.is_running = False
        logger.info("Data collector stopped")

    def collect_all_data(self):
        """Collect all available data"""
        logger.info("Starting data collection cycle")

        try:
            # Collect war status
            war_data = self.scraper.get_war_status()
            if war_data is not None:
                self.db.save_war_status(war_data)
                logger.info("War status collected")
            else:
                logger.warning("Failed to collect war status")

            # Collect statistics
            stats_data = self.scraper.get_statistics()
            if stats_data is not None:
                self.db.save_statistics(stats_data)
                logger.info("Statistics collected")
            else:
                logger.warning("Failed to collect statistics")

            # Collect planets
            planets = self.scraper.get_planets()
            if planets is not None:
                if planets:
                    for planet in planets:
                        planet_index = planet.get("index")
                        if planet_index:
                            self.db.save_planet_status(planet_index, planet)
                    logger.info(f"Collected data for {len(planets)} planets")
                else:
                    logger.info("Collected 0 planets (empty response)")
            else:
                logger.warning("Failed to collect planets")

            # Collect campaigns
            campaigns = self.scraper.get_campaign_info()
            if campaigns is not None:
                if campaigns:
                    for campaign in campaigns:
                        campaign_id = campaign.get("id")
                        planet_index = campaign.get("planet", {}).get("index")
                        if campaign_id and planet_index:
                            self.db.save_campaign(campaign_id, planet_index, campaign)
                    logger.info(f"Collected {len(campaigns)} campaigns")
                else:
                    logger.info("Collected 0 campaigns (empty response)")
            else:
                logger.warning("Failed to collect campaigns")

            # Collect assignments (Major Orders)
            assignments = self.scraper.get_assignments()
            if assignments is not None:
                if assignments:
                    self.db.save_assignments(assignments)
                    logger.info(f"Collected {len(assignments)} assignments")
                else:
                    logger.info("Collected 0 assignments (empty response)")
            else:
                logger.warning("Failed to collect assignments")

            # Collect dispatches (news)
            dispatches = self.scraper.get_dispatches()
            if dispatches is not None:
                if dispatches:
                    self.db.save_dispatches(dispatches)
                    logger.info(f"Collected {len(dispatches)} dispatches")
                else:
                    logger.info("Collected 0 dispatches (empty response)")
            else:
                logger.warning("Failed to collect dispatches")

            # Collect planet events
            events = self.scraper.get_planet_events()
            if events is not None:
                if events:
                    self.db.save_planet_events(events)
                    logger.info(f"Collected {len(events)} planet events")
                else:
                    logger.info("Collected 0 planet events (empty response)")
            else:
                logger.warning("Failed to collect planet events")

            logger.info("Data collection cycle completed successfully")
            # Mark upstream as available after successful collection
            self.db.set_upstream_status(True)

        except Exception as e:
            logger.error(f"Error during data collection: {e}")
            # Mark upstream as unavailable on any collection error
            self.db.set_upstream_status(False)

    def collect_planet_data(self, planet_index: int):
        """Collect data for a specific planet"""
        try:
            planet_data = self.scraper.get_planet_status(planet_index)
            if planet_data:
                self.db.save_planet_status(planet_index, planet_data)
                logger.info(f"Planet {planet_index} data collected")
                return planet_data
        except Exception as e:
            logger.error(f"Error collecting planet data: {e}")

        return None

