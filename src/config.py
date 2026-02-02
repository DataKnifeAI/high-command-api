import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    """Base configuration"""

    DEBUG = False
    TESTING = False

    # Database
    DATABASE_URL = os.getenv("DATABASE_URL", "")

    # API Settings
    API_TIMEOUT = 30
    SCRAPE_INTERVAL = 300  # 5 minutes

    # Hell Divers 2 API Endpoints (Community-maintained at api.helldivers2.dev)
    HELLDIVERS_API_BASE = os.getenv("HELLDIVERS_API_BASE", "NA")
    HELLDIVERS_API_CLIENT_NAME = os.getenv("HELLDIVERS_API_CLIENT_NAME", "NA")
    HELLDIVERS_API_CONTACT = os.getenv("HELLDIVERS_API_CONTACT", "NA")

    # Logging
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    # Claude API (optional - for UI Claude integration via backend proxy)
    CLAUDE_API_KEY = os.getenv("CLAUDE_API_KEY", "")


class DevelopmentConfig(Config):
    """Development configuration"""

    DEBUG = True
    SCRAPE_INTERVAL = 60  # 1 minute for testing


class ProductionConfig(Config):
    """Production configuration"""

    DEBUG = False
    SCRAPE_INTERVAL = 300  # 5 minutes


class TestingConfig(Config):
    """Testing configuration"""

    TESTING = True
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://test:test@localhost:5432/test_db")
    SCRAPE_INTERVAL = 60


config = {
    "development": DevelopmentConfig,
    "production": ProductionConfig,
    "testing": TestingConfig,
    "default": DevelopmentConfig,
}
