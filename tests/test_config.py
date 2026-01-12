#!/usr/bin/env python3
"""
Unit tests for configuration module.
Tests configuration loading, defaults, and environment variable handling.
"""

import os
from unittest.mock import patch
from src.config import Config, DevelopmentConfig, ProductionConfig, TestingConfig


class TestConfig:
    """Test base configuration"""

    def test_config_defaults(self):
        """Test default configuration values"""
        assert Config.DEBUG is False
        assert Config.TESTING is False
        assert Config.API_TIMEOUT == 30
        assert Config.SCRAPE_INTERVAL == 300
        assert Config.LOG_LEVEL == "INFO"

    def test_config_database_default(self):
        """Test default database URL"""
        # Default is empty string, or from DATABASE_URL env var if set
        # In CI, DATABASE_URL is set to test database
        assert Config.DATABASE_URL == "" or Config.DATABASE_URL.startswith("postgresql://")

    @patch.dict(os.environ, {"DATABASE_URL": "sqlite:///custom.db"})
    def test_config_database_env(self):
        """Test database URL from environment"""
        # Reload config to pick up environment variable
        from importlib import reload
        from src import config

        reload(config)
        assert config.Config.DATABASE_URL == "sqlite:///custom.db"

    @patch.dict(os.environ, {"LOG_LEVEL": "DEBUG"})
    def test_config_log_level_env(self):
        """Test log level from environment"""
        from importlib import reload
        from src import config

        reload(config)
        assert config.Config.LOG_LEVEL == "DEBUG"


class TestDevelopmentConfig:
    """Test development configuration"""

    def test_development_debug(self):
        """Test development mode has debug enabled"""
        assert DevelopmentConfig.DEBUG is True

    def test_development_scrape_interval(self):
        """Test development scrape interval is shorter"""
        assert DevelopmentConfig.SCRAPE_INTERVAL == 60


class TestProductionConfig:
    """Test production configuration"""

    def test_production_debug(self):
        """Test production mode has debug disabled"""
        assert ProductionConfig.DEBUG is False

    def test_production_scrape_interval(self):
        """Test production scrape interval is longer"""
        assert ProductionConfig.SCRAPE_INTERVAL == 300


class TestTestingConfig:
    """Test testing configuration"""

    def test_testing_flag(self):
        """Test testing flag is enabled"""
        assert TestingConfig.TESTING is True

    def test_testing_database(self):
        """Test testing uses PostgreSQL test database"""
        # TestingConfig uses DATABASE_URL from env or default PostgreSQL connection
        assert TestingConfig.DATABASE_URL.startswith("postgresql://")

    def test_testing_scrape_interval(self):
        """Test testing scrape interval"""
        assert TestingConfig.SCRAPE_INTERVAL == 60


class TestConfigMapping:
    """Test configuration mapping"""

    def test_config_mapping_exists(self):
        """Test configuration mapping dictionary exists"""
        from src.config import config

        assert config is not None
        assert isinstance(config, dict)

    def test_config_mapping_keys(self):
        """Test configuration mapping has expected keys"""
        from src.config import config

        assert "development" in config
        assert "production" in config
        assert "testing" in config
        assert "default" in config

    def test_config_mapping_values(self):
        """Test configuration mapping has correct values"""
        from src.config import config
        from src.config import DevelopmentConfig as DevConfig
        from src.config import ProductionConfig as ProdConfig
        from src.config import TestingConfig as TestConfig

        assert config["development"] == DevConfig
        assert config["production"] == ProdConfig
        assert config["testing"] == TestConfig
        assert config["default"] == DevConfig
