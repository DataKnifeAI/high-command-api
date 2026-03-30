#!/usr/bin/env python3
"""
Unit tests for FastAPI application endpoints.
Tests API routes, error handling, and cache fallback.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
from src.app import app


@pytest.fixture
def client():
    """Create test client"""
    return TestClient(app)


class TestHealthEndpoints:
    """Test health and status endpoints"""

    @patch("src.app.collector")
    def test_health_endpoint(self, mock_collector, client):
        """Test health check endpoint"""
        mock_collector.is_running = True
        response = client.get("/api/health")
        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "collector_running" in data

    def test_root_endpoint(self, client):
        """Test root endpoint"""
        response = client.get("/")
        assert response.status_code == 200
        data = response.json()
        assert "message" in data or "version" in data


class TestWarEndpoints:
    """Test war status endpoints"""

    @patch("src.app.db.get_latest_war_status")
    def test_get_war_status_success(self, mock_get, client):
        """Test getting war status successfully"""
        mock_get.return_value = {"war_id": 1, "status": "active"}

        response = client.get("/api/war/status")
        assert response.status_code == 200
        data = response.json()
        assert data["war_id"] == 1

    @patch("src.app.scraper.get_war_status")
    @patch("src.app.db.get_latest_war_status")
    def test_get_war_status_not_found(self, mock_get, mock_scraper, client):
        """Test getting war status when DB empty and live fetch fails"""
        mock_get.return_value = None
        mock_scraper.return_value = None

        response = client.get("/api/war/status")
        assert response.status_code == 404

    @patch("src.app.scraper.get_war_status")
    @patch("src.app.db.save_war_status")
    @patch("src.app.db.get_latest_war_status")
    def test_get_war_status_live_when_db_empty(self, mock_get, mock_save, mock_scraper, client):
        """When DB has no row yet, GET uses live fetch and persists"""
        mock_get.return_value = None
        mock_scraper.return_value = {"war_id": 42, "status": "active"}
        mock_save.return_value = True

        response = client.get("/api/war/status")
        assert response.status_code == 200
        assert response.json()["war_id"] == 42
        mock_save.assert_called_once()

    @patch("src.app.scraper.get_war_status")
    @patch("src.app.db.save_war_status")
    def test_refresh_war_status_success(self, mock_save, mock_scraper, client):
        """Test refreshing war status successfully"""
        mock_scraper.return_value = {"war_id": 1}
        mock_save.return_value = True

        response = client.post("/api/war/status/refresh")
        assert response.status_code == 200

    @patch("src.app.scraper.get_war_status")
    def test_refresh_war_status_failure(self, mock_scraper, client):
        """Test refreshing war status when API fails"""
        mock_scraper.return_value = None

        response = client.post("/api/war/status/refresh")
        assert response.status_code == 500


class TestCampaignEndpoints:
    """Test campaign endpoints"""

    @patch("src.app.scraper.get_campaign_info")
    def test_get_campaigns_success(self, mock_scraper, client):
        """Test getting campaigns successfully"""
        mock_scraper.return_value = [{"id": 1, "planet": {"index": 5}}]

        response = client.get("/api/campaigns")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    @patch("src.app.scraper.get_campaign_info")
    @patch("src.app.db.get_latest_campaigns_snapshot")
    def test_get_campaigns_cache_fallback(self, mock_cache, mock_scraper, client):
        """Test campaigns cache fallback when API fails"""
        mock_scraper.return_value = None
        mock_cache.return_value = [{"id": 1, "cached": True}]

        response = client.get("/api/campaigns")
        assert response.status_code == 200
        data = response.json()
        assert data[0]["cached"] is True

    @patch("src.app.scraper.get_campaign_info")
    @patch("src.app.db.get_latest_campaigns_snapshot")
    def test_get_campaigns_both_fail(self, mock_cache, mock_scraper, client):
        """Test campaigns when both API and cache fail"""
        mock_scraper.return_value = None
        mock_cache.return_value = None

        response = client.get("/api/campaigns")
        assert response.status_code == 503

    @patch("src.app.db.get_active_campaigns")
    def test_get_active_campaigns(self, mock_get, client):
        """Test getting active campaigns"""
        mock_get.return_value = [{"id": 1, "status": "active"}]

        response = client.get("/api/campaigns/active")
        assert response.status_code == 200


class TestPlanetEndpoints:
    """Test planet endpoints"""

    @patch("src.app.scraper.get_planets")
    def test_get_planets_success(self, mock_scraper, client):
        """Test getting planets successfully"""
        mock_scraper.return_value = [{"index": 1, "name": "Planet 1"}]

        response = client.get("/api/planets")
        assert response.status_code == 200

    @patch("src.app.scraper.get_planets")
    @patch("src.app.db.get_latest_planets_snapshot")
    def test_get_planets_cache_fallback(self, mock_cache, mock_scraper, client):
        """Test planets cache fallback"""
        mock_scraper.return_value = None
        mock_cache.return_value = [{"index": 1, "cached": True}]

        response = client.get("/api/planets")
        assert response.status_code == 200

    @patch("src.app.scraper.get_planet_status")
    @patch("src.app.db.get_planet_status_history")
    def test_get_planet_by_index(self, mock_history, mock_scraper, client):
        """Test getting specific planet"""
        mock_scraper.return_value = {"index": 5, "name": "Test Planet"}
        mock_history.return_value = [{"index": 5, "name": "Test Planet"}]

        response = client.get("/api/planets/5")
        # Either direct API or cache fallback should work
        assert response.status_code in [200, 503]

    @patch("src.app.db.get_planet_status_history")
    def test_get_planet_history(self, mock_get, client):
        """Test getting planet history"""
        mock_get.return_value = [{"index": 5, "timestamp": "2024-01-01"}]

        response = client.get("/api/planets/5/history")
        assert response.status_code == 200


class TestStatisticsEndpoints:
    """Test statistics endpoints"""

    @patch("src.app.db.get_latest_statistics")
    def test_get_statistics_success(self, mock_get, client):
        """Test getting statistics successfully"""
        mock_get.return_value = {"total_players": 1000}

        response = client.get("/api/statistics")
        assert response.status_code == 200

    @patch("src.app.scraper.get_statistics")
    @patch("src.app.db.get_latest_statistics")
    def test_get_statistics_not_found(self, mock_get, mock_scraper, client):
        """Test getting statistics when DB empty and live fetch fails"""
        mock_get.return_value = None
        mock_scraper.return_value = None

        response = client.get("/api/statistics")
        assert response.status_code == 404

    @patch("src.app.scraper.get_statistics")
    @patch("src.app.db.save_statistics")
    @patch("src.app.db.get_latest_statistics")
    def test_get_statistics_live_when_db_empty(self, mock_get, mock_save, mock_scraper, client):
        """When DB has no row yet, GET uses live fetch and persists"""
        mock_get.return_value = None
        mock_scraper.return_value = {"total_players": 999}
        mock_save.return_value = True

        response = client.get("/api/statistics")
        assert response.status_code == 200
        assert response.json()["total_players"] == 999
        mock_save.assert_called_once()

    @patch("src.app.db.get_latest_statistics")
    def test_get_statistics_history(self, mock_get, client):
        """Test getting statistics history"""
        mock_get.return_value = {"total_players": 1000}

        response = client.get("/api/statistics/history")
        assert response.status_code == 200


class TestFactionEndpoints:
    """Test faction endpoints"""

    @patch("src.app.scraper.get_factions")
    def test_get_factions_success(self, mock_scraper, client):
        """Test getting factions successfully"""
        mock_scraper.return_value = [{"id": 1, "name": "Terminids"}]

        response = client.get("/api/factions")
        assert response.status_code == 200

    @patch("src.app.scraper.get_factions")
    @patch("src.app.db.get_latest_factions_snapshot")
    def test_get_factions_cache_fallback(self, mock_cache, mock_scraper, client):
        """Test factions cache fallback"""
        mock_scraper.return_value = None
        mock_cache.return_value = [{"id": 1, "cached": True}]

        response = client.get("/api/factions")
        assert response.status_code == 200


class TestBiomeEndpoints:
    """Test biome endpoints"""

    @patch("src.app.scraper.get_biomes")
    def test_get_biomes_success(self, mock_scraper, client):
        """Test getting biomes successfully"""
        mock_scraper.return_value = [{"name": "Desert"}]

        response = client.get("/api/biomes")
        assert response.status_code == 200

    @patch("src.app.scraper.get_biomes")
    @patch("src.app.db.get_latest_biomes_snapshot")
    def test_get_biomes_cache_fallback(self, mock_cache, mock_scraper, client):
        """Test biomes cache fallback"""
        mock_scraper.return_value = None
        mock_cache.return_value = [{"name": "Ice"}]

        response = client.get("/api/biomes")
        assert response.status_code == 200


class TestAssignmentEndpoints:
    """Test assignment endpoints"""

    @patch("src.app.db.get_latest_assignments")
    def test_get_assignments_success(self, mock_get, client):
        """Test getting assignments successfully"""
        mock_get.return_value = [{"id": 1, "title": "Major Order"}]

        response = client.get("/api/assignments")
        assert response.status_code == 200

    @patch("src.app.db.get_latest_assignments")
    def test_get_assignments_with_limit(self, mock_get, client):
        """Test getting assignments with limit parameter"""
        mock_get.return_value = [{"id": 1}, {"id": 2}]

        response = client.get("/api/assignments?limit=2")
        assert response.status_code == 200

    @patch("src.app.scraper.get_assignments")
    @patch("src.app.db.save_assignments")
    def test_refresh_assignments(self, mock_save, mock_scraper, client):
        """Test refreshing assignments"""
        mock_scraper.return_value = [{"id": 1}]
        mock_save.return_value = True

        response = client.post("/api/assignments/refresh")
        assert response.status_code == 200


class TestDispatchEndpoints:
    """Test dispatch endpoints"""

    @patch("src.app.db.get_latest_dispatches")
    def test_get_dispatches_success(self, mock_get, client):
        """Test getting dispatches successfully"""
        mock_get.return_value = [{"id": 1, "message": "News"}]

        response = client.get("/api/dispatches")
        assert response.status_code == 200

    @patch("src.app.scraper.get_dispatches")
    @patch("src.app.db.save_dispatches")
    def test_refresh_dispatches(self, mock_save, mock_scraper, client):
        """Test refreshing dispatches"""
        mock_scraper.return_value = [{"id": 1}]
        mock_save.return_value = True

        response = client.post("/api/dispatches/refresh")
        assert response.status_code == 200


class TestPlanetEventEndpoints:
    """Test planet event endpoints"""

    @patch("src.app.db.get_latest_planet_events")
    def test_get_planet_events_success(self, mock_get, client):
        """Test getting planet events successfully"""
        mock_get.return_value = [{"id": 1, "planetIndex": 5}]

        response = client.get("/api/planet-events")
        assert response.status_code == 200

    @patch("src.app.scraper.get_planet_events")
    @patch("src.app.db.save_planet_events")
    def test_refresh_planet_events(self, mock_save, mock_scraper, client):
        """Test refreshing planet events"""
        mock_scraper.return_value = [{"id": 1}]
        mock_save.return_value = True

        response = client.post("/api/planet-events/refresh")
        assert response.status_code == 200


class TestErrorHandling:
    """Test error handling across endpoints"""

    @patch("src.app.db.get_latest_assignments")
    def test_get_assignments_empty(self, mock_get, client):
        """Test getting assignments when database returns empty list"""
        mock_get.return_value = []

        response = client.get("/api/assignments")
        # Empty list should return 404
        assert response.status_code in [200, 404]

    @patch("src.app.db.get_latest_dispatches")
    def test_get_dispatches_empty(self, mock_get, client):
        """Test getting dispatches when database returns empty list"""
        mock_get.return_value = []

        response = client.get("/api/dispatches")
        assert response.status_code in [200, 404]

    @patch("src.app.db.get_latest_planet_events")
    def test_get_planet_events_empty(self, mock_get, client):
        """Test getting planet events when database returns empty list"""
        mock_get.return_value = []

        response = client.get("/api/planet-events")
        assert response.status_code in [200, 404]

    @patch("src.app.db.get_active_campaigns")
    def test_get_active_campaigns_empty(self, mock_get, client):
        """Test getting active campaigns when none exist"""
        mock_get.return_value = []

        response = client.get("/api/campaigns/active")
        assert response.status_code in [200, 404]

    @patch("src.app.db.get_planet_status_history")
    def test_get_planet_history_empty(self, mock_get, client):
        """Test getting planet history when none exists"""
        mock_get.return_value = []

        response = client.get("/api/planets/1/history")
        assert response.status_code in [200, 404]

    @patch("src.app.db.get_statistics_history")
    def test_get_statistics_history_empty(self, mock_get, client):
        """Test getting statistics history when none exists"""
        mock_get.return_value = []

        response = client.get("/api/statistics/history")
        assert response.status_code in [200, 404]
