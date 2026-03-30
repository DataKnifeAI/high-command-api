#!/usr/bin/env python3
"""
Additional tests to improve code coverage for high-command-api.
Focuses on edge cases and error paths that weren't fully covered.
"""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
from src.app import app


@pytest.fixture
def client():
    """Create test client"""
    return TestClient(app)


class TestAppErrorPaths:
    """Test error paths and edge cases in app.py"""

    @patch("src.app.scraper.get_assignments")
    @patch("src.app.db.save_assignments")
    def test_refresh_assignments_none(self, mock_save, mock_scraper, client):
        """Test refresh assignments when API returns None"""
        mock_scraper.return_value = None
        response = client.post("/api/assignments/refresh")
        assert response.status_code == 500

    @patch("src.app.scraper.get_dispatches")
    @patch("src.app.db.save_dispatches")
    def test_refresh_dispatches_none(self, mock_save, mock_scraper, client):
        """Test refresh dispatches when API returns None"""
        mock_scraper.return_value = None
        response = client.post("/api/dispatches/refresh")
        assert response.status_code == 500

    @patch("src.app.scraper.get_planet_events")
    @patch("src.app.db.save_planet_events")
    def test_refresh_planet_events_none(self, mock_save, mock_scraper, client):
        """Test refresh planet events when API returns None"""
        mock_scraper.return_value = None
        response = client.post("/api/planet-events/refresh")
        assert response.status_code == 500

    @patch("src.app.db.get_latest_assignments")
    def test_get_assignments_sorted_oldest(self, mock_get, client):
        """Test getting assignments sorted as oldest"""
        mock_get.return_value = [
            {"id": 1, "created": 100},
            {"id": 2, "created": 200},
        ]
        response = client.get("/api/assignments?sort=oldest")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    @patch("src.app.db.get_latest_assignments")
    def test_get_assignments_with_active_only(self, mock_get, client):
        """Test getting assignments with active_only filter"""
        mock_get.return_value = [
            {"id": 1, "expired": False},
            {"id": 2, "expired": True},
        ]
        response = client.get("/api/assignments?active_only=true")
        assert response.status_code == 200
        data = response.json()
        # Should only have the non-expired assignment
        assert len(data) == 1

    @patch("src.app.db.get_latest_assignments")
    def test_get_assignments_dict_format_active_only(self, mock_get, client):
        """Test getting assignments in dict format with active_only"""
        mock_get.return_value = {
            "data": [
                {"id": 1, "expired": False},
                {"id": 2, "expired": True},
            ]
        }
        response = client.get("/api/assignments?active_only=true")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data
        assert len(data["data"]) == 1

    @patch("src.app.db.get_latest_assignments")
    def test_get_assignments_dict_format_sorted(self, mock_get, client):
        """Test getting assignments in dict format sorted oldest"""
        mock_get.return_value = {
            "data": [
                {"id": 1},
                {"id": 2},
            ]
        }
        response = client.get("/api/assignments?sort=oldest")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data

    @patch("src.app.db.get_latest_dispatches")
    def test_get_dispatches_sorted_oldest(self, mock_get, client):
        """Test getting dispatches sorted as oldest"""
        mock_get.return_value = [
            {"id": 1, "text": "hello"},
            {"id": 2, "text": "world"},
        ]
        response = client.get("/api/dispatches?sort=oldest")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    @patch("src.app.db.get_latest_dispatches")
    def test_get_dispatches_with_search(self, mock_get, client):
        """Test getting dispatches with search filter"""
        mock_get.return_value = [
            {"id": 1, "text": "hello world"},
            {"id": 2, "text": "goodbye"},
        ]
        response = client.get("/api/dispatches?search=hello")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1

    @patch("src.app.db.get_latest_dispatches")
    def test_get_dispatches_dict_format_sorted(self, mock_get, client):
        """Test getting dispatches in dict format sorted oldest"""
        mock_get.return_value = {
            "data": [
                {"id": 1, "text": "hello"},
                {"id": 2, "text": "world"},
            ]
        }
        response = client.get("/api/dispatches?sort=oldest")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data

    @patch("src.app.db.get_latest_dispatches")
    def test_get_dispatches_dict_format_search(self, mock_get, client):
        """Test getting dispatches in dict format with search"""
        mock_get.return_value = {
            "data": [
                {"id": 1, "text": "hello world"},
                {"id": 2, "text": "goodbye"},
            ]
        }
        response = client.get("/api/dispatches?search=hello")
        assert response.status_code == 200
        data = response.json()
        assert len(data["data"]) == 1

    @patch("src.app.db.get_latest_planet_events")
    def test_get_planet_events_sorted_oldest(self, mock_get, client):
        """Test getting planet events sorted as oldest"""
        mock_get.return_value = [
            {"id": 1},
            {"id": 2},
        ]
        response = client.get("/api/planet-events?sort=oldest")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

    @patch("src.app.db.get_latest_planet_events")
    def test_get_planet_events_dict_format_sorted(self, mock_get, client):
        """Test getting planet events in dict format sorted"""
        mock_get.return_value = {
            "data": [
                {"id": 1},
                {"id": 2},
            ]
        }
        response = client.get("/api/planet-events?sort=oldest")
        assert response.status_code == 200
        data = response.json()
        assert "data" in data

    @patch("src.app.scraper.get_statistics")
    @patch("src.app.db.save_statistics")
    def test_refresh_statistics_none(self, mock_save, mock_scraper, client):
        """Test refresh statistics when API returns None"""
        mock_scraper.return_value = None
        response = client.post("/api/statistics/refresh")
        assert response.status_code == 500


class TestDatabaseCoveragePaths:
    """Test database error and edge case paths"""

    @patch("src.app.scraper.get_statistics")
    @patch("src.app.db.get_latest_statistics")
    def test_statistics_not_found(self, mock_get, mock_scraper, client):
        """Test getting statistics when DB empty and live fetch fails"""
        mock_get.return_value = None
        mock_scraper.return_value = None
        response = client.get("/api/statistics")
        assert response.status_code == 404


class TestScraperCoveragePaths:
    """Test scraper error handling paths"""

    @patch("src.app.scraper.get_war_status")
    @patch("src.app.db.get_latest_war_status")
    def test_get_war_status_scraper_fails_fallback(self, mock_get, mock_scraper, client):
        """Test getting war status when scraper fails but cache exists"""
        mock_scraper.return_value = None
        mock_get.return_value = {"war_id": 1}
        response = client.get("/api/war/status")
        assert response.status_code == 200

    @patch("src.app.scraper.get_planets")
    @patch("src.app.db.get_latest_planets_snapshot")
    def test_get_planets_cache_fallback_both_none(self, mock_cache, mock_scraper, client):
        """Test getting planets when both scraper and cache fail"""
        mock_scraper.return_value = None
        mock_cache.return_value = None
        response = client.get("/api/planets")
        assert response.status_code == 503
