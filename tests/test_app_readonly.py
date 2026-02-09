"""Unit tests for app_readonly (read-only API used in production)."""

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch
from src.app_readonly import app


@pytest.fixture
def client():
    """Create test client for readonly app"""
    return TestClient(app)


class TestLivezEndpoint:
    """Test lightweight liveness probe - no DB dependency."""

    def test_livez_returns_200(self, client):
        """Liveness probe returns 200 without hitting DB."""
        response = client.get("/api/livez")
        assert response.status_code == 200
        data = response.json()
        assert data == {"status": "ok"}


class TestHealthEndpoint:
    """Test health endpoint (includes DB check)."""

    @patch("src.app_readonly.db.get_upstream_status")
    def test_health_upstream_online(self, mock_status, client):
        """Health check when upstream is online."""
        mock_status.return_value = True
        response = client.get("/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["mode"] == "read-only"
        assert data["upstream_api"] == "online"

    @patch("src.app_readonly.db.get_upstream_status")
    def test_health_upstream_offline(self, mock_status, client):
        """Health check when upstream is offline."""
        mock_status.return_value = False
        response = client.get("/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data["upstream_api"] == "offline"
