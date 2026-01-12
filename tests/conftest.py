"""
Pytest configuration and shared fixtures for database mocking.
"""
import pytest
import json
from unittest.mock import MagicMock, patch
from src.database import Database


@pytest.fixture
def mock_psycopg2():
    """Mock psycopg2 connection"""
    with patch('src.database.psycopg2') as mock_pg:
        # Mock connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)
        mock_conn.close.return_value = None
        
        # Mock connect
        mock_pg.connect.return_value = mock_conn
        
        # Mock OperationalError - use Exception as fallback
        try:
            import psycopg2.errors
            mock_pg.OperationalError = psycopg2.errors.OperationalError
        except (ImportError, AttributeError):
            class OperationalError(Exception):
                pass
            mock_pg.OperationalError = OperationalError
        
        # Default mock cursor behavior
        mock_cursor.fetchone.return_value = None
        mock_cursor.fetchall.return_value = []
        mock_cursor.execute.return_value = None
        
        yield mock_pg, mock_conn, mock_cursor


@pytest.fixture
def temp_db(mock_psycopg2):
    """Create a Database instance with mocked PostgreSQL connection"""
    mock_pg, mock_conn, mock_cursor = mock_psycopg2
    
    # Create database instance
    db = Database(database_url='postgresql://test:test@localhost:5432/test_db')
    
    # Override _get_connection to return our mock
    db._get_connection = lambda: mock_conn
    
    yield db
    
    # Cleanup
    del db
