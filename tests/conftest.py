#!/usr/bin/env python3
"""
Test suite configuration and shared utilities for Hell Divers 2 API tests.
Contains common testing utilities, mock helpers, and configuration.
"""

import json
import pytest
from unittest.mock import MagicMock, patch
from src.database import Database

# API Configuration
API_BASE = "http://localhost:5000/api"


class Colors:
    """ANSI color codes for terminal output"""

    HEADER = "\033[95m"
    BLUE = "\033[94m"
    CYAN = "\033[96m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    END = "\033[0m"
    BOLD = "\033[1m"
    UNDERLINE = "\033[4m"


def print_header(text):
    """Print a header with borders"""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*60}")
    print(f"  {text}")
    print(f"{'='*60}{Colors.END}\n")


def print_section(text):
    """Print a section header"""
    print(f"{Colors.BLUE}{Colors.BOLD}{text}{Colors.END}")


def print_success(text):
    """Print success message"""
    print(f"{Colors.GREEN}✓ {text}{Colors.END}")


def print_error(text):
    """Print error message"""
    print(f"{Colors.RED}✗ {text}{Colors.END}")


def print_info(text):
    """Print info message"""
    print(f"{Colors.CYAN}ℹ {text}{Colors.END}")


def pretty_print_json(data, title=""):
    """Pretty print JSON data with optional title"""
    if title:
        print_section(title)
    if data:
        print(json.dumps(data, indent=2))
    else:
        print_error("No data available")


# Database Mocking Fixtures
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
