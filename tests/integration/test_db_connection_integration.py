#!/usr/bin/env python3
"""
Test PostgreSQL database connection.

This script tests the connection to the PostgreSQL database using the settings
from the config.json file.
"""

import sys
import json
import os
from unittest.mock import patch, mock_open, MagicMock

import pytest
import psycopg2
from psycopg2 import sql

from tests.fixtures.mocks import MockDatabase


def load_config(config_path='config/config.json'):
    """Load configuration from a JSON file."""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        print(f"Error: Configuration file not found at {config_path}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in configuration file {config_path}")
        return None
    except Exception as e:
        print(f"Error loading configuration: {e}")
        return None


def get_db_config(config):
    """Extract database configuration from the config dictionary."""
    if not config or 'database' not in config:
        return None

    db_config = config['database']
    return {
        'dbname': db_config.get('dbname', 'skype_archive'),
        'user': db_config.get('user', 'postgres'),
        'password': db_config.get('password', ''),
        'host': db_config.get('host', 'localhost'),
        'port': db_config.get('port', 5432)
    }


@pytest.fixture
def sample_config():
    """Return a sample configuration for testing."""
    return {
        'database': {
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password',
            'host': 'localhost',
            'port': 5432
        }
    }


@pytest.fixture
def mock_config_file(sample_config):
    """Mock the config file with sample configuration."""
    config_json = json.dumps(sample_config)
    with patch('builtins.open', mock_open(read_data=config_json)):
        yield


@pytest.fixture
def mock_psycopg2_connect():
    """Mock the psycopg2.connect function."""
    with patch('psycopg2.connect') as mock_connect:
        # Create a mock connection and cursor
        mock_db = MockDatabase()
        mock_connect.return_value = mock_db

        # Set up the cursor to return a version string
        mock_db.mock_cursor.fetchone.return_value = ('PostgreSQL 14.0',)

        yield mock_connect


def test_load_config(mock_config_file):
    """Test loading configuration from a file."""
    config = load_config()
    assert config is not None
    assert 'database' in config
    assert config['database']['dbname'] == 'test_db'
    assert config['database']['user'] == 'test_user'


def test_load_config_file_not_found():
    """Test handling of missing configuration file."""
    with patch('builtins.open', side_effect=FileNotFoundError()):
        config = load_config()
        assert config is None


def test_load_config_invalid_json():
    """Test handling of invalid JSON in configuration file."""
    with patch('builtins.open', mock_open(read_data='invalid json')):
        config = load_config()
        assert config is None


def test_get_db_config(sample_config):
    """Test extracting database configuration."""
    db_config = get_db_config(sample_config)
    assert db_config is not None
    assert db_config['dbname'] == 'test_db'
    assert db_config['user'] == 'test_user'
    assert db_config['password'] == 'test_password'
    assert db_config['host'] == 'localhost'
    assert db_config['port'] == 5432


def test_get_db_config_missing():
    """Test handling of missing database configuration."""
    db_config = get_db_config({})
    assert db_config is None


def test_connection(mock_config_file, mock_psycopg2_connect):
    """Test the database connection."""
    success = test_connection()
    assert success is True
    mock_psycopg2_connect.assert_called_once()


def test_connection_failure(mock_config_file):
    """Test handling of connection failure."""
    with patch('psycopg2.connect', side_effect=psycopg2.OperationalError('Connection refused')):
        success = test_connection()
        assert success is False


def test_create_database_if_needed(mock_config_file, mock_psycopg2_connect):
    """Test creating the database if it doesn't exist."""
    # Set up the cursor to indicate the database doesn't exist
    mock_psycopg2_connect.return_value.mock_cursor.fetchone.return_value = None

    success = create_database_if_needed()
    assert success is True

    # Verify that CREATE DATABASE was executed
    executed_queries = [q.lower() for q in mock_psycopg2_connect.return_value.queries]
    assert any('create database' in q for q in executed_queries)


def test_create_database_already_exists(mock_config_file, mock_psycopg2_connect):
    """Test handling when the database already exists."""
    # Set up the cursor to indicate the database exists
    mock_psycopg2_connect.return_value.mock_cursor.fetchone.return_value = (1,)

    success = create_database_if_needed()
    assert success is True

    # Verify that CREATE DATABASE was not executed
    executed_queries = [q.lower() for q in mock_psycopg2_connect.return_value.queries]
    assert not any('create database' in q for q in executed_queries)


def test_create_database_failure(mock_config_file):
    """Test handling of database creation failure."""
    with patch('psycopg2.connect', side_effect=Exception('Connection failed')):
        success = create_database_if_needed()
        assert success is False


def test_connection():
    """Test the connection to the PostgreSQL database."""
    # Load configuration
    config = load_config()
    if not config:
        return False

    # Get database configuration
    db_config = get_db_config(config)
    if not db_config:
        print("Error: Database configuration not found in config file")
        return False

    # Print configuration (without password)
    print(f"Connecting to PostgreSQL database:")
    print(f"  Host: {db_config['host']}")
    print(f"  Port: {db_config['port']}")
    print(f"  Database: {db_config['dbname']}")
    print(f"  User: {db_config['user']}")

    try:
        # Attempt to connect
        print("Attempting to connect...")
        conn = psycopg2.connect(**db_config)

        # Test the connection by executing a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()

        # Close the cursor and connection
        cursor.close()
        conn.close()

        print(f"Connection successful!")
        print(f"PostgreSQL server version: {version[0]}")

        return True
    except psycopg2.OperationalError as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False


def create_database_if_needed():
    """Create the database if it doesn't exist."""
    config = load_config()
    if not config:
        return False

    db_config = get_db_config(config)
    if not db_config:
        print("Error: Database configuration not found in config file")
        return False

    db_name = db_config.pop('dbname')

    try:
        # Connect to default 'postgres' database to create our database
        conn = psycopg2.connect(dbname='postgres', **db_config)
        conn.autocommit = True
        cursor = conn.cursor()

        # Check if our database exists
        cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        exists = cursor.fetchone()

        if not exists:
            print(f"Database '{db_name}' does not exist. Creating it now...")
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
            print(f"Database '{db_name}' created successfully!")
        else:
            print(f"Database '{db_name}' already exists.")

        cursor.close()
        conn.close()
        return True
    except Exception as e:
        print(f"Error creating database: {e}")
        return False


if __name__ == '__main__':
    print("Testing PostgreSQL database connection...")
    create_database_if_needed()
    success = test_connection()

    if success:
        print("✅ Database connection verified successfully!")
        sys.exit(0)
    else:
        print("❌ Failed to connect to the database. Please check your configuration.")
        sys.exit(1)