#!/usr/bin/env python3
"""
Test PostgreSQL database connection.

This script tests the connection to the PostgreSQL database using the settings
from the config.json file.
"""

import sys
import json
import psycopg2
from psycopg2 import sql

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