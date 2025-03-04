"""
Database fixtures for ETL pipeline integration tests.

This module contains fixtures for setting up and tearing down database
connections and test data for integration tests.
"""

import os
import uuid
import psycopg2
from typing import Dict, Any, Generator, Optional
from contextlib import contextmanager


def get_test_db_config() -> Dict[str, Any]:
    """
    Get database configuration for tests from environment variables.

    Returns:
        Dict[str, Any]: Database configuration dictionary
    """
    return {
        "host": os.environ.get("POSTGRES_TEST_HOST", "localhost"),
        "port": os.environ.get("POSTGRES_TEST_PORT", 5432),
        "database": os.environ.get("POSTGRES_TEST_DB", "test_skype_parser"),
        "user": os.environ.get("POSTGRES_TEST_USER", "postgres"),
        "password": os.environ.get("POSTGRES_TEST_PASSWORD", "postgres"),
    }


@contextmanager
def test_db_connection(config: Optional[Dict[str, Any]] = None) -> Generator[psycopg2.extensions.connection, None, None]:
    """
    Create a test database connection with a unique schema.

    Args:
        config: Database configuration. If None, uses get_test_db_config()

    Yields:
        psycopg2.extensions.connection: Database connection

    Raises:
        psycopg2.Error: If connection fails
    """
    if config is None:
        config = get_test_db_config()

    # Connect to the database
    conn = psycopg2.connect(
        host=config["host"],
        port=config["port"],
        database=config["database"],
        user=config["user"],
        password=config["password"]
    )
    conn.autocommit = True

    # Create a unique schema for this test run
    schema_name = f"test_schema_{uuid.uuid4().hex[:8]}"

    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA {schema_name}")
            cur.execute(f"SET search_path TO {schema_name}")

            # Create tables
            cur.execute("""
                CREATE TABLE skype_raw_exports (
                    id SERIAL PRIMARY KEY,
                    user_id VARCHAR(255) NOT NULL,
                    export_date TIMESTAMP NOT NULL,
                    raw_data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cur.execute("""
                CREATE TABLE skype_conversations (
                    id SERIAL PRIMARY KEY,
                    export_id INTEGER REFERENCES skype_raw_exports(id),
                    conversation_id VARCHAR(255) NOT NULL,
                    display_name VARCHAR(255),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cur.execute("""
                CREATE TABLE skype_messages (
                    id SERIAL PRIMARY KEY,
                    conversation_id INTEGER REFERENCES skype_conversations(id),
                    message_id VARCHAR(255) NOT NULL,
                    sender VARCHAR(255),
                    content TEXT,
                    timestamp TIMESTAMP,
                    message_type VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

        yield conn

    finally:
        # Clean up by dropping the schema
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP SCHEMA {schema_name} CASCADE")
        except Exception as e:
            print(f"Error dropping schema {schema_name}: {e}")

        conn.close()


def is_db_available(config: Optional[Dict[str, Any]] = None) -> bool:
    """
    Check if the test database is available.

    Args:
        config: Database configuration. If None, uses get_test_db_config()

    Returns:
        bool: True if database is available, False otherwise
    """
    if config is None:
        config = get_test_db_config()

    try:
        conn = psycopg2.connect(
            host=config["host"],
            port=config["port"],
            database=config["database"],
            user=config["user"],
            password=config["password"],
            connect_timeout=3  # Short timeout for quick check
        )
        conn.close()
        return True
    except Exception:
        return False