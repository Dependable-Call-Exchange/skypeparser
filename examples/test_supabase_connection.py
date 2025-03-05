#!/usr/bin/env python3
"""
Test script for verifying connection to Supabase PostgreSQL.

This script loads connection details from a .env file and attempts to connect
to Supabase PostgreSQL to verify that the connection works.

Usage:
    python test_supabase_connection.py
"""

import os
import sys
import logging
from dotenv import load_dotenv
import psycopg2
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("supabase_connection_test.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def test_connection():
    """
    Test connection to Supabase PostgreSQL.

    Returns:
        bool: True if connection successful, False otherwise
    """
    # Load environment variables from .env file
    load_dotenv()

    # Get connection parameters from environment variables
    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    port = os.getenv('port')
    dbname = os.getenv('dbname')
    sslmode = os.getenv('sslmode', 'require')

    # Check if all required parameters are present
    required_params = ['user', 'host', 'port', 'dbname']
    missing_params = [param for param in required_params if not os.getenv(param)]
    if missing_params:
        logger.error(f"Missing required environment variables: {', '.join(missing_params)}")
        return False

    # Log connection attempt
    logger.info(f"Attempting to connect to {dbname} on {host}:{port} as {user}")

    try:
        # Connect to the database
        connection = psycopg2.connect(
            user=user,
            password=password,
            host=host,
            port=port,
            dbname=dbname,
            sslmode=sslmode
        )

        # Create a cursor
        cursor = connection.cursor()

        # Execute a simple query
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        logger.info(f"Connected to PostgreSQL: {version[0]}")

        # Test database permissions by listing tables
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        tables = cursor.fetchall()

        if tables:
            logger.info(f"Found {len(tables)} tables in the database:")
            for table in tables:
                logger.info(f"  - {table[0]}")
        else:
            logger.info("No tables found in the database.")

        # Close cursor and connection
        cursor.close()
        connection.close()
        logger.info("Connection closed successfully.")

        return True

    except Exception as e:
        logger.error(f"Connection failed: {e}")
        return False

def main():
    """
    Main function to run the connection test.
    """
    try:
        # Test connection
        success = test_connection()

        # Exit with appropriate status code
        if success:
            logger.info("Connection test completed successfully.")
            sys.exit(0)
        else:
            logger.error("Connection test failed.")
            sys.exit(1)

    except Exception as e:
        logger.exception(f"Error in connection test: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()