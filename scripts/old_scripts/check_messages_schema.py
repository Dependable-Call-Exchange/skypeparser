#!/usr/bin/env python3
"""
Script to check the schema of the messages table in Supabase.

This script connects to the Supabase PostgreSQL database and displays the schema
of the messages table to understand the correct column names to use for data insertion.

Usage:
    python scripts/check_messages_schema.py
"""

import os
import sys
import json
import logging
import psycopg2

# Add project root to Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_config():
    """
    Load configuration from config/config.json.

    Returns:
        dict: Configuration dictionary
    """
    try:
        config_path = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return None

def check_messages_schema():
    """
    Connect to Supabase PostgreSQL and display the schema of the messages table.
    """
    # Load configuration
    config = load_config()
    if not config:
        logger.error("Failed to load configuration")
        return False

    # Get database configuration
    db_config = config.get('database', {})

    # Extract connection parameters
    host = db_config.get('host', 'localhost')
    port = db_config.get('port', 5432)
    dbname = db_config.get('dbname', 'postgres')
    user = db_config.get('user', 'postgres')
    password = db_config.get('password', '')
    sslmode = db_config.get('sslmode', 'require')

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

        # Check if the messages table exists
        cursor.execute("""
            SELECT EXISTS(
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'messages'
            );
        """)
        table_exists = cursor.fetchone()[0]

        if not table_exists:
            logger.error("The 'messages' table does not exist in the database")
            return False

        # Get the column details for the messages table
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'messages'
            ORDER BY ordinal_position;
        """)
        columns = cursor.fetchall()

        if columns:
            logger.info("Schema of the 'messages' table:")
            for column in columns:
                col_name, data_type, is_nullable = column
                nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
                logger.info(f"  {col_name} - {data_type} - {nullable}")
        else:
            logger.info("No columns found in the 'messages' table.")

        # Close cursor and connection
        cursor.close()
        connection.close()
        logger.info("Connection closed successfully.")

        return True

    except Exception as e:
        logger.error(f"Database operation failed: {e}")
        return False

def main():
    """
    Main function to execute the script.
    """
    logger.info("Starting script to check messages table schema")
    success = check_messages_schema()
    if success:
        logger.info("Successfully checked messages table schema")
    else:
        logger.error("Failed to check messages table schema")

if __name__ == "__main__":
    main()