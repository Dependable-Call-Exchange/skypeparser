#!/usr/bin/env python3
"""
Check Archives Schema Script

This script connects to the Supabase PostgreSQL database and checks the schema
of the 'archives' table. It loads connection details from config/config.json.

Usage:
    python check_archives_schema.py
"""

import os
import sys
import json
import logging
import psycopg2

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def load_config():
    """Load database configuration from config file."""
    config_path = os.path.join("config", "config.json")
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        return config.get("database", {})
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        sys.exit(1)

def check_archives_schema():
    """Check the schema of the archives table."""
    db_config = load_config()

    # Extract connection parameters
    host = db_config.get("host")
    port = db_config.get("port")
    dbname = db_config.get("dbname")
    user = db_config.get("user")
    password = db_config.get("password")
    sslmode = db_config.get("sslmode", "require")

    logger.info(f"Attempting to connect to {dbname} on {host}:{port} as {user}")

    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            sslmode=sslmode
        )

        # Create a cursor
        cursor = conn.cursor()

        # Check if the archives table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = 'archives'
            );
        """)

        table_exists = cursor.fetchone()[0]

        if not table_exists:
            logger.error("The 'archives' table does not exist in the database.")
            conn.close()
            return

        # Get the schema of the archives table
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = 'archives'
            ORDER BY ordinal_position;
        """)

        columns = cursor.fetchall()

        logger.info("Schema of the 'archives' table:")
        for column in columns:
            column_name, data_type, is_nullable = column
            nullable = "NULL" if is_nullable == "YES" else "NOT NULL"
            logger.info(f"  {column_name} - {data_type} - {nullable}")

        # Close the connection
        conn.close()
        logger.info("Connection closed successfully.")

    except Exception as e:
        logger.error(f"Error checking archives schema: {e}")
        sys.exit(1)

def main():
    """Main function to run the script."""
    logger.info("Starting script to check archives table schema")
    check_archives_schema()
    logger.info("Successfully checked archives table schema")

if __name__ == "__main__":
    main()