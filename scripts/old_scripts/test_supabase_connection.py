#!/usr/bin/env python3
"""
Test script for verifying connection to Supabase PostgreSQL.

This script loads connection details from config/config.json and attempts to connect
to Supabase PostgreSQL to verify that the connection works.

Usage:
    python scripts/test_supabase_connection.py
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

def test_connection():
    """
    Test connection to Supabase PostgreSQL.

    Returns:
        bool: True if connection successful, False otherwise
    """
    # Load configuration
    config = load_config()
    if not config:
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

def create_database_schema():
    """
    Create the necessary database schema if tables don't exist.

    Returns:
        bool: True if successful, False otherwise
    """
    # Load configuration
    config = load_config()
    if not config:
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
        connection.autocommit = True

        # Create a cursor
        cursor = connection.cursor()

        # First, check for existing tables and their structure
        logger.info("Checking existing table structure...")
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
            ORDER BY table_name;
        """)
        existing_tables = [table[0] for table in cursor.fetchall()]
        logger.info(f"Existing tables: {existing_tables}")

        # Check if the messages table exists and get its structure
        message_id_column = "message_id"  # Default name
        if "messages" in existing_tables:
            cursor.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = 'messages';
            """)
            message_columns = [col[0] for col in cursor.fetchall()]
            logger.info(f"Existing columns in messages table: {message_columns}")

            # Determine the appropriate ID column
            if "id" in message_columns and "message_id" not in message_columns:
                message_id_column = "id"
                logger.info(f"Using '{message_id_column}' as the message ID column for foreign key references")

        # Check if the conversations table exists
        if "conversations" not in existing_tables:
            logger.info("Creating 'conversations' table...")
            cursor.execute("""
                CREATE TABLE conversations (
                    id SERIAL PRIMARY KEY,
                    conversation_id TEXT UNIQUE NOT NULL,
                    display_name TEXT,
                    thread_type TEXT,
                    created_at TIMESTAMP
                );
            """)
            logger.info("Created 'conversations' table.")
        else:
            logger.info("The 'conversations' table already exists.")

        # Check if the messages table exists
        if "messages" not in existing_tables:
            logger.info("Creating 'messages' table...")
            cursor.execute("""
                CREATE TABLE messages (
                    id SERIAL PRIMARY KEY,
                    message_id TEXT UNIQUE NOT NULL,
                    conversation_id TEXT NOT NULL,
                    content TEXT,
                    cleaned_content TEXT,
                    message_type TEXT,
                    from_id TEXT,
                    from_name TEXT,
                    timestamp TIMESTAMP,
                    is_edited BOOLEAN DEFAULT FALSE,
                    structured_data JSONB,
                    FOREIGN KEY (conversation_id) REFERENCES conversations (conversation_id)
                );
            """)
            logger.info("Created 'messages' table.")
        else:
            logger.info("The 'messages' table already exists.")

        # Check if the attachments table exists
        if "attachments" not in existing_tables:
            logger.info("Creating 'attachments' table...")

            # If we're using an existing messages table, we need to check its structure
            if "messages" in existing_tables and message_id_column != "message_id":
                # Use the existing column name for the foreign key reference
                cursor.execute(f"""
                    CREATE TABLE attachments (
                        id SERIAL PRIMARY KEY,
                        message_id TEXT NOT NULL,
                        name TEXT,
                        content_type TEXT,
                        size INTEGER,
                        path TEXT,
                        url TEXT,
                        FOREIGN KEY (message_id) REFERENCES messages ({message_id_column})
                    );
                """)
            else:
                cursor.execute("""
                    CREATE TABLE attachments (
                        id SERIAL PRIMARY KEY,
                        message_id TEXT NOT NULL,
                        name TEXT,
                        content_type TEXT,
                        size INTEGER,
                        path TEXT,
                        url TEXT,
                        FOREIGN KEY (message_id) REFERENCES messages (message_id)
                    );
                """)
            logger.info("Created 'attachments' table.")
        else:
            logger.info("The 'attachments' table already exists.")

        # Close cursor and connection
        cursor.close()
        connection.close()
        logger.info("Schema creation completed successfully.")

        return True

    except Exception as e:
        logger.error(f"Schema creation failed: {e}")
        return False

def main():
    """
    Main function to run the connection test.
    """
    try:
        # Test connection
        logger.info("Testing connection to Supabase PostgreSQL...")
        connection_success = test_connection()

        if connection_success:
            logger.info("Connection test completed successfully.")

            # Ask if the user wants to create the schema
            user_input = input("Would you like to create/verify the database schema? (y/n): ").strip().lower()
            if user_input in ('y', 'yes'):
                logger.info("Creating/verifying database schema...")
                schema_success = create_database_schema()
                if schema_success:
                    logger.info("Database schema is ready.")
                    sys.exit(0)
                else:
                    logger.error("Failed to create database schema.")
                    sys.exit(1)
            else:
                logger.info("Skipping database schema creation.")
                sys.exit(0)
        else:
            logger.error("Connection test failed.")
            sys.exit(1)

    except Exception as e:
        logger.exception(f"Error in connection test: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()