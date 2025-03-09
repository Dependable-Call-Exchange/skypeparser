#!/usr/bin/env python3
"""
Skype to PostgreSQL Importer

DEPRECATED: This module is deprecated. Please use the SkypeETLPipeline class instead.
See src/db/etl_pipeline.py for the recommended approach.

This script imports Skype conversation data from a Skype export file into a PostgreSQL database.
It uses the existing skype-parser.py functionality to parse the Skype export file and then
imports the data into a PostgreSQL database with the schema described in the documentation.

Usage:
    python skype_to_postgres.py -f <skype_export_file> -u <your_display_name> -d <database_name>
                               [-H <host>] [-P <port>] [-U <username>] [-W <password>]
                               [--select-json <json_file>] [--create-tables]

Example:
    python skype_to_postgres.py -f 8_live_dave.leathers113_export.tar -u "David Leathers"
                               -d skype_logs -U postgres --create-tables
"""

import os
import sys
import argparse
import logging
import datetime
import warnings
import psycopg2
from psycopg2.extras import execute_values

# Issue deprecation warning
warnings.warn(
    "The skype_to_postgres module is deprecated. "
    "Please use the SkypeETLPipeline class instead.",
    DeprecationWarning, stacklevel=2
)

# Import functions from parser module
from ..parser.core_parser import (
    timestamp_parser,
    content_parser
)

# Import file handling functions
from ..utils.file_handler import read_file, read_tarfile

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('skype-to-postgres')

def create_tables(conn):
    """
    Create the necessary tables in the PostgreSQL database.

    Args:
        conn: PostgreSQL connection object

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with conn.cursor() as cur:
            # Create conversations table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS conversations (
                    conversation_id VARCHAR(255) PRIMARY KEY,
                    display_name    VARCHAR(255),
                    start_time      TIMESTAMP,
                    end_time        TIMESTAMP
                );
            """)

            # Create participants table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS participants (
                    participant_id  VARCHAR(255) PRIMARY KEY,
                    display_name    VARCHAR(255)
                );
            """)

            # Create conversation_participants table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS conversation_participants (
                    conversation_id  VARCHAR(255) NOT NULL,
                    participant_id   VARCHAR(255) NOT NULL,
                    PRIMARY KEY (conversation_id, participant_id),
                    FOREIGN KEY (conversation_id) REFERENCES conversations(conversation_id),
                    FOREIGN KEY (participant_id)  REFERENCES participants(participant_id)
                );
            """)

            # Create messages table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    message_id       BIGSERIAL PRIMARY KEY,
                    conversation_id  VARCHAR(255) NOT NULL,
                    participant_id   VARCHAR(255) NOT NULL,
                    timestamp_utc    TIMESTAMP NOT NULL,
                    message_type     VARCHAR(50),
                    is_edited        BOOLEAN DEFAULT FALSE,
                    raw_content      TEXT,
                    cleaned_content  TEXT,
                    FOREIGN KEY (conversation_id) REFERENCES conversations(conversation_id),
                    FOREIGN KEY (participant_id)  REFERENCES participants(participant_id)
                );
            """)

            # Create indexes
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_messages_timestamp
                ON messages (timestamp_utc);
            """)

            # Add a tsvector column for full-text search
            cur.execute("""
                ALTER TABLE messages
                ADD COLUMN IF NOT EXISTS content_tsv tsvector
                GENERATED ALWAYS AS (to_tsvector('english', cleaned_content)) STORED;
            """)

            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_messages_content_tsv
                ON messages USING GIN (content_tsv);
            """)

            conn.commit()
            logger.info("Database tables created successfully")
            return True
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        conn.rollback()
        return False

def import_skype_data(conn, data, user_display_name):
    """
    Import Skype data into the PostgreSQL database.

    Args:
        conn: PostgreSQL connection object
        data (dict): Skype export data
        user_display_name (str): Display name of the user

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        with conn.cursor() as cur:
            # Extract user ID and export date with defaults for missing fields
            user_id = data.get('userId', 'unknown_user')
            if user_id == 'unknown_user':
                logger.warning("User ID not found in export data, using 'unknown_user'")

            export_date_time = data.get('exportDate', datetime.datetime.now().isoformat())
            if 'exportDate' not in data:
                logger.warning("Export date not found in data, using current time")

            export_date_str, export_time_str, export_datetime = timestamp_parser(export_date_time)

            # Map user ID to display name
            id_to_display_name = {user_id: user_display_name}

            # Insert the user as a participant
            cur.execute("""
                INSERT INTO participants (participant_id, display_name)
                VALUES (%s, %s)
                ON CONFLICT (participant_id) DO UPDATE
                    SET display_name = EXCLUDED.display_name;
            """, (user_id, user_display_name))

            # Process each conversation
            conversations = data.get('conversations', [])
            if not conversations:
                logger.warning("No conversations found in export data")

            for conversation in conversations:
                conv_id = conversation.get('id')
                if not conv_id:
                    logger.warning("Skipping conversation with missing ID")
                    continue

                display_name = conversation.get('displayName', conv_id)

                # Insert conversation
                cur.execute("""
                    INSERT INTO conversations (conversation_id, display_name)
                    VALUES (%s, %s)
                    ON CONFLICT (conversation_id) DO UPDATE
                        SET display_name = EXCLUDED.display_name;
                """, (conv_id, display_name))

                # Process messages to find participants and conversation time range
                messages = conversation.get('MessageList', [])
                if not messages:
                    logger.info(f"No messages found for conversation: {display_name}")

                participants = set()
                timestamps = []

                # First pass: collect participants and timestamps
                for msg in messages:
                    from_id = msg.get('from')
                    if from_id:
                        participants.add(from_id)

                        # Get display name if available
                        from_name = msg.get('imdisplayname', '')
                        if from_name and from_id not in id_to_display_name:
                            id_to_display_name[from_id] = from_name

                    # Collect timestamps for determining conversation time range
                    if 'originalarrivaltime' in msg:
                        _, _, dt_obj = timestamp_parser(msg['originalarrivaltime'])
                        if dt_obj:
                            timestamps.append(dt_obj)

                # Insert participants and link them to the conversation
                for participant_id in participants:
                    display_name = id_to_display_name.get(participant_id, participant_id)

                    # Insert participant
                    cur.execute("""
                        INSERT INTO participants (participant_id, display_name)
                        VALUES (%s, %s)
                        ON CONFLICT (participant_id) DO UPDATE
                            SET display_name = EXCLUDED.display_name;
                    """, (participant_id, display_name))

                    # Link participant to conversation
                    cur.execute("""
                        INSERT INTO conversation_participants (conversation_id, participant_id)
                        VALUES (%s, %s)
                        ON CONFLICT (conversation_id, participant_id) DO NOTHING;
                    """, (conv_id, participant_id))

                # Update conversation time range if timestamps exist
                if timestamps:
                    start_time = min(timestamps)
                    end_time = max(timestamps)

                    cur.execute("""
                        UPDATE conversations
                        SET start_time = %s, end_time = %s
                        WHERE conversation_id = %s;
                    """, (start_time, end_time, conv_id))

                # Second pass: insert messages
                message_data = []
                for msg in messages:
                    from_id = msg.get('from')
                    if not from_id:
                        logger.debug("Skipping message with missing sender ID")
                        continue

                    # Parse timestamp
                    timestamp_str = msg.get('originalarrivaltime')
                    if not timestamp_str:
                        logger.debug("Skipping message with missing timestamp")
                        continue

                    _, _, timestamp_dt = timestamp_parser(timestamp_str)
                    if not timestamp_dt:
                        logger.warning(f"Skipping message with invalid timestamp: {timestamp_str}")
                        continue

                    # Parse message type
                    msg_type = msg.get('messagetype', 'Unknown')

                    # Check if message was edited
                    is_edited = 'skypeeditedid' in msg

                    # Parse content
                    raw_content = msg.get('content', '')

                    # Clean content for display
                    cleaned_content = raw_content
                    if raw_content:
                        try:
                            cleaned_content = content_parser(raw_content)
                        except Exception as e:
                            logger.warning(f"Error cleaning content: {e}")

                    # Add message to batch
                    message_data.append((
                        conv_id, from_id, timestamp_dt, msg_type,
                        is_edited, raw_content, cleaned_content
                    ))

                # Batch insert messages in chunks to avoid memory issues with large datasets
                BATCH_SIZE = 1000
                for i in range(0, len(message_data), BATCH_SIZE):
                    batch = message_data[i:i+BATCH_SIZE]
                    if batch:
                        execute_values(
                            cur,
                            """
                            INSERT INTO messages
                            (conversation_id, participant_id, timestamp_utc, message_type,
                             is_edited, raw_content, cleaned_content)
                            VALUES %s
                            """,
                            batch
                        )

                logger.info(f"Imported {len(message_data)} messages for conversation: {display_name}")

            conn.commit()
            logger.info("Skype data imported successfully")
            return True
    except Exception as e:
        logger.error(f"Error importing Skype data: {e}")
        conn.rollback()
        return False

def get_commandline_args():
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description='Import Skype data into PostgreSQL')

    # File input options
    parser.add_argument('-f', '--filename', required=True, help='Path to the Skype export file')
    parser.add_argument('-t', '--tar', action='store_true', help='Input file is a tar archive')
    parser.add_argument('--select-json', help='Select a specific JSON file from the tar archive')

    # User options
    parser.add_argument('-u', '--user-display-name', help='Your display name in the Skype logs')

    # Database options
    parser.add_argument('-d', '--dbname', required=True, help='PostgreSQL database name')
    parser.add_argument('-H', '--host', default='localhost', help='PostgreSQL host (default: localhost)')
    parser.add_argument('-P', '--port', type=int, default=5432, help='PostgreSQL port (default: 5432)')
    parser.add_argument('-U', '--username', help='PostgreSQL username')
    parser.add_argument('-W', '--password', help='PostgreSQL password')
    parser.add_argument('--create-tables', action='store_true', help='Create database tables if they do not exist')

    return parser.parse_args()

def main():
    """
    Main function to import Skype data into PostgreSQL.
    """
    try:
        args = get_commandline_args()

        # Validate that the input file exists
        if not os.path.exists(args.filename):
            logger.error(f"File not found: {args.filename}")
            sys.exit(1)

        # Get user display name from command line or use default
        user_display_name = args.user_display_name
        if not user_display_name:
            logger.info("No user display name provided, using default: 'Me'")
            user_display_name = "Me"

        # Read the Skype export file
        try:
            main_file = read_file(args.filename) if not args.tar else read_tarfile(args.filename, args.select_json)
        except Exception as e:
            logger.error(f"Error reading file: {e}")
            sys.exit(1)

        # Connect to PostgreSQL
        try:
            conn = psycopg2.connect(
                dbname=args.dbname,
                user=args.username,
                password=args.password,
                host=args.host,
                port=args.port
            )
            logger.info(f"Connected to PostgreSQL database: {args.dbname}")
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL: {e}")
            sys.exit(1)

        # Create tables if requested
        if args.create_tables:
            if not create_tables(conn):
                conn.close()
                sys.exit(1)

        # Import Skype data
        if not import_skype_data(conn, main_file, user_display_name):
            conn.close()
            sys.exit(1)

        # Close connection
        conn.close()
        logger.info("Import completed successfully")

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()