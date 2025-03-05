"""
Loader module for the ETL pipeline.

This module handles loading transformed Skype data into the database,
including raw exports, conversations, and messages.
"""

import logging
import psycopg2
import psycopg2.extras
import json
from typing import Dict, List, Any, Optional, Tuple

from src.utils.validation import validate_db_config
from .context import ETLContext

logger = logging.getLogger(__name__)

# Database schema definitions
RAW_EXPORTS_TABLE = """
CREATE TABLE IF NOT EXISTS skype_raw_exports (
    export_id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    export_date TIMESTAMP NOT NULL,
    raw_data JSONB NOT NULL,
    file_source TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CONVERSATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS skype_conversations (
    conversation_id TEXT PRIMARY KEY,
    display_name TEXT,
    export_id INTEGER REFERENCES skype_raw_exports(export_id),
    first_message_time TIMESTAMP,
    last_message_time TIMESTAMP,
    message_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

MESSAGES_TABLE = """
CREATE TABLE IF NOT EXISTS skype_messages (
    message_id SERIAL PRIMARY KEY,
    conversation_id TEXT REFERENCES skype_conversations(conversation_id),
    timestamp TIMESTAMP NOT NULL,
    sender_id TEXT NOT NULL,
    sender_name TEXT,
    message_type TEXT,
    raw_content TEXT,
    cleaned_content TEXT,
    is_edited BOOLEAN DEFAULT FALSE,
    structured_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

class Loader:
    """Handles loading of transformed data into the database."""

    def __init__(self, context: ETLContext = None, db_config: Optional[Dict[str, Any]] = None, batch_size: int = 100):
        """Initialize the Loader.

        Args:
            context: Shared ETL context object
            db_config: Database configuration dictionary (used if context not provided)
            batch_size: Size of database batch operations (used if context not provided)
        """
        self.context = context

        # Use context settings if available, otherwise use parameters
        if context:
            self.db_config = context.db_config
            self.batch_size = context.batch_size
        else:
            self.db_config = db_config
            self.batch_size = batch_size

        # Validate database configuration
        if self.db_config:
            validate_db_config(self.db_config)

        self.conn = None
        self.transaction_active = False

        logger.info("Initialized loader")

    def connect_db(self) -> None:
        """Connect to the database and create tables if they don't exist."""
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self._create_tables()
            logger.info("Connected to database successfully")
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            raise

    def close_db(self) -> None:
        """Close the database connection."""
        if self.conn:
            try:
                self.conn.close()
                logger.info("Closed database connection")
            except Exception as e:
                logger.error(f"Error closing database connection: {e}")

    def _create_tables(self) -> None:
        """Create the necessary database tables if they don't exist."""
        with self.conn.cursor() as cursor:
            cursor.execute(RAW_EXPORTS_TABLE)
            cursor.execute(CONVERSATIONS_TABLE)
            cursor.execute(MESSAGES_TABLE)
        self.conn.commit()

    def load(self, raw_data: Dict[str, Any], transformed_data: Dict[str, Any], file_source: Optional[str] = None) -> int:
        """Load transformed data into the database.

        Args:
            raw_data: Raw data from the extractor
            transformed_data: Transformed data from the transformer
            file_source: Source of the data (e.g., file path)

        Returns:
            int: The export ID in the database

        Raises:
            ValueError: If database connection is not available
        """
        logger.info("Starting data loading")

        # Connect to database if not already connected
        if not self.conn:
            self.connect_db()

        if not self.conn:
            error_msg = "Database connection not available"
            logger.error(error_msg)

            # Record error in context if available
            if self.context:
                self.context.record_error("load", ValueError(error_msg), fatal=True)

            raise ValueError(error_msg)

        # Count total conversations and messages for progress tracking
        total_conversations = len(transformed_data.get('conversations', {}))
        total_messages = sum(len(conv.get('messages', [])) for conv in transformed_data.get('conversations', {}).values())

        # Update context if available
        if self.context:
            # Context phase is managed by the pipeline manager, but we can update counts
            self.context.progress_tracker.total_conversations = total_conversations
            self.context.progress_tracker.total_messages = total_messages

        logger.info(f"Loading {total_conversations} conversations with {total_messages} messages")

        try:
            # Begin transaction
            self._begin_transaction()

            # Insert raw export data and get export ID
            export_id = self._insert_raw_export(raw_data, transformed_data, file_source)

            # Insert conversations and messages
            self._insert_conversations_and_messages(transformed_data, export_id)

            # Commit transaction
            self._commit_transaction()

            # Update context if available
            if self.context:
                self.context.export_id = export_id
                self.context.check_memory()

            logger.info(f"Loading complete with export ID: {export_id}")

            return export_id

        except Exception as e:
            # Rollback transaction
            self._rollback_transaction()

            logger.error(f"Error during loading phase: {e}")

            # Record error in context if available
            if self.context:
                self.context.record_error("load", e, fatal=True)

            raise

    def _insert_raw_export(self, raw_data: Dict[str, Any], transformed_data: Dict[str, Any],
                          file_source: Optional[str] = None) -> int:
        """Insert the raw export data and return the export ID.

        Args:
            raw_data: Raw data to insert
            transformed_data: Transformed data containing metadata
            file_source: Optional source file path

        Returns:
            export_id: ID of the created export record
        """
        user_display_name = transformed_data['metadata'].get('user_display_name', '')
        export_time = transformed_data['metadata'].get('export_time', '')

        with self.conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO skype_raw_exports (user_id, export_date, raw_data, file_source)
                VALUES (%s, %s, %s, %s)
                RETURNING export_id
            """, (user_display_name, export_time, psycopg2.extras.Json(raw_data), file_source))

            export_id = cursor.fetchone()[0]
            return export_id

    def _insert_conversations_and_messages(self, transformed_data: Dict[str, Any], export_id: int) -> None:
        """Insert conversations and their messages into the database.

        Args:
            transformed_data: Transformed data containing conversations and messages
            export_id: ID of the export record
        """
        with self.conn.cursor() as cursor:
            for conv_id, conv_data in transformed_data['conversations'].items():
                # Insert conversation
                self._insert_conversation(cursor, conv_id, conv_data, export_id)

                # Insert messages for this conversation
                if conv_data.get('messages'):
                    self._insert_messages(cursor, conv_id, conv_data['messages'])

    def _insert_conversation(self, cursor, conv_id: str, conv_data: Dict[str, Any],
                           export_id: int) -> None:
        """Insert a single conversation into the database.

        Args:
            cursor: Database cursor
            conv_id: Conversation ID
            conv_data: Conversation data
            export_id: ID of the export record
        """
        cursor.execute("""
            INSERT INTO skype_conversations (
                conversation_id, display_name, export_id,
                first_message_time, last_message_time, message_count
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (conversation_id) DO UPDATE
            SET
                display_name = EXCLUDED.display_name,
                export_id = EXCLUDED.export_id,
                first_message_time = EXCLUDED.first_message_time,
                last_message_time = EXCLUDED.last_message_time,
                message_count = EXCLUDED.message_count,
                updated_at = CURRENT_TIMESTAMP
        """, (
            conv_id,
            conv_data.get('display_name', ''),
            export_id,
            conv_data.get('first_message_time'),
            conv_data.get('last_message_time'),
            conv_data.get('message_count', 0)
        ))

    def _insert_messages(self, cursor, conv_id: str, messages: List[Dict[str, Any]]) -> None:
        """Insert messages for a conversation into the database.

        Args:
            cursor: Database cursor
            conv_id: Conversation ID
            messages: List of messages to insert
        """
        for message in messages:
            cursor.execute("""
                INSERT INTO skype_messages (
                    conversation_id, timestamp, sender_id, sender_name,
                    message_type, raw_content, cleaned_content,
                    is_edited, structured_data
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                conv_id,
                message.get('timestamp'),
                message.get('sender_id'),
                message.get('sender_name'),
                message.get('message_type'),
                message.get('content'),
                message.get('cleaned_content'),
                message.get('is_edited', False),
                psycopg2.extras.Json(message.get('structured_data', {}))
                    if message.get('structured_data') else None
            ))

    def _begin_transaction(self) -> None:
        """Begin a transaction."""
        self.conn.autocommit = False
        self.transaction_active = True

    def _commit_transaction(self) -> None:
        """Commit the transaction."""
        self.conn.commit()
        self.conn.autocommit = True
        self.transaction_active = False

    def _rollback_transaction(self) -> None:
        """Rollback the transaction."""
        self.conn.rollback()
        self.conn.autocommit = True
        self.transaction_active = False
