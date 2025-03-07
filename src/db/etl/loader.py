"""
Loader module for the ETL pipeline.

This module handles loading transformed Skype data into the database,
including raw exports, conversations, and messages.
"""

import logging
import json
from typing import Dict, List, Any, Optional, Tuple
import datetime

from src.utils.interfaces import LoaderProtocol, DatabaseConnectionProtocol
from src.utils.di import get_service
from src.utils.validation import validate_db_config
from .context import ETLContext

logger = logging.getLogger(__name__)

# Database schema definitions
RAW_EXPORTS_TABLE = """
CREATE TABLE IF NOT EXISTS public.skype_raw_exports (
    export_id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    export_date TIMESTAMP NOT NULL,
    raw_data JSONB NOT NULL,
    file_source TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CONVERSATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS public.skype_conversations (
    conversation_id TEXT PRIMARY KEY,
    display_name TEXT,
    export_id INTEGER REFERENCES public.skype_raw_exports(export_id),
    first_message_time TIMESTAMP,
    last_message_time TIMESTAMP,
    message_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

MESSAGES_TABLE = """
CREATE TABLE IF NOT EXISTS public.skype_messages (
    message_id SERIAL PRIMARY KEY,
    conversation_id TEXT REFERENCES public.skype_conversations(conversation_id),
    timestamp TIMESTAMP NOT NULL,
    sender_id TEXT NOT NULL,
    sender_name TEXT,
    content TEXT,
    html_content TEXT,
    message_type TEXT NOT NULL,
    is_edited BOOLEAN DEFAULT FALSE,
    is_deleted BOOLEAN DEFAULT FALSE,
    reactions JSONB,
    attachments JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

class Loader(LoaderProtocol):
    """Handles loading of transformed Skype data into the database."""

    def __init__(self,
                 context: ETLContext = None,
                 db_config: Optional[Dict[str, Any]] = None,
                 batch_size: int = 100,
                 db_connection: Optional[DatabaseConnectionProtocol] = None):
        """Initialize the Loader.

        Args:
            context: Shared ETL context object
            db_config: Database configuration (used if context not provided)
            batch_size: Number of records to insert in a single batch
            db_connection: Optional database connection
        """
        self.context = context
        self.db_config = db_config if context is None else context.db_config
        self.batch_size = batch_size
        self.db_connection = db_connection or get_service(DatabaseConnectionProtocol)
        self._conn = None
        self._cursor = None

    def connect_db(self) -> None:
        """Connect to the database and create tables if they don't exist."""
        logger.info("Connecting to database")

        # Connect to the database
        self.db_connection.connect()

        # Create tables
        self._create_tables()

        logger.info("Database connection established")

    def close_db(self) -> None:
        """Close the database connection."""
        logger.info("Closing database connection")

        if self.db_connection:
            self.db_connection.disconnect()

        logger.info("Database connection closed")

    def _create_tables(self) -> None:
        """Create database tables if they don't exist."""
        logger.info("Creating database tables if they don't exist")

        # Execute table creation queries
        self.db_connection.execute(RAW_EXPORTS_TABLE)
        self.db_connection.execute(CONVERSATIONS_TABLE)
        self.db_connection.execute(MESSAGES_TABLE)

    def load(self, raw_data: Dict[str, Any], transformed_data: Dict[str, Any], file_source: Optional[str] = None) -> int:
        """Load transformed data into the database.

        Args:
            raw_data: Raw data from the extraction phase
            transformed_data: Transformed data from the transformation phase
            file_source: Original file source (path or name)

        Returns:
            The export ID of the loaded data

        Raises:
            ValueError: If the input data is invalid
            Exception: If there's an error during loading
        """
        # Validate input data
        self._validate_input_data(raw_data, transformed_data)

        # Validate database connection
        self._validate_database_connection()

        # Begin transaction
        self._begin_transaction()

        try:
            # Store raw export data
            export_id = self._store_raw_export(raw_data, file_source)

            # Store conversations
            self._store_conversations(transformed_data, export_id)

            # Store messages
            self._store_messages(transformed_data)

            # Commit transaction
            self._commit_transaction()

            # Update context if available
            if self.context:
                self.context.set_export_id(export_id)
                self.context.set_phase_status('load', 'completed')

            logger.info(f"Data loaded successfully with export ID: {export_id}")
            return export_id

        except Exception as e:
            # Rollback transaction on error
            self._rollback_transaction()

            # Log error
            logger.error(f"Error loading data: {e}")

            # Update context if available
            if self.context:
                self.context.record_error('load', str(e))

            # Re-raise exception
            raise

    def _validate_input_data(self, raw_data: Dict[str, Any], transformed_data: Dict[str, Any]) -> None:
        """Validate input data for loading.

        Args:
            raw_data: Raw data from the extraction phase
            transformed_data: Transformed data from the transformation phase

        Raises:
            ValueError: If the input data is invalid
        """
        # Check that raw_data is a dictionary
        if not isinstance(raw_data, dict):
            error_msg = f"Raw data must be a dictionary, got {type(raw_data).__name__}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Check that transformed_data is a dictionary
        if not isinstance(transformed_data, dict):
            error_msg = f"Transformed data must be a dictionary, got {type(transformed_data).__name__}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Check that transformed_data contains required keys
        required_keys = ['user_id', 'export_date', 'conversations']
        missing_keys = [key for key in required_keys if key not in transformed_data]
        if missing_keys:
            error_msg = f"Transformed data missing required keys: {', '.join(missing_keys)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Check that conversations is a dictionary
        if not isinstance(transformed_data['conversations'], dict):
            error_msg = "Transformed data 'conversations' must be a dictionary"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Check that at least one conversation exists
        if not transformed_data['conversations']:
            logger.warning("No conversations found in transformed data")

    def _validate_database_connection(self) -> None:
        """Validate database connection.

        Raises:
            ValueError: If the database connection is invalid
        """
        if not self.db_connection:
            error_msg = "Database connection not initialized"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def _store_raw_export(self, raw_data: Dict[str, Any], file_source: Optional[str] = None) -> int:
        """Store raw export data in the database.

        Args:
            raw_data: Raw data from the extraction phase
            file_source: Original file source (path or name)

        Returns:
            The export ID of the stored data
        """
        logger.info("Storing raw export data")

        # Get user ID and export date from context or use defaults
        user_id = self.context.user_id if self.context and hasattr(self.context, 'user_id') and self.context.user_id is not None else "unknown_user"
        export_date = self.context.export_date if self.context and hasattr(self.context, 'export_date') and self.context.export_date is not None else datetime.datetime.now().isoformat()

        # Insert raw export data
        query = """
        INSERT INTO public.skype_raw_exports (user_id, export_date, raw_data, file_source)
        VALUES (%s, %s, %s, %s)
        RETURNING export_id
        """

        # Convert raw_data to JSON string
        raw_data_json = json.dumps(raw_data)

        # Execute query
        result = self.db_connection.fetch_one(query, (user_id, export_date, raw_data_json, file_source))

        # Get export ID
        export_id = result[0]

        logger.info(f"Raw export data stored with ID: {export_id}")
        return export_id

    def _store_conversations(self, transformed_data: Dict[str, Any], export_id: int) -> None:
        """Store conversations in the database.

        Args:
            transformed_data: Transformed data from the transformation phase
            export_id: Export ID to associate with the conversations
        """
        logger.info("Storing conversations")

        # Get conversations from transformed data
        conversations = transformed_data['conversations']

        # Insert each conversation
        for conv_id, conv_data in conversations.items():
            self._insert_conversation(conv_id, conv_data, export_id)

        logger.info(f"Stored {len(conversations)} conversations")

    def _insert_conversation(self, conv_id: str, conv_data: Dict[str, Any], export_id: int) -> None:
        """Insert a conversation into the database.

        Args:
            conv_id: Conversation ID
            conv_data: Conversation data
            export_id: Export ID to associate with the conversation
        """
        # Extract conversation data
        display_name = conv_data.get('display_name', '')
        first_message_time = conv_data.get('first_message_time')
        last_message_time = conv_data.get('last_message_time')
        message_count = len(conv_data.get('messages', []))

        # Insert conversation
        query = """
        INSERT INTO public.skype_conversations
        (conversation_id, display_name, export_id, first_message_time, last_message_time, message_count)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (conversation_id)
        DO UPDATE SET
            display_name = EXCLUDED.display_name,
            export_id = EXCLUDED.export_id,
            first_message_time = EXCLUDED.first_message_time,
            last_message_time = EXCLUDED.last_message_time,
            message_count = EXCLUDED.message_count,
            updated_at = CURRENT_TIMESTAMP
        """

        # Execute query
        self.db_connection.execute(query, (
            conv_id,
            display_name,
            export_id,
            first_message_time,
            last_message_time,
            message_count
        ))

    def _store_messages(self, transformed_data: Dict[str, Any]) -> None:
        """Store messages in the database.

        Args:
            transformed_data: Transformed data from the transformation phase
        """
        logger.info("Storing messages")

        # Get conversations from transformed data
        conversations = transformed_data['conversations']

        # Track total messages
        total_messages = 0

        # Insert messages for each conversation
        for conv_id, conv_data in conversations.items():
            messages = conv_data.get('messages', [])
            if messages:
                self._insert_messages(conv_id, messages)
                total_messages += len(messages)

        logger.info(f"Stored {total_messages} messages")

    def _insert_messages(self, conv_id: str, messages: List[Dict[str, Any]]) -> None:
        """Insert messages into the database.

        Args:
            conv_id: Conversation ID
            messages: List of messages to insert
        """
        # Prepare batch insert
        batch_size = self.batch_size
        for i in range(0, len(messages), batch_size):
            batch = messages[i:i+batch_size]

            # Prepare parameters for batch insert
            params_list = []
            for msg in batch:
                # Get timestamp or use a default value
                timestamp = msg.get('timestamp')
                if not timestamp:
                    # Skip messages with empty timestamps
                    logger.warning(f"Skipping message with empty timestamp: {msg}")
                    continue

                params_list.append((
                    conv_id,
                    timestamp,
                    msg.get('sender_id', ''),
                    msg.get('sender_name', ''),
                    msg.get('content', ''),
                    msg.get('html_content', ''),
                    msg.get('message_type', 'text'),
                    msg.get('is_edited', False),
                    msg.get('is_deleted', False),
                    json.dumps(msg.get('reactions', {})),
                    json.dumps(msg.get('attachments', []))
                ))

            # Insert batch
            query = """
            INSERT INTO public.skype_messages
            (conversation_id, timestamp, sender_id, sender_name, content, html_content,
             message_type, is_edited, is_deleted, reactions, attachments)
            VALUES %s
            """

            # Execute batch insert using execute_values
            from psycopg2.extras import execute_values
            execute_values(self.db_connection.cursor, query, params_list, template=None, page_size=self.batch_size)

    def _begin_transaction(self) -> None:
        """Begin a database transaction."""
        logger.debug("Beginning database transaction")
        # Transaction is handled by the database connection

    def _commit_transaction(self) -> None:
        """Commit the current database transaction."""
        logger.debug("Committing database transaction")
        # Transaction is handled by the database connection

    def _rollback_transaction(self) -> None:
        """Rollback the current database transaction."""
        logger.debug("Rolling back database transaction")
        # Transaction is handled by the database connection
