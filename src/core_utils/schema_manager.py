"""
Schema manager for handling database schema creation and management.

This module provides the SchemaManager class that encapsulates
database schema creation, index management, and schema migrations.
"""

import logging
from typing import Dict, List, Optional

from src.logging.new_structured_logging import get_logger, log_execution_time, handle_errors

logger = get_logger(__name__)


class SchemaManager:
    """Manages database schema creation and management."""

    def __init__(self, db_manager):
        """Initialize the schema manager.

        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager
        logger.info("Initialized SchemaManager")

    @log_execution_time(level=logging.INFO)
    @handle_errors(log_level="ERROR", default_message="Error creating schema")
    def create_schema(self) -> None:
        """Create the database schema if it doesn't exist."""
        logger.info("Creating database schema")

        # Create tables
        self._create_conversations_table()
        self._create_messages_table()
        self._create_users_table()
        self._create_attachments_table()

        # Create indexes
        self._create_indexes()

        logger.info("Database schema created successfully")

    def _table_exists(self, table_name: str) -> bool:
        """Check if a table exists.

        Args:
            table_name: Name of the table to check

        Returns:
            True if the table exists, False otherwise
        """
        try:
            # Try to use the table_exists method if it exists
            if hasattr(self.db_manager, 'table_exists'):
                return self.db_manager.table_exists(table_name)

            # Fallback: Try to query the information schema
            try:
                query = """
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = %s
                    )
                """
                self.db_manager.execute(query, (table_name,))
                result = self.db_manager.fetch_one()
                return result[0] if result else False
            except Exception as e:
                logger.warning(f"Error checking if table {table_name} exists: {e}")
                return False
        except Exception as e:
            logger.warning(f"Error checking if table {table_name} exists: {e}")
            return False

    def _index_exists(self, index_name: str) -> bool:
        """Check if an index exists.

        Args:
            index_name: Name of the index to check

        Returns:
            True if the index exists, False otherwise
        """
        try:
            # Try to use the index_exists method if it exists
            if hasattr(self.db_manager, 'index_exists'):
                return self.db_manager.index_exists(index_name)

            # Fallback: Try to query the information schema
            try:
                query = """
                    SELECT EXISTS (
                        SELECT FROM pg_indexes
                        WHERE indexname = %s
                    )
                """
                self.db_manager.execute(query, (index_name,))
                result = self.db_manager.fetch_one()
                return result[0] if result else False
            except Exception as e:
                logger.warning(f"Error checking if index {index_name} exists: {e}")
                return False
        except Exception as e:
            logger.warning(f"Error checking if index {index_name} exists: {e}")
            return False

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error creating conversations table")
    def _create_conversations_table(self) -> None:
        """Create the conversations table if it doesn't exist."""
        if self._table_exists("conversations"):
            logger.debug("Conversations table already exists")
            return

        logger.info("Creating conversations table")

        query = """
            CREATE TABLE conversations (
                id TEXT PRIMARY KEY,
                display_name TEXT,
                type TEXT,
                version TEXT,
                properties JSONB,
                thread_properties JSONB,
                members JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """

        self.db_manager.execute(query)
        logger.info("Conversations table created")

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error creating messages table")
    def _create_messages_table(self) -> None:
        """Create the messages table if it doesn't exist."""
        if self._table_exists("messages"):
            logger.debug("Messages table already exists")
            return

        logger.info("Creating messages table")

        query = """
            CREATE TABLE messages (
                id TEXT PRIMARY KEY,
                conversation_id TEXT REFERENCES conversations(id),
                content TEXT,
                cleaned_content TEXT,
                message_type TEXT,
                from_id TEXT,
                from_name TEXT,
                timestamp TIMESTAMP,
                is_edited BOOLEAN,
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """

        self.db_manager.execute(query)
        logger.info("Messages table created")

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error creating users table")
    def _create_users_table(self) -> None:
        """Create the users table if it doesn't exist."""
        if self._table_exists("users"):
            logger.debug("Users table already exists")
            return

        logger.info("Creating users table")

        query = """
            CREATE TABLE users (
                id TEXT PRIMARY KEY,
                display_name TEXT,
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """

        self.db_manager.execute(query)
        logger.info("Users table created")

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error creating attachments table")
    def _create_attachments_table(self) -> None:
        """Create the attachments table if it doesn't exist."""
        if self._table_exists("attachments"):
            logger.debug("Attachments table already exists")
            return

        logger.info("Creating attachments table")

        query = """
            CREATE TABLE attachments (
                id TEXT PRIMARY KEY,
                message_id TEXT REFERENCES messages(id),
                file_name TEXT,
                file_size BIGINT,
                content_type TEXT,
                url TEXT,
                properties JSONB,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """

        self.db_manager.execute(query)
        logger.info("Attachments table created")

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error creating indexes")
    def _create_indexes(self) -> None:
        """Create indexes if they don't exist."""
        logger.info("Creating indexes")

        # Define indexes
        indexes = [
            {
                "name": "idx_messages_conversation_id",
                "table": "messages",
                "columns": ["conversation_id"],
            },
            {
                "name": "idx_messages_from_id",
                "table": "messages",
                "columns": ["from_id"],
            },
            {
                "name": "idx_messages_timestamp",
                "table": "messages",
                "columns": ["timestamp"],
            },
            {
                "name": "idx_attachments_message_id",
                "table": "attachments",
                "columns": ["message_id"],
            },
        ]

        # Create each index
        for index in indexes:
            self._create_index(index["name"], index["table"], index["columns"])

        logger.info("Indexes created successfully")

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error creating index")
    def _create_index(self, name: str, table: str, columns: List[str]) -> None:
        """Create an index if it doesn't exist.

        Args:
            name: Index name
            table: Table name
            columns: Column names
        """
        if self._index_exists(name):
            logger.debug(f"Index {name} already exists")
            return

        logger.info(f"Creating index {name} on {table}({', '.join(columns)})")

        # Build query
        columns_str = ", ".join(columns)
        query = f"CREATE INDEX {name} ON {table} ({columns_str})"

        self.db_manager.execute(query)
        logger.info(f"Index {name} created")

    @log_execution_time(level=logging.INFO)
    @handle_errors(log_level="ERROR", default_message="Error dropping schema")
    def drop_schema(self) -> None:
        """Drop the database schema."""
        logger.warning("Dropping database schema")

        # Drop tables in reverse order to avoid foreign key constraints
        tables = ["attachments", "messages", "conversations", "users"]

        for table in tables:
            if self._table_exists(table):
                logger.info(f"Dropping table {table}")
                self.db_manager.execute(f"DROP TABLE {table} CASCADE")

        logger.info("Database schema dropped successfully")