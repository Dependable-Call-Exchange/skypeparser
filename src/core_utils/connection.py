"""
Database connection module.

This module provides the DatabaseConnection class that implements the
DatabaseConnectionProtocol interface for database operations.
"""

import logging
import os
from typing import Dict, Any, List, Tuple, Optional, Union
import psycopg2
from psycopg2.extras import DictCursor, execute_values

from src.core_utils.interfaces import DatabaseConnectionProtocol
from src.core_utils.dependencies import get_psycopg2

logger = logging.getLogger(__name__)

class DatabaseConnection(DatabaseConnectionProtocol):
    """Handles database connections and operations."""

    def __init__(self, db_config: Dict[str, Any]):
        """Initialize the database connection.

        Args:
            db_config: Database configuration dictionary
        """
        self.db_config = db_config
        self.connection = None
        self.cursor = None
        self._validate_db_config()
        logger.info("Database connection initialized with config")

    def _validate_db_config(self) -> None:
        """Validate the database configuration.

        Raises:
            ValueError: If the database configuration is invalid
        """
        required_keys = ['host', 'port', 'dbname', 'user', 'password']
        missing_keys = [key for key in required_keys if key not in self.db_config]

        if missing_keys:
            error_msg = f"Missing required database configuration keys: {', '.join(missing_keys)}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def connect(self) -> None:
        """Connect to the database.

        Raises:
            Exception: If connection fails
        """
        if self.connection is not None:
            logger.warning("Already connected to database, reusing connection")
            return

        # Ensure psycopg2 is available
        get_psycopg2()

        try:
            logger.info(f"Connecting to database {self.db_config['dbname']} at {self.db_config['host']}:{self.db_config['port']}")
            self.connection = psycopg2.connect(
                host=self.db_config['host'],
                port=self.db_config['port'],
                dbname=self.db_config['dbname'],
                user=self.db_config['user'],
                password=self.db_config['password']
            )
            self.cursor = self.connection.cursor(cursor_factory=DictCursor)
            logger.info("Successfully connected to database")
        except Exception as e:
            error_msg = f"Failed to connect to database: {e}"
            logger.error(error_msg)
            raise

    def disconnect(self) -> None:
        """Disconnect from the database."""
        if self.cursor:
            self.cursor.close()
            self.cursor = None

        if self.connection:
            self.connection.close()
            self.connection = None
            logger.info("Disconnected from database")

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """Execute a database query.

        Args:
            query: SQL query to execute
            params: Query parameters

        Returns:
            Query result

        Raises:
            Exception: If query execution fails
        """
        logger.debug(f"Executing query: {query}")
        return self.execute_query(query, params)

    def execute_query(self, query: str, params: Optional[Union[Tuple, Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """Execute a query and return the results.

        Args:
            query: SQL query to execute
            params: Query parameters

        Returns:
            List of dictionaries containing the query results

        Raises:
            Exception: If query execution fails
        """
        self._ensure_connected()

        try:
            self.cursor.execute(query, params)

            # Check if the query returns results
            if self.cursor.description:
                results = [dict(row) for row in self.cursor.fetchall()]
                logger.debug(f"Query executed successfully, returned {len(results)} rows")
                return results
            else:
                logger.debug("Query executed successfully, no results returned")
                return []
        except Exception as e:
            error_msg = f"Error executing query: {e}"
            logger.error(error_msg)
            if self.connection:
                self.connection.rollback()
            raise

    def execute_batch(self, query: str, params_list: List[Dict[str, Any]]) -> None:
        """Execute a batch of database queries.

        Args:
            query: SQL query to execute
            params_list: List of query parameters

        Raises:
            Exception: If query execution fails
        """
        if not self.connection:
            self.connect()

        try:
            execute_values(self.cursor, query, params_list)
            logger.debug(f"Executed batch query with {len(params_list)} parameter sets")
        except Exception as e:
            logger.error(f"Batch query execution failed: {e}")
            raise

    def fetch_one(self, query: str, params: Optional[Union[Tuple, Dict[str, Any]]] = None) -> Optional[Tuple]:
        """
        Execute a query and fetch one result.

        Args:
            query: SQL query to execute
            params: Parameters for the query

        Returns:
            A single result row or None if no results

        Raises:
            Exception: If query execution fails
        """
        if not self.connection:
            self.connect()

        try:
            self.cursor.execute(query, params)
            result = self.cursor.fetchone()
            logger.debug(f"Executed query and fetched one result: {result}")
            return result
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise

    def commit(self) -> None:
        """Commit the current transaction.

        Raises:
            Exception: If commit fails
        """
        self._ensure_connected()

        try:
            self.connection.commit()
            logger.debug("Transaction committed successfully")
        except Exception as e:
            error_msg = f"Error committing transaction: {e}"
            logger.error(error_msg)
            raise

    def rollback(self) -> None:
        """Rollback the current transaction.

        Raises:
            Exception: If rollback fails
        """
        if not self.connection:
            logger.warning("No active connection to rollback")
            return

        try:
            self.connection.rollback()
            logger.debug("Transaction rolled back successfully")
        except Exception as e:
            error_msg = f"Error rolling back transaction: {e}"
            logger.error(error_msg)
            raise

    def create_tables(self, schema_file: Optional[str] = None) -> None:
        """Create database tables from a schema file or default schema.

        Args:
            schema_file: Path to SQL schema file (optional)

        Raises:
            Exception: If table creation fails
        """
        self._ensure_connected()

        try:
            # Use provided schema file or default schema
            if schema_file and os.path.exists(schema_file):
                logger.info(f"Creating tables from schema file: {schema_file}")
                with open(schema_file, 'r') as f:
                    schema_sql = f.read()
            else:
                logger.info("Creating tables from default schema")
                schema_sql = self._get_default_schema()

            # Execute schema SQL
            self.cursor.execute(schema_sql)
            self.connection.commit()
            logger.info("Tables created successfully")
        except Exception as e:
            error_msg = f"Error creating tables: {e}"
            logger.error(error_msg)
            if self.connection:
                self.connection.rollback()
            raise

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the database.

        Args:
            table_name: Name of the table to check

        Returns:
            True if the table exists, False otherwise
        """
        self._ensure_connected()

        try:
            query = """
                SELECT EXISTS (
                    SELECT FROM information_schema.tables
                    WHERE table_schema = 'public'
                    AND table_name = %s
                );
            """
            self.cursor.execute(query, (table_name,))
            result = self.cursor.fetchone()[0]
            return result
        except Exception as e:
            logger.error(f"Error checking if table exists: {e}")
            return False

    def _ensure_connected(self) -> None:
        """Ensure that we have an active database connection.

        Raises:
            Exception: If not connected to the database
        """
        if not self.connection or not self.cursor:
            error_msg = "Not connected to database"
            logger.error(error_msg)
            raise Exception(error_msg)

    def _get_default_schema(self) -> str:
        """Get the default database schema SQL.

        Returns:
            SQL for creating the default schema
        """
        return """
        -- Create exports table
        CREATE TABLE IF NOT EXISTS exports (
            id SERIAL PRIMARY KEY,
            file_source TEXT,
            export_date TIMESTAMP,
            user_id TEXT,
            user_display_name TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Create conversations table
        CREATE TABLE IF NOT EXISTS conversations (
            id TEXT PRIMARY KEY,
            export_id INTEGER REFERENCES exports(id) ON DELETE CASCADE,
            display_name TEXT,
            type TEXT,
            created_at TIMESTAMP,
            last_message_at TIMESTAMP,
            participant_count INTEGER,
            message_count INTEGER
        );

        -- Create participants table
        CREATE TABLE IF NOT EXISTS participants (
            id SERIAL PRIMARY KEY,
            conversation_id TEXT REFERENCES conversations(id) ON DELETE CASCADE,
            user_id TEXT,
            display_name TEXT,
            is_self BOOLEAN,
            UNIQUE(conversation_id, user_id)
        );

        -- Create messages table
        CREATE TABLE IF NOT EXISTS messages (
            id TEXT PRIMARY KEY,
            conversation_id TEXT REFERENCES conversations(id) ON DELETE CASCADE,
            sender_id TEXT,
            sender_name TEXT,
            timestamp TIMESTAMP,
            content_text TEXT,
            content_html TEXT,
            message_type TEXT,
            is_edited BOOLEAN DEFAULT FALSE
        );

        -- Create message_attachments table
        CREATE TABLE IF NOT EXISTS message_attachments (
            id SERIAL PRIMARY KEY,
            message_id TEXT REFERENCES messages(id) ON DELETE CASCADE,
            type TEXT,
            name TEXT,
            url TEXT,
            content_type TEXT,
            size INTEGER
        );

        -- Create indexes
        CREATE INDEX IF NOT EXISTS idx_conversations_export_id ON conversations(export_id);
        CREATE INDEX IF NOT EXISTS idx_participants_conversation_id ON participants(conversation_id);
        CREATE INDEX IF NOT EXISTS idx_messages_conversation_id ON messages(conversation_id);
        CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp);
        CREATE INDEX IF NOT EXISTS idx_message_attachments_message_id ON message_attachments(message_id);
        """