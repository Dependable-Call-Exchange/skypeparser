"""
Database manager for handling database connections and operations.

This module provides the DatabaseManager class that encapsulates
database connection handling, transaction management, and query execution.
"""

import logging
import os
from typing import Any, Dict, List, Optional, Tuple, Union

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection, cursor
from psycopg2.extras import execute_values

from src.utils.interfaces import DatabaseConnectionProtocol
from src.utils.new_structured_logging import get_logger, log_execution_time, handle_errors

logger = get_logger(__name__)


class DatabaseManager(DatabaseConnectionProtocol):
    """Manages database connections and operations."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        dbname: str = "skypeparser",
        user: str = "postgres",
        password: str = "",
        connection_timeout: int = 30,
        application_name: str = "SkypeParser",
    ):
        """Initialize the database manager.

        Args:
            host: Database host
            port: Database port
            dbname: Database name
            user: Database user
            password: Database password
            connection_timeout: Connection timeout in seconds
            application_name: Application name for connection identification
        """
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password
        self.connection_timeout = connection_timeout
        self.application_name = application_name
        self._conn = None
        self._cursor = None

        logger.info(
            f"Initialized DatabaseManager for {dbname} on {host}:{port}",
            extra={
                "host": host,
                "port": port,
                "dbname": dbname,
                "user": user,
                "connection_timeout": connection_timeout,
                "application_name": application_name,
            }
        )

    @property
    def conn(self) -> connection:
        """Get the database connection, creating it if necessary.

        Returns:
            Database connection
        """
        if self._conn is None or self._conn.closed:
            self._connect()
        return self._conn

    @property
    def cursor(self) -> cursor:
        """Get the database cursor, creating it if necessary.

        Returns:
            Database cursor
        """
        if self._cursor is None or self._cursor.closed:
            self._cursor = self.conn.cursor()
        return self._cursor

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error connecting to database")
    def _connect(self) -> None:
        """Connect to the database."""
        logger.info(f"Connecting to database {self.dbname} on {self.host}:{self.port}")

        # Build connection string
        conn_params = {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "connect_timeout": self.connection_timeout,
            "application_name": self.application_name,
        }

        # Connect to database
        self._conn = psycopg2.connect(**conn_params)
        self._conn.autocommit = False

        logger.info(f"Connected to database {self.dbname}")

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error closing database connection")
    def close(self) -> None:
        """Close the database connection."""
        if self._cursor is not None and not self._cursor.closed:
            self._cursor.close()
            self._cursor = None

        if self._conn is not None and not self._conn.closed:
            self._conn.close()
            self._conn = None

        logger.info("Database connection closed")

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error executing query")
    def execute(self, query: str, params: Optional[Tuple] = None) -> None:
        """Execute a query.

        Args:
            query: SQL query to execute
            params: Query parameters
        """
        logger.debug(f"Executing query: {query}")
        self.cursor.execute(query, params)

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error executing query and fetching results")
    def execute_and_fetch(self, query: str, params: Optional[Tuple] = None) -> List[Tuple]:
        """Execute a query and fetch all results.

        Args:
            query: SQL query to execute
            params: Query parameters

        Returns:
            Query results
        """
        self.execute(query, params)
        return self.cursor.fetchall()

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error executing query and fetching one result")
    def execute_and_fetch_one(self, query: str, params: Optional[Tuple] = None) -> Optional[Tuple]:
        """Execute a query and fetch one result.

        Args:
            query: SQL query to execute
            params: Query parameters

        Returns:
            Query result or None if no result
        """
        self.execute(query, params)
        return self.cursor.fetchone()

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error executing bulk insert")
    def bulk_insert(
        self, table: str, columns: List[str], values: List[Tuple], page_size: int = 1000
    ) -> int:
        """Insert multiple rows into a table.

        Args:
            table: Table name
            columns: Column names
            values: Values to insert
            page_size: Number of rows to insert at once

        Returns:
            Number of rows inserted
        """
        if not values:
            logger.warning(f"No values to insert into {table}")
            return 0

        logger.info(f"Bulk inserting {len(values)} rows into {table}")

        # Build query
        column_names = sql.SQL(', ').join(sql.Identifier(col) for col in columns)
        query = sql.SQL("INSERT INTO {} ({}) VALUES %s").format(
            sql.Identifier(table), column_names
        )

        # Insert in batches
        total_inserted = 0
        for i in range(0, len(values), page_size):
            batch = values[i:i + page_size]
            execute_values(self.cursor, query.as_string(self.conn), batch)
            total_inserted += len(batch)
            logger.debug(f"Inserted batch {i // page_size + 1} ({len(batch)} rows)")

        logger.info(f"Inserted {total_inserted} rows into {table}")
        return total_inserted

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error beginning transaction")
    def begin_transaction(self) -> None:
        """Begin a transaction."""
        logger.debug("Beginning transaction")
        if self.conn.autocommit:
            self.conn.autocommit = False

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error committing transaction")
    def commit(self) -> None:
        """Commit the current transaction."""
        logger.debug("Committing transaction")
        self.conn.commit()

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error rolling back transaction")
    def rollback(self) -> None:
        """Roll back the current transaction."""
        logger.debug("Rolling back transaction")
        self.conn.rollback()

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error checking if table exists")
    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists.

        Args:
            table_name: Table name

        Returns:
            Whether the table exists
        """
        query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name = %s
            )
        """
        result = self.execute_and_fetch_one(query, (table_name,))
        return result[0] if result else False

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error checking if index exists")
    def index_exists(self, index_name: str) -> bool:
        """Check if an index exists.

        Args:
            index_name: Index name

        Returns:
            Whether the index exists
        """
        query = """
            SELECT EXISTS (
                SELECT FROM pg_indexes
                WHERE indexname = %s
            )
        """
        result = self.execute_and_fetch_one(query, (index_name,))
        return result[0] if result else False