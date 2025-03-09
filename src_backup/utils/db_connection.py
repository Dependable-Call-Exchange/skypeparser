#!/usr/bin/env python3
"""
Database Connection Module

This module provides a database connection class that implements the DatabaseConnectionProtocol
interface for interacting with PostgreSQL databases.
"""

import logging
from typing import Dict, Any, List, Tuple, Optional, Union
import contextlib

from src.utils.interfaces import DatabaseConnectionProtocol
from src.utils.dependencies import get_psycopg2
from src.utils.validation import validate_db_config

# Set up logging
logger = logging.getLogger(__name__)


class DatabaseConnection(DatabaseConnectionProtocol):
    """
    Database connection class for PostgreSQL databases.

    This class implements the DatabaseConnectionProtocol interface and provides
    methods for connecting to and interacting with PostgreSQL databases.
    """

    def __init__(self, db_config: Dict[str, Any]):
        """
        Initialize the database connection.

        Args:
            db_config: Database configuration dictionary with host, database, user, password

        Raises:
            ValueError: If the database configuration is invalid
        """
        # Validate the database configuration
        validate_db_config(db_config)

        # Store the configuration
        self.db_config = db_config

        # Get the psycopg2 module
        self.psycopg2 = get_psycopg2()

        # Connection and cursor
        self._connection = None
        self._cursor = None

        logger.debug("DatabaseConnection initialized with config")

    def connect(self) -> None:
        """
        Connect to the database.

        Raises:
            Exception: If connection fails
        """
        if self._connection is not None and not self._connection.closed:
            logger.debug("Already connected to database")
            return

        try:
            # Connect to the database
            self._connection = self.psycopg2.connect(
                host=self.db_config['host'],
                database=self.db_config['database'],
                user=self.db_config['user'],
                password=self.db_config['password'],
                port=self.db_config.get('port', 5432)
            )

            # Create a cursor
            self._cursor = self._connection.cursor()

            logger.info(f"Connected to database {self.db_config['database']} on {self.db_config['host']}")
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise

    def disconnect(self) -> None:
        """
        Disconnect from the database.
        """
        if self._cursor is not None:
            self._cursor.close()
            self._cursor = None

        if self._connection is not None:
            self._connection.close()
            self._connection = None

        logger.info("Disconnected from database")

    def execute(self, query: str, params: Optional[Union[Tuple, Dict[str, Any]]] = None) -> None:
        """
        Execute a query without returning results.

        Args:
            query: SQL query to execute
            params: Parameters for the query

        Raises:
            Exception: If query execution fails
        """
        self._ensure_connected()

        try:
            self._cursor.execute(query, params)
            self._connection.commit()
            logger.debug(f"Executed query: {query[:100]}...")
        except Exception as e:
            if self._connection:
                self._connection.rollback()
            logger.error(f"Query execution failed: {e}")
            raise

    def execute_many(self, query: str, params_list: List[Union[Tuple, Dict[str, Any]]]) -> None:
        """
        Execute a query multiple times with different parameters.

        Args:
            query: SQL query to execute
            params_list: List of parameter sets for the query

        Raises:
            Exception: If query execution fails
        """
        self._ensure_connected()

        try:
            self._cursor.executemany(query, params_list)
            self._connection.commit()
            logger.debug(f"Executed query {len(params_list)} times: {query[:100]}...")
        except Exception as e:
            if self._connection:
                self._connection.rollback()
            logger.error(f"Query execution failed: {e}")
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
        self._ensure_connected()

        try:
            self._cursor.execute(query, params)
            result = self._cursor.fetchone()
            logger.debug(f"Fetched one result for query: {query[:100]}...")
            return result
        except Exception as e:
            if self._connection:
                self._connection.rollback()
            logger.error(f"Query execution failed: {e}")
            raise

    def fetch_all(self, query: str, params: Optional[Union[Tuple, Dict[str, Any]]] = None) -> List[Tuple]:
        """
        Execute a query and fetch all results.

        Args:
            query: SQL query to execute
            params: Parameters for the query

        Returns:
            List of result rows

        Raises:
            Exception: If query execution fails
        """
        self._ensure_connected()

        try:
            self._cursor.execute(query, params)
            results = self._cursor.fetchall()
            logger.debug(f"Fetched {len(results)} results for query: {query[:100]}...")
            return results
        except Exception as e:
            if self._connection:
                self._connection.rollback()
            logger.error(f"Query execution failed: {e}")
            raise

    def transaction(self):
        """
        Create a transaction context manager.

        Returns:
            A context manager for a database transaction
        """
        return self._Transaction(self)

    def _ensure_connected(self) -> None:
        """
        Ensure that we are connected to the database.

        Raises:
            Exception: If connection fails
        """
        if self._connection is None or self._connection.closed:
            self.connect()

    class _Transaction:
        """
        Context manager for database transactions.
        """

        def __init__(self, db_connection):
            """
            Initialize the transaction.

            Args:
                db_connection: DatabaseConnection instance
            """
            self.db_connection = db_connection

        def __enter__(self):
            """
            Enter the transaction context.

            Returns:
                The database connection
            """
            self.db_connection._ensure_connected()
            logger.debug("Starting database transaction")
            return self.db_connection

        def __exit__(self, exc_type, exc_val, exc_tb):
            """
            Exit the transaction context.

            Args:
                exc_type: Exception type if an exception was raised
                exc_val: Exception value if an exception was raised
                exc_tb: Exception traceback if an exception was raised
            """
            if exc_type is not None:
                # An exception occurred, roll back the transaction
                if self.db_connection._connection:
                    self.db_connection._connection.rollback()
                logger.debug("Rolling back database transaction due to exception")
            else:
                # No exception, commit the transaction
                if self.db_connection._connection:
                    self.db_connection._connection.commit()
                logger.debug("Committing database transaction")


# Factory function for creating database connections
def create_database_connection(db_config: Dict[str, Any]) -> DatabaseConnection:
    """
    Create a new database connection.

    Args:
        db_config: Database configuration dictionary

    Returns:
        A new DatabaseConnection instance
    """
    return DatabaseConnection(db_config)