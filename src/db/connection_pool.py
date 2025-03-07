#!/usr/bin/env python3
"""
Database Connection Pool

This module provides a connection pool for PostgreSQL database connections,
improving performance by reusing connections instead of creating new ones
for each database operation.
"""

import logging
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple, Union

import psycopg2
from psycopg2 import pool
from psycopg2.extras import DictCursor, execute_batch

from src.utils.error_handling import ErrorContext, handle_errors, report_error
from src.utils.interfaces import DatabaseConnectionProtocol
from src.utils.structured_logging import get_logger, log_execution_time

# Set up logger
logger = get_logger(__name__)


class ConnectionPool:
    """
    A pool of database connections that can be reused.

    This class manages a pool of database connections, improving performance
    by avoiding the overhead of creating new connections for each operation.
    """

    _instance = None

    @classmethod
    def get_instance(
        cls, db_config: Dict[str, Any], min_conn: int = 1, max_conn: int = 5
    ):
        """
        Get or create a singleton instance of the connection pool.

        Args:
            db_config: Database configuration
            min_conn: Minimum number of connections in the pool
            max_conn: Maximum number of connections in the pool

        Returns:
            ConnectionPool instance
        """
        if cls._instance is None:
            cls._instance = cls(db_config, min_conn, max_conn)
        return cls._instance

    def __init__(self, db_config: Dict[str, Any], min_conn: int = 1, max_conn: int = 5):
        """
        Initialize the connection pool.

        Args:
            db_config: Database configuration
            min_conn: Minimum number of connections in the pool
            max_conn: Maximum number of connections in the pool
        """
        with ErrorContext(component="ConnectionPool", operation="initialization"):
            self.db_config = db_config
            self.min_conn = min_conn
            self.max_conn = max_conn

            # Set up connection parameters
            self.conn_params = {
                "dbname": db_config.get("dbname", "skype_archive"),
                "user": db_config.get("user", "postgres"),
                "password": db_config.get("password", ""),
                "host": db_config.get("host", "localhost"),
                "port": db_config.get("port", 5432),
            }

            # Create the connection pool
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=min_conn, maxconn=max_conn, **self.conn_params
            )

            logger.info(
                f"Connection pool initialized with {min_conn}-{max_conn} connections"
            )

    @contextmanager
    def get_connection(self):
        """
        Get a connection from the pool.

        This context manager automatically returns the connection to the pool when done.

        Yields:
            A connection from the pool

        Raises:
            psycopg2.Error: If a database error occurs
        """
        conn = None
        try:
            # Get a connection from the pool
            conn = self.pool.getconn()

            # Set autocommit to False for transaction control
            conn.autocommit = False

            # Yield the connection
            yield conn

        except Exception as e:
            # Log the error and re-raise
            logger.error(f"Error getting connection from pool: {e}")
            raise

        finally:
            # Return the connection to the pool
            if conn:
                self.pool.putconn(conn)

    @contextmanager
    def get_cursor(self, cursor_factory=DictCursor):
        """
        Get a cursor for executing database operations.

        This context manager automatically handles connection and transaction management.

        Args:
            cursor_factory: Factory for creating cursor objects

        Yields:
            A cursor for executing database operations

        Raises:
            psycopg2.Error: If a database error occurs
        """
        with self.get_connection() as conn:
            # Create a cursor
            cursor = conn.cursor(cursor_factory=cursor_factory)

            try:
                # Yield the cursor
                yield cursor

                # Commit the transaction
                conn.commit()

            except Exception as e:
                # Rollback the transaction on error
                conn.rollback()
                logger.error(f"Error executing database operation: {e}")
                raise

            finally:
                # Close the cursor
                cursor.close()

    def close(self):
        """Close all connections in the pool."""
        if self.pool:
            self.pool.closeall()
            logger.info("Closed all connections in the pool")


class PooledDatabaseConnection(DatabaseConnectionProtocol):
    """
    A database connection implementation that uses a connection pool.

    This class implements the DatabaseConnectionProtocol interface and uses
    a connection pool for improved performance.
    """

    def __init__(self, db_config: Dict[str, Any], min_conn: int = 1, max_conn: int = 5):
        """
        Initialize the pooled database connection.

        Args:
            db_config: Database configuration
            min_conn: Minimum number of connections in the pool
            max_conn: Maximum number of connections in the pool
        """
        self.db_config = db_config
        self.pool = ConnectionPool.get_instance(db_config, min_conn, max_conn)
        self.transaction_active = False
        self.cursor = None
        self.conn = None
        logger.info("Pooled database connection initialized")

    @handle_errors(log_level="ERROR", default_message="Error connecting to database")
    def connect(self) -> None:
        """
        Connect to the database.

        In this implementation, this is a no-op since connections are managed by the pool.
        """
        logger.debug("Using connection pool for database operations")

    def close(self) -> None:
        """
        Close the database connection.

        In this implementation, this is a no-op since connections are managed by the pool.
        """
        # If a transaction is active, roll it back
        if self.transaction_active and self.conn:
            self.conn.rollback()
            self.transaction_active = False

        # If a cursor is open, close it
        if self.cursor:
            self.cursor.close()
            self.cursor = None

        # If a connection is allocated, return it to the pool
        if self.conn:
            self.pool.putconn(self.conn)
            self.conn = None

    @log_execution_time(logger)
    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a database query.

        Args:
            query: SQL query to execute
            params: Query parameters

        Returns:
            Query results

        Raises:
            psycopg2.Error: If a database error occurs
        """
        with ErrorContext(
            component="PooledDatabaseConnection", operation="execute", query=query
        ):
            with self.pool.get_cursor() as cursor:
                # Execute the query
                cursor.execute(query, params)

                # Return results for queries that return data
                if cursor.description:
                    return cursor.fetchall()

                # Return rowcount for operations that don't return data
                return cursor.rowcount

    @log_execution_time(logger)
    def execute_batch(self, query: str, params_list: List[Dict[str, Any]]) -> None:
        """
        Execute a batch of queries.

        Args:
            query: SQL query to execute
            params_list: List of parameter dictionaries

        Raises:
            psycopg2.Error: If a database error occurs
        """
        with ErrorContext(
            component="PooledDatabaseConnection", operation="execute_batch", query=query
        ):
            with self.pool.get_cursor() as cursor:
                # Execute the batch
                execute_batch(cursor, query, params_list)

    def begin_transaction(self) -> None:
        """
        Begin a database transaction.

        This method acquires a connection and cursor for a multi-statement transaction.
        """
        # Ensure no transaction is active
        if self.transaction_active:
            raise ValueError("Transaction already active")

        # Get a connection from the pool
        self.conn = self.pool.getconn()
        self.conn.autocommit = False

        # Create a cursor
        self.cursor = self.conn.cursor(cursor_factory=DictCursor)

        # Mark transaction as active
        self.transaction_active = True

        logger.debug("Transaction started")

    def commit_transaction(self) -> None:
        """
        Commit the current transaction.

        Raises:
            ValueError: If no transaction is active
        """
        # Ensure transaction is active
        if not self.transaction_active:
            raise ValueError("No active transaction")

        # Commit the transaction
        self.conn.commit()

        # Clean up
        self.cursor.close()
        self.cursor = None
        self.pool.putconn(self.conn)
        self.conn = None
        self.transaction_active = False

        logger.debug("Transaction committed")

    def rollback_transaction(self) -> None:
        """
        Roll back the current transaction.

        Raises:
            ValueError: If no transaction is active
        """
        # Ensure transaction is active
        if not self.transaction_active:
            raise ValueError("No active transaction")

        # Roll back the transaction
        self.conn.rollback()

        # Clean up
        self.cursor.close()
        self.cursor = None
        self.pool.putconn(self.conn)
        self.conn = None
        self.transaction_active = False

        logger.debug("Transaction rolled back")

    def transaction_execute(
        self, query: str, params: Optional[Dict[str, Any]] = None
    ) -> Any:
        """
        Execute a query within an active transaction.

        Args:
            query: SQL query to execute
            params: Query parameters

        Returns:
            Query results

        Raises:
            ValueError: If no transaction is active
            psycopg2.Error: If a database error occurs
        """
        # Ensure transaction is active
        if not self.transaction_active:
            raise ValueError("No active transaction")

        # Execute the query
        self.cursor.execute(query, params)

        # Return results for queries that return data
        if self.cursor.description:
            return self.cursor.fetchall()

        # Return rowcount for operations that don't return data
        return self.cursor.rowcount
