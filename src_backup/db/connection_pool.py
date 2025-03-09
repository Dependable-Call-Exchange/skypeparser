#!/usr/bin/env python3
"""
Database Connection Pool

This module provides a connection pool for PostgreSQL database connections,
improving performance by reusing connections instead of creating new ones
for each database operation.
"""

import logging
import time
import threading
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple, Union

import psycopg2
from psycopg2 import pool
from psycopg2.extras import DictCursor, execute_batch

from src.utils.error_handling import ErrorContext, handle_errors, report_error
from src.utils.interfaces import DatabaseConnectionProtocol, ConnectionPoolProtocol
from src.utils.structured_logging import get_logger, log_execution_time

# Set up logger
logger = get_logger(__name__)


class PostgresConnectionPool(ConnectionPoolProtocol):
    """
    A connection pool for PostgreSQL database connections.

    This class provides a thread-safe connection pool that manages
    database connections efficiently, reducing the overhead of
    creating new connections for each operation.
    """

    def __init__(
        self,
        db_config: Dict[str, Any],
        min_connections: int = 2,
        max_connections: int = 10,
        connection_timeout: float = 30.0,
        idle_timeout: float = 300.0,
        max_age: float = 1800.0,
    ):
        """
        Initialize the connection pool.

        Args:
            db_config: Database configuration dictionary
            min_connections: Minimum number of connections to keep in the pool
            max_connections: Maximum number of connections allowed in the pool
            connection_timeout: Timeout in seconds when acquiring a connection
            idle_timeout: Time in seconds after which idle connections are closed
            max_age: Maximum age of a connection in seconds before it's recycled
        """
        self.db_config = db_config
        self.min_connections = min_connections
        self.max_connections = max_connections
        self.connection_timeout = connection_timeout
        self.idle_timeout = idle_timeout
        self.max_age = max_age

        # Connection tracking
        self.pool = None
        self.lock = threading.RLock()
        self.connection_timestamps = {}
        self.in_use_connections = set()

        # Initialize the pool
        self._initialize_pool()

        # Start maintenance thread
        self._start_maintenance_thread()

        logger.info(f"Initialized PostgreSQL connection pool with {min_connections}-{max_connections} connections")

    def _initialize_pool(self) -> None:
        """Initialize the connection pool."""
        try:
            # Create the connection pool
            self.pool = pool.ThreadedConnectionPool(
                minconn=self.min_connections,
                maxconn=self.max_connections,
                **self.db_config
            )

            # Initialize connection timestamps
            current_time = time.time()
            for _ in range(self.min_connections):
                conn = self.pool.getconn()
                self.connection_timestamps[id(conn)] = current_time
                self.pool.putconn(conn)

            logger.info(f"Connection pool initialized with {self.min_connections} connections")
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise

    def _start_maintenance_thread(self) -> None:
        """Start a background thread for pool maintenance."""
        maintenance_thread = threading.Thread(
            target=self._maintenance_worker,
            daemon=True,
            name="ConnectionPoolMaintenance"
        )
        maintenance_thread.start()
        logger.debug("Connection pool maintenance thread started")

    def _maintenance_worker(self) -> None:
        """Background worker that performs pool maintenance."""
        while True:
            try:
                # Sleep for a while before performing maintenance
                time.sleep(60)  # Check every minute

                with self.lock:
                    self._cleanup_idle_connections()
                    self._recycle_old_connections()
            except Exception as e:
                logger.error(f"Error in connection pool maintenance: {e}")

    def _cleanup_idle_connections(self) -> None:
        """Close idle connections that exceed the idle timeout."""
        if not self.pool:
            return

        current_time = time.time()
        connections_to_close = []

        # Identify idle connections to close
        for conn_id, timestamp in list(self.connection_timestamps.items()):
            if (conn_id not in self.in_use_connections and
                current_time - timestamp > self.idle_timeout and
                len(self.connection_timestamps) > self.min_connections):
                connections_to_close.append(conn_id)

        # Close identified connections
        for conn_id in connections_to_close:
            try:
                # Find the connection object
                for conn in self.pool._pool:
                    if id(conn) == conn_id:
                        self.pool._pool.remove(conn)
                        conn.close()
                        del self.connection_timestamps[conn_id]
                        logger.debug(f"Closed idle connection (idle for {current_time - timestamp:.1f}s)")
                        break
            except Exception as e:
                logger.warning(f"Error closing idle connection: {e}")

    def _recycle_old_connections(self) -> None:
        """Recycle connections that exceed the maximum age."""
        if not self.pool:
            return

        current_time = time.time()
        connections_to_recycle = []

        # Identify old connections to recycle
        for conn_id, timestamp in list(self.connection_timestamps.items()):
            if (conn_id not in self.in_use_connections and
                current_time - timestamp > self.max_age):
                connections_to_recycle.append(conn_id)

        # Recycle identified connections
        for conn_id in connections_to_recycle:
            try:
                # Find the connection object
                for conn in self.pool._pool:
                    if id(conn) == conn_id:
                        self.pool._pool.remove(conn)
                        conn.close()

                        # Create a new connection
                        new_conn = psycopg2.connect(**self.db_config)
                        self.pool._pool.append(new_conn)
                        self.connection_timestamps[id(new_conn)] = current_time

                        del self.connection_timestamps[conn_id]
                        logger.debug(f"Recycled old connection (age: {current_time - timestamp:.1f}s)")
                        break
            except Exception as e:
                logger.warning(f"Error recycling old connection: {e}")

    def get_connection(self) -> Tuple[Any, DictCursor]:
        """
        Get a connection from the pool.

        Returns:
            A tuple containing (connection, cursor)

        Raises:
            Exception: If unable to get a connection from the pool
        """
        if not self.pool:
            self._initialize_pool()

        start_time = time.time()
        conn = None

        try:
            # Try to get a connection with timeout
            while time.time() - start_time < self.connection_timeout:
                try:
                    with self.lock:
                        conn = self.pool.getconn()
                        self.in_use_connections.add(id(conn))
                        self.connection_timestamps[id(conn)] = time.time()
                        break
                except pool.PoolError:
                    # Pool is exhausted, wait and retry
                    time.sleep(0.1)

            if conn is None:
                raise Exception(f"Timed out waiting for database connection after {self.connection_timeout}s")

            # Create a cursor
            cursor = conn.cursor(cursor_factory=DictCursor)

            logger.debug(f"Acquired connection from pool (waited {time.time() - start_time:.3f}s)")
            return conn, cursor

        except Exception as e:
            # If we got a connection but failed to create a cursor, return it to the pool
            if conn:
                with self.lock:
                    self.in_use_connections.remove(id(conn))
                    self.pool.putconn(conn)

            logger.error(f"Failed to get connection from pool: {e}")
            raise

    def release_connection(self, conn: Any, cursor: Any) -> None:
        """
        Release a connection back to the pool.

        Args:
            conn: The connection to release
            cursor: The cursor to close
        """
        if not conn:
            return

        try:
            # Close the cursor
            if cursor:
                cursor.close()

            with self.lock:
                # Update the timestamp
                self.connection_timestamps[id(conn)] = time.time()

                # Remove from in-use set
                if id(conn) in self.in_use_connections:
                    self.in_use_connections.remove(id(conn))

                # Return to the pool
                self.pool.putconn(conn)

            logger.debug("Released connection back to pool")
        except Exception as e:
            logger.error(f"Error releasing connection: {e}")

            # Try to close the connection if it can't be returned to the pool
            try:
                conn.close()
            except:
                pass

    def close_all(self) -> None:
        """Close all connections in the pool."""
        if not self.pool:
            return

        logger.info("Closing all connections in the pool")

        with self.lock:
            try:
                self.pool.closeall()
                self.connection_timestamps.clear()
                self.in_use_connections.clear()
                self.pool = None
                logger.info("All connections closed")
            except Exception as e:
                logger.error(f"Error closing connections: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the connection pool.

        Returns:
            Dictionary with pool statistics
        """
        with self.lock:
            return {
                "min_connections": self.min_connections,
                "max_connections": self.max_connections,
                "current_connections": len(self.connection_timestamps) if self.pool else 0,
                "in_use_connections": len(self.in_use_connections) if self.pool else 0,
                "available_connections": (len(self.connection_timestamps) - len(self.in_use_connections)) if self.pool else 0,
            }


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
        self.pool = PostgresConnectionPool(db_config, min_conn, max_conn)
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
            self.pool.release_connection(self.conn, self.cursor)
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
            with self.pool.get_connection()[1] as cursor:
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
            with self.pool.get_connection()[1] as cursor:
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
        self.conn, self.cursor = self.pool.get_connection()
        self.conn.autocommit = False

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
        self.pool.release_connection(self.conn, self.cursor)
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
        self.pool.release_connection(self.conn, self.cursor)
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
