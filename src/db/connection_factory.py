#!/usr/bin/env python3
"""
Database Connection Factory

This module provides a factory for creating and managing database connections,
including connection pooling for improved performance.
"""

import logging
import os
from typing import Any, Dict, Optional

from src.utils.error_handling import ErrorContext, handle_errors
from src.utils.interfaces import DatabaseConnectionProtocol
from src.utils.structured_logging import get_logger

# Set up logger
logger = get_logger(__name__)


class DatabaseConnectionFactory:
    """
    Factory for creating and managing database connections.

    This factory supports different connection types, including direct
    connections and pooled connections for better performance.
    """

    def __init__(self):
        """Initialize the database connection factory."""
        self.conn_types = {
            "direct": self._create_direct_connection,
            "pooled": self._create_pooled_connection,
        }

        # Set the default connection type
        self.default_conn_type = "pooled"

        logger.info("Database connection factory initialized")

    @handle_errors(
        log_level="ERROR", default_message="Error creating database connection"
    )
    def create_connection(
        self, db_config: Dict[str, Any], conn_type: Optional[str] = None, **kwargs
    ) -> DatabaseConnectionProtocol:
        """
        Create a database connection.

        Args:
            db_config: Database configuration
            conn_type: Type of connection to create ("direct" or "pooled")
            **kwargs: Additional parameters for the connection

        Returns:
            Database connection instance

        Raises:
            ValueError: If the connection type is invalid
        """
        # Use default connection type if not specified
        if conn_type is None:
            conn_type = self.default_conn_type

        # Get the connection factory function
        if conn_type not in self.conn_types:
            raise ValueError(f"Invalid connection type: {conn_type}")

        conn_factory = self.conn_types[conn_type]

        # Create and return the connection
        with ErrorContext(
            component="DatabaseConnectionFactory",
            operation="create_connection",
            conn_type=conn_type,
        ):
            return conn_factory(db_config, **kwargs)

    def _create_direct_connection(
        self, db_config: Dict[str, Any], **kwargs
    ) -> DatabaseConnectionProtocol:
        """
        Create a direct database connection.

        Args:
            db_config: Database configuration
            **kwargs: Additional parameters for the connection

        Returns:
            Direct database connection
        """
        from src.db.connection import DatabaseConnection

        return DatabaseConnection(
            dbname=db_config.get("dbname", "skype_archive"),
            user=db_config.get("user", "postgres"),
            password=db_config.get("password", ""),
            host=db_config.get("host", "localhost"),
            port=db_config.get("port", 5432),
        )

    def _create_pooled_connection(
        self, db_config: Dict[str, Any], min_conn: int = 1, max_conn: int = 5, **kwargs
    ) -> DatabaseConnectionProtocol:
        """
        Create a pooled database connection.

        Args:
            db_config: Database configuration
            min_conn: Minimum number of connections in the pool
            max_conn: Maximum number of connections in the pool
            **kwargs: Additional parameters for the connection

        Returns:
            Pooled database connection
        """
        from src.db.connection_pool import PooledDatabaseConnection

        # Allow kwargs to override min_conn and max_conn
        min_conn = kwargs.get("min_conn", min_conn)
        max_conn = kwargs.get("max_conn", max_conn)

        return PooledDatabaseConnection(
            db_config=db_config, min_conn=min_conn, max_conn=max_conn
        )


# Singleton instance
_connection_factory = None


def get_connection_factory() -> DatabaseConnectionFactory:
    """
    Get the singleton database connection factory instance.

    Returns:
        DatabaseConnectionFactory instance
    """
    global _connection_factory
    if _connection_factory is None:
        _connection_factory = DatabaseConnectionFactory()
    return _connection_factory


def create_db_connection(
    db_config: Dict[str, Any], conn_type: Optional[str] = None, **kwargs
) -> DatabaseConnectionProtocol:
    """
    Create a database connection using the factory.

    Args:
        db_config: Database configuration
        conn_type: Type of connection to create
        **kwargs: Additional parameters for the connection

    Returns:
        Database connection instance
    """
    factory = get_connection_factory()
    return factory.create_connection(db_config, conn_type, **kwargs)


# Register the connection factory with the dependency injection system
def register_with_di():
    """Register the connection factory with the dependency injection system."""
    from src.utils.di import get_service_provider
    from src.utils.interfaces import DatabaseConnectionProtocol

    # Get the service provider
    service_provider = get_service_provider()

    # Register a factory function for creating database connections
    def db_connection_factory():
        """Factory function for creating database connections."""
        # Get database configuration from environment or default
        db_config = {
            "dbname": os.environ.get("POSTGRES_DB", "skype_archive"),
            "user": os.environ.get("POSTGRES_USER", "postgres"),
            "password": os.environ.get("POSTGRES_PASSWORD", ""),
            "host": os.environ.get("POSTGRES_HOST", "localhost"),
            "port": int(os.environ.get("POSTGRES_PORT", "5432")),
        }

        # Create and return the connection
        return create_db_connection(db_config)

    # Register the factory function
    service_provider.register_factory(DatabaseConnectionProtocol, db_connection_factory)

    logger.info("Registered database connection factory with DI system")


# Automatically register with DI on import
register_with_di()
