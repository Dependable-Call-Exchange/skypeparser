"""
Database connection factory for creating database connections.

This module provides the DatabaseConnectionFactory class that creates
database connections based on configuration.
"""

import logging
import os
from typing import Dict, Optional, Any

from src.core_utils.interfaces import DatabaseConnectionProtocol
from src.logging.new_structured_logging import get_logger, handle_errors

from src.database_manager import DatabaseManager

logger = get_logger(__name__)


class DatabaseConnectionFactory:
    """Factory for creating database connections."""

    @staticmethod
    @handle_errors(log_level="ERROR", default_message="Error creating database connection")
    def create_connection(
        config: Optional[Dict[str, Any]] = None,
        connection_type: str = "postgres",
    ) -> DatabaseConnectionProtocol:
        """Create a database connection.

        Args:
            config: Database configuration
            connection_type: Type of database connection to create

        Returns:
            Database connection

        Raises:
            ValueError: If the connection type is not supported
        """
        # Use default config if none provided
        if config is None:
            config = DatabaseConnectionFactory._get_default_config()

        # Create connection based on type
        if connection_type.lower() == "postgres":
            return DatabaseConnectionFactory._create_postgres_connection(config)
        elif connection_type.lower() == "sqlite":
            return DatabaseConnectionFactory._create_sqlite_connection(config)
        else:
            raise ValueError(f"Unsupported connection type: {connection_type}")

    @staticmethod
    def _get_default_config() -> Dict[str, Any]:
        """Get default database configuration.

        Returns:
            Default database configuration
        """
        return {
            "host": os.environ.get("DB_HOST", "localhost"),
            "port": int(os.environ.get("DB_PORT", "5432")),
            "dbname": os.environ.get("DB_NAME", "skypeparser"),
            "user": os.environ.get("DB_USER", "postgres"),
            "password": os.environ.get("DB_PASSWORD", ""),
            "connection_timeout": int(os.environ.get("DB_CONNECTION_TIMEOUT", "30")),
            "application_name": os.environ.get("DB_APPLICATION_NAME", "SkypeParser"),
        }

    @staticmethod
    def _create_postgres_connection(config: Dict[str, Any]) -> DatabaseConnectionProtocol:
        """Create a PostgreSQL database connection.

        Args:
            config: Database configuration

        Returns:
            PostgreSQL database connection
        """
        logger.info("Creating PostgreSQL database connection")

        # Extract configuration
        host = config.get("host", "localhost")
        port = config.get("port", 5432)
        dbname = config.get("dbname", "skypeparser")
        user = config.get("user", "postgres")
        password = config.get("password", "")
        connection_timeout = config.get("connection_timeout", 30)
        application_name = config.get("application_name", "SkypeParser")

        # If the DatabaseManager is trying to connect to localhost but we have Supabase config,
        # use the config values directly instead of using localhost
        if host == "localhost" and dbname == "skypeparser":
            # Check if we have a service provider with Supabase config
            try:
                from src.core_utils.di import get_service_provider
                provider = get_service_provider()
                # Try to get the config directly
                if hasattr(provider, "config") and "database" in provider.config:
                    db_config = provider.config["database"]
                    host = db_config.get("host", host)
                    port = db_config.get("port", port)
                    dbname = db_config.get("dbname", dbname)
                    user = db_config.get("user", user)
                    password = db_config.get("password", password)
                    logger.info(f"Using Supabase connection parameters from config: {host}:{port}/{dbname}")
            except Exception as e:
                logger.warning(f"Error getting Supabase config, using defaults: {e}")

        # Create connection
        return DatabaseManager(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            connection_timeout=connection_timeout,
            application_name=application_name,
        )

    @staticmethod
    def _create_sqlite_connection(config: Dict[str, Any]) -> DatabaseConnectionProtocol:
        """Create a SQLite database connection.

        Args:
            config: Database configuration

        Returns:
            SQLite database connection

        Raises:
            NotImplementedError: SQLite support is not implemented yet
        """
        logger.info("Creating SQLite database connection")

        # SQLite support is not implemented yet
        raise NotImplementedError("SQLite support is not implemented yet")