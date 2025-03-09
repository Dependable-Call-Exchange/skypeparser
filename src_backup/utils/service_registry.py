#!/usr/bin/env python3
"""
Service Registry

This module provides functions for registering services with the dependency
injection container. It centralizes service registration to ensure consistent
configuration across the application.
"""

import logging
from typing import Dict, Any, Optional

from src.utils.di import get_service_provider, ServiceProvider
from src.utils.interfaces import (
    ContentExtractorProtocol,
    MessageHandlerFactoryProtocol,
    FileHandlerProtocol,
    DatabaseConnectionProtocol,
    ExtractorProtocol,
    TransformerProtocol,
    LoaderProtocol,
    StructuredDataExtractorProtocol,
    ValidationServiceProtocol
)

# Import concrete implementations
from src.parser.content_extractor import ContentExtractor
from src.utils.message_type_handlers import SkypeMessageHandlerFactory
from src.utils.file_handler import FileHandler
from src.utils.structured_data_extractor import StructuredDataExtractor
from src.utils.validation import ValidationService
from src.db.connection import DatabaseConnection
from src.db.etl.extractor import Extractor
from src.db.etl.transformer import Transformer
from src.db.etl.loader import Loader
from src.db.etl.context import ETLContext

# Set up logging
logger = logging.getLogger(__name__)


def register_core_services(provider: Optional[ServiceProvider] = None) -> ServiceProvider:
    """
    Register core services with the dependency injection container.

    Args:
        provider: Optional service provider to use. If None, uses the global provider.

    Returns:
        The service provider with registered services
    """
    # Get the service provider
    provider = provider or get_service_provider()

    # Register content extractor
    provider.register_singleton_class(ContentExtractorProtocol, ContentExtractor)
    # Also register by string name for consistency
    content_extractor = ContentExtractor()
    provider.register_singleton("content_extractor", content_extractor)

    # Register message handler factory
    provider.register_singleton_class(MessageHandlerFactoryProtocol, SkypeMessageHandlerFactory)
    # Also register by string name for consistency
    message_handler_factory = SkypeMessageHandlerFactory()
    provider.register_singleton("message_handler_factory", message_handler_factory)

    # Register file handler
    provider.register_singleton_class(FileHandlerProtocol, FileHandler)
    # Also register file_handler by name for backward compatibility
    file_handler = FileHandler()
    provider.register_singleton("file_handler", file_handler)

    # Register structured data extractor
    provider.register_singleton_class(StructuredDataExtractorProtocol, StructuredDataExtractor)
    # Also register by string name for consistency
    structured_data_extractor = StructuredDataExtractor()
    provider.register_singleton("structured_data_extractor", structured_data_extractor)

    # Register validation service
    provider.register_singleton_class(ValidationServiceProtocol, ValidationService)
    # Also register validation_service by name for backward compatibility
    validation_service = ValidationService()
    provider.register_singleton("validation_service", validation_service)

    logger.info("Registered core services")
    return provider


def register_database_connection(
    db_config: Dict[str, Any],
    provider: Optional[ServiceProvider] = None
) -> ServiceProvider:
    """
    Register database connection with the dependency injection container.

    Args:
        db_config: Database configuration
        provider: Optional service provider to use. If None, uses the global provider.

    Returns:
        The service provider with registered database connection
    """
    # Get the service provider
    provider = provider or get_service_provider()

    # Check if we should use connection pooling
    use_pooling = True

    try:
        # Import the pooled connection implementation
        from src.db.connection_pool import PooledDatabaseConnection

        # Create and register pooled database connection for better performance
        db_connection = PooledDatabaseConnection(
            db_config,
            min_conn=2,  # Maintain at least 2 connections
            max_conn=10  # Allow up to 10 connections during peak loads
        )
        logger.info("Using pooled database connection for improved performance")
    except (ImportError, ModuleNotFoundError):
        # Fall back to regular connection if pooling not available
        from src.db.connection import DatabaseConnection
        db_connection = DatabaseConnection(db_config)
        logger.info("Using standard database connection (connection pooling not available)")

    # Register the connection
    provider.register_singleton(DatabaseConnectionProtocol, db_connection)

    logger.info("Registered database connection")
    return provider


def register_etl_services(
    db_config: Dict[str, Any],
    output_dir: Optional[str] = None,
    provider: Optional[ServiceProvider] = None
) -> ServiceProvider:
    """
    Register ETL services with the dependency injection container.

    Args:
        db_config: Database configuration
        output_dir: Output directory for ETL results
        provider: Optional service provider to use. If None, uses the global provider.

    Returns:
        The service provider with registered ETL services
    """
    # Get the service provider
    provider = provider or get_service_provider()

    # Store the configuration in the service provider for later use
    if not hasattr(provider, 'config'):
        provider.config = {}
    provider.config['database'] = db_config

    # Register core services
    register_core_services(provider=provider)

    # Register database connection
    register_database_connection(db_config, provider=provider)

    # Create ETL context
    context = ETLContext(db_config=db_config, output_dir=output_dir)
    provider.register_singleton(ETLContext, context)

    # Register ETL components
    provider.register_singleton_class(ExtractorProtocol, Extractor)
    provider.register_singleton_class(TransformerProtocol, Transformer)
    provider.register_singleton_class(LoaderProtocol, Loader)

    logger.info("Registered ETL services")
    return provider


def register_all_services(
    db_config: Dict[str, Any],
    output_dir: Optional[str] = None,
    provider: Optional[ServiceProvider] = None
) -> ServiceProvider:
    """
    Register all services with the dependency injection container.

    Args:
        db_config: Database configuration
        output_dir: Optional output directory
        provider: Optional service provider to use. If None, uses the global provider.

    Returns:
        The service provider with registered services
    """
    # Get the service provider
    provider = provider or get_service_provider()

    # Register core services
    register_core_services(provider)

    # Register ETL services (which includes database connection)
    register_etl_services(db_config, output_dir, provider)

    logger.info("Registered all services")
    return provider