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
    StructuredDataExtractorProtocol
)

# Import concrete implementations
from src.parser.content_extractor import ContentExtractor
from src.utils.message_type_handlers import get_handler_for_message_type, SkypeMessageHandlerFactory
from src.utils.file_handler import FileHandler
from src.utils.structured_data_extractor import StructuredDataExtractor
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

    # Register message handler factory
    provider.register_factory(MessageHandlerFactoryProtocol,
                             lambda: get_handler_for_message_type)

    # Register file handler
    provider.register_singleton_class(FileHandlerProtocol, FileHandler)

    # Register structured data extractor
    provider.register_singleton_class(StructuredDataExtractorProtocol, StructuredDataExtractor)

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

    # Create and register database connection
    db_connection = DatabaseConnection(db_config)
    provider.register_singleton(DatabaseConnectionProtocol, db_connection)

    logger.info("Registered database connection")
    return provider


def register_etl_services(
    db_config: Dict[str, Any],
    output_dir: Optional[str] = None,
    provider: Optional[ServiceProvider] = None
) -> ServiceProvider:
    """
    Register ETL pipeline services with the dependency injection container.

    Args:
        db_config: Database configuration
        output_dir: Optional output directory
        provider: Optional service provider to use. If None, uses the global provider.

    Returns:
        The service provider with registered services
    """
    # Get the service provider
    provider = provider or get_service_provider()

    # Register database connection first
    register_database_connection(db_config, provider)

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