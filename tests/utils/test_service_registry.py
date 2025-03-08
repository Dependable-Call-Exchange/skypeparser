"""
Unit tests for the service registry.

This module tests the service registry functions for registering services
with the dependency injection container.
"""

from unittest.mock import MagicMock, Mock, patch

import pytest

from src.utils.di import ServiceProvider, get_service_provider
from src.utils.interfaces import (
    ContentExtractorProtocol,
    DatabaseConnectionProtocol,
    ExtractorProtocol,
    FileHandlerProtocol,
    LoaderProtocol,
    MessageHandlerFactoryProtocol,
    StructuredDataExtractorProtocol,
    TransformerProtocol,
)
from src.utils.service_registry import (
    register_all_services,
    register_core_services,
    register_etl_services,
)


@pytest.fixture
def service_provider():
    """Set up a fresh service provider for each test."""
    return ServiceProvider()


@pytest.fixture
def mock_services(service_provider):
    """Set up mock services and patches."""
    # Create patch for get_service_provider to return our test provider
    with patch(
        "src.utils.service_registry.get_service_provider", return_value=service_provider
    ) as get_provider_mock:
        # Create mocks for the concrete implementations
        content_extractor_mock = Mock()
        content_extractor_mock.__name__ = "ContentExtractor"

        message_handler_factory_mock = Mock()
        message_handler_factory_mock.__name__ = "SkypeMessageHandlerFactory"

        file_handler_mock = Mock()
        file_handler_mock.__name__ = "FileHandler"

        database_connection_mock = Mock()
        database_connection_mock.__name__ = "DatabaseConnection"

        extractor_mock = Mock()
        extractor_mock.__name__ = "Extractor"

        transformer_mock = Mock()
        transformer_mock.__name__ = "Transformer"

        loader_mock = Mock()
        loader_mock.__name__ = "Loader"

        etl_context_mock = Mock()
        etl_context_mock.__name__ = "ETLContext"

        structured_data_extractor_mock = Mock()
        structured_data_extractor_mock.__name__ = "StructuredDataExtractor"

        # Create patches for the concrete implementations
        patches = [
            patch(
                "src.utils.service_registry.ContentExtractor", content_extractor_mock
            ),
            patch(
                "src.utils.service_registry.SkypeMessageHandlerFactory",
                message_handler_factory_mock,
            ),
            patch("src.utils.service_registry.FileHandler", file_handler_mock),
            patch(
                "src.utils.service_registry.StructuredDataExtractor",
                structured_data_extractor_mock,
            ),
            patch(
                "src.utils.service_registry.DatabaseConnection",
                database_connection_mock,
            ),
            patch("src.utils.service_registry.Extractor", extractor_mock),
            patch("src.utils.service_registry.Transformer", transformer_mock),
            patch("src.utils.service_registry.Loader", loader_mock),
            patch("src.utils.service_registry.ETLContext", etl_context_mock),
        ]

        # Start all patches
        for p in patches:
            p.start()

        yield {
            "provider": service_provider,
            "content_extractor": content_extractor_mock,
            "message_handler_factory": message_handler_factory_mock,
            "file_handler": file_handler_mock,
            "database_connection": database_connection_mock,
            "extractor": extractor_mock,
            "transformer": transformer_mock,
            "loader": loader_mock,
            "etl_context": etl_context_mock,
            "structured_data_extractor": structured_data_extractor_mock,
        }

        # Clean up patches
        for p in patches:
            p.stop()


def test_register_core_services(mock_services):
    """Test registering core services."""
    provider = mock_services["provider"]

    # Call the function
    register_core_services()

    # Check that the services were registered
    assert ContentExtractorProtocol in provider._singletons
    assert MessageHandlerFactoryProtocol in provider._singletons
    assert FileHandlerProtocol in provider._singletons
    assert StructuredDataExtractorProtocol in provider._singletons


def test_register_etl_services(mock_services):
    """Test registering ETL services."""
    provider = mock_services["provider"]
    etl_context_mock = mock_services["etl_context"]

    # Create a mock db_config
    db_config = {
        "host": "localhost",
        "port": 5432,
        "dbname": "test",
        "user": "test",
        "password": "test",
    }

    # Call the function
    register_etl_services(db_config, output_dir="/tmp")

    # Check that the services were registered
    assert DatabaseConnectionProtocol in provider._singletons
    assert ExtractorProtocol in provider._singletons
    assert TransformerProtocol in provider._singletons
    assert LoaderProtocol in provider._singletons

    # Check that ETLContext was created with the right parameters
    etl_context_mock.assert_called_once_with(db_config=db_config, output_dir="/tmp")


def test_register_all_services(mock_services):
    """Test registering all services."""
    # Create a mock db_config
    db_config = {
        "host": "localhost",
        "port": 5432,
        "dbname": "test",
        "user": "test",
        "password": "test",
    }

    # Create spies for the individual registration functions
    with patch(
        "src.utils.service_registry.register_core_services"
    ) as register_core_mock:
        with patch(
            "src.utils.service_registry.register_etl_services"
        ) as register_etl_mock:
            # Call the function
            register_all_services(db_config, output_dir="/tmp")

            # Check that the individual functions were called
            register_core_mock.assert_called_once()

            # Check that register_etl_services was called with the right parameters
            # We need to check the first two parameters explicitly since the third parameter (provider)
            # is passed internally and we don't care about its exact value
            call_args = register_etl_mock.call_args[0]
            assert call_args[0] == db_config
            assert call_args[1] == "/tmp"


def test_register_core_services_with_custom_provider(mock_services):
    """Test registering core services with a custom provider."""
    # Create a custom provider
    custom_provider = ServiceProvider()

    # Call the function with the custom provider
    register_core_services(custom_provider)

    # Check that the services were registered
    assert ContentExtractorProtocol in custom_provider._singletons
    assert MessageHandlerFactoryProtocol in custom_provider._singletons
    assert FileHandlerProtocol in custom_provider._singletons
    assert StructuredDataExtractorProtocol in custom_provider._singletons


def test_register_etl_services_with_custom_provider(mock_services):
    """Test registering ETL services with a custom provider."""
    etl_context_mock = mock_services["etl_context"]

    # Create a custom provider
    custom_provider = ServiceProvider()

    # Create a mock db_config
    db_config = {
        "host": "localhost",
        "port": 5432,
        "dbname": "test",
        "user": "test",
        "password": "test",
    }

    # Call the function with the custom provider
    register_etl_services(db_config, output_dir="/tmp", provider=custom_provider)

    # Check that the services were registered
    assert DatabaseConnectionProtocol in custom_provider._singletons
    assert ExtractorProtocol in custom_provider._singletons
    assert TransformerProtocol in custom_provider._singletons
    assert LoaderProtocol in custom_provider._singletons

    # Check that ETLContext was created with the right parameters
    etl_context_mock.assert_called_with(db_config=db_config, output_dir="/tmp")
