"""
Unit tests for the service registry.

This module tests the service registry functions for registering services
with the dependency injection container.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock

from src.utils.di import ServiceProvider, get_service_provider
from src.utils.interfaces import (
    ContentExtractorProtocol,
    MessageHandlerFactoryProtocol,
    FileHandlerProtocol,
    DatabaseConnectionProtocol,
    ExtractorProtocol,
    TransformerProtocol,
    LoaderProtocol
)
from src.utils.service_registry import (
    register_core_services,
    register_etl_services,
    register_all_services
)


class TestServiceRegistry(unittest.TestCase):
    """Test the service registry functions."""

    def setUp(self):
        """Set up a fresh service provider for each test."""
        self.provider = ServiceProvider()

        # Create patch for get_service_provider to return our test provider
        self.get_provider_patch = patch('src.utils.service_registry.get_service_provider', return_value=self.provider)
        self.get_provider_mock = self.get_provider_patch.start()

        # Create mocks for the concrete implementations
        self.content_extractor_mock = Mock()
        self.message_handler_factory_mock = Mock()
        self.file_handler_mock = Mock()
        self.database_connection_mock = Mock()
        self.extractor_mock = Mock()
        self.transformer_mock = Mock()
        self.loader_mock = Mock()
        self.etl_context_mock = Mock()

        # Create patches for the concrete implementations
        self.patches = [
            patch('src.utils.service_registry.ContentExtractor', return_value=self.content_extractor_mock),
            patch('src.utils.service_registry.SkypeMessageHandlerFactory', return_value=self.message_handler_factory_mock),
            patch('src.utils.service_registry.FileHandler', return_value=self.file_handler_mock),
            patch('src.utils.service_registry.DatabaseConnection', return_value=self.database_connection_mock),
            patch('src.utils.service_registry.Extractor', return_value=self.extractor_mock),
            patch('src.utils.service_registry.Transformer', return_value=self.transformer_mock),
            patch('src.utils.service_registry.Loader', return_value=self.loader_mock),
            patch('src.utils.service_registry.ETLContext', return_value=self.etl_context_mock)
        ]

        # Start all patches
        for p in self.patches:
            p.start()

    def tearDown(self):
        """Clean up patches."""
        self.get_provider_patch.stop()
        for p in self.patches:
            p.stop()

    def test_register_core_services(self):
        """Test registering core services."""
        # Call the function
        register_core_services()

        # Check that the services were registered
        self.assertIn(ContentExtractorProtocol, self.provider._services)
        self.assertIn(MessageHandlerFactoryProtocol, self.provider._services)
        self.assertIn(FileHandlerProtocol, self.provider._services)

    def test_register_etl_services(self):
        """Test registering ETL services."""
        # Create a mock db_config
        db_config = {'host': 'localhost', 'port': 5432, 'dbname': 'test', 'user': 'test', 'password': 'test'}

        # Call the function
        register_etl_services(db_config, output_dir='/tmp')

        # Check that the services were registered
        self.assertIn(DatabaseConnectionProtocol, self.provider._services)
        self.assertIn(ExtractorProtocol, self.provider._services)
        self.assertIn(TransformerProtocol, self.provider._services)
        self.assertIn(LoaderProtocol, self.provider._services)

        # Check that ETLContext was created with the right parameters
        from src.utils.service_registry import ETLContext
        ETLContext.assert_called_once_with(db_config=db_config, output_dir='/tmp')

    def test_register_all_services(self):
        """Test registering all services."""
        # Create a mock db_config
        db_config = {'host': 'localhost', 'port': 5432, 'dbname': 'test', 'user': 'test', 'password': 'test'}

        # Create spies for the individual registration functions
        with patch('src.utils.service_registry.register_core_services') as register_core_mock:
            with patch('src.utils.service_registry.register_etl_services') as register_etl_mock:
                # Call the function
                register_all_services(db_config, output_dir='/tmp')

                # Check that the individual functions were called
                register_core_mock.assert_called_once()
                register_etl_mock.assert_called_once_with(db_config, '/tmp')

    def test_register_core_services_with_custom_provider(self):
        """Test registering core services with a custom provider."""
        # Create a custom provider
        custom_provider = ServiceProvider()

        # Call the function with the custom provider
        register_core_services(custom_provider)

        # Check that the services were registered with the custom provider
        self.assertIn(ContentExtractorProtocol, custom_provider._services)
        self.assertIn(MessageHandlerFactoryProtocol, custom_provider._services)
        self.assertIn(FileHandlerProtocol, custom_provider._services)

        # Check that the global provider was not used
        self.get_provider_mock.assert_not_called()

    def test_register_etl_services_with_custom_provider(self):
        """Test registering ETL services with a custom provider."""
        # Create a custom provider
        custom_provider = ServiceProvider()

        # Create a mock db_config
        db_config = {'host': 'localhost', 'port': 5432, 'dbname': 'test', 'user': 'test', 'password': 'test'}

        # Call the function with the custom provider
        register_etl_services(db_config, output_dir='/tmp', provider=custom_provider)

        # Check that the services were registered with the custom provider
        self.assertIn(DatabaseConnectionProtocol, custom_provider._services)
        self.assertIn(ExtractorProtocol, custom_provider._services)
        self.assertIn(TransformerProtocol, custom_provider._services)
        self.assertIn(LoaderProtocol, custom_provider._services)

        # Check that the global provider was not used
        self.get_provider_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()