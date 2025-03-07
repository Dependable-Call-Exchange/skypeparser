#!/usr/bin/env python3
"""
Tests for DI-specific service resolution with ETL components.

This module tests that the dependency injection framework correctly resolves
services for the ETL pipeline components.
"""

import unittest
from unittest.mock import patch, Mock
import os
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.utils.di import get_service_provider, get_service
from src.utils.service_registry import register_all_services
from src.utils.interfaces import (
    ContentExtractorProtocol,
    MessageHandlerFactoryProtocol,
    FileHandlerProtocol,
    DatabaseConnectionProtocol,
    ExtractorProtocol,
    TransformerProtocol,
    LoaderProtocol
)
from src.db.etl.context import ETLContext


class TestETLDIIntegration(unittest.TestCase):
    """Test DI-specific service resolution with ETL components."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a mock database configuration
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password'
        }

        # Create a temporary output directory
        self.output_dir = os.path.join(os.path.dirname(__file__), 'test_output')
        os.makedirs(self.output_dir, exist_ok=True)

        # Get a clean service provider
        self.provider = get_service_provider()

        # Clear any existing registrations
        self._clear_service_provider()

    def tearDown(self):
        """Clean up test fixtures."""
        # Clear service provider
        self._clear_service_provider()

        # Remove temporary directory
        if os.path.exists(self.output_dir):
            import shutil
            shutil.rmtree(self.output_dir)

    def _clear_service_provider(self):
        """Clear the service provider."""
        provider = get_service_provider()
        if hasattr(provider, '_singletons'):
            provider._singletons.clear()
        if hasattr(provider, '_transients'):
            provider._transients.clear()
        if hasattr(provider, '_factories'):
            provider._factories.clear()
        if hasattr(provider, '_services'):
            provider._services.clear()

    def test_register_and_resolve_all_services(self):
        """Test registering and resolving all services."""
        # Register all services
        register_all_services(self.db_config, output_dir=self.output_dir)

        # Resolve each service and verify it's the correct type
        content_extractor = get_service(ContentExtractorProtocol)
        self.assertIsNotNone(content_extractor)

        message_handler_factory = get_service(MessageHandlerFactoryProtocol)
        self.assertIsNotNone(message_handler_factory)

        file_handler = get_service(FileHandlerProtocol)
        self.assertIsNotNone(file_handler)

        db_connection = get_service(DatabaseConnectionProtocol)
        self.assertIsNotNone(db_connection)

        extractor = get_service(ExtractorProtocol)
        self.assertIsNotNone(extractor)

        transformer = get_service(TransformerProtocol)
        self.assertIsNotNone(transformer)

        loader = get_service(LoaderProtocol)
        self.assertIsNotNone(loader)

        # Verify that the ETL context is registered and accessible
        context = get_service(ETLContext)
        self.assertIsNotNone(context)

        # Verify that the context has the correct configuration
        self.assertEqual(context.db_config, self.db_config)
        self.assertEqual(context.output_dir, self.output_dir)

    def test_service_singleton_behavior(self):
        """Test that services are singletons."""
        # Register all services
        register_all_services(self.db_config, output_dir=self.output_dir)

        # Get services twice
        content_extractor1 = get_service(ContentExtractorProtocol)
        content_extractor2 = get_service(ContentExtractorProtocol)

        # Verify they are the same instance
        self.assertIs(content_extractor1, content_extractor2)

        # Do the same for other services
        db_connection1 = get_service(DatabaseConnectionProtocol)
        db_connection2 = get_service(DatabaseConnectionProtocol)
        self.assertIs(db_connection1, db_connection2)

        extractor1 = get_service(ExtractorProtocol)
        extractor2 = get_service(ExtractorProtocol)
        self.assertIs(extractor1, extractor2)

        transformer1 = get_service(TransformerProtocol)
        transformer2 = get_service(TransformerProtocol)
        self.assertIs(transformer1, transformer2)

        loader1 = get_service(LoaderProtocol)
        loader2 = get_service(LoaderProtocol)
        self.assertIs(loader1, loader2)

    def test_etl_component_dependencies(self):
        """Test that ETL components have their dependencies injected."""
        # Register all services
        register_all_services(self.db_config, output_dir=self.output_dir)

        # Get the ETL components
        extractor = get_service(ExtractorProtocol)
        transformer = get_service(TransformerProtocol)
        loader = get_service(LoaderProtocol)

        # Verify that they have their dependencies
        self.assertIsNotNone(extractor.context)
        self.assertIsNotNone(transformer.context)
        self.assertIsNotNone(loader.context)

        # Verify that they share the same context
        self.assertIs(extractor.context, transformer.context)
        self.assertIs(transformer.context, loader.context)

        # Verify that the transformer has a content extractor
        self.assertIsNotNone(getattr(transformer, 'content_extractor', None))

        # Verify that the loader has a database connection
        self.assertIsNotNone(getattr(loader, 'db_connection', None))

    def test_message_handler_factory_resolution(self):
        """Test that the message handler factory is correctly resolved and works."""
        # Register all services
        register_all_services(self.db_config, output_dir=self.output_dir)

        # Get the message handler factory
        factory = get_service(MessageHandlerFactoryProtocol)

        # Verify that it can get handlers for different message types
        poll_handler = factory.get_handler('Poll')
        self.assertIsNotNone(poll_handler)
        self.assertTrue(poll_handler.can_handle('Poll'))

        media_handler = factory.get_handler('RichText/Media_Video')
        self.assertIsNotNone(media_handler)
        self.assertTrue(media_handler.can_handle('RichText/Media_Video'))

        # Verify that it returns None or a fallback handler for unknown types
        unknown_handler = factory.get_handler('Unknown')
        # Either None or a fallback handler that can handle 'Unknown'
        if unknown_handler is not None:
            self.assertTrue(unknown_handler.can_handle('Unknown'))


if __name__ == '__main__':
    unittest.main()