#!/usr/bin/env python3
"""
Unit tests for the ETL Pipeline with dependency injection.

These tests validate the ETL pipeline orchestration with dependency injection,
ensuring that the pipeline correctly uses injected dependencies.
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock, Mock
import tempfile
import json

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl.pipeline_manager import ETLPipeline
from src.utils.interfaces import (
    ExtractorProtocol,
    TransformerProtocol,
    LoaderProtocol,
    DatabaseConnectionProtocol
)
from src.db.etl.context import ETLContext

class TestETLPipelineDI(unittest.TestCase):
    """Test cases for the ETLPipeline class with dependency injection."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()

        # Create a mock database configuration
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password'
        }

        # Create mock dependencies
        self.mock_extractor = Mock(spec=ExtractorProtocol)
        self.mock_transformer = Mock(spec=TransformerProtocol)
        self.mock_loader = Mock(spec=LoaderProtocol)

        # Configure mock return values
        self.mock_extractor.extract.return_value = {
            'userId': 'test-user-id',
            'exportDate': '2023-01-01T00:00:00Z',
            'conversations': {
                'conv1': {
                    'Properties': {
                        'displayName': 'Test Conversation'
                    },
                    'MessageList': [
                        {
                            'id': 'msg1',
                            'content': 'Test message'
                        }
                    ]
                }
            }
        }

        self.mock_transformer.transform.return_value = {
            'user': {
                'id': 'test-user-id',
                'display_name': 'Test User'
            },
            'conversations': {
                'conv1': {
                    'id': 'conv1',
                    'display_name': 'Test Conversation',
                    'messages': [
                        {
                            'id': 'msg1',
                            'content_text': 'Test message'
                        }
                    ]
                }
            },
            'metadata': {
                'conversation_count': 1,
                'message_count': 1
            }
        }

        self.mock_loader.load.return_value = 123  # Export ID

        # Create a test file
        self.test_file_path = os.path.join(self.temp_dir, 'test.json')
        with open(self.test_file_path, 'w') as f:
            json.dump({'test': 'data'}, f)

    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary directory and files
        if os.path.exists(self.test_file_path):
            os.remove(self.test_file_path)
        if os.path.exists(self.temp_dir):
            os.rmdir(self.temp_dir)

    @patch('src.utils.service_registry.register_all_services')
    @patch('src.utils.di.get_service')
    def test_pipeline_with_di_enabled(self, mock_get_service, mock_register_all_services):
        """Test the ETL pipeline with dependency injection enabled."""
        # Configure mock get_service to return our mock dependencies
        def get_service_side_effect(protocol):
            if protocol == ExtractorProtocol:
                return self.mock_extractor
            elif protocol == TransformerProtocol:
                return self.mock_transformer
            elif protocol == LoaderProtocol:
                return self.mock_loader
            else:
                return None

        mock_get_service.side_effect = get_service_side_effect

        # Create a pipeline with DI enabled
        pipeline = ETLPipeline(
            db_config=self.db_config,
            output_dir=self.temp_dir,
            use_di=True
        )

        # Run the pipeline
        result = pipeline.run_pipeline(file_path=self.test_file_path, user_display_name='Test User')

        # Verify that services were registered
        mock_register_all_services.assert_called_once_with(db_config=self.db_config, output_dir=self.temp_dir)

        # Verify that get_service was called for each dependency
        mock_get_service.assert_any_call(ExtractorProtocol)
        mock_get_service.assert_any_call(TransformerProtocol)
        mock_get_service.assert_any_call(LoaderProtocol)

        # Verify that the mock dependencies were called
        self.mock_extractor.extract.assert_called_once_with(file_path=self.test_file_path, file_obj=None)
        self.mock_transformer.transform.assert_called_once_with(self.mock_extractor.extract.return_value, 'Test User')
        self.mock_loader.load.assert_called_once_with(
            self.mock_extractor.extract.return_value,
            self.mock_transformer.transform.return_value,
            self.test_file_path
        )

        # Verify the result
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['export_id'], 123)
        self.assertEqual(result['conversation_count'], 1)
        self.assertEqual(result['message_count'], 1)

    def test_pipeline_with_di_disabled(self):
        """Test the ETL pipeline with dependency injection disabled."""
        # Create a pipeline with DI disabled
        with patch('src.db.etl.pipeline_manager.Extractor') as mock_extractor_class, \
             patch('src.db.etl.pipeline_manager.Transformer') as mock_transformer_class, \
             patch('src.db.etl.pipeline_manager.Loader') as mock_loader_class:

            # Configure mock classes to return our mock instances
            mock_extractor_class.return_value = self.mock_extractor
            mock_transformer_class.return_value = self.mock_transformer
            mock_loader_class.return_value = self.mock_loader

            # Create the pipeline
            pipeline = ETLPipeline(
                db_config=self.db_config,
                output_dir=self.temp_dir,
                use_di=False
            )

            # Run the pipeline
            result = pipeline.run_pipeline(file_path=self.test_file_path, user_display_name='Test User')

            # Verify that the mock classes were instantiated
            mock_extractor_class.assert_called_once()
            mock_transformer_class.assert_called_once()
            mock_loader_class.assert_called_once()

            # Verify that the mock dependencies were called
            self.mock_extractor.extract.assert_called_once_with(file_path=self.test_file_path, file_obj=None)
            self.mock_transformer.transform.assert_called_once_with(self.mock_extractor.extract.return_value, 'Test User')
            self.mock_loader.load.assert_called_once_with(
                self.mock_extractor.extract.return_value,
                self.mock_transformer.transform.return_value,
                self.test_file_path
            )

            # Verify the result
            self.assertEqual(result['status'], 'completed')
            self.assertEqual(result['export_id'], 123)
            self.assertEqual(result['conversation_count'], 1)
            self.assertEqual(result['message_count'], 1)

    def test_pipeline_with_custom_dependencies(self):
        """Test the ETL pipeline with custom dependencies injected directly."""
        # Create a pipeline with custom dependencies
        pipeline = ETLPipeline(
            db_config=self.db_config,
            output_dir=self.temp_dir,
            use_di=False  # Disable DI to use our custom dependencies
        )

        # Replace the dependencies with our mocks
        pipeline.extractor = self.mock_extractor
        pipeline.transformer = self.mock_transformer
        pipeline.loader = self.mock_loader

        # Run the pipeline
        result = pipeline.run_pipeline(file_path=self.test_file_path, user_display_name='Test User')

        # Verify that the mock dependencies were called
        self.mock_extractor.extract.assert_called_once_with(file_path=self.test_file_path, file_obj=None)
        self.mock_transformer.transform.assert_called_once_with(self.mock_extractor.extract.return_value, 'Test User')
        self.mock_loader.load.assert_called_once_with(
            self.mock_extractor.extract.return_value,
            self.mock_transformer.transform.return_value,
            self.test_file_path
        )

        # Verify the result
        self.assertEqual(result['status'], 'completed')
        self.assertEqual(result['export_id'], 123)
        self.assertEqual(result['conversation_count'], 1)
        self.assertEqual(result['message_count'], 1)

    def test_pipeline_with_error_handling(self):
        """Test the ETL pipeline error handling."""
        # Configure the mock extractor to raise an exception
        self.mock_extractor.extract.side_effect = ValueError("Test error")

        # Create a pipeline with custom dependencies
        pipeline = ETLPipeline(
            db_config=self.db_config,
            output_dir=self.temp_dir,
            use_di=False  # Disable DI to use our custom dependencies
        )

        # Replace the dependencies with our mocks
        pipeline.extractor = self.mock_extractor
        pipeline.transformer = self.mock_transformer
        pipeline.loader = self.mock_loader

        # Run the pipeline and expect an exception
        with self.assertRaises(ValueError):
            pipeline.run_pipeline(file_path=self.test_file_path, user_display_name='Test User')


if __name__ == '__main__':
    unittest.main()