#!/usr/bin/env python3
"""
Unit tests for the ETL Extractor class.

These tests validate the extraction logic of the ETL pipeline,
ensuring that Skype export files are correctly read and processed.
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock, Mock
import tempfile
import json
import tarfile
import io

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl.extractor import Extractor
from src.utils.interfaces import FileHandlerProtocol
from src.db.etl.context import ETLContext

class TestExtractor(unittest.TestCase):
    """Test cases for the Extractor class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()

        # Create a mock ETL context
        self.mock_context = MagicMock(spec=ETLContext)
        self.mock_context.output_dir = self.temp_dir

        # Create a mock file handler
        self.mock_file_handler = Mock(spec=FileHandlerProtocol)
        self.mock_file_handler.read_file.return_value = {
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
        self.mock_file_handler.read_file_obj.return_value = self.mock_file_handler.read_file.return_value

        # Create an extractor instance with mocked dependencies
        self.extractor = Extractor(
            context=self.mock_context,
            file_handler=self.mock_file_handler
        )

        # Create test files
        self.json_file_path = os.path.join(self.temp_dir, 'test.json')
        self.tar_file_path = os.path.join(self.temp_dir, 'test.tar')

        # Create a sample JSON file
        with open(self.json_file_path, 'w') as f:
            json.dump({
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
            }, f)

        # Create a sample TAR file
        with tarfile.open(self.tar_file_path, 'w') as tar:
            info = tarfile.TarInfo('messages.json')
            data = json.dumps({
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
            }).encode('utf-8')
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))

    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary directory and files
        for file_path in [self.json_file_path, self.tar_file_path]:
            if os.path.exists(file_path):
                os.remove(file_path)
        if os.path.exists(self.temp_dir):
            os.rmdir(self.temp_dir)

    def test_extract_from_file_path(self):
        """Test extraction from a file path."""
        # Extract data from the JSON file
        raw_data = self.extractor.extract(file_path=self.json_file_path)

        # Verify that the mock file handler was called
        self.mock_file_handler.read_file.assert_called_once_with(self.json_file_path)

        # Verify the extracted data
        self.assertEqual(raw_data['userId'], 'test-user-id')
        self.assertEqual(raw_data['exportDate'], '2023-01-01T00:00:00Z')
        self.assertIn('conv1', raw_data['conversations'])

        # Verify that the context was updated
        self.mock_context.set_raw_data.assert_called_once_with(raw_data)

    def test_extract_from_file_obj(self):
        """Test extraction from a file object."""
        # Create a file object
        with open(self.json_file_path, 'rb') as file_obj:
            # Extract data from the file object
            raw_data = self.extractor.extract(file_obj=file_obj)

        # Verify that the mock file handler was called
        self.mock_file_handler.read_file_obj.assert_called_once()

        # Verify the extracted data
        self.assertEqual(raw_data['userId'], 'test-user-id')
        self.assertEqual(raw_data['exportDate'], '2023-01-01T00:00:00Z')
        self.assertIn('conv1', raw_data['conversations'])

        # Verify that the context was updated
        self.mock_context.set_raw_data.assert_called_once_with(raw_data)

    def test_extract_with_invalid_input(self):
        """Test extraction with invalid input."""
        # Test with no file path or file object
        with self.assertRaises(ValueError):
            self.extractor.extract()

        # Test with non-existent file
        with self.assertRaises(ValueError):
            self.extractor.extract(file_path='non_existent_file.json')

        # Test with invalid file object
        with self.assertRaises(ValueError):
            self.extractor.extract(file_obj="not a file object")

    def test_extract_with_di_dependencies(self):
        """Test extraction using dependency injection for all dependencies."""
        # Create a custom mock file handler
        custom_file_handler = Mock(spec=FileHandlerProtocol)
        custom_file_handler.read_file.return_value = {
            'userId': 'custom-user-id',
            'exportDate': '2023-02-01T00:00:00Z',
            'conversations': {
                'custom-conv': {
                    'Properties': {
                        'displayName': 'Custom Conversation'
                    },
                    'MessageList': [
                        {
                            'id': 'custom-msg',
                            'content': 'Custom message'
                        }
                    ]
                }
            }
        }

        # Create an extractor with the custom file handler
        custom_extractor = Extractor(
            context=self.mock_context,
            file_handler=custom_file_handler
        )

        # Extract data using the custom extractor
        raw_data = custom_extractor.extract(file_path=self.json_file_path)

        # Verify that the custom file handler was called
        custom_file_handler.read_file.assert_called_once_with(self.json_file_path)

        # Verify the extracted data
        self.assertEqual(raw_data['userId'], 'custom-user-id')
        self.assertEqual(raw_data['exportDate'], '2023-02-01T00:00:00Z')
        self.assertIn('custom-conv', raw_data['conversations'])

        # Verify that the context was updated
        self.mock_context.set_raw_data.assert_called_once_with(raw_data)

    def test_extract_with_output_dir(self):
        """Test extraction with output directory."""
        # Create a custom output directory
        output_dir = os.path.join(self.temp_dir, 'output')
        os.makedirs(output_dir, exist_ok=True)

        # Update the mock context
        self.mock_context.output_dir = output_dir

        # Extract data
        raw_data = self.extractor.extract(file_path=self.json_file_path)

        # Verify that the raw data was saved
        self.mock_context.set_raw_data.assert_called_once_with(raw_data)

        # Clean up
        os.rmdir(output_dir)

    def test_extract_with_no_context(self):
        """Test extraction without a context."""
        # Create an extractor without a context
        extractor = Extractor(file_handler=self.mock_file_handler)

        # Extract data
        raw_data = extractor.extract(file_path=self.json_file_path)

        # Verify the extracted data
        self.assertEqual(raw_data['userId'], 'test-user-id')
        self.assertEqual(raw_data['exportDate'], '2023-01-01T00:00:00Z')
        self.assertIn('conv1', raw_data['conversations'])


if __name__ == '__main__':
    unittest.main()