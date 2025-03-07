#!/usr/bin/env python3
"""
Unit tests for the ETL Loader class.

These tests validate the loading logic of the ETL pipeline,
ensuring that transformed data is correctly loaded into the database.
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock, Mock
import tempfile
import json

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl.loader import Loader
from src.utils.interfaces import DatabaseConnectionProtocol
from src.db.etl.context import ETLContext

class TestLoader(unittest.TestCase):
    """Test cases for the Loader class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()

        # Create a mock ETL context
        self.mock_context = MagicMock(spec=ETLContext)
        self.mock_context.output_dir = self.temp_dir

        # Create a mock database connection
        self.mock_db_connection = Mock(spec=DatabaseConnectionProtocol)
        self.mock_db_connection.execute_query.return_value = [{'id': 1}]
        self.mock_db_connection.execute_batch.return_value = 10

        # Create a loader instance with mocked dependencies
        self.loader = Loader(
            context=self.mock_context,
            db_connection=self.mock_db_connection
        )

        # Sample raw data
        self.raw_data = {
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

        # Sample transformed data
        self.transformed_data = {
            'user': {
                'id': 'test-user-id',
                'display_name': 'Test User'
            },
            'conversations': {
                'conv1': {
                    'id': 'conv1',
                    'display_name': 'Test Conversation',
                    'type': 'personal',
                    'participants': [
                        {
                            'id': 'test-user-id',
                            'display_name': 'Test User',
                            'is_self': True
                        },
                        {
                            'id': 'other-user-id',
                            'display_name': 'Other User',
                            'is_self': False
                        }
                    ],
                    'messages': [
                        {
                            'id': 'msg1',
                            'timestamp': '2023-01-01T12:00:00Z',
                            'sender_id': 'test-user-id',
                            'sender_name': 'Test User',
                            'message_type': 'text',
                            'content_text': 'Test message',
                            'content_html': '<p>Test message</p>',
                            'is_edited': False
                        }
                    ]
                }
            },
            'metadata': {
                'conversation_count': 1,
                'message_count': 1
            }
        }

    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary directory
        if os.path.exists(self.temp_dir):
            os.rmdir(self.temp_dir)

    def test_connect_db(self):
        """Test connecting to the database."""
        # Connect to the database
        self.loader.connect_db()

        # Verify that the mock database connection was called
        self.mock_db_connection.connect.assert_called_once()
        self.mock_db_connection.create_tables.assert_called_once()

    def test_close_db(self):
        """Test closing the database connection."""
        # Close the database connection
        self.loader.close_db()

        # Verify that the mock database connection was called
        self.mock_db_connection.disconnect.assert_called_once()

    def test_load(self):
        """Test loading data into the database."""
        # Load the data
        export_id = self.loader.load(self.raw_data, self.transformed_data, 'test.json')

        # Verify that the mock database connection was called
        self.mock_db_connection.execute_query.assert_called()
        self.mock_db_connection.execute_batch.assert_called()
        self.mock_db_connection.commit.assert_called()

        # Verify the export ID
        self.assertEqual(export_id, 1)

        # Verify that the context was updated
        self.mock_context.set_export_id.assert_called_once_with(1)

    def test_load_with_invalid_input(self):
        """Test loading with invalid input."""
        # Test with None raw data
        with self.assertRaises(ValueError):
            self.loader.load(None, self.transformed_data, 'test.json')

        # Test with None transformed data
        with self.assertRaises(ValueError):
            self.loader.load(self.raw_data, None, 'test.json')

        # Test with invalid transformed data (missing user)
        invalid_transformed_data = {
            'conversations': {},
            'metadata': {}
        }
        with self.assertRaises(ValueError):
            self.loader.load(self.raw_data, invalid_transformed_data, 'test.json')

        # Test with invalid transformed data (missing conversations)
        invalid_transformed_data = {
            'user': {},
            'metadata': {}
        }
        with self.assertRaises(ValueError):
            self.loader.load(self.raw_data, invalid_transformed_data, 'test.json')

    def test_load_with_di_dependencies(self):
        """Test loading using dependency injection for all dependencies."""
        # Create a custom mock database connection
        custom_db_connection = Mock(spec=DatabaseConnectionProtocol)
        custom_db_connection.execute_query.return_value = [{'id': 999}]
        custom_db_connection.execute_batch.return_value = 20

        # Create a loader with the custom database connection
        custom_loader = Loader(
            context=self.mock_context,
            db_connection=custom_db_connection
        )

        # Load the data using the custom loader
        export_id = custom_loader.load(self.raw_data, self.transformed_data, 'test.json')

        # Verify that the custom database connection was called
        custom_db_connection.execute_query.assert_called()
        custom_db_connection.execute_batch.assert_called()
        custom_db_connection.commit.assert_called()

        # Verify the export ID
        self.assertEqual(export_id, 999)

        # Verify that the context was updated
        self.mock_context.set_export_id.assert_called_once_with(999)

    def test_load_with_transaction_error(self):
        """Test loading with a transaction error."""
        # Configure the mock database connection to raise an exception
        self.mock_db_connection.execute_query.side_effect = Exception("Database error")

        # Load the data
        with self.assertRaises(Exception):
            self.loader.load(self.raw_data, self.transformed_data, 'test.json')

        # Verify that rollback was called
        self.mock_db_connection.rollback.assert_called_once()

    def test_load_with_no_context(self):
        """Test loading without a context."""
        # Create a loader without a context
        loader = Loader(db_connection=self.mock_db_connection)

        # Load the data
        export_id = loader.load(self.raw_data, self.transformed_data, 'test.json')

        # Verify the export ID
        self.assertEqual(export_id, 1)


if __name__ == '__main__':
    unittest.main()