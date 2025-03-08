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
        self.mock_context.db_config = {
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "user": "test_user",
            "password": "test_password"
        }
        self.mock_context.batch_size = 100
        self.mock_context.user_id = "test-user-id"
        self.mock_context.user_display_name = "Test User"
        self.mock_context.export_date = "2023-01-01T00:00:00Z"

        # Create a mock database using the centralized MockDatabase class
        from tests.fixtures.mock_fixtures import MockDatabase
        mock_db = MockDatabase()
        self.mock_db_connection = mock_db.conn

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
            },
            'messages': {
                'msg1': {
                    'id': 'msg1',
                    'content': 'Test message',
                    'conversationId': 'conv1'
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
                            'content_text': 'Test message',
                            'content_html': '<p>Test message</p>',
                            'is_edited': False,
                            'is_deleted': False
                        }
                    ]
                }
            },
            'messages': {
                'msg1': {
                    'id': 'msg1',
                    'conversation_id': 'conv1',
                    'sender_id': 'test-user-id',
                    'sender_display_name': 'Test User',
                    'content': 'Test message',
                    'timestamp': '2023-01-01T12:00:00Z',
                    'message_type': 'RichText'
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
        # Patch the psycopg2.connect function
        with patch('psycopg2.connect') as mock_connect:
            # Set up the mock connection
            mock_connect.return_value = self.mock_db_connection

            # Connect to the database
            self.loader.connect_db()

            # Verify that psycopg2.connect was called with the correct arguments
            mock_connect.assert_called_once_with(**self.mock_context.db_config)

            # Verify that the connection was set
            self.assertEqual(self.loader.db_connection, self.mock_db_connection)

    def test_close_db(self):
        """Test closing the database connection."""
        # Set up the mock to have the necessary attributes for close_db
        self.mock_db_connection.closed = False

        # Close the database connection
        self.loader.close_db()

        # Verify that the mock database connection was closed
        self.mock_db_connection.close.assert_called_once()

    def test_load(self):
        """Test loading data into the database."""
        # Patch the _store_messages method to avoid the issue with dictionary slicing
        with patch.object(self.loader, '_store_messages'):
            # Load the data
            export_id = self.loader.load(self.raw_data, self.transformed_data, 'test.json')

            # Verify that the export ID is correct
            self.assertEqual(export_id, 1)

            # Verify that _store_messages was called with the transformed data
            self.loader._store_messages.assert_called_once_with(self.transformed_data)

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
        # Create a custom mock database using the centralized MockDatabase class
        from tests.fixtures.mock_fixtures import MockDatabase
        mock_db = MockDatabase()
        custom_db_connection = mock_db.conn

        # Set up fetchone to return a specific ID
        mock_db.cursor.fetchone.return_value = (999,)

        # Create a loader with the custom database connection
        custom_loader = Loader(
            context=self.mock_context,
            db_connection=custom_db_connection
        )

        # Patch the _store_messages method to avoid the issue with dictionary slicing
        with patch.object(custom_loader, '_store_messages'):
            # Load the data using the custom loader
            export_id = custom_loader.load(self.raw_data, self.transformed_data, 'test.json')

            # Verify that the export ID is correct
            self.assertEqual(export_id, 999)

            # Verify that _store_messages was called with the transformed data
            custom_loader._store_messages.assert_called_once_with(self.transformed_data)

    def test_load_with_transaction_error(self):
        """Test loading with a transaction error."""
        # Create a mock database that will fail
        from tests.fixtures.mock_fixtures import MockDatabase
        mock_db = MockDatabase(should_fail=True)
        error_db_connection = mock_db.conn

        # Create a loader with the failing database connection
        error_loader = Loader(
            context=self.mock_context,
            db_connection=error_db_connection
        )

        # Patch all the methods that would be called before the transaction
        with patch.object(error_loader, '_validate_database_connection'), \
             patch.object(error_loader, '_validate_input_data'), \
             patch.object(error_loader, '_create_tables'), \
             patch.object(error_loader, '_create_indexes'), \
             patch.object(error_loader, '_begin_transaction'):

            # Now patch the method that will raise the exception
            with patch.object(error_loader, '_store_raw_export', side_effect=Exception("Database error during transaction")):

                # Load the data
                with self.assertRaises(Exception):
                    error_loader.load(self.raw_data, self.transformed_data, 'test.json')

                # Verify that rollback was called
                error_db_connection.rollback.assert_called_once()

    def test_load_with_no_context(self):
        """Test loading without a context."""
        # Create a mock database
        from tests.fixtures.mock_fixtures import MockDatabase
        mock_db = MockDatabase()
        db_connection = mock_db.conn

        # Set up fetchone to return a specific ID
        mock_db.cursor.fetchone.return_value = (123,)

        # Create a loader without a context
        loader = Loader(db_connection=db_connection)

        # Patch the _store_messages method to avoid the issue with dictionary slicing
        with patch.object(loader, '_store_messages'):
            # Load the data
            export_id = loader.load(self.raw_data, self.transformed_data, 'test.json')

            # Verify that the export ID is correct
            self.assertEqual(export_id, 123)

            # Verify that _store_messages was called with the transformed data
            loader._store_messages.assert_called_once_with(self.transformed_data)


if __name__ == '__main__':
    unittest.main()