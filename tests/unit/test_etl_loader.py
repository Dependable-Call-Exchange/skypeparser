#!/usr/bin/env python3
"""
Unit tests for the ETL Loader class.

These tests validate the database loading functionality of the ETL pipeline,
ensuring that transformed data is correctly loaded into the database.
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock, call

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl.loader import Loader

class TestLoader(unittest.TestCase):
    """Test cases for the Loader class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a test database configuration
        self.db_config = {
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password',
            'host': 'localhost',
            'port': 5432
        }

        # Create a loader instance
        self.loader = Loader(db_config=self.db_config)

        # Sample raw data for testing
        self.raw_data = {
            'conversations': [
                {
                    'id': 'conv1',
                    'displayName': 'Test Conversation 1',
                    'MessageList': [
                        {'id': 'msg1', 'content': 'Hello world'}
                    ]
                }
            ]
        }

        # Sample transformed data for testing
        self.transformed_data = {
            'metadata': {
                'user_display_name': 'Test User',
                'export_time': '2023-01-01T12:00:00Z',
                'total_conversations': 1,
                'total_messages': 2
            },
            'conversations': {
                'conv1': {
                    'display_name': 'Test Conversation 1',
                    'first_message_time': '2023-01-01T12:00:00Z',
                    'last_message_time': '2023-01-01T12:05:00Z',
                    'message_count': 2,
                    'messages': [
                        {
                            'timestamp': '2023-01-01T12:00:00Z',
                            'sender_id': 'user1',
                            'sender_name': 'User 1',
                            'message_type': 'RichText',
                            'content': 'Hello world',
                            'cleaned_content': 'Hello world',
                            'is_edited': False,
                            'structured_data': {'key': 'value'}
                        },
                        {
                            'timestamp': '2023-01-01T12:05:00Z',
                            'sender_id': 'user2',
                            'sender_name': 'User 2',
                            'message_type': 'RichText',
                            'content': 'Hi there',
                            'cleaned_content': 'Hi there',
                            'is_edited': True
                        }
                    ]
                }
            }
        }

    @patch('src.db.etl.loader.validate_db_config')
    def test_init(self, mock_validate_db_config):
        """Test initialization of the Loader class."""
        loader = Loader(db_config=self.db_config)

        # Verify the db_config was validated
        mock_validate_db_config.assert_called_once_with(self.db_config)

        # Verify the db_config was stored
        self.assertEqual(loader.db_config, self.db_config)

        # Verify the connection is initially None
        self.assertIsNone(loader.conn)

    @patch('src.db.etl.loader.psycopg2.connect')
    def test_connect_db(self, mock_connect):
        """Test connecting to the database."""
        # Set up the mock
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Call the method
        self.loader.connect_db()

        # Verify the connection was established
        mock_connect.assert_called_once_with(**self.db_config)

        # Verify the connection was stored
        self.assertEqual(self.loader.conn, mock_conn)

        # Verify the tables were created
        self.assertEqual(mock_cursor.execute.call_count, 3)
        mock_conn.commit.assert_called_once()

    @patch('src.db.etl.loader.psycopg2.connect')
    def test_connect_db_error(self, mock_connect):
        """Test error handling when connecting to the database."""
        # Set up the mock to raise an exception
        mock_connect.side_effect = Exception("Connection error")

        # Call the method and verify it raises an exception
        with self.assertRaises(Exception):
            self.loader.connect_db()

    def test_close_db(self):
        """Test closing the database connection."""
        # Set up a mock connection
        self.loader.conn = MagicMock()

        # Call the method
        self.loader.close_db()

        # Verify the connection was closed
        self.loader.conn.close.assert_called_once()

    def test_close_db_no_connection(self):
        """Test closing when there is no connection."""
        # Ensure the connection is None
        self.loader.conn = None

        # Call the method (should not raise an exception)
        self.loader.close_db()

    def test_close_db_error(self):
        """Test error handling when closing the connection."""
        # Set up a mock connection that raises an exception
        self.loader.conn = MagicMock()
        self.loader.conn.close.side_effect = Exception("Close error")

        # Call the method (should not raise an exception)
        self.loader.close_db()

    @patch('src.db.etl.loader.psycopg2.extras.Json')
    def test_load(self, mock_json):
        """Test loading data into the database."""
        # Set up mocks
        self.loader.conn = MagicMock()
        mock_cursor = MagicMock()
        self.loader.conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_cursor.fetchone.return_value = [123]  # Export ID

        # Mock the Json constructor to return the input
        mock_json.side_effect = lambda x: x

        # Call the method
        export_id = self.loader.load(
            self.raw_data,
            self.transformed_data,
            'test.tar'
        )

        # Verify the export ID
        self.assertEqual(export_id, 123)

        # Verify the raw export was inserted
        mock_cursor.execute.assert_any_call(
            """
                INSERT INTO skype_raw_exports (user_id, export_date, raw_data, file_source)
                VALUES (%s, %s, %s, %s)
                RETURNING export_id
            """,
            ('Test User', '2023-01-01T12:00:00Z', self.raw_data, 'test.tar')
        )

        # Verify the conversation was inserted
        mock_cursor.execute.assert_any_call(
            """
            INSERT INTO skype_conversations (
                conversation_id, display_name, export_id,
                first_message_time, last_message_time, message_count
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (conversation_id) DO UPDATE
            SET
                display_name = EXCLUDED.display_name,
                export_id = EXCLUDED.export_id,
                first_message_time = EXCLUDED.first_message_time,
                last_message_time = EXCLUDED.last_message_time,
                message_count = EXCLUDED.message_count,
                updated_at = CURRENT_TIMESTAMP
        """,
            ('conv1', 'Test Conversation 1', 123, '2023-01-01T12:00:00Z',
             '2023-01-01T12:05:00Z', 2)
        )

        # Verify the messages were inserted (2 messages)
        self.assertEqual(
            mock_cursor.execute.call_count,
            4  # 1 for raw export + 1 for conversation + 2 for messages
        )

        # Verify the transaction was committed
        self.loader.conn.commit.assert_called_once()

    def test_load_error(self):
        """Test error handling when loading data."""
        # Set up a mock connection
        self.loader.conn = MagicMock()

        # Set up the cursor to raise an exception
        mock_cursor = MagicMock()
        mock_cursor.__enter__.side_effect = Exception("Load error")
        self.loader.conn.cursor.return_value = mock_cursor

        # Call the method and verify it raises an exception
        with self.assertRaises(Exception):
            self.loader.load(self.raw_data, self.transformed_data, 'test.tar')

        # Verify the transaction was rolled back
        self.loader.conn.rollback.assert_called_once()

    @patch('src.db.etl.loader.psycopg2.extras.Json')
    def test_insert_messages(self, mock_json):
        """Test inserting messages into the database."""
        # Set up mocks
        mock_cursor = MagicMock()

        # Mock the Json constructor to return the input
        mock_json.side_effect = lambda x: x

        # Call the method
        self.loader._insert_messages(
            mock_cursor,
            'conv1',
            self.transformed_data['conversations']['conv1']['messages']
        )

        # Verify the cursor was called twice (once for each message)
        self.assertEqual(mock_cursor.execute.call_count, 2)

        # Verify the first message was inserted correctly
        mock_cursor.execute.assert_any_call(
            """
                INSERT INTO skype_messages (
                    conversation_id, timestamp, sender_id, sender_name,
                    message_type, raw_content, cleaned_content,
                    is_edited, structured_data
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            ('conv1', '2023-01-01T12:00:00Z', 'user1', 'User 1', 'RichText',
             'Hello world', 'Hello world', False, {'key': 'value'})
        )

        # Verify the second message was inserted correctly
        mock_cursor.execute.assert_any_call(
            """
                INSERT INTO skype_messages (
                    conversation_id, timestamp, sender_id, sender_name,
                    message_type, raw_content, cleaned_content,
                    is_edited, structured_data
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            ('conv1', '2023-01-01T12:05:00Z', 'user2', 'User 2', 'RichText',
             'Hi there', 'Hi there', True, None)
        )

if __name__ == '__main__':
    unittest.main()