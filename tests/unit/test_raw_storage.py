#!/usr/bin/env python3
"""
Tests for the raw_storage module.

This module contains tests for the functionality in src.db.raw_storage.
"""

import os
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, mock_open, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.db.raw_storage.models import (
    User,
    Conversation,
    Message,
    SkypeData
)
from src.db.raw_storage.storage import (
    RawStorage,
    PostgresRawStorage,
    store_skype_data,
    retrieve_skype_data
)


class TestRawStorageModels(unittest.TestCase):
    """Test cases for the raw_storage models."""

    def test_user_model(self):
        """Test User model."""
        user = User(id='user1', display_name='Test User')
        self.assertEqual(user.id, 'user1')
        self.assertEqual(user.display_name, 'Test User')

        # Test to_dict method
        user_dict = user.to_dict()
        self.assertEqual(user_dict['id'], 'user1')
        self.assertEqual(user_dict['display_name'], 'Test User')

        # Test from_dict method
        user2 = User.from_dict(user_dict)
        self.assertEqual(user2.id, 'user1')
        self.assertEqual(user2.display_name, 'Test User')

    def test_message_model(self):
        """Test Message model."""
        message = Message(
            id='message1',
            content='Hello, world!',
            sender_id='user1',
            sender_name='Test User',
            timestamp='2023-01-01T12:00:00Z',
            message_type='RichText'
        )
        self.assertEqual(message.id, 'message1')
        self.assertEqual(message.content, 'Hello, world!')
        self.assertEqual(message.sender_id, 'user1')
        self.assertEqual(message.sender_name, 'Test User')
        self.assertEqual(message.timestamp, '2023-01-01T12:00:00Z')
        self.assertEqual(message.message_type, 'RichText')

        # Test to_dict method
        message_dict = message.to_dict()
        self.assertEqual(message_dict['id'], 'message1')
        self.assertEqual(message_dict['content'], 'Hello, world!')

        # Test from_dict method
        message2 = Message.from_dict(message_dict)
        self.assertEqual(message2.id, 'message1')
        self.assertEqual(message2.content, 'Hello, world!')

    def test_conversation_model(self):
        """Test Conversation model."""
        message = Message(
            id='message1',
            content='Hello, world!',
            sender_id='user1',
            sender_name='Test User',
            timestamp='2023-01-01T12:00:00Z',
            message_type='RichText'
        )

        conversation = Conversation(
            id='conversation1',
            display_name='Test Conversation',
            messages=[message]
        )

        self.assertEqual(conversation.id, 'conversation1')
        self.assertEqual(conversation.display_name, 'Test Conversation')
        self.assertEqual(len(conversation.messages), 1)
        self.assertEqual(conversation.messages[0].id, 'message1')

        # Test to_dict method
        conversation_dict = conversation.to_dict()
        self.assertEqual(conversation_dict['id'], 'conversation1')
        self.assertEqual(conversation_dict['display_name'], 'Test Conversation')
        self.assertEqual(len(conversation_dict['messages']), 1)

        # Test from_dict method
        conversation2 = Conversation.from_dict(conversation_dict)
        self.assertEqual(conversation2.id, 'conversation1')
        self.assertEqual(conversation2.display_name, 'Test Conversation')
        self.assertEqual(len(conversation2.messages), 1)

    def test_skype_data_model(self):
        """Test SkypeData model."""
        user_info = {
            'user_id': 'test_user',
            'display_name': 'Test User',
            'export_date': '2023-01-01'
        }

        message = Message(
            id='message1',
            content='Hello, world!',
            sender_id='user1',
            sender_name='Test User',
            timestamp='2023-01-01T12:00:00Z',
            message_type='RichText'
        )

        conversation = Conversation(
            id='conversation1',
            display_name='Test Conversation',
            messages=[message]
        )

        skype_data = SkypeData(
            user_info=user_info,
            conversations=[conversation]
        )

        self.assertEqual(skype_data.user_info, user_info)
        self.assertEqual(len(skype_data.conversations), 1)
        self.assertEqual(skype_data.conversations[0].id, 'conversation1')

        # Test to_dict method
        skype_data_dict = skype_data.to_dict()
        self.assertEqual(skype_data_dict['user_info'], user_info)
        self.assertEqual(len(skype_data_dict['conversations']), 1)

        # Test from_dict method
        skype_data2 = SkypeData.from_dict(skype_data_dict)
        self.assertEqual(skype_data2.user_info, user_info)
        self.assertEqual(len(skype_data2.conversations), 1)


class TestRawStorage(unittest.TestCase):
    """Test cases for the RawStorage class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.TemporaryDirectory()

        # Sample Skype data for testing
        self.user_info = {
            'user_id': 'test_user',
            'display_name': 'Test User',
            'export_date': '2023-01-01'
        }

        self.message = Message(
            id='message1',
            content='Hello, world!',
            sender_id='user1',
            sender_name='Test User',
            timestamp='2023-01-01T12:00:00Z',
            message_type='RichText'
        )

        self.conversation = Conversation(
            id='conversation1',
            display_name='Test Conversation',
            messages=[self.message]
        )

        self.skype_data = SkypeData(
            user_info=self.user_info,
            conversations=[self.conversation]
        )

    def tearDown(self):
        """Clean up test fixtures."""
        self.temp_dir.cleanup()

    def test_raw_storage_initialization(self):
        """Test RawStorage initialization."""
        storage = RawStorage()
        self.assertIsNotNone(storage)

    @patch('src.db.raw_storage.storage.open', new_callable=mock_open)
    def test_store_skype_data(self, mock_file):
        """Test store_skype_data function."""
        output_file = os.path.join(self.temp_dir.name, 'skype_data.json')

        # Mock the json.dump function
        with patch('json.dump') as mock_json_dump:
            store_skype_data(self.skype_data, output_file)

            # Verify that the file was opened for writing
            mock_file.assert_called_once_with(output_file, 'w')

            # Verify that json.dump was called with the skype_data
            mock_json_dump.assert_called_once()

    @patch('src.db.raw_storage.storage.open', new_callable=mock_open,
           read_data=json.dumps({'user_info': {'user_id': 'test_user'}, 'conversations': []}))
    def test_retrieve_skype_data(self, mock_file):
        """Test retrieve_skype_data function."""
        input_file = os.path.join(self.temp_dir.name, 'skype_data.json')

        skype_data = retrieve_skype_data(input_file)

        # Verify that the file was opened for reading
        mock_file.assert_called_once_with(input_file, 'r')

        # Verify that the returned data is a SkypeData object
        self.assertIsInstance(skype_data, SkypeData)
        self.assertEqual(skype_data.user_info['user_id'], 'test_user')


class TestPostgresRawStorage(unittest.TestCase):
    """Test cases for the PostgresRawStorage class."""

    def setUp(self):
        """Set up test fixtures."""
        # Sample Skype data for testing
        self.user_info = {
            'user_id': 'test_user',
            'display_name': 'Test User',
            'export_date': '2023-01-01'
        }

        self.message = Message(
            id='message1',
            content='Hello, world!',
            sender_id='user1',
            sender_name='Test User',
            timestamp='2023-01-01T12:00:00Z',
            message_type='RichText'
        )

        self.conversation = Conversation(
            id='conversation1',
            display_name='Test Conversation',
            messages=[self.message]
        )

        self.skype_data = SkypeData(
            user_info=self.user_info,
            conversations=[self.conversation]
        )

        # Mock database configuration
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_password'
        }

    @patch('src.db.raw_storage.storage.psycopg2.connect')
    def test_postgres_raw_storage_initialization(self, mock_connect):
        """Test PostgresRawStorage initialization."""
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        storage = PostgresRawStorage(self.db_config)

        # Verify that the connection was established
        mock_connect.assert_called_once()

        # Verify that the cursor was created
        mock_conn.cursor.assert_called_once()

        # Verify that the connection and cursor were stored
        self.assertEqual(storage.conn, mock_conn)
        self.assertEqual(storage.cursor, mock_cursor)

    @patch('src.db.raw_storage.storage.psycopg2.connect')
    def test_store_skype_data_postgres(self, mock_connect):
        """Test store_skype_data method of PostgresRawStorage."""
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        storage = PostgresRawStorage(self.db_config)

        # Store the Skype data
        storage.store_skype_data(self.skype_data)

        # Verify that execute was called multiple times (for each table)
        self.assertTrue(mock_cursor.execute.call_count > 0)

        # Verify that commit was called
        mock_conn.commit.assert_called_once()

    @patch('src.db.raw_storage.storage.psycopg2.connect')
    def test_retrieve_skype_data_postgres(self, mock_connect):
        """Test retrieve_skype_data method of PostgresRawStorage."""
        # Mock the connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = mock_conn
        mock_conn.cursor.return_value = mock_cursor

        # Mock the cursor fetchall method to return sample data
        mock_cursor.fetchall.side_effect = [
            [('test_user', 'Test User', '2023-01-01')],  # user_info
            [('conversation1', 'Test Conversation')],     # conversations
            [('message1', 'Hello, world!', 'user1', 'Test User', '2023-01-01T12:00:00Z', 'RichText', 'conversation1')]  # messages
        ]

        storage = PostgresRawStorage(self.db_config)

        # Retrieve the Skype data
        skype_data = storage.retrieve_skype_data('test_user')

        # Verify that execute was called multiple times (for each table)
        self.assertTrue(mock_cursor.execute.call_count > 0)

        # Verify that the returned data is a SkypeData object
        self.assertIsInstance(skype_data, SkypeData)
        self.assertEqual(skype_data.user_info['user_id'], 'test_user')


if __name__ == '__main__':
    unittest.main()