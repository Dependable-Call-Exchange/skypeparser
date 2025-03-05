#!/usr/bin/env python3
"""
Tests for the raw_storage module.

This module contains tests for the functionality in src.db.raw_storage.
"""

import os
import json
import tempfile
import unittest
import hashlib
from datetime import datetime
from pathlib import Path
from unittest.mock import patch, mock_open, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.db.raw_storage.models import (
    CREATE_RAW_TABLES_SQL,
    INSERT_RAW_DATA_SQL,
    INSERT_CLEANED_DATA_SQL,
    CHECK_DUPLICATE_SQL,
    GET_LATEST_CLEANED_SQL
)
from src.db.raw_storage.storage import SkypeDataStorage


class TestSkypeDataStorage(unittest.TestCase):
    """Test cases for the SkypeDataStorage class."""

    def setUp(self):
        """Set up test fixtures."""
        # Mock connection parameters
        self.connection_params = {
            'host': 'localhost',
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_password'
        }

        # Sample data for testing
        self.sample_data = {
            "metadata": {
                "userId": "test_user",
                "userDisplayName": "Test User",
                "exportDate": "2023-01-01T12:00:00Z",
                "exportDateFormatted": "2023-01-01 12:00:00",
                "conversationCount": 1
            },
            "conversations": {
                "conversation1": {
                    "id": "conversation1",
                    "displayName": "Test Conversation 1",
                    "messageCount": 2,
                    "firstMessageTime": "2023-01-01T12:30:00Z",
                    "lastMessageTime": "2023-01-01T12:35:00Z",
                    "messages": [
                        {
                            "timestamp": "2023-01-01T12:30:00Z",
                            "timestampFormatted": "2023-01-01 12:30:00",
                            "date": "2023-01-01",
                            "time": "12:30:00",
                            "fromId": "user1",
                            "fromName": "User 1",
                            "type": "RichText",
                            "rawContent": "Hello, world!",
                            "isEdited": False
                        }
                    ]
                }
            }
        }

        # Expected hash for the sample data
        data_str = json.dumps(self.sample_data, sort_keys=True)
        self.expected_hash = hashlib.sha256(data_str.encode()).hexdigest()

    @patch('src.db.raw_storage.storage.SimpleConnectionPool')
    def test_initialization(self, mock_pool):
        """Test SkypeDataStorage initialization."""
        # Mock the connection pool
        mock_pool.return_value = MagicMock()

        # Create a storage instance with mocked connection pool
        with patch.object(SkypeDataStorage, 'ensure_tables_exist') as mock_ensure_tables:
            storage = SkypeDataStorage(self.connection_params)

            # Verify that the connection pool was initialized
            mock_pool.assert_called_once_with(
                SkypeDataStorage.MIN_CONNECTIONS,
                SkypeDataStorage.MAX_CONNECTIONS,
                **self.connection_params
            )

            # Verify that ensure_tables_exist was called
            mock_ensure_tables.assert_called_once()

            # Verify that the connection parameters were stored
            self.assertEqual(storage.connection_params, self.connection_params)

    @patch('src.db.raw_storage.storage.SimpleConnectionPool')
    def test_ensure_tables_exist(self, mock_pool):
        """Test ensure_tables_exist method."""
        # Mock the connection pool and connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        mock_pool_instance = MagicMock()
        mock_pool_instance.getconn.return_value = mock_conn
        mock_pool.return_value = mock_pool_instance

        # Create a storage instance with mocked connection pool
        storage = SkypeDataStorage(self.connection_params)

        # Reset the mock to clear the call from initialization
        mock_cursor.reset_mock()

        # Call ensure_tables_exist directly
        storage.ensure_tables_exist()

        # Verify that the cursor executed the CREATE_RAW_TABLES_SQL
        mock_cursor.execute.assert_called_once_with(CREATE_RAW_TABLES_SQL)

        # Verify that the connection was committed
        mock_conn.commit.assert_called_once()

        # Verify that the connection was returned to the pool
        mock_pool_instance.putconn.assert_called_once_with(mock_conn)

    @patch('src.db.raw_storage.storage.SimpleConnectionPool')
    def test_calculate_file_hash(self, mock_pool):
        """Test calculate_file_hash method."""
        # Mock the connection pool
        mock_pool.return_value = MagicMock()

        # Create a storage instance with mocked connection pool
        with patch.object(SkypeDataStorage, 'ensure_tables_exist'):
            storage = SkypeDataStorage(self.connection_params)

            # Calculate the hash of the sample data
            file_hash = storage.calculate_file_hash(self.sample_data)

            # Verify that the hash matches the expected hash
            self.assertEqual(file_hash, self.expected_hash)

    @patch('src.db.raw_storage.storage.SimpleConnectionPool')
    def test_check_duplicate(self, mock_pool):
        """Test check_duplicate method."""
        # Mock the connection pool and connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock the cursor fetchone to return a result
        mock_cursor.fetchone.return_value = (1, 'test_file.json', datetime.now())

        mock_pool_instance = MagicMock()
        mock_pool_instance.getconn.return_value = mock_conn
        mock_pool.return_value = mock_pool_instance

        # Create a storage instance with mocked connection pool
        with patch.object(SkypeDataStorage, 'ensure_tables_exist'):
            storage = SkypeDataStorage(self.connection_params)

            # Check for a duplicate file
            result = storage.check_duplicate(self.expected_hash)

            # Verify that the cursor executed the CHECK_DUPLICATE_SQL
            mock_cursor.execute.assert_called_once_with(CHECK_DUPLICATE_SQL, (self.expected_hash,))

            # Verify that the result is not None
            self.assertIsNotNone(result)
            self.assertEqual(result['id'], 1)
            self.assertEqual(result['file_name'], 'test_file.json')

            # Verify that the connection was returned to the pool
            mock_pool_instance.putconn.assert_called_once_with(mock_conn)

    @patch('src.db.raw_storage.storage.SimpleConnectionPool')
    def test_store_raw_data(self, mock_pool):
        """Test store_raw_data method."""
        # Mock the connection pool and connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock the cursor fetchone to return a raw_id
        mock_cursor.fetchone.return_value = (1,)

        mock_pool_instance = MagicMock()
        mock_pool_instance.getconn.return_value = mock_conn
        mock_pool.return_value = mock_pool_instance

        # Create a storage instance with mocked connection pool
        with patch.object(SkypeDataStorage, 'ensure_tables_exist'), \
             patch.object(SkypeDataStorage, 'check_duplicate', return_value=None), \
             patch.object(SkypeDataStorage, 'verify_data_integrity', return_value=True):

            storage = SkypeDataStorage(self.connection_params)

            # Store raw data
            file_name = 'test_file.json'
            export_date = datetime.now()
            raw_id = storage.store_raw_data(self.sample_data, file_name, export_date)

            # Verify that the cursor executed the INSERT_RAW_DATA_SQL
            from psycopg2.extras import Json
            mock_cursor.execute.assert_called_once()

            # Verify that the connection was committed
            mock_conn.commit.assert_called_once()

            # Verify that the raw_id is correct
            self.assertEqual(raw_id, 1)

            # Verify that the connection was returned to the pool
            self.assertEqual(mock_pool_instance.putconn.call_count, 1)

    @patch('src.db.raw_storage.storage.SimpleConnectionPool')
    def test_store_cleaned_data(self, mock_pool):
        """Test store_cleaned_data method."""
        # Mock the connection pool and connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Mock the cursor fetchone to return a cleaned_id
        mock_cursor.fetchone.return_value = (1,)

        mock_pool_instance = MagicMock()
        mock_pool_instance.getconn.return_value = mock_conn
        mock_pool.return_value = mock_pool_instance

        # Create a storage instance with mocked connection pool
        with patch.object(SkypeDataStorage, 'ensure_tables_exist'):
            storage = SkypeDataStorage(self.connection_params)

            # Store cleaned data
            raw_export_id = 1
            cleaned_id = storage.store_cleaned_data(raw_export_id, self.sample_data)

            # Verify that the cursor executed the INSERT_CLEANED_DATA_SQL
            from psycopg2.extras import Json
            mock_cursor.execute.assert_called_once()

            # Verify that the connection was committed
            mock_conn.commit.assert_called_once()

            # Verify that the cleaned_id is correct
            self.assertEqual(cleaned_id, 1)

            # Verify that the connection was returned to the pool
            mock_pool_instance.putconn.assert_called_once_with(mock_conn)

    @patch('src.db.raw_storage.storage.SimpleConnectionPool')
    def test_store_skype_export(self, mock_pool):
        """Test store_skype_export method."""
        # Mock the connection pool and connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        mock_pool_instance = MagicMock()
        mock_pool_instance.getconn.return_value = mock_conn
        mock_pool.return_value = mock_pool_instance

        # Create a storage instance with mocked connection pool
        with patch.object(SkypeDataStorage, 'ensure_tables_exist'), \
             patch.object(SkypeDataStorage, 'store_raw_data', return_value=1), \
             patch.object(SkypeDataStorage, 'store_cleaned_data', return_value=2):

            storage = SkypeDataStorage(self.connection_params)

            # Store Skype export
            file_name = 'test_file.json'
            export_date = datetime.now()
            raw_id, cleaned_id = storage.store_skype_export(
                self.sample_data,
                self.sample_data,
                file_name,
                export_date
            )

            # Verify that store_raw_data and store_cleaned_data were called
            storage.store_raw_data.assert_called_once_with(
                self.sample_data,
                file_name,
                export_date
            )
            storage.store_cleaned_data.assert_called_once_with(1, self.sample_data)

            # Verify that the IDs are correct
            self.assertEqual(raw_id, 1)
            self.assertEqual(cleaned_id, 2)

    @patch('src.db.raw_storage.storage.SimpleConnectionPool')
    def test_close(self, mock_pool):
        """Test close method."""
        # Mock the connection pool
        mock_pool_instance = MagicMock()
        mock_pool.return_value = mock_pool_instance

        # Create a storage instance with mocked connection pool
        with patch.object(SkypeDataStorage, 'ensure_tables_exist'):
            storage = SkypeDataStorage(self.connection_params)

            # Close the storage
            storage.close()

            # Verify that the pool was closed
            mock_pool_instance.closeall.assert_called_once()


if __name__ == '__main__':
    unittest.main()