#!/usr/bin/env python3
"""
Unit tests for the ETL pipeline module.
"""

import os
import sys
import unittest
import tempfile
import json
import io
from unittest.mock import patch, MagicMock, mock_open

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db.etl_pipeline import SkypeETLPipeline


class TestETLPipeline(unittest.TestCase):
    """Test cases for the ETL pipeline."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a mock database configuration
        self.db_config = {
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password',
            'host': 'localhost',
            'port': 5432
        }

        # Create a temporary directory for output
        self.temp_dir = tempfile.mkdtemp()

        # Create a sample Skype export data
        self.sample_data = {
            "userId": "test_user",
            "exportDate": "2023-01-01T12:00:00Z",
            "conversations": [
                {
                    "id": "conversation1",
                    "displayName": "Test Conversation 1",
                    "messages": [
                        {
                            "id": "msg1",
                            "timestamp": "2023-01-01T12:30:00Z",
                            "from": {"id": "user1", "displayName": "User 1"},
                            "content": "Hello, world!",
                            "messageType": "RichText"
                        },
                        {
                            "id": "msg2",
                            "timestamp": "2023-01-01T12:35:00Z",
                            "from": {"id": "user2", "displayName": "User 2"},
                            "content": "Hi there!",
                            "messageType": "RichText"
                        }
                    ]
                }
            ]
        }

        # Create a mock file object
        self.mock_file = io.BytesIO(json.dumps(self.sample_data).encode('utf-8'))

        # Create the ETL pipeline with mocked database connection
        with patch('psycopg2.connect'):
            self.pipeline = SkypeETLPipeline(db_config=self.db_config, output_dir=self.temp_dir)
            # Mock the database connection and cursor
            self.pipeline.conn = MagicMock()
            self.pipeline.cursor = MagicMock()

    def tearDown(self):
        """Tear down test fixtures."""
        # Remove the temporary directory
        import shutil
        shutil.rmtree(self.temp_dir)

    @patch('src.utils.file_handler.read_file')
    def test_extract_from_file_path(self, mock_read_file):
        """Test extraction from a file path."""
        # Set up the mock
        mock_read_file.return_value = self.sample_data

        # Call the extract method
        result = self.pipeline.extract(file_path='test.json')

        # Verify the result
        self.assertEqual(result, self.sample_data)
        mock_read_file.assert_called_once_with('test.json')

    @patch('src.utils.file_handler.read_file_object')
    def test_extract_from_file_object(self, mock_read_file_object):
        """Test extraction from a file object."""
        # Set up the mock
        mock_read_file_object.return_value = self.sample_data

        # Call the extract method
        result = self.pipeline.extract(file_obj=self.mock_file)

        # Verify the result
        self.assertEqual(result, self.sample_data)
        mock_read_file_object.assert_called_once()

    def test_transform(self):
        """Test transformation of raw data."""
        # Call the transform method
        result = self.pipeline.transform(self.sample_data, user_display_name="Test User")

        # Verify the result
        self.assertIn('conversations', result)
        self.assertEqual(len(result['conversations']), 1)
        self.assertEqual(result['conversations'][0]['display_name'], "Test Conversation 1")
        self.assertEqual(result['conversations'][0]['message_count'], 2)
        self.assertIn('messages', result)
        self.assertEqual(len(result['messages']), 2)
        self.assertEqual(result['messages'][0]['sender_name'], "User 1")
        self.assertEqual(result['messages'][1]['sender_name'], "User 2")

    def test_load(self):
        """Test loading data into the database."""
        # Transform the data first
        transformed_data = self.pipeline.transform(self.sample_data, user_display_name="Test User")

        # Mock the cursor's fetchone method to return a specific export_id
        self.pipeline.cursor.fetchone.return_value = (1,)

        # Call the load method
        export_id = self.pipeline.load(self.sample_data, transformed_data, file_source="test.json")

        # Verify the result
        self.assertEqual(export_id, 1)

        # Verify that the cursor's execute method was called the expected number of times
        # 1 for inserting raw data, 1 for each conversation, and 1 for each message
        expected_calls = 1 + 1 + 2
        self.assertEqual(self.pipeline.cursor.execute.call_count, expected_calls)

    @patch('src.utils.file_handler.read_file')
    def test_run_pipeline(self, mock_read_file):
        """Test running the entire pipeline."""
        # Set up the mock
        mock_read_file.return_value = self.sample_data

        # Mock the cursor's fetchone method to return a specific export_id
        self.pipeline.cursor.fetchone.return_value = (1,)

        # Call the run_pipeline method
        result = self.pipeline.run_pipeline(file_path='test.json', user_display_name="Test User")

        # Verify the result
        self.assertIn('export_id', result)
        self.assertEqual(result['export_id'], 1)
        self.assertIn('conversations', result)
        self.assertEqual(len(result['conversations']), 1)
        self.assertIn('message_count', result)
        self.assertEqual(result['message_count'], 2)


if __name__ == '__main__':
    unittest.main()