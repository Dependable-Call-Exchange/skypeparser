#!/usr/bin/env python3
"""
Unit tests for the ETL pipeline module.

This test suite provides comprehensive testing for the ETL pipeline,
including edge cases, error handling, and integration with the database.
"""

import os
import sys
import unittest
import tempfile
import json
import io
import datetime
import psycopg2
from unittest.mock import patch, MagicMock, mock_open

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db.etl_pipeline import SkypeETLPipeline
from src.utils.config import load_config, get_db_config


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

        # Create a more complex sample with various message types and edge cases
        self.complex_sample_data = {
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
                        },
                        {
                            "id": "msg3",
                            "timestamp": "2023-01-01T12:40:00Z",
                            "from": {"id": "user1", "displayName": "User 1"},
                            "content": "<b>Bold text</b> and <i>italic text</i>",
                            "messageType": "RichText"
                        },
                        {
                            "id": "msg4",
                            "timestamp": "2023-01-01T12:45:00Z",
                            "from": {"id": "user2", "displayName": "User 2"},
                            "content": "",
                            "messageType": "Event/Call"
                        },
                        {
                            "id": "msg5",
                            "timestamp": "2023-01-01T12:50:00Z",
                            "from": {"id": "user1", "displayName": "User 1"},
                            "content": "https://example.com/image.jpg",
                            "messageType": "RichText/UriObject"
                        }
                    ]
                },
                {
                    "id": "conversation2",
                    "displayName": "Test Conversation 2",
                    "messages": []
                },
                {
                    "id": "conversation3",
                    "displayName": None,
                    "messages": [
                        {
                            "id": "msg6",
                            "timestamp": "2023-01-01T13:00:00Z",
                            "from": {"id": "user3", "displayName": "User 3"},
                            "content": "Message in conversation with no display name",
                            "messageType": "RichText"
                        }
                    ]
                }
            ]
        }

        # Create a sample with invalid data
        self.invalid_sample_data = {
            "userId": "test_user",
            "exportDate": "invalid_date",
            "conversations": [
                {
                    "id": "conversation1",
                    "displayName": "Test Conversation 1",
                    "messages": [
                        {
                            "id": "msg1",
                            "timestamp": "invalid_timestamp",
                            "from": {"id": "user1", "displayName": "User 1"},
                            "content": "Hello, world!",
                            "messageType": "RichText"
                        }
                    ]
                }
            ]
        }

        # Create a mock file object
        self.mock_file = io.BytesIO(json.dumps(self.sample_data).encode('utf-8'))
        self.complex_mock_file = io.BytesIO(json.dumps(self.complex_sample_data).encode('utf-8'))
        self.invalid_mock_file = io.BytesIO(json.dumps(self.invalid_sample_data).encode('utf-8'))

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

    @patch('src.utils.file_handler.read_tarfile')
    def test_extract_from_tar_file(self, mock_read_tarfile):
        """Test extraction from a TAR file."""
        # Set up the mock
        mock_read_tarfile.return_value = self.sample_data

        # Call the extract method
        result = self.pipeline.extract(file_path='test.tar')

        # Verify the result
        self.assertEqual(result, self.sample_data)
        mock_read_tarfile.assert_called_once_with('test.tar', auto_select=True)

    def test_extract_with_no_file(self):
        """Test extraction with no file provided."""
        # Call the extract method with no file
        with self.assertRaises(ValueError):
            self.pipeline.extract()

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

    def test_transform_complex_data(self):
        """Test transformation of complex data with various message types and edge cases."""
        # Call the transform method
        result = self.pipeline.transform(self.complex_sample_data, user_display_name="Test User")

        # Verify the result
        self.assertIn('conversations', result)
        self.assertEqual(len(result['conversations']), 3)

        # Check conversation with messages
        self.assertEqual(result['conversations'][0]['display_name'], "Test Conversation 1")
        self.assertEqual(result['conversations'][0]['message_count'], 5)

        # Check empty conversation
        self.assertEqual(result['conversations'][1]['display_name'], "Test Conversation 2")
        self.assertEqual(result['conversations'][1]['message_count'], 0)

        # Check conversation with no display name
        self.assertIsNotNone(result['conversations'][2]['display_name'])  # Should have a default name
        self.assertEqual(result['conversations'][2]['message_count'], 1)

        # Check messages
        self.assertIn('messages', result)
        self.assertEqual(len(result['messages']), 6)

        # Check HTML content parsing
        html_message = next(msg for msg in result['messages'] if msg['message_id'] == 'msg3')
        self.assertNotIn('<b>', html_message['cleaned_content'])
        self.assertNotIn('<i>', html_message['cleaned_content'])

        # Check special message type
        call_message = next(msg for msg in result['messages'] if msg['message_id'] == 'msg4')
        self.assertEqual(call_message['message_type'], 'Event/Call')

        # Check URI object message
        uri_message = next(msg for msg in result['messages'] if msg['message_id'] == 'msg5')
        self.assertEqual(uri_message['message_type'], 'RichText/UriObject')

    def test_transform_with_invalid_data(self):
        """Test transformation with invalid data."""
        # Call the transform method with invalid data
        # This should handle the errors gracefully
        result = self.pipeline.transform(self.invalid_sample_data, user_display_name="Test User")

        # Verify the result still has the basic structure
        self.assertIn('conversations', result)
        self.assertIn('messages', result)

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

    def test_load_with_no_connection(self):
        """Test loading with no database connection."""
        # Create a pipeline with no database connection
        pipeline = SkypeETLPipeline(output_dir=self.temp_dir)

        # Transform the data
        transformed_data = pipeline.transform(self.sample_data, user_display_name="Test User")

        # Call the load method
        export_id = pipeline.load(self.sample_data, transformed_data, file_source="test.json")

        # Verify the result is None (no database connection)
        self.assertIsNone(export_id)

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

    @patch('src.utils.file_handler.read_file_object')
    def test_run_pipeline_with_file_object(self, mock_read_file_object):
        """Test running the pipeline with a file object."""
        # Set up the mock
        mock_read_file_object.return_value = self.sample_data

        # Mock the cursor's fetchone method to return a specific export_id
        self.pipeline.cursor.fetchone.return_value = (1,)

        # Call the run_pipeline method
        result = self.pipeline.run_pipeline(file_obj=self.mock_file, user_display_name="Test User")

        # Verify the result
        self.assertIn('export_id', result)
        self.assertEqual(result['export_id'], 1)
        self.assertIn('conversations', result)
        self.assertEqual(len(result['conversations']), 1)
        self.assertIn('message_count', result)
        self.assertEqual(result['message_count'], 2)

    @patch('src.utils.file_handler.read_file')
    def test_run_pipeline_with_complex_data(self, mock_read_file):
        """Test running the pipeline with complex data."""
        # Set up the mock
        mock_read_file.return_value = self.complex_sample_data

        # Mock the cursor's fetchone method to return a specific export_id
        self.pipeline.cursor.fetchone.return_value = (1,)

        # Call the run_pipeline method
        result = self.pipeline.run_pipeline(file_path='test.json', user_display_name="Test User")

        # Verify the result
        self.assertIn('export_id', result)
        self.assertEqual(result['export_id'], 1)
        self.assertIn('conversations', result)
        self.assertEqual(len(result['conversations']), 3)
        self.assertIn('message_count', result)
        self.assertEqual(result['message_count'], 6)

    @patch('src.utils.file_handler.read_file')
    def test_run_pipeline_with_no_db(self, mock_read_file):
        """Test running the pipeline without a database connection."""
        # Set up the mock
        mock_read_file.return_value = self.sample_data

        # Create a pipeline with no database connection
        pipeline = SkypeETLPipeline(output_dir=self.temp_dir)

        # Call the run_pipeline method
        result = pipeline.run_pipeline(file_path='test.json', user_display_name="Test User")

        # Verify the result
        self.assertNotIn('export_id', result)  # No export ID without DB
        self.assertIn('conversations', result)
        self.assertEqual(len(result['conversations']), 1)
        self.assertIn('message_count', result)
        self.assertEqual(result['message_count'], 2)

    @patch('src.utils.file_handler.read_file')
    def test_run_pipeline_with_invalid_data(self, mock_read_file):
        """Test running the pipeline with invalid data."""
        # Set up the mock
        mock_read_file.return_value = self.invalid_sample_data

        # Call the run_pipeline method
        # This should handle the errors gracefully
        result = self.pipeline.run_pipeline(file_path='test.json', user_display_name="Test User")

        # Verify the result still has the basic structure
        self.assertIn('conversations', result)
        self.assertIn('message_count', result)


class TestETLPipelineIntegration(unittest.TestCase):
    """Integration tests for the ETL pipeline with a real database.

    These tests are skipped by default unless the POSTGRES_TEST_DB environment variable is set.
    """

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures for the integration tests."""
        # Check if we should run integration tests
        if not os.environ.get('POSTGRES_TEST_DB'):
            cls.skipTest = True
            return

        cls.skipTest = False

        # Load configuration
        config_file = os.environ.get('CONFIG_FILE')
        cls.config = load_config(config_file)

        # Override with test database
        cls.config['database']['dbname'] = os.environ.get('POSTGRES_TEST_DB')

        # Create a temporary directory for output
        cls.temp_dir = tempfile.mkdtemp()

        # Create a sample Skype export data
        cls.sample_data = {
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

        # Create a file with the sample data
        cls.sample_file = os.path.join(cls.temp_dir, 'sample.json')
        with open(cls.sample_file, 'w') as f:
            json.dump(cls.sample_data, f)

    @classmethod
    def tearDownClass(cls):
        """Tear down test fixtures for the integration tests."""
        if cls.skipTest:
            return

        # Remove the temporary directory
        import shutil
        shutil.rmtree(cls.temp_dir)

        # Clean up the test database
        try:
            conn = psycopg2.connect(**get_db_config(cls.config))
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS skype_messages;")
                cur.execute("DROP TABLE IF EXISTS skype_conversations;")
                cur.execute("DROP TABLE IF EXISTS skype_raw_exports;")
                conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error cleaning up test database: {e}")

    def setUp(self):
        """Set up test fixtures."""
        if self.skipTest:
            self.skipTest("Integration tests disabled. Set POSTGRES_TEST_DB to enable.")

        # Create the ETL pipeline with real database connection
        self.pipeline = SkypeETLPipeline(
            db_config=get_db_config(self.config),
            output_dir=self.temp_dir
        )

    def tearDown(self):
        """Tear down test fixtures."""
        if self.skipTest:
            return

        # Close the database connection
        if self.pipeline.conn:
            self.pipeline.conn.close()

    def test_integration_run_pipeline(self):
        """Test running the pipeline with a real database."""
        # Run the pipeline
        result = self.pipeline.run_pipeline(
            file_path=self.sample_file,
            user_display_name="Test User"
        )

        # Verify the result
        self.assertIn('export_id', result)
        self.assertIsNotNone(result['export_id'])
        self.assertIn('conversations', result)
        self.assertEqual(len(result['conversations']), 1)
        self.assertIn('message_count', result)
        self.assertEqual(result['message_count'], 2)

        # Verify the data was stored in the database
        with self.pipeline.conn.cursor() as cur:
            # Check raw exports
            cur.execute("SELECT COUNT(*) FROM skype_raw_exports")
            count = cur.fetchone()[0]
            self.assertGreaterEqual(count, 1)

            # Check conversations
            cur.execute("SELECT COUNT(*) FROM skype_conversations")
            count = cur.fetchone()[0]
            self.assertGreaterEqual(count, 1)

            # Check messages
            cur.execute("SELECT COUNT(*) FROM skype_messages")
            count = cur.fetchone()[0]
            self.assertGreaterEqual(count, 2)


if __name__ == '__main__':
    unittest.main()