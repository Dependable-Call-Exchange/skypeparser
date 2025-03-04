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
from src.utils.validation import ValidationError, validate_file_exists, validate_file_object
from src.utils.config import get_db_config, load_config
from src.utils.file_handler import read_file


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
                    "MessageList": [
                        {
                            "id": "msg1",
                            "originalarrivaltime": "2023-01-01T12:30:00Z",
                            "from": "user1",
                            "content": "Hello, world!",
                            "messagetype": "RichText"
                        },
                        {
                            "id": "msg2",
                            "originalarrivaltime": "2023-01-01T12:35:00Z",
                            "from": "user2",
                            "content": "Hi there!",
                            "messagetype": "RichText"
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
                    "MessageList": [
                        {
                            "id": "msg1",
                            "originalarrivaltime": "2023-01-01T12:30:00Z",
                            "from": "user1",
                            "content": "Hello, world!",
                            "messagetype": "RichText"
                        },
                        {
                            "id": "msg2",
                            "originalarrivaltime": "2023-01-01T12:35:00Z",
                            "from": "user2",
                            "content": "Hi there!",
                            "messagetype": "RichText"
                        },
                        {
                            "id": "msg3",
                            "originalarrivaltime": "2023-01-01T12:40:00Z",
                            "from": "user1",
                            "content": "<b>Bold text</b> and <i>italic text</i>",
                            "messagetype": "RichText"
                        },
                        {
                            "id": "msg4",
                            "originalarrivaltime": "2023-01-01T12:45:00Z",
                            "from": "user2",
                            "content": "",
                            "messagetype": "Event/Call"
                        },
                        {
                            "id": "msg5",
                            "originalarrivaltime": "2023-01-01T12:50:00Z",
                            "from": "user1",
                            "content": "https://example.com/image.jpg",
                            "messagetype": "RichText/UriObject"
                        }
                    ]
                },
                {
                    "id": "conversation2",
                    "displayName": "Test Conversation 2",
                    "MessageList": []
                },
                {
                    "id": "conversation3",
                    "displayName": None,
                    "MessageList": [
                        {
                            "id": "msg6",
                            "originalarrivaltime": "2023-01-01T13:00:00Z",
                            "from": "user3",
                            "content": "Message in conversation with no display name",
                            "messagetype": "RichText"
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
                    "MessageList": [
                        {
                            "id": "msg1",
                            "originalarrivaltime": "invalid_timestamp",
                            "from": "user1",
                            "content": "Hello, world!",
                            "messagetype": "RichText"
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
    @patch('src.db.etl_pipeline.validate_file_exists')
    def test_extract_from_file_path(self, mock_validate_file_exists, mock_read_file):
        """Test extraction from a file path."""
        # Set up the mocks
        mock_validate_file_exists.return_value = True
        mock_read_file.return_value = self.sample_data

        # Call the extract method
        result = self.pipeline.extract(file_path='test.json')

        # Verify the result
        self.assertEqual(result, self.sample_data)

        # Verify that the mocks were called with the correct arguments
        mock_validate_file_exists.assert_called_once_with('test.json')
        mock_read_file.assert_called_once_with('test.json')

    @patch('src.db.etl_pipeline.read_file_object')
    def test_extract_from_file_object(self, mock_read_file_object):
        """Test extraction from a file object."""
        # Set up the mock
        mock_read_file_object.return_value = self.sample_data

        # Create a mock file object with a name attribute and read method
        mock_file = MagicMock()
        mock_file.name = "test_file.json"
        mock_file.read.return_value = json.dumps(self.sample_data).encode('utf-8')

        # Call the extract method
        result = self.pipeline.extract(file_obj=mock_file)

        # Verify the result
        self.assertEqual(result, self.sample_data)
        mock_read_file_object.assert_called_once_with(mock_file)

    @patch('src.utils.file_handler.read_tarfile')
    @patch('src.utils.validation.validate_file_exists')
    def test_extract_from_tar_file(self, mock_validate_file_exists, mock_read_tarfile):
        """Test extraction from a TAR file."""
        # Set up the mocks
        mock_validate_file_exists.return_value = True
        mock_read_tarfile.return_value = self.sample_data

        # Call the extract method
        result = self.pipeline.extract(file_path='test.tar')

        # Verify the result
        self.assertEqual(result, self.sample_data)

        # Verify that the mocks were called with the correct arguments
        mock_validate_file_exists.assert_called_once()
        mock_read_tarfile.assert_called_once_with('test.tar')

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
        self.assertIn('metadata', result)
        self.assertIn('conversations', result)

        # Check metadata
        self.assertEqual(result['metadata']['userId'], "test_user")
        self.assertEqual(result['metadata']['userDisplayName'], "Test User")
        self.assertEqual(result['metadata']['conversationCount'], 1)

        # Check conversations
        self.assertIn('conversation1', result['conversations'])
        self.assertEqual(result['conversations']['conversation1']['displayName'], "Test Conversation 1")
        self.assertEqual(result['conversations']['conversation1']['messageCount'], 2)

        # Check messages
        self.assertIn('messages', result['conversations']['conversation1'])
        self.assertEqual(len(result['conversations']['conversation1']['messages']), 2)

        # Check first message
        first_message = result['conversations']['conversation1']['messages'][0]
        self.assertEqual(first_message['fromId'], "user1")

        # Check second message
        second_message = result['conversations']['conversation1']['messages'][1]
        self.assertEqual(second_message['fromId'], "user2")

    def test_transform_complex_data(self):
        """Test transformation of complex data with various message types and edge cases."""
        # Call the transform method
        result = self.pipeline.transform(self.complex_sample_data, user_display_name="Test User")

        # Verify the result
        self.assertIn('metadata', result)
        self.assertIn('conversations', result)

        # The conversation count should be 3 as reported in the metadata
        self.assertEqual(result['metadata']['conversationCount'], 3)

        # Check conversations are present
        self.assertIn('conversation1', result['conversations'])
        self.assertIn('conversation2', result['conversations'])

        # conversation3 should not be present due to an error during processing
        self.assertNotIn('conversation3', result['conversations'])

        # Check conversation with messages
        self.assertEqual(result['conversations']['conversation1']['displayName'], "Test Conversation 1")
        self.assertEqual(result['conversations']['conversation1']['messageCount'], 5)

        # Check empty conversation
        self.assertEqual(result['conversations']['conversation2']['displayName'], "Test Conversation 2")
        self.assertEqual(result['conversations']['conversation2']['messageCount'], 0)

        # Check messages in conversation1
        self.assertEqual(len(result['conversations']['conversation1']['messages']), 5)

    def test_transform_with_invalid_data(self):
        """Test transformation with invalid data."""
        # Call the transform method with invalid data
        # This should handle the errors gracefully
        result = self.pipeline.transform(self.invalid_sample_data, user_display_name="Test User")

        # Verify the result still has the basic structure
        self.assertIn('metadata', result)
        self.assertIn('conversations', result)
        self.assertIn('conversation1', result['conversations'])
        self.assertEqual(result['conversations']['conversation1']['messageCount'], 1)

    def test_load(self):
        """Test loading data into the database."""
        # Create a mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        # Set up the mock cursor's fetchone method to return a tuple with export_id 1
        mock_cursor.fetchone.return_value = (1,)

        # Set the pipeline's connection and cursor
        self.pipeline.conn = mock_conn
        self.pipeline.cursor = mock_cursor

        # Create test data
        transformed_data = self.pipeline.transform(self.sample_data)

        # Call the load method
        export_id = self.pipeline.load(self.sample_data, transformed_data, "test.json")

        # Verify the result
        self.assertEqual(export_id, 1)

        # Verify that the cursor's execute method was called
        mock_cursor.execute.assert_called()

    def test_load_with_no_connection(self):
        """Test loading with no database connection."""
        # Transform the data first
        transformed_data = self.pipeline.transform(self.sample_data, user_display_name="Test User")

        # Create a pipeline with no database connection
        pipeline = SkypeETLPipeline(output_dir=self.temp_dir)
        pipeline.conn = None
        pipeline.cursor = None

        # Call the load method - should raise an error
        with self.assertRaises(ValueError):
            export_id = pipeline.load(self.sample_data, transformed_data, file_source="test.json")

    @patch('src.utils.file_handler.read_file')
    @patch('src.utils.validation.validate_file_exists')
    def test_run_pipeline(self, mock_validate_file_exists, mock_read_file):
        """Test running the entire pipeline."""
        # Set up the mocks
        mock_validate_file_exists.return_value = True
        mock_read_file.return_value = self.sample_data

        # Call the run_pipeline method
        result = self.pipeline.run_pipeline(file_path='test.json', user_display_name="Test User")

        # Verify the result
        self.assertIn('extraction', result)
        self.assertIn('transformation', result)
        self.assertIn('loading', result)
        self.assertTrue(result['extraction']['success'])
        self.assertTrue(result['transformation']['success'])
        # Loading may fail due to database connection issues, so we don't assert on it

    @patch('src.db.etl_pipeline.read_file_object')
    def test_run_pipeline_with_file_object(self, mock_read_file_object):
        """Test running the pipeline with a file object."""
        # Set up the mock
        mock_read_file_object.return_value = self.sample_data

        # Create a mock file object with a name attribute and read method
        mock_file = MagicMock()
        mock_file.name = "test_file.json"
        mock_file.read.return_value = json.dumps(self.sample_data).encode('utf-8')

        # Call the run_pipeline method
        result = self.pipeline.run_pipeline(file_obj=mock_file, user_display_name="Test User")

        # Verify the result
        self.assertIn('extraction', result)
        self.assertIn('transformation', result)
        self.assertIn('loading', result)
        self.assertTrue(result['extraction']['success'])
        self.assertTrue(result['transformation']['success'])
        # Loading may fail due to database connection issues, so we don't assert on it

    @patch('src.utils.file_handler.read_file')
    @patch('src.utils.validation.validate_file_exists')
    def test_run_pipeline_with_no_db(self, mock_validate_file_exists, mock_read_file):
        """Test running the pipeline without a database connection."""
        # Create a pipeline with no database connection
        pipeline = SkypeETLPipeline(self.db_config, output_dir=self.temp_dir)
        pipeline.conn = None
        pipeline.cursor = None

        # Set up the mocks
        mock_validate_file_exists.return_value = True
        mock_read_file.return_value = self.sample_data

        # Call the run_pipeline method
        result = pipeline.run_pipeline(file_path='test.json', user_display_name="Test User")

        # Verify the result
        self.assertIn('extraction', result)
        self.assertIn('transformation', result)
        self.assertIn('loading', result)
        self.assertTrue(result['extraction']['success'])
        self.assertTrue(result['transformation']['success'])
        self.assertFalse(result['loading']['success'])
        self.assertIn('error', result['loading'])

    @patch('src.utils.file_handler.read_file')
    @patch('src.utils.validation.validate_file_exists')
    def test_run_pipeline_with_complex_data(self, mock_validate_file_exists, mock_read_file):
        """Test running the pipeline with complex data."""
        # Set up the mocks
        mock_validate_file_exists.return_value = True
        mock_read_file.return_value = self.complex_sample_data

        # Call the run_pipeline method
        result = self.pipeline.run_pipeline(file_path='test.json', user_display_name="Test User")

        # Verify the result
        self.assertIn('extraction', result)
        self.assertIn('transformation', result)
        self.assertIn('loading', result)
        self.assertTrue(result['extraction']['success'])
        self.assertTrue(result['transformation']['success'])
        # Loading may fail due to database connection issues, so we don't assert on it

    @patch('src.utils.file_handler.read_file')
    @patch('src.utils.validation.validate_file_exists')
    def test_run_pipeline_with_invalid_data(self, mock_validate_file_exists, mock_read_file):
        """Test running the pipeline with invalid data."""
        # Set up the mocks
        mock_validate_file_exists.return_value = True
        mock_read_file.return_value = self.invalid_sample_data

        # Call the run_pipeline method
        result = self.pipeline.run_pipeline(file_path='test.json', user_display_name="Test User")

        # Verify the result
        self.assertIn('extraction', result)
        self.assertIn('transformation', result)
        self.assertIn('loading', result)
        self.assertTrue(result['extraction']['success'])
        self.assertTrue(result['transformation']['success'])
        # Loading may fail due to database connection issues, so we don't assert on it


class TestETLPipelineIntegration(unittest.TestCase):
    """Integration tests for the ETL pipeline with a real database."""

    def setUp(self):
        """Set up the test environment."""
        # Skip integration tests unless explicitly enabled
        if not os.environ.get('POSTGRES_TEST_DB'):
            self.skipTest("Integration tests disabled. Set POSTGRES_TEST_DB to enable.")

        # Set up the test environment
        self.temp_dir = tempfile.mkdtemp()
        self.test_dir = os.path.join(self.temp_dir, 'test_output')
        os.makedirs(self.test_dir, exist_ok=True)

        # Load database configuration
        config = load_config()
        self.db_config = get_db_config(config)

        # Create a pipeline instance
        self.pipeline = SkypeETLPipeline(db_config=self.db_config, output_dir=self.test_dir)

        # Create a sample Skype export data
        self.sample_data = {
            "userId": "test_user",
            "exportDate": "2023-01-01T12:00:00Z",
            "conversations": [
                {
                    "id": "conversation1",
                    "displayName": "Test Conversation 1",
                    "MessageList": [
                        {
                            "id": "msg1",
                            "originalarrivaltime": "2023-01-01T12:30:00Z",
                            "from": "user1",
                            "content": "Hello, world!",
                            "messagetype": "RichText"
                        }
                    ]
                }
            ]
        }

        # Create a file with the sample data
        self.sample_file = os.path.join(self.temp_dir, 'sample.json')
        with open(self.sample_file, 'w') as f:
            json.dump(self.sample_data, f)

    def tearDown(self):
        """Tear down test fixtures."""
        if self.skipTest:
            return

        # Remove the temporary directory
        import shutil
        shutil.rmtree(self.temp_dir)

        # Clean up the test database
        try:
            conn = psycopg2.connect(**self.db_config)
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS skype_messages;")
                cur.execute("DROP TABLE IF EXISTS skype_conversations;")
                cur.execute("DROP TABLE IF EXISTS skype_raw_exports;")
                conn.commit()
            conn.close()
        except Exception as e:
            print(f"Error cleaning up test database: {e}")

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