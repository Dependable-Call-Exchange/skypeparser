#!/usr/bin/env python3
"""
Unit tests for the ETL pipeline module.

This test suite provides comprehensive testing for the ETL pipeline,
using dependency injection with TestableETLPipeline instead of extensive patching.
"""

import os
import sys
import unittest
import tempfile
import json
import io
import pytest
from unittest.mock import MagicMock

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.testable_etl_pipeline import TestableETLPipeline
from src.utils.validation import ValidationError
from tests.fixtures import (
    BASIC_SKYPE_DATA,
    COMPLEX_SKYPE_DATA,
    INVALID_SKYPE_DATA,
    MockFileReader,
    MockDatabase,
    create_mock_file_environment
)


@pytest.mark.etl_pipeline
class TestETLPipeline(unittest.TestCase):
    """Test cases for the ETL pipeline using TestableETLPipeline."""

    def setUp(self):
        """Set up the test environment."""
        # Create a temporary directory for testing
        self.temp_dir = tempfile.mkdtemp()

        # Set up mock file reader with sample data
        self.mock_file_reader = MockFileReader(BASIC_SKYPE_DATA)
        self.mock_file_reader.add_file('complex.json', COMPLEX_SKYPE_DATA)
        self.mock_file_reader.add_file('invalid.json', INVALID_SKYPE_DATA)

        # Set up mock database
        self.mock_db = MockDatabase()

        # Set up mock validation functions
        self.mock_validate_file_exists = MagicMock(return_value=True)
        self.mock_validate_json_file = MagicMock(return_value=BASIC_SKYPE_DATA)
        self.mock_validate_skype_data = MagicMock(return_value=True)
        self.mock_validate_user_display_name = MagicMock(return_value="Test User")

        # Create testable pipeline with injected dependencies
        self.pipeline = TestableETLPipeline(
            output_dir=self.temp_dir,
            # File operations
            read_file_func=self.mock_file_reader.read_file,
            read_file_object_func=self.mock_file_reader.read_file_object,
            # Validation functions
            validate_file_exists_func=self.mock_validate_file_exists,
            validate_json_file_func=self.mock_validate_json_file,
            validate_skype_data_func=self.mock_validate_skype_data,
            validate_user_display_name_func=self.mock_validate_user_display_name,
            # Database connection
            db_connection=self.mock_db.conn
        )

    def tearDown(self):
        """Tear down test fixtures."""
        # Clean up temporary directory
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_extract_from_file_path(self):
        """Test extracting data from a file path."""
        # Extract data from a file path
        result = self.pipeline.extract(file_path='test.json')

        # Verify the result
        self.assertEqual(result['userId'], BASIC_SKYPE_DATA['userId'])
        self.assertEqual(result['exportDate'], BASIC_SKYPE_DATA['exportDate'])
        self.assertEqual(len(result['conversations']), len(BASIC_SKYPE_DATA['conversations']))

        # Verify that the injected functions were called
        self.mock_validate_file_exists.assert_called_with('test.json')
        self.mock_validate_json_file.assert_called_with('test.json')
        self.mock_validate_skype_data.assert_called()

    def test_extract_from_file_object(self):
        """Test extracting data from a file object."""
        # Create a file-like object
        file_obj = io.StringIO(json.dumps(BASIC_SKYPE_DATA))
        file_obj.name = 'test.json'

        # Set up mock for file object validation
        mock_validate_file_object = MagicMock(return_value=True)

        # Create a new pipeline with the mock
        pipeline = TestableETLPipeline(
            read_file_object_func=self.mock_file_reader.read_file_object,
            validate_file_object_func=mock_validate_file_object,
            validate_skype_data_func=self.mock_validate_skype_data
        )

        # Extract data from the file object
        result = pipeline.extract(file_obj=file_obj)

        # Verify the result
        self.assertEqual(result['userId'], BASIC_SKYPE_DATA['userId'])
        self.assertEqual(result['exportDate'], BASIC_SKYPE_DATA['exportDate'])

        # Verify that the injected functions were called
        mock_validate_file_object.assert_called_once()
        self.mock_validate_skype_data.assert_called()

    def test_extract_from_tar_file(self):
        """Test extracting data from a TAR file."""
        # Set up mock for tar file operations
        mock_read_tarfile = MagicMock(return_value=BASIC_SKYPE_DATA)
        mock_validate_tar_file = MagicMock(return_value=True)

        # Create a new pipeline with the mocks
        pipeline = TestableETLPipeline(
            read_tarfile_func=mock_read_tarfile,
            validate_file_exists_func=self.mock_validate_file_exists,
            validate_tar_file_func=mock_validate_tar_file,
            validate_skype_data_func=self.mock_validate_skype_data
        )

        # Extract data from a tar file
        result = pipeline.extract(file_path='test.tar')

        # Verify the result
        self.assertEqual(result['userId'], BASIC_SKYPE_DATA['userId'])

        # Verify that the injected functions were called
        self.mock_validate_file_exists.assert_called_with('test.tar')
        mock_validate_tar_file.assert_called_with('test.tar')
        mock_read_tarfile.assert_called_with('test.tar', auto_select=True)
        self.mock_validate_skype_data.assert_called()

    def test_extract_with_no_file(self):
        """Test extracting data with no file provided."""
        with self.assertRaises(ValueError):
            self.pipeline.extract()

    def test_transform(self):
        """Test transforming Skype data with descriptive assertions."""
        # Transform data
        result = self.pipeline.transform(BASIC_SKYPE_DATA)

        # Verify the structure of the result
        self.assertIn('metadata', result, "Result should contain metadata")
        self.assertIn('conversations', result, "Result should contain conversations")

        # Verify metadata
        metadata = result['metadata']
        self.assertEqual(metadata['userId'], BASIC_SKYPE_DATA['userId'], "User ID should be preserved")
        self.assertEqual(metadata['exportDate'], BASIC_SKYPE_DATA['exportDate'], "Export date should be preserved")
        self.assertEqual(metadata['conversationCount'], len(BASIC_SKYPE_DATA['conversations']),
                         "Conversation count should match input")

        # Verify conversations
        conversations = result['conversations']
        self.assertEqual(len(conversations), len(BASIC_SKYPE_DATA['conversations']),
                         "Number of conversations should match input")

        # Check the first conversation
        conv_id = BASIC_SKYPE_DATA['conversations'][0]['id']
        self.assertIn(conv_id, conversations, "Conversation ID should be preserved")

        conversation = conversations[conv_id]
        self.assertEqual(conversation['id'], conv_id, "Conversation ID should match")
        self.assertEqual(conversation['displayName'], BASIC_SKYPE_DATA['conversations'][0]['displayName'],
                         "Display name should be preserved")

        # Check messages
        messages = conversation['messages']
        self.assertGreaterEqual(len(messages), 1, "Should have at least one message")

        message = messages[0]
        self.assertIn('timestamp', message, "Message should have timestamp")
        self.assertIn('fromId', message, "Message should have sender ID")
        self.assertIn('rawContent', message, "Message should have content")

    def test_transform_complex_data(self):
        """Test transforming complex Skype data."""
        # Transform complex data
        result = self.pipeline.transform(COMPLEX_SKYPE_DATA)

        # Verify the structure of the result
        self.assertIn('metadata', result)
        self.assertIn('conversations', result)

        # Verify metadata
        metadata = result['metadata']
        self.assertEqual(metadata['userId'], COMPLEX_SKYPE_DATA['userId'])
        self.assertEqual(metadata['conversationCount'], len(COMPLEX_SKYPE_DATA['conversations']))

        # Verify conversations
        conversations = result['conversations']
        self.assertEqual(len(conversations), len(COMPLEX_SKYPE_DATA['conversations']))

        # Check that all conversation IDs are preserved
        for conv in COMPLEX_SKYPE_DATA['conversations']:
            self.assertIn(conv['id'], conversations)

    def test_transform_with_invalid_data(self):
        """Test transforming invalid Skype data."""
        # Set up mock to raise ValidationError
        mock_validate_skype_data = MagicMock(side_effect=ValidationError("Invalid data"))

        # Create a new pipeline with the mock
        pipeline = TestableETLPipeline(
            validate_skype_data_func=mock_validate_skype_data
        )

        # Attempt to transform invalid data
        with self.assertRaises(ValidationError):
            pipeline.transform(INVALID_SKYPE_DATA)

    def test_transform_skips_conversations_with_none_display_name(self):
        """Test that conversations with None display name are skipped during transformation."""
        # Create test data with a conversation that has None display name
        test_data = {
            "userId": "test_user",
            "exportDate": "2023-01-01T12:00:00Z",
            "conversations": [
                {
                    "id": "conversation1",
                    "displayName": None,
                    "MessageList": [
                        {
                            "id": "msg1",
                            "originalarrivaltime": "2023-01-01T12:30:00Z",
                            "from": "user1",
                            "content": "Hello, world!",
                            "messagetype": "RichText"
                        }
                    ]
                },
                {
                    "id": "conversation2",
                    "displayName": "",
                    "MessageList": [
                        {
                            "id": "msg2",
                            "originalarrivaltime": "2023-01-01T12:35:00Z",
                            "from": "user2",
                            "content": "Hello again!",
                            "messagetype": "RichText"
                        }
                    ]
                }
            ]
        }

        # Transform the data
        result = self.pipeline.transform(test_data)

        # Verify that only one conversation remains (the one with empty string display name)
        self.assertEqual(len(result['conversations']), 1, "Conversation with None display name should be skipped")
        self.assertIn('conversation2', result['conversations'], "Conversation with empty string should be kept")
        self.assertNotIn('conversation1', result['conversations'], "Conversation with None display name should be skipped")

    def test_load(self):
        """Test loading data into the database with specific query assertions."""
        # Transform data first
        transformed_data = self.pipeline.transform(BASIC_SKYPE_DATA)

        # Load data
        export_id = self.pipeline.load(BASIC_SKYPE_DATA, transformed_data, file_source='test.json')

        # Verify the result
        self.assertIsNotNone(export_id)

        # Verify that queries were executed
        executed_queries = self.mock_db.get_executed_queries()
        self.assertGreaterEqual(len(executed_queries), 1)

        # Check for specific query patterns
        insert_patterns = [
            "INSERT INTO skype_raw_exports",
            "INSERT INTO skype_conversations",
            "INSERT INTO skype_messages"
        ]

        for pattern in insert_patterns:
            self.assertTrue(
                any(pattern in query for query in executed_queries),
                f"Expected query pattern '{pattern}' not found in executed queries"
            )

    def test_load_with_no_connection(self):
        """Test loading data with no database connection."""
        # Create a pipeline with no database connection
        pipeline = TestableETLPipeline()

        # Transform data
        transformed_data = pipeline.transform(BASIC_SKYPE_DATA)

        # Attempt to load data
        with self.assertRaises(ValueError):
            pipeline.load(BASIC_SKYPE_DATA, transformed_data)

    def test_run_pipeline(self):
        """Test running the complete pipeline."""
        # Run the pipeline
        result = self.pipeline.run_pipeline(file_path='test.json', user_display_name="Test User")

        # Verify the results
        self.assertIn('extraction', result)
        self.assertIn('transformation', result)
        self.assertIn('loading', result)
        self.assertTrue(result['extraction']['success'])
        self.assertTrue(result['transformation']['success'])
        self.assertTrue(result['loading']['success'])

        # Verify that the injected functions were called
        self.mock_validate_file_exists.assert_called_with('test.json')
        self.mock_validate_json_file.assert_called_with('test.json')
        self.mock_validate_skype_data.assert_called()
        self.mock_validate_user_display_name.assert_called_with("Test User")

    def test_run_pipeline_with_file_object(self):
        """Test running the pipeline with a file object."""
        # Create a file-like object
        file_obj = io.StringIO(json.dumps(BASIC_SKYPE_DATA))
        file_obj.name = 'test.json'

        # Set up mock for file object validation
        mock_validate_file_object = MagicMock(return_value=True)

        # Create a new pipeline with the mock
        pipeline = TestableETLPipeline(
            read_file_object_func=self.mock_file_reader.read_file_object,
            validate_file_object_func=mock_validate_file_object,
            validate_skype_data_func=self.mock_validate_skype_data,
            validate_user_display_name_func=self.mock_validate_user_display_name,
            db_connection=self.mock_db.conn
        )

        # Run the pipeline with the file object
        result = pipeline.run_pipeline(file_obj=file_obj, user_display_name="Test User")

        # Verify the results
        self.assertTrue(result['extraction']['success'])
        self.assertTrue(result['transformation']['success'])
        self.assertTrue(result['loading']['success'])

    def test_run_pipeline_with_no_db(self):
        """Test running the pipeline without a database connection."""
        # Create a pipeline with no database connection
        pipeline = TestableETLPipeline(
            read_file_func=self.mock_file_reader.read_file,
            validate_file_exists_func=self.mock_validate_file_exists,
            validate_json_file_func=self.mock_validate_json_file,
            validate_skype_data_func=self.mock_validate_skype_data,
            validate_user_display_name_func=self.mock_validate_user_display_name
        )

        # Run the pipeline
        result = pipeline.run_pipeline(file_path='test.json', user_display_name="Test User")

        # Verify the results
        self.assertTrue(result['extraction']['success'])
        self.assertTrue(result['transformation']['success'])
        self.assertIsNone(result['loading'])

    def test_run_pipeline_with_complex_data(self):
        """Test running the pipeline with complex data."""
        # Set up the mock to return complex data
        self.mock_validate_json_file.return_value = COMPLEX_SKYPE_DATA

        # Run the pipeline
        result = self.pipeline.run_pipeline(file_path='complex.json', user_display_name="Test User")

        # Verify the results
        self.assertTrue(result['extraction']['success'])
        self.assertTrue(result['transformation']['success'])
        self.assertTrue(result['loading']['success'])
        self.assertEqual(result['extraction']['userId'], COMPLEX_SKYPE_DATA['userId'])
        self.assertEqual(result['transformation']['conversationCount'], len(COMPLEX_SKYPE_DATA['conversations']))

    def test_run_pipeline_with_invalid_data(self):
        """Test running the pipeline with invalid data."""
        # Set up the mock to return invalid data
        self.mock_validate_json_file.return_value = INVALID_SKYPE_DATA

        # Set up mock to raise ValidationError
        mock_validate_skype_data = MagicMock(side_effect=ValidationError("Invalid data"))

        # Create a new pipeline with the mock
        pipeline = TestableETLPipeline(
            read_file_func=self.mock_file_reader.read_file,
            validate_file_exists_func=self.mock_validate_file_exists,
            validate_json_file_func=self.mock_validate_json_file,
            validate_skype_data_func=mock_validate_skype_data
        )

        # Attempt to run the pipeline
        with self.assertRaises(ValidationError):
            pipeline.run_pipeline(file_path='invalid.json')


if __name__ == '__main__':
    unittest.main()