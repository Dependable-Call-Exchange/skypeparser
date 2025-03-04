#!/usr/bin/env python3
"""
Tests for the TestableETLPipeline class.

This module demonstrates how to use the TestableETLPipeline class with
dependency injection to simplify testing without extensive patching.
"""

import unittest
import pytest
from unittest.mock import MagicMock

from src.db.testable_etl_pipeline import TestableETLPipeline
from tests.fixtures import (
    BASIC_SKYPE_DATA,
    COMPLEX_SKYPE_DATA,
    MockFileReader,
    MockDatabase,
    create_mock_file_environment
)


@pytest.mark.etl_pipeline
class TestTestableETLPipeline(unittest.TestCase):
    """
    Test cases for the TestableETLPipeline class.
    """

    def setUp(self):
        """
        Set up test fixtures before each test method.
        """
        # Create mock file reader
        self.mock_file_reader = MockFileReader(BASIC_SKYPE_DATA)
        self.mock_file_reader.add_file('complex.json', COMPLEX_SKYPE_DATA)

        # Create mock database
        self.mock_db = MockDatabase()

        # Create mock validation functions
        self.mock_validate_file_exists = MagicMock(return_value=True)
        self.mock_validate_json_file = MagicMock(return_value=BASIC_SKYPE_DATA)
        self.mock_validate_skype_data = MagicMock(return_value=True)
        self.mock_validate_user_display_name = MagicMock(return_value="Test User")

        # Create mock config functions
        self.mock_get_message_type_description = MagicMock(return_value="Test Message Type")

        # Create testable pipeline with injected dependencies
        self.pipeline = TestableETLPipeline(
            # File operations
            read_file_func=self.mock_file_reader.read_file,
            read_file_object_func=self.mock_file_reader.read_file_object,
            # Validation functions
            validate_file_exists_func=self.mock_validate_file_exists,
            validate_json_file_func=self.mock_validate_json_file,
            validate_skype_data_func=self.mock_validate_skype_data,
            validate_user_display_name_func=self.mock_validate_user_display_name,
            # Database connection
            db_connection=self.mock_db.conn,
            # Config functions
            get_message_type_description_func=self.mock_get_message_type_description,
        )

    def test_extract_with_dependency_injection(self):
        """
        Test the extract method with dependency injection.
        """
        # Extract data from a file path
        result = self.pipeline.extract(file_path='test.json')

        # Verify the result
        self.assertEqual(result['userId'], BASIC_SKYPE_DATA['userId'])
        self.assertEqual(result['exportDate'], BASIC_SKYPE_DATA['exportDate'])
        self.assertEqual(len(result['conversations']), len(BASIC_SKYPE_DATA['conversations']))

        # Verify that the injected functions were called
        self.mock_validate_file_exists.assert_called_once_with('test.json')
        self.mock_validate_json_file.assert_called_once_with('test.json')
        self.mock_validate_skype_data.assert_called_once()

    def test_transform_with_dependency_injection(self):
        """
        Test the transform method with dependency injection.
        """
        # Transform data
        result = self.pipeline.transform(BASIC_SKYPE_DATA, user_display_name="Test User")

        # Verify the result
        self.assertIn('metadata', result)
        self.assertIn('conversations', result)
        self.assertEqual(result['metadata']['userDisplayName'], "Test User")
        self.assertEqual(result['metadata']['userId'], BASIC_SKYPE_DATA['userId'])

        # Verify that the injected functions were called
        self.mock_validate_skype_data.assert_called_once()
        self.mock_validate_user_display_name.assert_called_once_with("Test User")

    def test_load_with_dependency_injection(self):
        """
        Test the load method with dependency injection.
        """
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

    def test_run_pipeline_with_dependency_injection(self):
        """
        Test the run_pipeline method with dependency injection.
        """
        # Run the pipeline
        results = self.pipeline.run_pipeline(file_path='test.json', user_display_name="Test User")

        # Verify the results
        self.assertIn('extraction', results)
        self.assertIn('transformation', results)
        self.assertIn('loading', results)
        self.assertTrue(results['extraction']['success'])
        self.assertTrue(results['transformation']['success'])
        self.assertTrue(results['loading']['success'])

        # Verify that the injected functions were called
        self.mock_validate_file_exists.assert_called_with('test.json')
        self.mock_validate_json_file.assert_called_with('test.json')
        self.mock_validate_skype_data.assert_called()
        self.mock_validate_user_display_name.assert_called_with("Test User")

    def test_using_mock_file_environment(self):
        """
        Test using the create_mock_file_environment fixture.
        """
        # Create a mock environment
        mock_env = create_mock_file_environment({
            'test.json': BASIC_SKYPE_DATA,
            'complex.json': COMPLEX_SKYPE_DATA
        })

        # Create a pipeline with the mock environment
        pipeline = TestableETLPipeline(
            validate_file_exists_func=mock_env['validate_file_exists'],
            validate_json_file_func=mock_env['validate_json_file'],
            read_file_func=mock_env['read_file'],
            db_connection=self.mock_db.conn
        )

        # Run the pipeline
        results = pipeline.run_pipeline(file_path='complex.json')

        # Verify the results
        self.assertTrue(results['extraction']['success'])
        self.assertTrue(results['transformation']['success'])
        self.assertTrue(results['loading']['success'])
        self.assertEqual(results['extraction']['userId'], COMPLEX_SKYPE_DATA['userId'])
        self.assertEqual(
            results['transformation']['conversationCount'],
            len(COMPLEX_SKYPE_DATA['conversations'])
        )


if __name__ == '__main__':
    unittest.main()