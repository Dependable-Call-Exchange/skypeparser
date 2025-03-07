#!/usr/bin/env python3
"""
Example of using SOLIDSkypeETLPipeline with proper dependency injection.

This module demonstrates how to use the SOLIDSkypeETLPipeline class with
proper dependency injection, following SOLID principles.
"""

import unittest
from unittest.mock import MagicMock

from src.db.etl_pipeline import SOLIDSkypeETLPipeline, create_solid_skype_etl_pipeline
from src.utils.file_handler import FileHandler
from tests.fixtures import (
    BASIC_SKYPE_DATA,
    MockDatabase,
    MockValidationService,
    create_mock_file_environment,
)


class TestSOLIDSkypeETLPipeline(unittest.TestCase):
    """
    Tests for the SOLIDSkypeETLPipeline class.
    """

    def setUp(self):
        """
        Set up test fixtures before each test method.
        """
        # Setup test data
        self.test_file_path = "test.json"
        self.test_user_display_name = "Test User"
        self.test_data = BASIC_SKYPE_DATA

        # Add a minimal db_config for tests
        self.test_db_config = {
            "host": "localhost",
            "port": 5432,
            "dbname": "test_db",
            "user": "test_user",
            "password": "test_password",
        }

    def test_solid_pipeline_with_dependency_injection(self):
        """
        Test the SOLIDSkypeETLPipeline with proper dependency injection.
        """
        # Create mock environment
        mock_env = create_mock_file_environment({self.test_file_path: self.test_data})

        # Create mock database
        mock_db = MockDatabase()

        # Create mock validation service
        mock_validation_service = MockValidationService(
            {self.test_file_path: self.test_data}
        )
        mock_validation_service.user_display_name_return_value = (
            self.test_user_display_name
        )

        # Create mock file handler
        mock_file_handler = MagicMock(spec=FileHandler)
        mock_file_handler.read_file.return_value = self.test_data

        # Create mock content extractor
        mock_content_extractor = MagicMock()
        mock_content_extractor.extract_mentions = MagicMock(return_value=[])
        mock_content_extractor.extract_content = MagicMock(return_value="Test content")
        mock_content_extractor.extract_html_content = MagicMock(
            return_value="<p>Test content</p>"
        )

        # Create mock structured data extractor
        mock_structured_data_extractor = MagicMock()
        mock_structured_data_extractor.extract_structured_data = MagicMock(
            return_value={}
        )

        # Create mock message handler factory
        mock_message_handler = MagicMock()
        mock_message_handler.can_handle = MagicMock(return_value=True)
        mock_message_handler.extract_data = MagicMock(return_value={})

        mock_message_handler_factory = MagicMock()
        mock_message_handler_factory.get_handler = MagicMock(
            return_value=mock_message_handler
        )

        # Create pipeline with all dependencies injected
        pipeline = SOLIDSkypeETLPipeline(
            db_config=self.test_db_config,
            file_handler=mock_file_handler,
            validation_service=mock_validation_service,
            db_connection=mock_db.conn,
            content_extractor=mock_content_extractor,
            structured_data_extractor=mock_structured_data_extractor,
            message_handler_factory=mock_message_handler_factory,
        )

        # Run the pipeline
        result = pipeline.run_pipeline(
            file_path=self.test_file_path, user_display_name=self.test_user_display_name
        )

        # Assertions
        self.assertIn("extraction", result)
        self.assertIn("transformation", result)
        self.assertIn("loading", result)
        self.assertTrue(result["extraction"]["success"])
        self.assertTrue(result["transformation"]["success"])
        self.assertTrue(result["loading"]["success"])

        # Verify function calls
        mock_validation_service.validate_file_exists_mock.assert_called_with(
            self.test_file_path
        )
        mock_validation_service.validate_user_display_name_mock.assert_called_with(
            self.test_user_display_name
        )
        mock_file_handler.read_file.assert_called_with(self.test_file_path)

        # Check database queries
        executed_queries = mock_db.get_executed_queries()
        self.assertTrue(any("INSERT INTO" in query for query in executed_queries))

    def test_factory_function(self):
        """
        Test the factory function for creating a SOLIDSkypeETLPipeline.
        """
        # Create mock validation service
        mock_validation_service = MockValidationService()
        mock_validation_service.user_display_name_return_value = (
            self.test_user_display_name
        )

        # Create mock database
        mock_db = MockDatabase()

        # Create pipeline using factory function
        pipeline = create_solid_skype_etl_pipeline(
            db_config=self.test_db_config,
            validation_service=mock_validation_service,
            db_connection=mock_db.conn,
        )

        # Assertions
        self.assertIsInstance(pipeline, SOLIDSkypeETLPipeline)
        self.assertEqual(pipeline.validation_service, mock_validation_service)
        self.assertEqual(pipeline.db_connection, mock_db.conn)


if __name__ == "__main__":
    unittest.main()
