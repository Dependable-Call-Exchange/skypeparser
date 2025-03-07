#!/usr/bin/env python3
"""
Example of using SOLIDSkypeETLPipeline with improved mock functions.

This module demonstrates how to use the SOLIDSkypeETLPipeline class with
the improved create_mock_functions approach for testing.
"""

import os
import unittest
from unittest.mock import MagicMock, patch

from src.db.etl_pipeline import SOLIDSkypeETLPipeline, create_solid_skype_etl_pipeline
from src.parser.content_extractor import ContentExtractor
from src.utils.file_handler import FileHandler
from src.utils.interfaces import (
    ContentExtractorProtocol,
    MessageHandlerFactoryProtocol,
    StructuredDataExtractorProtocol,
)
from tests.fixtures import BASIC_SKYPE_DATA, MockDatabase
from tests.fixtures.mock_fixtures import create_mock_functions


class TestImprovedSOLIDSkypeETLPipeline(unittest.TestCase):
    """
    Tests for the SOLIDSkypeETLPipeline class using the improved mock functions approach.
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

    @patch("os.path.exists")
    def test_solid_pipeline_with_improved_mocks(self, mock_exists):
        """
        Test the SOLIDSkypeETLPipeline with the improved mock functions approach.
        """
        # Mock file existence check
        mock_exists.return_value = True

        # Create mock functions
        mock_funcs = create_mock_functions({self.test_file_path: self.test_data})

        # Configure validation service
        validation_service = mock_funcs["validation_service"]
        validation_service.user_display_name_return_value = self.test_user_display_name

        # Create mock database
        mock_db = MockDatabase()

        # Create mock file handler
        mock_file_handler = MagicMock(spec=FileHandler)
        mock_file_handler.read_file.return_value = self.test_data

        # Create mock content extractor
        # Use ContentExtractor as spec to ensure all methods are available
        mock_content_extractor = MagicMock(spec=ContentExtractor)
        mock_content_extractor.extract_content.return_value = "Test content"
        mock_content_extractor.extract_html_content.return_value = "<p>Test content</p>"
        mock_content_extractor.extract_cleaned_content.return_value = (
            "Cleaned test content"
        )

        # Create mock structured data extractor
        mock_structured_data_extractor = MagicMock(spec=StructuredDataExtractorProtocol)
        mock_structured_data_extractor.extract_structured_data.return_value = {}

        # Create mock message handler factory
        mock_message_handler = MagicMock()
        mock_message_handler.can_handle.return_value = True
        mock_message_handler.extract_data.return_value = {}

        mock_message_handler_factory = MagicMock(spec=MessageHandlerFactoryProtocol)
        mock_message_handler_factory.get_handler.return_value = mock_message_handler

        # Create pipeline with all dependencies injected
        pipeline = SOLIDSkypeETLPipeline(
            db_config=self.test_db_config,
            file_handler=mock_file_handler,
            validation_service=validation_service,
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
        validation_service.validate_file_exists_mock.assert_called_with(
            self.test_file_path
        )
        validation_service.validate_user_display_name_mock.assert_called_with(
            self.test_user_display_name
        )
        mock_file_handler.read_file.assert_called_with(self.test_file_path)

        # Check database queries
        executed_queries = mock_db.get_executed_queries()
        self.assertTrue(any("INSERT INTO" in query for query in executed_queries))

    @patch("os.path.exists")
    def test_factory_function_with_improved_mocks(self, mock_exists):
        """
        Test the factory function with the improved mock functions approach.
        """
        # Mock file existence check
        mock_exists.return_value = True

        # Create mock functions
        mock_funcs = create_mock_functions({self.test_file_path: self.test_data})

        # Configure validation service
        validation_service = mock_funcs["validation_service"]
        validation_service.user_display_name_return_value = self.test_user_display_name

        # Create mock database
        mock_db = MockDatabase()

        # Create mock file handler
        mock_file_handler = MagicMock(spec=FileHandler)
        mock_file_handler.read_file.return_value = self.test_data

        # Create mock content extractor for the factory function
        mock_content_extractor = MagicMock(spec=ContentExtractor)
        mock_content_extractor.extract_content.return_value = "Test content"
        mock_content_extractor.extract_html_content.return_value = "<p>Test content</p>"
        mock_content_extractor.extract_cleaned_content.return_value = (
            "Cleaned test content"
        )

        # Create pipeline using factory function with our mock file handler and content extractor
        pipeline = create_solid_skype_etl_pipeline(
            db_config=self.test_db_config,
            validation_service=validation_service,
            db_connection=mock_db.conn,
            file_handler=mock_file_handler,
            content_extractor=mock_content_extractor,
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
        validation_service.validate_file_exists_mock.assert_called_with(
            self.test_file_path
        )
        validation_service.validate_user_display_name_mock.assert_called_with(
            self.test_user_display_name
        )
        mock_file_handler.read_file.assert_called_with(self.test_file_path)

        # Check database queries
        executed_queries = mock_db.get_executed_queries()
        self.assertTrue(any("INSERT INTO" in query for query in executed_queries))


if __name__ == "__main__":
    unittest.main()
