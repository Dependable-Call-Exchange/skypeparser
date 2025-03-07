#!/usr/bin/env python3
"""
Example of using the ImprovedTestableETLPipeline for testing.

This module demonstrates how to use the ImprovedTestableETLPipeline class
to test the ETL pipeline with dependency injection.
"""

import os
import unittest
from unittest.mock import MagicMock, patch

from src.db.testable_etl_pipeline import (
    ImprovedTestableETLPipeline,
    MockFileHandler,
    MockValidationService,
    create_testable_etl_pipeline,
)
from tests.fixtures import BASIC_SKYPE_DATA, MockDatabase, MockServiceFactory


class TestImprovedTestableETLPipeline(unittest.TestCase):
    """
    Tests demonstrating how to use the ImprovedTestableETLPipeline.
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
    def test_with_direct_dependency_injection(self, mock_exists):
        """
        Test the ImprovedTestableETLPipeline with direct dependency injection.
        """
        # Setup
        mock_exists.return_value = True

        # Create mock services
        file_handler = MockFileHandler(read_file_func=lambda path: self.test_data)

        validation_service = MockValidationService(
            validate_file_exists_func=lambda path: True,
            validate_user_display_name_func=lambda name: name,
        )

        # Create mock database
        mock_db = MockDatabase()

        # Create mock content extractor
        content_extractor = MockServiceFactory.create_content_extractor()

        # Create mock structured data extractor
        structured_data_extractor = (
            MockServiceFactory.create_structured_data_extractor()
        )

        # Create mock message handler factory
        message_handler_factory = MockServiceFactory.create_message_handler_factory()

        # Create pipeline with all dependencies injected
        pipeline = ImprovedTestableETLPipeline(
            db_config=self.test_db_config,
            file_handler=file_handler,
            validation_service=validation_service,
            db_connection=mock_db.conn,
            content_extractor=content_extractor,
            structured_data_extractor=structured_data_extractor,
            message_handler_factory=message_handler_factory,
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

        # Check database queries
        executed_queries = mock_db.get_executed_queries()
        self.assertTrue(any("INSERT INTO" in query for query in executed_queries))

    @patch("os.path.exists")
    def test_with_factory_function(self, mock_exists):
        """
        Test the ImprovedTestableETLPipeline using the factory function.
        """
        # Setup
        mock_exists.return_value = True

        # Create mock database
        mock_db = MockDatabase()

        # Create pipeline using factory function
        pipeline = create_testable_etl_pipeline(
            db_config=self.test_db_config,
            db_connection=mock_db.conn,
            read_file_func=lambda path: self.test_data,
            validate_file_exists_func=lambda path: True,
            validate_user_display_name_func=lambda name: name,
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
        pipeline.validation_service.validate_file_exists_mock.assert_called_with(
            self.test_file_path
        )
        pipeline.validation_service.validate_user_display_name_mock.assert_called_with(
            self.test_user_display_name
        )

        # Check database queries
        executed_queries = mock_db.get_executed_queries()
        self.assertTrue(any("INSERT INTO" in query for query in executed_queries))

    @patch("os.path.exists")
    def test_with_mock_service_factory(self, mock_exists):
        """
        Test the ImprovedTestableETLPipeline using the MockServiceFactory.
        """
        # Setup
        mock_exists.return_value = True

        # Create mock services using MockServiceFactory
        file_handler = MockServiceFactory.create_file_handler(
            read_file_return=self.test_data
        )

        validation_service = MockServiceFactory.create_validation_service(
            file_exists_return=True,
            user_display_name_return=self.test_user_display_name,
        )

        content_extractor = MockServiceFactory.create_content_extractor()
        structured_data_extractor = (
            MockServiceFactory.create_structured_data_extractor()
        )
        message_handler_factory = MockServiceFactory.create_message_handler_factory()

        # Create mock database
        mock_db = MockDatabase()

        # Create pipeline with all dependencies injected
        pipeline = ImprovedTestableETLPipeline(
            db_config=self.test_db_config,
            file_handler=file_handler,
            validation_service=validation_service,
            db_connection=mock_db.conn,
            content_extractor=content_extractor,
            structured_data_extractor=structured_data_extractor,
            message_handler_factory=message_handler_factory,
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

        # Check database queries
        executed_queries = mock_db.get_executed_queries()
        self.assertTrue(any("INSERT INTO" in query for query in executed_queries))


if __name__ == "__main__":
    unittest.main()
