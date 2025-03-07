#!/usr/bin/env python3
"""
Example of using specialized ETL fixtures for testing.

This module demonstrates how to use the specialized ETL fixtures
to simplify test setup and improve test readability.
"""

import os
import unittest
from unittest.mock import patch

import pytest

from src.db.etl_pipeline import SOLIDSkypeETLPipeline
from tests.fixtures import (  # ETL component fixtures; Other fixtures
    BASIC_SKYPE_DATA,
    MockDatabase,
    configured_extractor,
    configured_loader,
    configured_transformer,
    etl_context,
    etl_context_with_error,
    etl_context_with_phases,
    extraction_error_scenario,
    loading_error_scenario,
    mock_content_extractor,
    mock_file_handler,
    mock_message_handler_factory,
    mock_structured_data_extractor,
    mock_validation_service,
    pipeline_test_environment,
    temp_invalid_json_file,
    temp_json_file,
    transformation_error_scenario,
)


class TestETLComponentsWithFixtures(unittest.TestCase):
    """
    Tests demonstrating how to use the specialized ETL fixtures.
    """

    @patch("os.path.exists")
    def test_extractor_with_fixtures(
        self, mock_exists, configured_extractor, mock_file_handler
    ):
        """
        Test the Extractor using specialized fixtures.
        """
        # Setup
        mock_exists.return_value = True
        test_file_path = "test.json"

        # Execute
        result = configured_extractor.extract(file_path=test_file_path)

        # Verify
        self.assertIsNotNone(result)
        mock_file_handler.read_file.assert_called_with(test_file_path)

    def test_transformer_with_fixtures(self, configured_transformer):
        """
        Test the Transformer using specialized fixtures.
        """
        # Setup
        raw_data = BASIC_SKYPE_DATA
        user_display_name = "Test User"

        # Execute
        result = configured_transformer.transform(raw_data, user_display_name)

        # Verify
        self.assertIsNotNone(result)
        self.assertIn("conversations", result)

    def test_loader_with_fixtures(self, configured_loader):
        """
        Test the Loader using specialized fixtures.
        """
        # Setup
        raw_data = BASIC_SKYPE_DATA
        transformed_data = {
            "conversations": [
                {
                    "id": "conv1",
                    "display_name": "Test Conversation",
                    "messages": [
                        {
                            "id": "msg1",
                            "content": "Test message",
                            "timestamp": "2023-01-01T00:00:00Z",
                            "sender": "Test User",
                        }
                    ],
                }
            ]
        }

        # Execute
        result = configured_loader.load(raw_data, transformed_data)

        # Verify
        self.assertIsNotNone(result)
        self.assertGreater(result, 0)  # Should return an ID


class TestErrorScenariosWithFixtures(unittest.TestCase):
    """
    Tests demonstrating how to use the error scenario fixtures.
    """

    def test_extraction_error(self, extraction_error_scenario):
        """
        Test extraction error handling using the error scenario fixture.
        """
        # Setup
        extractor = extraction_error_scenario["extractor"]
        context = extraction_error_scenario["context"]
        expected_error = extraction_error_scenario["expected_error"]

        # Execute and verify
        with self.assertRaises(ValueError) as context:
            extractor.extract(file_path="test.json")

        self.assertIn(expected_error, str(context.exception))
        self.assertEqual(
            context.get_phase_status("extraction"),
            extraction_error_scenario["expected_phase_status"],
        )

    def test_transformation_error(self, transformation_error_scenario):
        """
        Test transformation error handling using the error scenario fixture.
        """
        # Setup
        transformer = transformation_error_scenario["transformer"]
        context = transformation_error_scenario["context"]
        raw_data = transformation_error_scenario["raw_data"]
        expected_error = transformation_error_scenario["expected_error"]

        # Execute
        result = transformer.transform(raw_data)

        # Verify
        self.assertEqual(
            context.get_phase_status("transformation"),
            transformation_error_scenario["expected_phase_status"],
        )

    def test_loading_error(self, loading_error_scenario):
        """
        Test loading error handling using the error scenario fixture.
        """
        # Setup
        loader = loading_error_scenario["loader"]
        context = loading_error_scenario["context"]
        raw_data = loading_error_scenario["raw_data"]
        transformed_data = loading_error_scenario["transformed_data"]
        expected_error = loading_error_scenario["expected_error"]

        # Execute and verify
        with self.assertRaises(Exception) as context:
            loader.load(raw_data, transformed_data)

        self.assertIn(expected_error, str(context.exception))
        self.assertEqual(
            context.get_phase_status("loading"),
            loading_error_scenario["expected_phase_status"],
        )


class TestPipelineWithFixtures(unittest.TestCase):
    """
    Tests demonstrating how to use the pipeline test environment fixture.
    """

    @patch("os.path.exists")
    def test_complete_pipeline(self, mock_exists, pipeline_test_environment):
        """
        Test the complete ETL pipeline using the pipeline test environment fixture.
        """
        # Setup
        mock_exists.return_value = True
        env = pipeline_test_environment

        # Create pipeline with all dependencies injected
        pipeline = SOLIDSkypeETLPipeline(
            db_config={"host": "localhost", "port": 5432, "dbname": "test_db"},
            file_handler=env["file_handler"],
            validation_service=env["validation_service"],
            db_connection=env["db_connection"],
            content_extractor=env["content_extractor"],
            structured_data_extractor=env["structured_data_extractor"],
            message_handler_factory=env["message_handler_factory"],
        )

        # Execute
        result = pipeline.run_pipeline(
            file_path=env["test_file_path"], user_display_name=env["user_display_name"]
        )

        # Verify
        self.assertIn("extraction", result)
        self.assertIn("transformation", result)
        self.assertIn("loading", result)
        self.assertTrue(result["extraction"]["success"])
        self.assertTrue(result["transformation"]["success"])
        self.assertTrue(result["loading"]["success"])


class TestFileFixtures(unittest.TestCase):
    """
    Tests demonstrating how to use the file fixtures.
    """

    def test_with_temp_json_file(self, temp_json_file):
        """
        Test using a temporary JSON file fixture.
        """
        # Setup
        file_path, data = temp_json_file

        # Verify
        self.assertTrue(os.path.exists(file_path))
        self.assertIsNotNone(data)

        # Use the file
        with open(file_path, "r") as f:
            content = f.read()
            self.assertIn("userId", content)

    def test_with_invalid_json_file(self, temp_invalid_json_file):
        """
        Test using an invalid JSON file fixture.
        """
        # Setup
        file_path = temp_invalid_json_file

        # Verify
        self.assertTrue(os.path.exists(file_path))

        # Use the file
        with open(file_path, "r") as f:
            content = f.read()
            self.assertEqual(content, '{"this is not valid JSON": ')


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
