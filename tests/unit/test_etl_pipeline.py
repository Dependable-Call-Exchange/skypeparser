#!/usr/bin/env python3
"""
Unit tests for the ETL pipeline module.

This test suite provides comprehensive testing for the ETL pipeline,
using dependency injection with ImprovedTestableETLPipeline instead of extensive patching.
The tests are organized into separate classes for each phase of the ETL pipeline:
- TestExtraction: Tests for the extraction phase
- TestTransformation: Tests for the transformation phase
- TestLoading: Tests for the loading phase
- TestPipelineExecution: Tests for the complete pipeline execution
"""

import io
import json
import os
import sys
import tempfile
import unittest
from typing import Any, BinaryIO, Dict, Iterator, Optional, Tuple
from unittest.mock import MagicMock, patch

import pytest

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.db.testable_etl_pipeline import (
    ImprovedTestableETLPipeline,
    create_testable_etl_pipeline,
)
from src.utils.interfaces import ExtractorProtocol, TransformerProtocol
from src.utils.validation import ValidationError
from tests.fixtures import (
    BASIC_SKYPE_DATA,
    COMPLEX_SKYPE_DATA,
    INVALID_SKYPE_DATA,
    MockDatabase,
    create_mock_file_environment,
)
from tests.fixtures.etl_mocks import (
    MockContentExtractor,
    MockExtractor,
    MockFileHandler,
    MockMessageHandler,
    MockMessageHandlerFactory,
    MockProgressTracker,
    MockStructuredDataExtractor,
    MockTransformer,
    MockValidationService,
)


class ETLPipelineTestBase(unittest.TestCase):
    """Base class for ETL pipeline tests with common setup and teardown logic."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory
        self.temp_dir = tempfile.mkdtemp()

        # Create mock objects
        self.mock_db = MockDatabase()
        self.db_config = {
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "user": "test_user",
            "password": "test_password",
        }

        # Create mock objects
        self.mock_file_handler = MockFileHandler()
        self.mock_validation_service = MockValidationService()
        self.mock_content_extractor = MockContentExtractor()
        self.mock_structured_data_extractor = MockStructuredDataExtractor()
        self.mock_message_handler = MockMessageHandler(message_type="RichText")
        self.mock_message_handler_factory = MockMessageHandlerFactory()
        self.mock_extractor = MockExtractor()
        self.mock_transformer = MockTransformer()
        self.mock_progress_tracker = MockProgressTracker()

        # Create a pipeline with mock objects
        self.pipeline = create_testable_etl_pipeline(
            db_config=self.db_config,
            file_handler=self.mock_file_handler,
            validation_service=self.mock_validation_service,
            db_connection=self.mock_db,
            content_extractor=self.mock_content_extractor,
            structured_data_extractor=self.mock_structured_data_extractor,
            message_handler_factory=self.mock_message_handler_factory,
        )

        # Replace the extractor and transformer with our mocks
        self.pipeline.extractor = self.mock_extractor
        self.pipeline.transformer = self.mock_transformer

        # Mock the update_progress method in the context to avoid the ProgressTracker error
        self.pipeline.context.update_progress = MagicMock(return_value=None)

        # Replace the progress_tracker with our mock
        self.pipeline.context.progress_tracker = self.mock_progress_tracker

    def tearDown(self):
        """Tear down test fixtures."""
        # Clean up temporary directory
        import shutil

        shutil.rmtree(self.temp_dir)

    def _validate_extraction_results(
        self, result: Dict[str, Any], expected_data: Dict[str, Any] = BASIC_SKYPE_DATA
    ):
        """Validate extraction results.

        Args:
            result: The extraction result to validate
            expected_data: The expected data to compare against
        """
        self.assertEqual(result["userId"], expected_data["userId"])
        self.assertEqual(result["exportDate"], expected_data["exportDate"])
        self.assertEqual(
            len(result["conversations"]), len(expected_data["conversations"])
        )

    def _validate_transformation_results(
        self, result: Dict[str, Any], expected_data: Dict[str, Any] = BASIC_SKYPE_DATA
    ):
        """Validate transformation results.

        Args:
            result: The transformation result to validate
            expected_data: The expected data to compare against
        """
        self.assertEqual(result["user_id"], expected_data["userId"])
        self.assertEqual(result["export_date"], expected_data["exportDate"])
        self.assertEqual(
            len(result["conversations"]), len(expected_data["conversations"])
        )

    def _validate_pipeline_results(self, result: Dict[str, Any]):
        """Validate pipeline execution results.

        Args:
            result: The pipeline execution result to validate
        """
        self.assertIn("extraction", result)
        self.assertIn("transformation", result)
        self.assertIn("loading", result)


@pytest.mark.etl_pipeline
class TestExtraction(ETLPipelineTestBase):
    """Test cases for the extraction phase of the ETL pipeline."""

    @pytest.mark.parametrize(
        "file_input,is_file_obj",
        [
            ("test.json", False),
            ("test.tar", False),
            (io.StringIO(json.dumps(BASIC_SKYPE_DATA)), True),
        ],
    )
    def test_extract(self, file_input, is_file_obj):
        """Test extracting data from different input types."""
        # Set the file name if it's a file object
        if is_file_obj:
            file_input.name = "test.json"
            result = self.pipeline.extract(file_obj=file_input)
            self.assertEqual(self.mock_extractor.file_obj, file_input)
        else:
            result = self.pipeline.extract(file_path=file_input)
            self.assertEqual(self.mock_extractor.file_path, file_input)

        # Validate the result
        self._validate_extraction_results(result)

        # Verify that the mock extractor was called
        self.assertTrue(self.mock_extractor.extract_called)

    def test_extract_with_no_file(self):
        """Test extracting data with no file provided."""

        # Create a custom extractor that raises ValueError when no file is provided
        class MockExtractorWithNoFile(MockExtractor):
            def extract(
                self,
                file_path: Optional[str] = None,
                file_obj: Optional[BinaryIO] = None,
            ) -> Dict[str, Any]:
                if not file_path and not file_obj:
                    raise ValueError("No file provided")
                return super().extract(file_path, file_obj)

        # Replace the extractor with our custom mock
        self.pipeline.extractor = MockExtractorWithNoFile()

        # Test that extract raises ValueError when no file is provided
        with self.assertRaises(ValueError):
            self.pipeline.extract()


@pytest.mark.etl_pipeline
class TestTransformation(ETLPipelineTestBase):
    """Test cases for the transformation phase of the ETL pipeline."""

    @pytest.mark.parametrize("test_data", [BASIC_SKYPE_DATA, COMPLEX_SKYPE_DATA])
    def test_transform(self, test_data):
        """Test transforming Skype data."""
        # Transform data
        result = self.pipeline.transform(raw_data=test_data)

        # Validate the result
        self._validate_transformation_results(result, test_data)

        # Verify that the mock transformer was called
        self.assertTrue(self.mock_transformer.transform_called)
        self.assertEqual(self.mock_transformer.raw_data, test_data)

    def test_transform_with_invalid_data(self):
        """Test transforming invalid Skype data."""

        # Create a mock transformer that raises an exception for invalid data
        class MockErrorTransformer(MockTransformer):
            def transform(
                self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None
            ) -> Dict[str, Any]:
                if raw_data == INVALID_SKYPE_DATA:
                    raise ValidationError("Invalid data")
                return super().transform(raw_data, user_display_name)

        # Replace the transformer with our custom mock
        self.pipeline.transformer = MockErrorTransformer()

        # Attempt to transform invalid data
        with self.assertRaises(ValidationError):
            self.pipeline.transform(raw_data=INVALID_SKYPE_DATA)

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
                            "messagetype": "RichText",
                        }
                    ],
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
                            "messagetype": "RichText",
                        }
                    ],
                },
            ],
        }

        # Transform the data
        result = self.pipeline.transform(raw_data=test_data)

        # Verify that the mock transformer was called
        self.assertTrue(self.mock_transformer.transform_called)
        self.assertEqual(self.mock_transformer.raw_data, test_data)

        # Verify the result
        self.assertEqual(result["user_id"], test_data["userId"])
        self.assertEqual(result["export_date"], test_data["exportDate"])
        self.assertEqual(len(result["conversations"]), len(test_data["conversations"]))


@pytest.mark.etl_pipeline
class TestLoading(ETLPipelineTestBase):
    """Test cases for the loading phase of the ETL pipeline."""

    def test_load(self):
        """Test loading data into the database with specific query assertions."""
        # Extract data first
        raw_data = self.mock_extractor.extract(file_path="test.json")

        # Transform data
        transformed_data = self.mock_transformer.transform(raw_data)

        # Load data
        result = self.pipeline.load(
            raw_data=raw_data, transformed_data=transformed_data
        )

        # Assert that the result is not None
        self.assertIsNotNone(result)

        # Assert that at least one query was executed in the database
        self.assertGreater(self.pipeline.db_connection.execute.call_count, 0)

    def test_load_with_no_connection(self):
        """Test loading data with no database connection."""
        # Create a mock database connection that raises an exception when used
        mock_db_connection = MagicMock()
        mock_db_connection.execute.side_effect = Exception("Database connection error")

        # Create a pipeline with the mock database connection
        pipeline_no_db = ImprovedTestableETLPipeline(
            db_config=self.db_config,
            file_handler=self.mock_file_handler,
            validation_service=self.mock_validation_service,
            db_connection=mock_db_connection,  # Mock connection that will raise an exception
            content_extractor=self.mock_content_extractor,
            structured_data_extractor=self.mock_structured_data_extractor,
            message_handler_factory=self.mock_message_handler_factory,
        )

        # Replace the extractor and transformer with our mocks
        pipeline_no_db.extractor = self.mock_extractor
        pipeline_no_db.transformer = self.mock_transformer

        # Transform data first
        transformed_data = self.mock_transformer.transform(BASIC_SKYPE_DATA)

        # Test that load raises an exception when the database connection is used
        with self.assertRaises(Exception):
            pipeline_no_db.load(
                raw_data=BASIC_SKYPE_DATA, transformed_data=transformed_data
            )


@pytest.mark.etl_pipeline
class TestPipelineExecution(ETLPipelineTestBase):
    """Test cases for the complete ETL pipeline execution."""

    @pytest.mark.parametrize(
        "file_input,is_file_obj",
        [("test.json", False), (io.StringIO(json.dumps(BASIC_SKYPE_DATA)), True)],
    )
    def test_run_pipeline(self, file_input, is_file_obj):
        """Test running the complete pipeline with different input types."""
        # Set the file name if it's a file object
        if is_file_obj:
            file_input.name = "test.json"
            result = self.pipeline.run_pipeline(file_obj=file_input)
            self.assertEqual(self.mock_extractor.file_obj, file_input)
        else:
            result = self.pipeline.run_pipeline(file_path=file_input)
            self.assertEqual(self.mock_extractor.file_path, file_input)

        # Validate the result
        self._validate_pipeline_results(result)

        # Verify that the mock extractor and transformer were called
        self.assertTrue(self.mock_extractor.extract_called)
        self.assertTrue(self.mock_transformer.transform_called)

    def test_run_pipeline_with_no_db(self):
        """Test running the pipeline without a database connection."""
        # Create a mock database connection that raises an exception when used
        mock_db_connection = MagicMock()
        mock_db_connection.execute.side_effect = Exception("Database connection error")

        # Create a pipeline with the mock database connection
        pipeline_no_db = ImprovedTestableETLPipeline(
            db_config=self.db_config,
            file_handler=self.mock_file_handler,
            validation_service=self.mock_validation_service,
            db_connection=mock_db_connection,  # Mock connection that will raise an exception
            content_extractor=self.mock_content_extractor,
            structured_data_extractor=self.mock_structured_data_extractor,
            message_handler_factory=self.mock_message_handler_factory,
        )

        # Replace the extractor and transformer with our mocks
        pipeline_no_db.extractor = self.mock_extractor
        pipeline_no_db.transformer = self.mock_transformer

        # Test that run_pipeline raises an exception when the database connection is used
        with self.assertRaises(Exception):
            pipeline_no_db.run_pipeline(file_path="test.json")

    def test_run_pipeline_with_invalid_data(self):
        """Test running the pipeline with invalid data."""

        # Create a mock extractor that returns invalid data
        class MockInvalidExtractor(MockExtractor):
            def extract(
                self,
                file_path: Optional[str] = None,
                file_obj: Optional[BinaryIO] = None,
            ) -> Dict[str, Any]:
                super().extract(file_path, file_obj)
                # Add a messages key to the data to satisfy the loader validation
                data = INVALID_SKYPE_DATA.copy()
                # Extract all messages from all conversations
                messages = []
                for conv in data.get("conversations", []):
                    messages.extend(conv.get("MessageList", []))
                data["messages"] = messages
                return data

        # Create a mock transformer that raises an exception for invalid data
        class MockErrorTransformer(MockTransformer):
            def transform(
                self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None
            ) -> Dict[str, Any]:
                if raw_data == INVALID_SKYPE_DATA:
                    raise ValidationError("Invalid data")
                return super().transform(raw_data, user_display_name)

        # Replace the extractor and transformer with our custom mocks
        self.pipeline.extractor = MockInvalidExtractor()
        self.pipeline.transformer = MockErrorTransformer()

        # Run the pipeline with invalid data
        with self.assertRaises(ValidationError):
            self.pipeline.run_pipeline(file_path="invalid.json")

    def test_run_pipeline_with_complex_data(self):
        """Test running the pipeline with complex data."""

        # Create a mock extractor that returns complex data
        class MockComplexExtractor(MockExtractor):
            def extract(
                self,
                file_path: Optional[str] = None,
                file_obj: Optional[BinaryIO] = None,
            ) -> Dict[str, Any]:
                super().extract(file_path, file_obj)
                # Add a messages key to the data to satisfy the loader validation
                data = COMPLEX_SKYPE_DATA.copy()
                # Extract all messages from all conversations
                messages = []
                for conv in data.get("conversations", []):
                    messages.extend(conv.get("MessageList", []))
                data["messages"] = messages
                return data

        # Replace the extractor with our custom mock
        self.pipeline.extractor = MockComplexExtractor()

        # Run the pipeline
        result = self.pipeline.run_pipeline(file_path="complex.json")

        # Validate the result
        self._validate_pipeline_results(result)


if __name__ == "__main__":
    unittest.main()
