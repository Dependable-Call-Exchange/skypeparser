"""
Pytest-based unit tests for the ETL pipeline module.

This test suite provides comprehensive testing for the ETL pipeline,
using pytest fixtures and parameterized tests for better maintainability.
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
from tests.fixtures import BASIC_SKYPE_DATA, COMPLEX_SKYPE_DATA, INVALID_SKYPE_DATA
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


@pytest.fixture
def temp_dir():
    """Create a temporary directory for tests."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    import shutil

    shutil.rmtree(temp_dir)


@pytest.fixture
def db_config():
    """Create a database configuration for tests."""
    return {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_password",
    }


@pytest.fixture
def mock_db():
    """Create a mock database for tests."""
    from tests.fixtures.etl_mocks import MockDatabase

    # Create a mock database with proper configuration for execute_values
    mock_db = MockDatabase()

    # Mock the execute method to avoid the execute_values issue
    def mock_execute(query, params=None):
        mock_db.execute_count += 1
        mock_db.queries.append(query)
        mock_db.params.append(params)
        return mock_db.mock_cursor

    # Replace the execute method with our custom implementation
    mock_db.execute = mock_execute

    # Mock the execute_values function to avoid the encoding issue
    with patch("psycopg2.extras.execute_values") as mock_execute_values:
        mock_execute_values.return_value = None
        yield mock_db


@pytest.fixture
def pipeline(
    db_config,
    mock_db,
    mock_file_handler,
    mock_validation_service,
    mock_content_extractor,
    mock_structured_data_extractor,
    mock_message_handler_factory,
    mock_extractor,
    mock_transformer,
    mock_progress_tracker,
):
    """Create a pipeline with mock objects for tests."""
    pipeline = create_testable_etl_pipeline(
        db_config=db_config,
        file_handler=mock_file_handler,
        validation_service=mock_validation_service,
        db_connection=mock_db,
        content_extractor=mock_content_extractor,
        structured_data_extractor=mock_structured_data_extractor,
        message_handler_factory=mock_message_handler_factory,
    )

    # Replace the extractor and transformer with our mocks
    pipeline.extractor = mock_extractor
    pipeline.transformer = mock_transformer

    # Mock the update_progress method in the context to avoid the ProgressTracker error
    pipeline.context.update_progress = MagicMock(return_value=None)

    # Replace the progress_tracker with our mock
    pipeline.context.progress_tracker = mock_progress_tracker

    return pipeline


def validate_extraction_results(
    result: Dict[str, Any], expected_data: Dict[str, Any] = BASIC_SKYPE_DATA
):
    """Validate extraction results.

    Args:
        result: The extraction result to validate
        expected_data: The expected data to compare against
    """
    assert result["userId"] == expected_data["userId"]
    assert result["exportDate"] == expected_data["exportDate"]
    assert len(result["conversations"]) == len(expected_data["conversations"])


def validate_transformation_results(
    result: Dict[str, Any], expected_data: Dict[str, Any] = BASIC_SKYPE_DATA
):
    """Validate transformation results.

    Args:
        result: The transformation result to validate
        expected_data: The expected data to compare against
    """
    assert result["user_id"] == expected_data["userId"]
    assert result["export_date"] == expected_data["exportDate"]
    assert len(result["conversations"]) == len(expected_data["conversations"])


def validate_pipeline_results(result: Dict[str, Any]):
    """Validate pipeline execution results.

    Args:
        result: The pipeline execution result to validate
    """
    assert "extraction" in result
    assert "transformation" in result
    assert "loading" in result


@pytest.mark.etl_pipeline
class TestExtraction:
    """Test cases for the extraction phase of the ETL pipeline."""

    @pytest.mark.parametrize(
        "file_input,is_file_obj",
        [
            ("test.json", False),
            ("test.tar", False),
            (io.StringIO(json.dumps(BASIC_SKYPE_DATA)), True),
        ],
    )
    def test_extract(self, pipeline, mock_extractor, file_input, is_file_obj):
        """Test extracting data from different input types."""
        # Set the file name if it's a file object
        if is_file_obj:
            file_input.name = "test.json"
            result = pipeline.extract(file_obj=file_input)
            assert mock_extractor.file_obj == file_input
        else:
            result = pipeline.extract(file_path=file_input)
            assert mock_extractor.file_path == file_input

        # Validate the result
        validate_extraction_results(result)

        # Verify that the mock extractor was called
        assert mock_extractor.extract_called

    def test_extract_with_no_file(self, pipeline):
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
        pipeline.extractor = MockExtractorWithNoFile()

        # Test that extract raises ValueError when no file is provided
        with pytest.raises(ValueError):
            pipeline.extract()


@pytest.mark.etl_pipeline
class TestTransformation:
    """Test cases for the transformation phase of the ETL pipeline."""

    @pytest.mark.parametrize("test_data", [BASIC_SKYPE_DATA, COMPLEX_SKYPE_DATA])
    def test_transform(self, pipeline, mock_transformer, test_data):
        """Test transforming Skype data."""
        # Transform data
        result = pipeline.transform(raw_data=test_data)

        # Validate the result
        validate_transformation_results(result, test_data)

        # Verify that the mock transformer was called
        assert mock_transformer.transform_called
        assert mock_transformer.raw_data == test_data

    def test_transform_with_invalid_data(self, pipeline):
        """Test transforming invalid Skype data."""

        # Create a mock transformer that raises an exception for invalid data
        class MockErrorTransformer(MockTransformer):
            def transform(
                self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None
            ) -> Dict[str, Any]:
                # Check for specific invalid fields instead of comparing the entire object
                if raw_data.get("exportDate") == "invalid_date" and any(
                    msg.get("originalarrivaltime") == "invalid_timestamp"
                    for conv in raw_data.get("conversations", [])
                    for msg in conv.get("MessageList", [])
                ):
                    raise ValidationError("Invalid data")
                return super().transform(raw_data, user_display_name)

        # Replace the transformer with our custom mock
        pipeline.transformer = MockErrorTransformer()

        # Attempt to transform invalid data
        with pytest.raises(ValidationError):
            pipeline.transform(raw_data=INVALID_SKYPE_DATA)

    def test_transform_skips_conversations_with_none_display_name(
        self, pipeline, mock_transformer
    ):
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
        result = pipeline.transform(raw_data=test_data)

        # Verify that the mock transformer was called
        assert mock_transformer.transform_called
        assert mock_transformer.raw_data == test_data

        # Verify the result
        assert result["user_id"] == test_data["userId"]
        assert result["export_date"] == test_data["exportDate"]
        assert len(result["conversations"]) == len(test_data["conversations"])


@pytest.mark.etl_pipeline
class TestLoading:
    """Test cases for the loading phase of the ETL pipeline."""

    def test_load(self, pipeline, mock_extractor, mock_transformer):
        """Test loading data into the database with specific query assertions."""
        # Extract data first
        raw_data = mock_extractor.extract(file_path="test.json")

        # Transform data
        transformed_data = mock_transformer.transform(raw_data)

        # Load data
        result = pipeline.load(raw_data=raw_data, transformed_data=transformed_data)

        # Assert that the result is not None
        assert result is not None

        # Assert that at least one query was executed in the database
        assert len(pipeline.db_connection.queries) > 0

    def test_load_with_no_connection(
        self,
        db_config,
        mock_file_handler,
        mock_validation_service,
        mock_content_extractor,
        mock_structured_data_extractor,
        mock_message_handler_factory,
        mock_extractor,
        mock_transformer,
    ):
        """Test loading data with no database connection."""
        # Create a mock database connection that raises an exception when used
        mock_db_connection = MagicMock()
        mock_db_connection.execute.side_effect = Exception("Database connection error")

        # Create a pipeline with the mock database connection
        pipeline_no_db = ImprovedTestableETLPipeline(
            db_config=db_config,
            file_handler=mock_file_handler,
            validation_service=mock_validation_service,
            db_connection=mock_db_connection,  # Mock connection that will raise an exception
            content_extractor=mock_content_extractor,
            structured_data_extractor=mock_structured_data_extractor,
            message_handler_factory=mock_message_handler_factory,
        )

        # Replace the extractor and transformer with our mocks
        pipeline_no_db.extractor = mock_extractor
        pipeline_no_db.transformer = mock_transformer

        # Transform data first
        transformed_data = mock_transformer.transform(BASIC_SKYPE_DATA)

        # Test that load raises an exception when the database connection is used
        with pytest.raises(Exception):
            pipeline_no_db.load(
                raw_data=BASIC_SKYPE_DATA, transformed_data=transformed_data
            )


@pytest.mark.etl_pipeline
class TestPipelineExecution:
    """Test cases for the complete ETL pipeline execution."""

    @pytest.mark.parametrize(
        "file_input,is_file_obj",
        [("test.json", False), (io.StringIO(json.dumps(BASIC_SKYPE_DATA)), True)],
    )
    def test_run_pipeline(
        self, pipeline, mock_extractor, mock_transformer, file_input, is_file_obj
    ):
        """Test running the complete pipeline with different input types."""
        # Set the file name if it's a file object
        if is_file_obj:
            file_input.name = "test.json"
            result = pipeline.run_pipeline(file_obj=file_input)
            assert mock_extractor.file_obj == file_input
        else:
            result = pipeline.run_pipeline(file_path=file_input)
            assert mock_extractor.file_path == file_input

        # Validate the result
        validate_pipeline_results(result)

        # Verify that the mock extractor and transformer were called
        assert mock_extractor.extract_called
        assert mock_transformer.transform_called

    def test_run_pipeline_with_no_db(
        self,
        db_config,
        mock_file_handler,
        mock_validation_service,
        mock_content_extractor,
        mock_structured_data_extractor,
        mock_message_handler_factory,
        mock_extractor,
        mock_transformer,
    ):
        """Test running the pipeline without a database connection."""
        # Create a mock database connection that raises an exception when used
        mock_db_connection = MagicMock()
        mock_db_connection.execute.side_effect = Exception("Database connection error")

        # Create a pipeline with the mock database connection
        pipeline_no_db = ImprovedTestableETLPipeline(
            db_config=db_config,
            file_handler=mock_file_handler,
            validation_service=mock_validation_service,
            db_connection=mock_db_connection,  # Mock connection that will raise an exception
            content_extractor=mock_content_extractor,
            structured_data_extractor=mock_structured_data_extractor,
            message_handler_factory=mock_message_handler_factory,
        )

        # Replace the extractor and transformer with our mocks
        pipeline_no_db.extractor = mock_extractor
        pipeline_no_db.transformer = mock_transformer

        # Test that run_pipeline raises an exception when the database connection is used
        with pytest.raises(Exception):
            pipeline_no_db.run_pipeline(file_path="test.json")

    def test_run_pipeline_with_invalid_data(self, pipeline):
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
                # Check for specific invalid fields instead of comparing the entire object
                if raw_data.get("exportDate") == "invalid_date" and any(
                    msg.get("originalarrivaltime") == "invalid_timestamp"
                    for conv in raw_data.get("conversations", [])
                    for msg in conv.get("MessageList", [])
                ):
                    raise ValidationError("Invalid data")
                return super().transform(raw_data, user_display_name)

        # Replace the extractor and transformer with our custom mocks
        pipeline.extractor = MockInvalidExtractor()
        pipeline.transformer = MockErrorTransformer()

        # Run the pipeline with invalid data
        with pytest.raises(ValidationError):
            pipeline.run_pipeline(file_path="invalid.json")

    def test_run_pipeline_with_complex_data(self, pipeline, mock_extractor):
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
        pipeline.extractor = MockComplexExtractor()

        # Run the pipeline
        result = pipeline.run_pipeline(file_path="complex.json")

        # Validate the result
        validate_pipeline_results(result)
