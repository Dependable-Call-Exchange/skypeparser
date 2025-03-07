"""
Specialized test fixtures for ETL pipeline components.

This module provides fixtures specifically designed for testing ETL pipeline
components, including extractors, transformers, loaders, and the full pipeline.
"""

import json
import os
import tempfile
from typing import Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import MagicMock

import pytest

from src.db.etl.context import ETLContext
from src.db.etl.extractor import Extractor
from src.db.etl.loader import Loader
from src.db.etl.transformer import Transformer
from src.parser.content_extractor import ContentExtractor
from src.utils.file_handler import FileHandler
from src.utils.interfaces import (
    ContentExtractorProtocol,
    MessageHandlerFactoryProtocol,
    StructuredDataExtractorProtocol,
    ValidationServiceProtocol,
)
from src.utils.message_type_handlers import SkypeMessageHandlerFactory
from src.utils.structured_data_extractor import StructuredDataExtractor
from src.utils.validation import ValidationService
from tests.factories import (
    SkypeConversationFactory,
    SkypeDataFactory,
    SkypeMessageFactory,
)
from tests.fixtures.mock_fixtures import MockDatabase, create_mock_functions

# ETL Context Fixtures


@pytest.fixture
def etl_context():
    """
    Fixture for a basic ETLContext instance.

    Returns:
        ETLContext: A configured ETLContext instance
    """
    return ETLContext(
        db_config={"host": "localhost", "port": 5432, "dbname": "test_db"},
        output_dir=tempfile.mkdtemp(),
    )


@pytest.fixture
def etl_context_with_phases():
    """
    Fixture for an ETLContext with pre-configured phases.

    Returns:
        ETLContext: An ETLContext with extraction and transformation phases started
    """
    context = ETLContext(
        db_config={"host": "localhost", "port": 5432, "dbname": "test_db"},
        output_dir=tempfile.mkdtemp(),
    )

    # Start and complete extraction phase
    context.start_phase("extraction", total_conversations=2, total_messages=5)
    context.end_phase(
        {"status": "completed", "conversations_processed": 2, "messages_processed": 5}
    )

    # Start transformation phase
    context.start_phase("transformation", total_conversations=2, total_messages=5)

    return context


@pytest.fixture
def etl_context_with_error():
    """
    Fixture for an ETLContext with an error condition.

    Returns:
        ETLContext: An ETLContext with an error in the extraction phase
    """
    context = ETLContext(
        db_config={"host": "localhost", "port": 5432, "dbname": "test_db"},
        output_dir=tempfile.mkdtemp(),
    )

    # Start extraction phase
    context.start_phase("extraction")

    # Record an error
    context.record_error("extraction", "File not found: test.json")

    # End phase with error
    context.end_phase({"status": "failed", "error": "File not found: test.json"})

    return context


# Component Fixtures


@pytest.fixture
def mock_file_handler():
    """
    Fixture for a mock FileHandler.

    Returns:
        MagicMock: A mock FileHandler instance
    """
    handler = MagicMock(spec=FileHandler)
    handler.read_file.return_value = SkypeDataFactory.build()
    handler.read_file_object.return_value = SkypeDataFactory.build()
    return handler


@pytest.fixture
def mock_validation_service():
    """
    Fixture for a mock ValidationService.

    Returns:
        MagicMock: A mock ValidationService instance
    """
    service = MagicMock(spec=ValidationServiceProtocol)
    service.validate_file_exists.return_value = True
    service.validate_file_object.return_value = True
    service.validate_json_file.return_value = SkypeDataFactory.build()
    service.validate_user_display_name.return_value = "Test User"
    return service


@pytest.fixture
def mock_content_extractor():
    """
    Fixture for a mock ContentExtractor.

    Returns:
        MagicMock: A mock ContentExtractor instance
    """
    extractor = MagicMock(spec=ContentExtractorProtocol)
    extractor.extract_content.return_value = "Test content"
    extractor.extract_html_content.return_value = "<p>Test content</p>"
    extractor.extract_cleaned_content.return_value = "Test content"
    return extractor


@pytest.fixture
def mock_structured_data_extractor():
    """
    Fixture for a mock StructuredDataExtractor.

    Returns:
        MagicMock: A mock StructuredDataExtractor instance
    """
    extractor = MagicMock(spec=StructuredDataExtractorProtocol)
    extractor.extract_structured_data.return_value = {
        "type": "text",
        "mentions": [],
        "links": [],
        "formatted": False,
    }
    return extractor


@pytest.fixture
def mock_message_handler_factory():
    """
    Fixture for a mock MessageHandlerFactory.

    Returns:
        MagicMock: A mock MessageHandlerFactory instance
    """
    handler = MagicMock()
    handler.can_handle.return_value = True
    handler.extract_data.return_value = {"type": "text", "content": "Test content"}

    factory = MagicMock(spec=MessageHandlerFactoryProtocol)
    factory.get_handler.return_value = handler

    return factory


# ETL Component Fixtures


@pytest.fixture
def configured_extractor(mock_file_handler, mock_validation_service, etl_context):
    """
    Fixture for a configured Extractor instance.

    Args:
        mock_file_handler: Mock file handler
        mock_validation_service: Mock validation service
        etl_context: ETL context

    Returns:
        Extractor: A configured Extractor instance
    """
    return Extractor(
        file_handler=mock_file_handler,
        validation_service=mock_validation_service,
        context=etl_context,
    )


@pytest.fixture
def configured_transformer(
    mock_content_extractor,
    mock_structured_data_extractor,
    mock_message_handler_factory,
    etl_context,
):
    """
    Fixture for a configured Transformer instance.

    Args:
        mock_content_extractor: Mock content extractor
        mock_structured_data_extractor: Mock structured data extractor
        mock_message_handler_factory: Mock message handler factory
        etl_context: ETL context

    Returns:
        Transformer: A configured Transformer instance
    """
    return Transformer(
        content_extractor=mock_content_extractor,
        structured_data_extractor=mock_structured_data_extractor,
        message_handler_factory=mock_message_handler_factory,
        context=etl_context,
    )


@pytest.fixture
def configured_loader(etl_context):
    """
    Fixture for a configured Loader instance.

    Args:
        etl_context: ETL context

    Returns:
        Loader: A configured Loader instance
    """
    mock_db = MockDatabase()

    return Loader(db_connection=mock_db.conn, context=etl_context)


# Test Data File Fixtures


@pytest.fixture
def temp_json_file():
    """
    Fixture that creates a temporary JSON file with Skype data.

    Returns:
        Tuple[str, Dict[str, Any]]: A tuple containing the file path and the data
    """
    data = SkypeDataFactory.build()

    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as temp:
        temp.write(json.dumps(data).encode("utf-8"))
        temp_path = temp.name

    yield temp_path, data

    # Clean up
    if os.path.exists(temp_path):
        os.unlink(temp_path)


@pytest.fixture
def temp_invalid_json_file():
    """
    Fixture that creates a temporary invalid JSON file.

    Returns:
        str: The path to the invalid JSON file
    """
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as temp:
        temp.write(b'{"this is not valid JSON": ')
        temp_path = temp.name

    yield temp_path

    # Clean up
    if os.path.exists(temp_path):
        os.unlink(temp_path)


# Error Condition Fixtures


@pytest.fixture
def extraction_error_scenario():
    """
    Fixture that sets up an extraction error scenario.

    Returns:
        Dict[str, Any]: A dictionary with components and expected error details
    """
    context = ETLContext(
        db_config={"host": "localhost", "port": 5432, "dbname": "test_db"},
        output_dir=tempfile.mkdtemp(),
    )

    file_handler = MagicMock(spec=FileHandler)
    file_handler.read_file.side_effect = ValueError("File not found or invalid format")

    validation_service = MagicMock(spec=ValidationServiceProtocol)
    validation_service.validate_file_exists.return_value = True

    extractor = Extractor(
        file_handler=file_handler,
        validation_service=validation_service,
        context=context,
    )

    return {
        "context": context,
        "extractor": extractor,
        "expected_error": "File not found or invalid format",
        "expected_phase_status": "failed",
    }


@pytest.fixture
def transformation_error_scenario():
    """
    Fixture that sets up a transformation error scenario.

    Returns:
        Dict[str, Any]: A dictionary with components and expected error details
    """
    context = ETLContext(
        db_config={"host": "localhost", "port": 5432, "dbname": "test_db"},
        output_dir=tempfile.mkdtemp(),
    )

    content_extractor = MagicMock(spec=ContentExtractorProtocol)
    content_extractor.extract_content.side_effect = ValueError("Invalid content format")

    structured_data_extractor = MagicMock(spec=StructuredDataExtractorProtocol)
    message_handler_factory = MagicMock(spec=MessageHandlerFactoryProtocol)

    transformer = Transformer(
        content_extractor=content_extractor,
        structured_data_extractor=structured_data_extractor,
        message_handler_factory=message_handler_factory,
        context=context,
    )

    return {
        "context": context,
        "transformer": transformer,
        "raw_data": SkypeDataFactory.build(),
        "expected_error": "Invalid content format",
        "expected_phase_status": "failed",
    }


@pytest.fixture
def loading_error_scenario():
    """
    Fixture that sets up a loading error scenario.

    Returns:
        Dict[str, Any]: A dictionary with components and expected error details
    """
    context = ETLContext(
        db_config={"host": "localhost", "port": 5432, "dbname": "test_db"},
        output_dir=tempfile.mkdtemp(),
    )

    mock_db = MockDatabase(should_fail=True)

    loader = Loader(db_connection=mock_db.conn, context=context)

    return {
        "context": context,
        "loader": loader,
        "raw_data": SkypeDataFactory.build(),
        "transformed_data": {
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
        },
        "expected_error": "Database error",
        "expected_phase_status": "failed",
    }


# Complete Pipeline Test Fixtures


@pytest.fixture
def pipeline_test_environment():
    """
    Fixture that sets up a complete environment for testing the ETL pipeline.

    Returns:
        Dict[str, Any]: A dictionary with all components needed for pipeline testing
    """
    # Create test data
    test_data = SkypeDataFactory.build()
    test_file_path = "test.json"

    # Create mock functions
    mock_funcs = create_mock_functions({test_file_path: test_data})

    # Configure validation service
    validation_service = mock_funcs["validation_service"]
    validation_service.user_display_name_return_value = "Test User"

    # Create mock database
    mock_db = MockDatabase()

    # Create ETL context
    context = ETLContext(
        db_config={"host": "localhost", "port": 5432, "dbname": "test_db"},
        output_dir=tempfile.mkdtemp(),
    )

    # Create mock file handler
    mock_file_handler = MagicMock(spec=FileHandler)
    mock_file_handler.read_file.return_value = test_data

    # Create mock content extractor
    mock_content_extractor = MagicMock(spec=ContentExtractor)
    mock_content_extractor.extract_content.return_value = "Test content"
    mock_content_extractor.extract_html_content.return_value = "<p>Test content</p>"
    mock_content_extractor.extract_cleaned_content.return_value = "Test content"

    # Create mock structured data extractor
    mock_structured_data_extractor = MagicMock(spec=StructuredDataExtractorProtocol)
    mock_structured_data_extractor.extract_structured_data.return_value = {
        "type": "text",
        "mentions": [],
        "links": [],
        "formatted": False,
    }

    # Create mock message handler factory
    mock_message_handler = MagicMock()
    mock_message_handler.can_handle.return_value = True
    mock_message_handler.extract_data.return_value = {
        "type": "text",
        "content": "Test content",
    }

    mock_message_handler_factory = MagicMock(spec=MessageHandlerFactoryProtocol)
    mock_message_handler_factory.get_handler.return_value = mock_message_handler

    return {
        "test_data": test_data,
        "test_file_path": test_file_path,
        "mock_funcs": mock_funcs,
        "validation_service": validation_service,
        "mock_db": mock_db,
        "context": context,
        "file_handler": mock_file_handler,
        "content_extractor": mock_content_extractor,
        "structured_data_extractor": mock_structured_data_extractor,
        "message_handler_factory": mock_message_handler_factory,
        "db_connection": mock_db.conn,
        "user_display_name": "Test User",
    }
