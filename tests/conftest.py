"""
Pytest configuration and fixtures for the Skype Parser test suite.

This module provides fixtures for tests, including factory fixtures for
generating test data.
"""

import os
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Callable, Optional, TypeVar, Union, Type

import pytest

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.db.connection import DatabaseConnection
from src.db.etl.extractor import Extractor
from src.db.etl.loader import Loader
from src.db.etl.transformer import Transformer

# Import dependencies
from src.utils.di import ServiceProvider, get_service_provider
from src.utils.file_handler import FileHandler
from src.utils.interfaces import (
    ContentExtractorProtocol,
    DatabaseConnectionProtocol,
    ExtractorProtocol,
    FileHandlerProtocol,
    LoaderProtocol,
    MessageHandlerFactoryProtocol,
    StructuredDataExtractorProtocol,
    TransformerProtocol,
    ValidationServiceProtocol,
)
from src.utils.message_type_handlers import SkypeMessageHandlerFactory
from tests.fixtures import (
    DatabaseRecordFactory,
    SkypeConversationFactory,
    SkypeDataFactory,
    SkypeMessageFactory,
    is_db_available,
)
from tests.fixtures.expected_data import get_expected_handler_data
from tests.fixtures import test_db_connection as _test_db_connection
# Import consolidated mocks from the mocks directory
from tests.fixtures.mocks import (
    MockContentExtractor,
    MockDatabase,
    MockExtractor,
    MockFileHandler,
    MockLoader,
    MockMessageHandler,
    MockMessageHandlerFactory,
    MockMessageProcessor,
    MockProgressTracker,
    MockStructuredDataExtractor,
    MockTransformer,
    MockValidationService,
)
from tests.fixtures.expected_data import (
    BASIC_TRANSFORMED_MESSAGE,
    BASIC_TRANSFORMED_CONVERSATION,
    BASIC_TRANSFORMED_DATA,
    MESSAGE_TYPE_DESCRIPTIONS,
    EXPECTED_DB_QUERIES,
    get_expected_transformed_message,
    get_expected_transformed_conversation,
    TransformedMessage,
    TransformedConversation,
    ApiResponseSuccess,
    ApiResponseError,
)

# Define expected data constants for fixtures
EXPECTED_MEDIA_MESSAGE_DATA = {
    "media_filename": "test_file.jpg",
    "media_filesize": "1024",
    "media_type": "image/jpeg",
    "media_url": "https://example.com/test_file.jpg"
}

EXPECTED_SCHEDULED_CALL_DATA = {
    "call_title": "Team Meeting",
    "start_time": "2023-01-01T12:00:00Z",
    "end_time": "2023-01-01T13:00:00Z"
}

EXPECTED_POLL_DATA = {
    "poll_question": "What's your favorite color?",
    "poll_options": ["Red", "Blue", "Green"]
}

# Register dependencies
@pytest.fixture(scope="session", autouse=True)
def register_dependencies():
    """Register dependencies in the dependency injection system."""
    provider = get_service_provider()

    # Register FileHandlerProtocol
    file_handler = FileHandler()
    provider.register_singleton(FileHandlerProtocol, file_handler)

    # Register MessageHandlerFactoryProtocol
    message_handler_factory = SkypeMessageHandlerFactory()
    provider.register_singleton(MessageHandlerFactoryProtocol, message_handler_factory)

    # Register ETL components for integration tests
    from src.db.connection import DatabaseConnection
    from src.db.etl.extractor import Extractor
    from src.db.etl.loader import Loader
    from src.db.etl.transformer import Transformer
    from src.utils.interfaces import (
        ContentExtractorProtocol,
        DatabaseConnectionProtocol,
        ExtractorProtocol,
        LoaderProtocol,
        StructuredDataExtractorProtocol,
        TransformerProtocol,
        ValidationServiceProtocol,
    )
    # Import consolidated mocks from the mocks directory
    from tests.fixtures.mocks import (
        MockContentExtractor,
        MockDatabase,
        MockStructuredDataExtractor,
        MockValidationService,
    )

    # Create mock services
    validation_service = MockValidationService()
    content_extractor = MockContentExtractor()
    structured_data_extractor = MockStructuredDataExtractor()
    db_connection = MockDatabase()
    progress_tracker = MockProgressTracker()

    # Register ETL components with explicit dependencies
    provider.register_singleton(
        ExtractorProtocol,
        Extractor(file_handler=file_handler, validation_service=validation_service),
    )
    provider.register_singleton(
        TransformerProtocol,
        Transformer(
            content_extractor=content_extractor,
            message_handler_factory=message_handler_factory,
            structured_data_extractor=structured_data_extractor,
        ),
    )
    provider.register_singleton(
        LoaderProtocol,
        Loader(
            db_connection=db_connection,
            create_schema=False
        )
    )
    provider.register_singleton(DatabaseConnectionProtocol, db_connection)
    provider.register_singleton(ValidationServiceProtocol, validation_service)
    provider.register_singleton(ContentExtractorProtocol, content_extractor)
    provider.register_singleton(
        StructuredDataExtractorProtocol, structured_data_extractor
    )

    # Also register by string name for legacy code
    provider.register_singleton("file_handler", file_handler)
    provider.register_singleton("validation_service", validation_service)
    provider.register_singleton("content_extractor", content_extractor)
    provider.register_singleton("structured_data_extractor", structured_data_extractor)
    provider.register_singleton("message_handler_factory", message_handler_factory)
    provider.register_singleton("db_connection", db_connection)
    provider.register_singleton("progress_tracker", progress_tracker)

    return provider


# ETL mock fixtures
@pytest.fixture
def mock_extractor():
    """Fixture for a MockExtractor instance."""
    return MockExtractor()


@pytest.fixture
def mock_transformer():
    """Fixture for a MockTransformer instance."""
    return MockTransformer()


@pytest.fixture
def mock_file_handler():
    """Fixture for a MockFileHandler instance."""
    return MockFileHandler()


@pytest.fixture
def mock_validation_service():
    """Fixture for a MockValidationService instance."""
    return MockValidationService()


@pytest.fixture
def mock_content_extractor():
    """Fixture for a MockContentExtractor instance."""
    return MockContentExtractor()


@pytest.fixture
def mock_structured_data_extractor():
    """Fixture for a MockStructuredDataExtractor instance."""
    return MockStructuredDataExtractor()


@pytest.fixture
def mock_message_handler():
    """Fixture for a MockMessageHandler instance."""
    return MockMessageHandler(message_type="RichText")


@pytest.fixture
def mock_message_handler_factory():
    """Fixture for a MockMessageHandlerFactory instance."""
    return MockMessageHandlerFactory()


@pytest.fixture
def mock_progress_tracker():
    """Fixture for a MockProgressTracker instance."""
    return MockProgressTracker()


@pytest.fixture
def mock_message_processor():
    """Fixture for a MockMessageProcessor instance."""
    return MockMessageProcessor()


@pytest.fixture
def mock_loader():
    """Fixture for a MockLoader instance."""
    return MockLoader()


# Basic fixtures for common test data


@pytest.fixture
def basic_skype_data():
    """
    Fixture for basic Skype data with one conversation and one message.
    """
    return SkypeDataFactory.build(
        userId="test-user-id",
        exportDate="2023-01-01T12:00:00Z",
        conversations=[
            SkypeConversationFactory.build(
                id="conversation1",
                displayName="Test Conversation",
                MessageList=[
                    SkypeMessageFactory.build(
                        id="message1",
                        originalarrivaltime="2023-01-01T12:00:00Z",
                        from_id="user1",
                        from_name="User 1",
                        content="Hello world",
                    )
                ],
            )
        ],
    )


@pytest.fixture
def complex_skype_data():
    """
    Fixture for complex Skype data with multiple conversations and message types.
    """
    return SkypeDataFactory.build(
        userId="test-user-id",
        exportDate="2023-01-01T12:00:00Z",
        conversations=[
            SkypeConversationFactory.build(
                id="conversation1",
                displayName="Test Conversation 1",
                MessageList=[
                    SkypeMessageFactory.build(
                        id="msg1",
                        originalarrivaltime="2023-01-01T12:30:00Z",
                        from_id="user1",
                        from_name="User 1",
                        content="Hello, world!",
                    ),
                    SkypeMessageFactory.build(
                        id="msg2",
                        originalarrivaltime="2023-01-01T12:35:00Z",
                        from_id="user2",
                        from_name="User 2",
                        content="Hi there!",
                    ),
                    SkypeMessageFactory.build(
                        id="msg3",
                        originalarrivaltime="2023-01-01T12:40:00Z",
                        from_id="user1",
                        from_name="User 1",
                        content="<b>Bold text</b> and <i>italic text</i>",
                        messagetype="RichText/HTML",
                    ),
                ],
            ),
            SkypeConversationFactory.build(
                id="conversation2",
                displayName="Test Conversation 2",
                MessageList=[
                    SkypeMessageFactory.build(
                        id="msg4",
                        originalarrivaltime="2023-01-01T13:00:00Z",
                        from_id="user3",
                        from_name="User 3",
                        content="Message with emoji 😊",
                    ),
                    SkypeMessageFactory.build(
                        id="msg5",
                        originalarrivaltime="2023-01-01T13:05:00Z",
                        from_id="user1",
                        from_name="User 1",
                        content="https://example.com",
                        messagetype="RichText/Link",
                    ),
                ],
            ),
        ],
    )


@pytest.fixture
def invalid_skype_data():
    """
    Fixture for invalid Skype data for testing error handling.
    """
    return SkypeDataFactory.build(
        userId=None,  # Missing user ID
        exportDate="invalid-date",  # Invalid date format
        conversations=[],  # Empty conversations list
    )


@pytest.fixture
def skype_data_with_missing_display_name():
    """
    Fixture for Skype data with a conversation missing a display name.
    """
    return SkypeDataFactory.build(
        conversations=[
            SkypeConversationFactory.build(
                id="conv1",
                displayName=None,  # Missing display name
                MessageList=[
                    SkypeMessageFactory.build(
                        id="msg1", content="This message should be skipped"
                    )
                ],
            ),
            SkypeConversationFactory.build(
                id="conv2",
                displayName="Valid Conversation",
                MessageList=[
                    SkypeMessageFactory.build(
                        id="msg2", content="This message should be processed"
                    )
                ],
            ),
        ]
    )


@pytest.fixture
def skype_data_with_various_message_types():
    """
    Fixture for Skype data with various message types.
    """
    return SkypeDataFactory.build(
        conversations=[
            SkypeConversationFactory.build(
                id="conv1",
                displayName="Message Types Test",
                MessageList=[
                    SkypeMessageFactory.build(id="msg1", content="Regular message"),
                    SkypeMessageFactory.build(
                        id="msg2",
                        content="<b>Bold text</b>",
                        messagetype="RichText/HTML",
                    ),
                    SkypeMessageFactory.build(
                        id="msg3",
                        content="https://example.com",
                        messagetype="RichText/Link",
                    ),
                    SkypeMessageFactory.build(
                        id="msg4", content="Call started", messagetype="Event/Call"
                    ),
                    SkypeMessageFactory.build(
                        id="msg5", content="User joined", messagetype="SystemMessage"
                    ),
                ],
            )
        ]
    )


@pytest.fixture
def skype_data_with_edited_messages():
    """
    Fixture for Skype data with edited messages.
    """
    return SkypeDataFactory.build(
        conversations=[
            SkypeConversationFactory.build(
                id="conv1",
                displayName="Edited Messages Test",
                MessageList=[
                    SkypeMessageFactory.build(id="msg1", content="Original message"),
                    SkypeMessageFactory.build(
                        id="msg2",
                        content="Edited message",
                        edittime="2023-01-01T13:00:00Z",
                    ),
                ],
            )
        ]
    )


@pytest.fixture
def large_skype_data():
    """
    Fixture for large Skype data for performance testing.
    This fixture has a session scope to avoid recreating it for each test.
    """
    return SkypeDataFactory.build(with_conversation_count=10)


# Database record fixtures


@pytest.fixture
def conversation_record():
    """
    Fixture for a conversation database record.
    """
    return DatabaseRecordFactory.build(conversation=True)


@pytest.fixture
def message_record():
    """
    Fixture for a message database record.
    """
    return DatabaseRecordFactory.build(message=True)


# Utility fixtures


@pytest.fixture
def temp_dir():
    """
    Fixture for a temporary directory that is cleaned up after the test.
    """
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    import shutil

    shutil.rmtree(temp_dir)


# Create a fixture for test_db_connection to avoid pytest collecting it as a test
@pytest.fixture
def test_db_connection():
    """
    Fixture that wraps the test_db_connection function from fixtures.

    This prevents pytest from collecting the original function as a test.
    """
    # Use assert to verify the database is available instead of returning the context manager
    assert is_db_available(), "Database is not available for testing"

    # Return a function that creates a connection when called
    # This avoids returning the context manager directly
    def _get_connection(config=None):
        return _test_db_connection(config)

    return _get_connection


# Add these fixtures near the end of the file, before the data fixtures

@pytest.fixture
def expected_transformed_message() -> TransformedMessage:
    """Fixture for a basic expected transformed message."""
    return BASIC_TRANSFORMED_MESSAGE


@pytest.fixture
def expected_transformed_conversation() -> TransformedConversation:
    """Fixture for a basic expected transformed conversation."""
    return BASIC_TRANSFORMED_CONVERSATION


@pytest.fixture
def expected_transformed_data() -> Dict[str, Any]:
    """Fixture for a basic expected transformed data structure."""
    return BASIC_TRANSFORMED_DATA


@pytest.fixture
def expected_db_queries() -> Dict[str, str]:
    """Fixture for expected database queries."""
    return EXPECTED_DB_QUERIES


@pytest.fixture
def message_type_descriptions() -> Dict[str, str]:
    """Fixture for expected message type descriptions."""
    return MESSAGE_TYPE_DESCRIPTIONS


@pytest.fixture
def custom_expected_message() -> Callable[..., TransformedMessage]:
    """Fixture that allows customizing an expected message."""
    def _custom_expected_message(**kwargs: Any) -> TransformedMessage:
        return get_expected_transformed_message(**kwargs)
    return _custom_expected_message


@pytest.fixture
def custom_expected_conversation() -> Callable[..., TransformedConversation]:
    """Fixture that allows customizing an expected conversation."""
    def _custom_expected_conversation(**kwargs: Any) -> TransformedConversation:
        return get_expected_transformed_conversation(**kwargs)
    return _custom_expected_conversation


@pytest.fixture
def expected_media_data() -> Dict[str, str]:
    """Return expected data for media message handler."""
    return EXPECTED_MEDIA_MESSAGE_DATA


@pytest.fixture
def expected_scheduled_call_data() -> Dict[str, str]:
    """Return expected data for scheduled call handler."""
    return EXPECTED_SCHEDULED_CALL_DATA


@pytest.fixture
def expected_poll_data() -> Dict[str, Union[str, List[str]]]:
    """Return expected data for poll handler."""
    return EXPECTED_POLL_DATA


@pytest.fixture
def custom_handler_data() -> Callable[[str, ...], Dict[str, Any]]:
    """Return a function to get customized handler data."""
    return get_expected_handler_data


@pytest.fixture
def expected_api_response() -> Callable[[str, ...], Dict[str, Any]]:
    """Return a function to get expected API responses."""
    return get_expected_api_response
