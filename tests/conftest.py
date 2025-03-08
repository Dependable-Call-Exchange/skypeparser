"""
Pytest configuration and fixtures for the Skype Parser test suite.

This module provides fixtures for tests, including factory fixtures for
generating test data.
"""

import os
import sys
import tempfile
from typing import Any, Dict

import pytest

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tests.fixtures import (
    DatabaseRecordFactory,
    SkypeConversationFactory,
    SkypeDataFactory,
    SkypeMessageFactory,
    is_db_available,
)
from tests.fixtures import test_db_connection as _test_db_connection

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
