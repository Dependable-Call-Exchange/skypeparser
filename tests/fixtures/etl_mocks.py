"""
Mock implementations for ETL pipeline components.

This module provides mock implementations of the ETL pipeline components
for use in unit tests. These mocks follow the same interfaces as the real
components but provide simplified behavior for testing.
"""

import io
import json
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Tuple

from src.utils.interfaces import (
    ContentExtractorProtocol,
    ExtractorProtocol,
    FileHandlerProtocol,
    MessageHandlerFactoryProtocol,
    MessageHandlerProtocol,
    StructuredDataExtractorProtocol,
    TransformerProtocol,
    ValidationServiceProtocol,
)
from unittest.mock import MagicMock

from .skype_data import BASIC_SKYPE_DATA, COMPLEX_SKYPE_DATA, INVALID_SKYPE_DATA


class MockDatabase:
    """Mock database for testing."""

    def __init__(self, should_fail=False):
        """Initialize the mock database."""
        self.should_fail = should_fail
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.execute_count = 0
        self.queries = []
        self.params = []

        # Create a mock cursor with a connection attribute that has encoding
        self.mock_cursor = MagicMock()
        self.mock_cursor.connection = MagicMock()
        self.mock_cursor.connection.encoding = 'UTF8'

        # Set up fetchone to return a mock result with __getitem__ implemented
        mock_result = MagicMock()
        mock_result.__getitem__.side_effect = lambda key: "mock_value"
        self.mock_cursor.fetchone.return_value = mock_result

        # Set up mogrify to return bytes instead of a MagicMock
        self.mock_cursor.mogrify.side_effect = lambda template, args: b'MOCK_MOGRIFIED_SQL'

        # Set up execute to increment the execute_count and track queries
        original_execute = self.mock_cursor.execute
        def execute_with_count(query, params=None):
            self.execute_count += 1
            self.queries.append(query)
            self.params.append(params)
            return original_execute(query, params)
        self.mock_cursor.execute = execute_with_count

        # Set up __enter__ and __exit__ for cursor context manager support
        self.mock_cursor.__enter__ = MagicMock(return_value=self.mock_cursor)
        self.mock_cursor.__exit__ = MagicMock(return_value=None)

        # Make self a context manager too
        self.__enter__ = MagicMock(return_value=self)
        self.__exit__ = MagicMock(return_value=None)

    def cursor(self):
        """Return a mock cursor."""
        return self.mock_cursor

    def execute(self, query, params=None):
        """Execute a query."""
        if self.should_fail:
            raise Exception("Mock database error")
        self.execute_count += 1
        self.queries.append(query)
        self.params.append(params)
        return self.mock_cursor

    def commit(self):
        """Commit the transaction."""
        self.committed = True

    def rollback(self):
        """Rollback the transaction."""
        self.rolled_back = True

    def close(self):
        """Close the connection."""
        self.closed = True


class MockMessageProcessor:
    """
    Mock message processor for handling message data extraction and transformation.

    This class extracts the message processing logic from the MockExtractor and
    MockTransformer classes to follow the Single Responsibility Principle.
    """

    @staticmethod
    def extract_messages_from_conversations(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract all messages from all conversations in the data.

        Args:
            data: The raw Skype data

        Returns:
            List of extracted messages
        """
        messages = []
        for conv in data.get("conversations", []):
            messages.extend(conv.get("MessageList", []))
        return messages

    @staticmethod
    def transform_message(msg: Dict[str, Any], conv_id: str) -> Dict[str, Any]:
        """
        Transform a raw message into a structured format.

        Args:
            msg: The raw message data
            conv_id: The conversation ID

        Returns:
            Transformed message
        """
        return {
            "id": msg.get("id", "unknown"),
            "timestamp": msg.get("originalarrivaltime", "unknown"),
            "sender": msg.get("from", "unknown"),
            "content": msg.get("content", ""),
            "message_type": msg.get("messagetype", "unknown"),
            "conversation_id": conv_id
        }

    @staticmethod
    def transform_conversation_message(msg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a message for inclusion in a conversation.

        Args:
            msg: The raw message data

        Returns:
            Transformed message for conversation
        """
        return {
            "id": msg.get("id", "unknown"),
            "timestamp": msg.get("originalarrivaltime", "unknown"),
            "sender": msg.get("from", "unknown"),
            "content": msg.get("content", ""),
            "message_type": msg.get("messagetype", "unknown")
        }


class MockExtractor(ExtractorProtocol):
    """Mock extractor for testing."""

    def __init__(self):
        self.extract_called = False
        self.file_path = None
        self.file_obj = None
        self.message_processor = MockMessageProcessor()

    def extract(self, file_path: Optional[str] = None, file_obj: Optional[BinaryIO] = None) -> Dict[str, Any]:
        """Mock extract method."""
        self.extract_called = True
        self.file_path = file_path
        self.file_obj = file_obj

        # Add a messages key to the data to satisfy the loader validation
        data = BASIC_SKYPE_DATA.copy()

        # Extract all messages from all conversations
        data["messages"] = self.message_processor.extract_messages_from_conversations(data)
        return data


class MockTransformer(TransformerProtocol):
    """Mock transformer for testing."""

    def __init__(self):
        self.transform_called = False
        self.raw_data = None
        self.user_display_name = None
        self.message_processor = MockMessageProcessor()

    def transform(self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None) -> Dict[str, Any]:
        """Mock transform method."""
        self.transform_called = True
        self.raw_data = raw_data
        self.user_display_name = user_display_name

        # Extract all messages from all conversations
        all_messages = {}

        # Create conversations as a dictionary keyed by conversation ID
        conversations = {}
        for conv in raw_data.get("conversations", []):
            conv_id = conv.get("id", "unknown")

            # Create messages for this conversation
            conv_messages = []
            all_messages[conv_id] = []

            for msg in conv.get("MessageList", []):
                # Transform the message for the all_messages dictionary
                message = self.message_processor.transform_message(msg, conv_id)

                # Transform the message for the conversation
                conv_message = self.message_processor.transform_conversation_message(msg)

                conv_messages.append(conv_message)
                all_messages[conv_id].append(message)

            # Add this conversation to the dictionary
            conversations[conv_id] = {
                "id": conv_id,
                "display_name": conv.get("displayName", "unknown"),
                "messages": conv_messages
            }

        return {
            "user_id": raw_data.get("userId", "unknown"),
            "export_date": raw_data.get("exportDate", "unknown"),
            "conversations": conversations,  # Now a dictionary keyed by conversation ID
            "messages": all_messages  # Dictionary with conversation_id as keys
        }


class MockFileHandler(FileHandlerProtocol):
    """Mock file handler for testing."""

    def __init__(self, default_data: Dict[str, Any] = None):
        self.default_data = default_data or BASIC_SKYPE_DATA
        self.read_file_called = False
        self.read_file_object_called = False
        self.read_tarfile_called = False
        self.read_tarfile_object_called = False
        self.file_path = None
        self.file_obj = None

    def read_file(self, file_path: str) -> Dict[str, Any]:
        """Mock read_file method."""
        self.read_file_called = True
        self.file_path = file_path
        return self.default_data

    def read_file_object(self, file_obj: BinaryIO, file_name: Optional[str] = None) -> Dict[str, Any]:
        """Mock read_file_object method."""
        self.read_file_object_called = True
        self.file_obj = file_obj
        return self.default_data

    # Alias for backward compatibility
    read_file_obj = read_file_object

    def read_tarfile(self, file_path: str, auto_select: bool = False, select_json: Optional[int] = None) -> Dict[str, Any]:
        """Mock read_tarfile method."""
        self.read_tarfile_called = True
        self.file_path = file_path
        return self.default_data

    def read_tarfile_object(self, file_obj: BinaryIO, auto_select: bool = False, select_json: Optional[int] = None) -> Dict[str, Any]:
        """Mock read_tarfile_object method."""
        self.read_tarfile_object_called = True
        self.file_obj = file_obj
        return self.default_data

    def read_tarfile_streaming(self, file_path: str, auto_select: bool = False) -> Iterator[Tuple[str, Any]]:
        """Mock read_tarfile_streaming method."""
        yield "test.json", self.default_data


class MockValidationService(ValidationServiceProtocol):
    """Mock validation service for testing."""

    def __init__(self):
        self.validate_file_exists_called = False
        self.validate_json_file_called = False
        self.validate_file_object_called = False
        self.validate_user_display_name_called = False
        self.file_path = None
        self.file_obj = None
        self.user_display_name = None

    def validate_file_exists(
        self,
        path: str,
        base_dir: Optional[str] = None,
        allow_absolute: bool = False,
        allow_symlinks: bool = False,
    ) -> bool:
        """Mock validate_file_exists method."""
        self.validate_file_exists_called = True
        self.file_path = path
        return True

    def validate_json_file(
        self,
        file_path: str,
        base_dir: Optional[str] = None,
        allow_absolute: bool = False,
        allow_symlinks: bool = False,
    ) -> Dict[str, Any]:
        """Mock validate_json_file method."""
        self.validate_json_file_called = True
        self.file_path = file_path
        return BASIC_SKYPE_DATA

    def validate_file_object(self, file_obj: BinaryIO) -> bool:
        """Mock validate_file_object method."""
        self.validate_file_object_called = True
        self.file_obj = file_obj
        return True

    def validate_user_display_name(self, name: str) -> str:
        """Mock validate_user_display_name method."""
        self.validate_user_display_name_called = True
        self.user_display_name = name
        return name


class MockContentExtractor(ContentExtractorProtocol):
    """Mock content extractor for testing."""

    def __init__(self):
        self.extract_content_called = False
        self.extract_html_content_called = False
        self.extract_cleaned_content_called = False
        self.message = None

    def extract_content(self, message: Dict[str, Any]) -> str:
        """Mock extract_content method."""
        self.extract_content_called = True
        self.message = message
        return message.get("content", "")

    def extract_html_content(self, message: Dict[str, Any]) -> str:
        """Mock extract_html_content method."""
        self.extract_html_content_called = True
        self.message = message
        return message.get("content", "")

    def extract_cleaned_content(self, message: Dict[str, Any]) -> str:
        """Mock extract_cleaned_content method."""
        self.extract_cleaned_content_called = True
        self.message = message
        return message.get("content", "")


class MockStructuredDataExtractor(StructuredDataExtractorProtocol):
    """Mock structured data extractor for testing."""

    def __init__(self):
        self.extract_structured_data_called = False
        self.message = None

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Mock extract_structured_data method."""
        self.extract_structured_data_called = True
        self.message = message
        return {
            "id": message.get("id", ""),
            "timestamp": message.get("originalarrivaltime", ""),
            "sender": message.get("from", ""),
            "content": message.get("content", ""),
            "message_type": message.get("messagetype", "")
        }


class MockMessageHandler(MessageHandlerProtocol):
    """Mock message handler for testing."""

    def __init__(self, message_type: str):
        self.can_handle_called = False
        self.extract_data_called = False
        self.message_type = message_type
        self.message = None

    def can_handle(self, message_type: str) -> bool:
        """Mock can_handle method."""
        self.can_handle_called = True
        return message_type == self.message_type

    def extract_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Mock extract_data method."""
        self.extract_data_called = True
        self.message = message
        return {
            "id": message.get("id", ""),
            "timestamp": message.get("originalarrivaltime", ""),
            "sender": message.get("from", ""),
            "content": message.get("content", ""),
            "message_type": message.get("messagetype", "")
        }


class MockMessageHandlerFactory(MessageHandlerFactoryProtocol):
    """Mock message handler factory for testing."""

    def __init__(self):
        self.get_handler_called = False
        self.message_type = None
        self.handlers = {
            "RichText": MockMessageHandler("RichText"),
            "Text": MockMessageHandler("Text"),
            "Media": MockMessageHandler("Media"),
            "Poll": MockMessageHandler("Poll"),
            "Call": MockMessageHandler("Call"),
            "Location": MockMessageHandler("Location"),
            "Contacts": MockMessageHandler("Contacts"),
            "Event": MockMessageHandler("Event"),
            "ScheduledCall": MockMessageHandler("ScheduledCall")
        }

    def get_handler(self, message_type: str) -> MessageHandlerProtocol:
        """Mock get_handler method."""
        self.get_handler_called = True
        self.message_type = message_type
        return self.handlers.get(message_type, MockMessageHandler("Unknown"))


class MockProgressTracker:
    """Mock progress tracker for testing."""

    def __init__(self):
        """Initialize the mock progress tracker."""
        self.phases = {}
        self.current_phase = None
        self.total_items = 0
        self.processed_items = 0
        self.item_type = None
        self.update_count = 0

    def update(self, phase, current, total, item_type=None):
        """Update the progress tracker.

        Args:
            phase (str): The current phase of the ETL process
            current (int): The current number of items processed
            total (int): The total number of items to process
            item_type (str, optional): The type of items being processed
        """
        self.current_phase = phase
        self.processed_items = current
        self.total_items = total
        self.item_type = item_type
        self.phases[phase] = {
            'current': current,
            'total': total,
            'item_type': item_type
        }
        self.update_count += 1