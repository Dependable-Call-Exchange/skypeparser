"""
Mock implementations of ETL components for testing.

This module provides mock implementations of ETL components
(Extractor, Transformer, Loader) for use in unit tests.
"""

import io
import json
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Tuple

from src.utils.interfaces import (
    ExtractorProtocol,
    LoaderProtocol,
    TransformerProtocol,
)

from .message_processor import MockMessageProcessor
from ..skype_data import BASIC_SKYPE_DATA


class MockExtractor(ExtractorProtocol):
    """
    Mock extractor for testing.

    This class provides a mock implementation of the Extractor
    with configurable behavior for testing different scenarios.
    """

    def __init__(self, extract_return=None):
        """
        Initialize the mock extractor.

        Args:
            extract_return: Value to return from extract method
        """
        self.extract_return = extract_return
        self.extract_called = False
        self.file_path = None
        self.file_obj = None
        self.message_processor = MockMessageProcessor()

    def extract(
        self, file_path: Optional[str] = None, file_obj: Optional[BinaryIO] = None
    ) -> Dict[str, Any]:
        """
        Mock extract method.

        Args:
            file_path: Path to the file to extract data from
            file_obj: File-like object to extract data from

        Returns:
            Extracted data
        """
        self.extract_called = True
        self.file_path = file_path
        self.file_obj = file_obj

        if self.extract_return is not None:
            return self.extract_return

        # Add a messages key to the data to satisfy the loader validation
        data = BASIC_SKYPE_DATA.copy()

        # Extract all messages from all conversations
        data["messages"] = self.message_processor.extract_messages_from_conversations(
            data
        )
        return data


class MockTransformer(TransformerProtocol):
    """
    Mock transformer for testing.

    This class provides a mock implementation of the Transformer
    with configurable behavior for testing different scenarios.
    """

    def __init__(self, transform_return=None):
        """
        Initialize the mock transformer.

        Args:
            transform_return: Value to return from transform method
        """
        self.transform_return = transform_return
        self.transform_called = False
        self.raw_data = None
        self.user_display_name = None
        self.message_processor = MockMessageProcessor()

    def transform(
        self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Mock transform method.

        Args:
            raw_data: Raw data to transform
            user_display_name: Display name of the user

        Returns:
            Transformed data
        """
        self.transform_called = True
        self.raw_data = raw_data
        self.user_display_name = user_display_name

        if self.transform_return is not None:
            return self.transform_return

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
                conv_message = self.message_processor.transform_conversation_message(
                    msg
                )

                conv_messages.append(conv_message)
                all_messages[conv_id].append(message)

            # Add this conversation to the dictionary
            conversations[conv_id] = {
                "id": conv_id,
                "display_name": conv.get("displayName", "unknown"),
                "messages": conv_messages,
            }

        return {
            "user_id": raw_data.get("userId", "unknown"),
            "export_date": raw_data.get("exportDate", "unknown"),
            "conversations": conversations,  # Now a dictionary keyed by conversation ID
            "messages": all_messages,  # Dictionary with conversation_id as keys
        }


class MockLoader(LoaderProtocol):
    """
    Mock loader for testing.

    This class provides a mock implementation of the Loader
    with configurable behavior for testing different scenarios.
    """

    def __init__(self, load_return=None, should_fail=False):
        """
        Initialize the mock loader.

        Args:
            load_return: Value to return from load method
            should_fail: Whether the load method should fail
        """
        self.load_return = load_return or {"exportId": "mock-export-id"}
        self.should_fail = should_fail
        self.load_called = False
        self.transformed_data = None
        self.user_display_name = None

    def load(
        self, transformed_data: Dict[str, Any], user_display_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Mock load method.

        Args:
            transformed_data: Transformed data to load
            user_display_name: Display name of the user

        Returns:
            Load result

        Raises:
            Exception: If should_fail is True
        """
        self.load_called = True
        self.transformed_data = transformed_data
        self.user_display_name = user_display_name

        if self.should_fail:
            raise Exception("Mock loader error")

        return self.load_return