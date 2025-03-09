"""
Mock implementation of message processor for testing.

This module provides a mock implementation of the message processor
for use in unit tests.
"""

from typing import Any, Dict, List


class MockMessageProcessor:
    """
    Mock message processor for handling message data extraction and transformation.

    This class extracts the message processing logic from the MockExtractor and
    MockTransformer classes to follow the Single Responsibility Principle.
    """

    def __init__(self, extract_messages_return=None, transform_message_return=None):
        """
        Initialize the mock message processor.

        Args:
            extract_messages_return: Value to return from extract_messages_from_conversations
            transform_message_return: Value to return from transform_message
        """
        self.extract_messages_return = extract_messages_return
        self.transform_message_return = transform_message_return

        # Track method calls for verification in tests
        self.extract_messages_calls = []
        self.transform_message_calls = []
        self.transform_conversation_message_calls = []

    def extract_messages_from_conversations(
        self, data: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Extract all messages from all conversations in the data.

        Args:
            data: The raw Skype data

        Returns:
            List of extracted messages
        """
        self.extract_messages_calls.append(data)

        if self.extract_messages_return is not None:
            return self.extract_messages_return

        messages = []
        for conv in data.get("conversations", []):
            messages.extend(conv.get("MessageList", []))
        return messages

    def transform_message(self, msg: Dict[str, Any], conv_id: str) -> Dict[str, Any]:
        """
        Transform a raw message into a structured format.

        Args:
            msg: The raw message data
            conv_id: The conversation ID

        Returns:
            Transformed message
        """
        self.transform_message_calls.append((msg, conv_id))

        if self.transform_message_return is not None:
            return self.transform_message_return

        return {
            "id": msg.get("id", "unknown"),
            "timestamp": msg.get("originalarrivaltime", "unknown"),
            "sender": msg.get("from", "unknown"),
            "content": msg.get("content", ""),
            "message_type": msg.get("messagetype", "unknown"),
            "conversation_id": conv_id,
        }

    def transform_conversation_message(self, msg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform a message for inclusion in a conversation.

        Args:
            msg: The raw message data

        Returns:
            Transformed message for conversation
        """
        self.transform_conversation_message_calls.append(msg)

        return {
            "id": msg.get("id", "unknown"),
            "timestamp": msg.get("originalarrivaltime", "unknown"),
            "sender": msg.get("from", "unknown"),
            "content": msg.get("content", ""),
            "message_type": msg.get("messagetype", "unknown"),
        }