"""
Mock implementations of message handlers for testing.

This module provides mock implementations of message handlers and
message handler factories for use in unit tests.
"""

from typing import Any, Callable, Dict, Optional


class MockMessageHandler:
    """
    Mock implementation of a message handler for testing.

    This class provides a mock implementation of a message handler
    with configurable return values for testing different scenarios.
    """

    def __init__(self, return_value=None, message_type="RichText"):
        """
        Initialize the mock message handler.

        Args:
            return_value: Value to return when called
            message_type: Type of message this handler handles
        """
        self.return_value = return_value or {"type": "mock_handler"}
        self.message_type = message_type
        self.calls = []

    def __call__(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Mock implementation of the handler.

        Args:
            message: Message to handle

        Returns:
            Structured data
        """
        self.calls.append(message)
        return self.return_value

    def extract_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract structured data from a message.

        Args:
            message: Message to extract data from

        Returns:
            Structured data
        """
        self.calls.append(message)
        return self.return_value


class MockMessageHandlerFactory:
    """
    Mock implementation of a message handler factory for testing.

    This class provides a mock implementation of a message handler factory
    with configurable behavior for testing different scenarios.
    """

    def __init__(self, handlers=None):
        """
        Initialize the mock message handler factory.

        Args:
            handlers: Dictionary mapping message types to handlers
        """
        self.handlers = handlers or {
            "RichText": MockMessageHandler({"type": "text"}),
            "RichText/HTML": MockMessageHandler({"type": "html"}),
            "RichText/Link": MockMessageHandler({"type": "link"}),
            "Event/Call": MockMessageHandler({"type": "call"}),
            "SystemMessage": MockMessageHandler({"type": "system"}),
        }
        self.get_handler_calls = []

    def get_handler(self, message_type: str) -> Optional[Callable]:
        """
        Get a handler for a message type.

        Args:
            message_type: Type of message

        Returns:
            Handler for the message type or None
        """
        self.get_handler_calls.append(message_type)
        return self.handlers.get(message_type)


def mock_message_handler_factory(message_type: str) -> Optional[Callable]:
    """
    Mock implementation of get_handler_for_message_type.

    Args:
        message_type: Type of message

    Returns:
        Mock handler or None
    """
    handlers = {
        "RichText": MockMessageHandler({"type": "text"}),
        "RichText/HTML": MockMessageHandler({"type": "html"}),
        "RichText/Link": MockMessageHandler({"type": "link"}),
        "Event/Call": MockMessageHandler({"type": "call"}),
        "SystemMessage": MockMessageHandler({"type": "system"}),
    }
    return handlers.get(message_type)