"""
Mock classes for testing.

This module provides mock implementations of various classes used in the Skype Parser,
making it easier to test components in isolation.
"""

from typing import Dict, List, Any, Optional, Callable


class MockContentExtractor:
    """
    Mock implementation of ContentExtractor for testing.
    """

    def __init__(self, extract_all_return=None, clean_content_return=None):
        """
        Initialize the mock content extractor.

        Args:
            extract_all_return: Value to return from extract_all method
            clean_content_return: Value to return from clean_content method
        """
        self.extract_all_return = extract_all_return or {"links": [], "mentions": []}
        self.clean_content_return = clean_content_return or "Cleaned content"
        self.extract_all_calls = []
        self.clean_content_calls = []

    def extract_all(self, content: str) -> Dict[str, Any]:
        """
        Mock implementation of extract_all.

        Args:
            content: Message content

        Returns:
            Dict with extracted data
        """
        self.extract_all_calls.append(content)
        return self.extract_all_return

    def clean_content(self, content: str) -> str:
        """
        Mock implementation of clean_content.

        Args:
            content: Message content

        Returns:
            Cleaned content
        """
        self.clean_content_calls.append(content)
        return self.clean_content_return

    def extract_mentions(self, content: str) -> List[Dict[str, str]]:
        """
        Mock implementation of extract_mentions.

        Args:
            content: Message content

        Returns:
            List of mentions
        """
        return self.extract_all_return.get("mentions", [])

    def extract_links(self, content: str) -> List[Dict[str, str]]:
        """
        Mock implementation of extract_links.

        Args:
            content: Message content

        Returns:
            List of links
        """
        return self.extract_all_return.get("links", [])


class MockMessageHandler:
    """
    Mock implementation of a message handler for testing.
    """

    def __init__(self, return_value=None):
        """
        Initialize the mock message handler.

        Args:
            return_value: Value to return when called
        """
        self.return_value = return_value or {"type": "mock_handler"}
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


def mock_structured_data_extractor(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Mock implementation of extract_structured_data.

    Args:
        message: Message to extract data from

    Returns:
        Structured data
    """
    message_type = message.get("messagetype", "")
    if message_type == "RichText/Link":
        return {"url": "https://example.com"}
    elif message_type == "Event/Call":
        return {"duration": 60}
    elif message_type == "RichText/HTML":
        return {"html_elements": ["b", "i"]}
    return {}