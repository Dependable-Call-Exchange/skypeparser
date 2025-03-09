"""
Mock implementations of structured data extractors for testing.

This module provides mock implementations of structured data extractors
for use in unit tests.
"""

from typing import Any, Dict


class MockStructuredDataExtractor:
    """
    Mock implementation of StructuredDataExtractor for testing.

    This class provides a mock implementation of the StructuredDataExtractor
    with configurable return values for testing different scenarios.
    """

    def __init__(self, extract_structured_data_return=None, extract_return=None):
        """
        Initialize the mock structured data extractor.

        Args:
            extract_structured_data_return: Value to return from extract_structured_data
            extract_return: Value to return from extract
        """
        self.extract_structured_data_return = extract_structured_data_return
        self.extract_return = extract_return
        self.extract_structured_data_calls = []
        self.extract_calls = []

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract structured data from a message.

        Args:
            message: Message to extract data from

        Returns:
            Structured data
        """
        self.extract_structured_data_calls.append(message)

        if self.extract_structured_data_return is not None:
            return self.extract_structured_data_return

        message_type = message.get("messagetype", "")
        if message_type == "RichText/Link":
            return {"url": "https://example.com"}
        elif message_type == "Event/Call":
            return {"duration": 60}
        elif message_type == "RichText/HTML":
            return {"html_elements": ["b", "i"]}
        else:
            return {"text": message.get("content", "")}

    def extract(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract structured data from raw Skype export data.

        This method handles the new Skype export format with a 'messages' array
        containing user, export date, and conversations info.

        Args:
            raw_data: The raw data from the Skype export

        Returns:
            Structured data containing user_id, export_date, and conversations
        """
        self.extract_calls.append(raw_data)

        if self.extract_return is not None:
            return self.extract_return

        # Default structure
        structured_data = {
            "user_id": "",
            "export_date": "",
            "conversations": []
        }

        # Check for new format with messages array
        if "messages" in raw_data and isinstance(raw_data["messages"], list) and len(raw_data["messages"]) > 0:
            message_obj = raw_data["messages"][0]

            # Extract user ID and export date
            structured_data["user_id"] = message_obj.get("userId", "")
            structured_data["export_date"] = message_obj.get("exportDate", "")

            # Extract conversations
            if "conversations" in message_obj:
                structured_data["conversations"] = message_obj.get("conversations", [])
        else:
            # Try to handle the data directly
            structured_data["user_id"] = raw_data.get("userId", raw_data.get("user_id", ""))
            structured_data["export_date"] = raw_data.get("exportDate", raw_data.get("export_date", ""))
            structured_data["conversations"] = raw_data.get("conversations", [])

        return structured_data


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