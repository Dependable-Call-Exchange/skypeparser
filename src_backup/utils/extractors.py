"""
Extractor strategy pattern for structured data extraction.

This module provides the IExtractor interface and concrete implementations
for extracting structured data from raw input.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, Optional, Callable


class IExtractor(ABC):
    """Abstract base class for structured data extractors."""

    @abstractmethod
    def extract(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from raw input.

        Args:
            raw_data: Raw data to extract structured data from

        Returns:
            Structured data
        """
        pass


class DefaultExtractor(IExtractor):
    """Default structured data extractor."""

    def extract(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data using default implementation.

        Args:
            raw_data: Raw data to extract structured data from

        Returns:
            Structured data
        """
        # Extract user information
        user_id = ""
        user_display_name = ""

        # Try to extract user information from various sources
        if "userProfile" in raw_data:
            user_profile = raw_data["userProfile"]
            user_id = user_profile.get("id", "")
            user_display_name = user_profile.get("displayName", "")
        elif "user" in raw_data:
            user = raw_data["user"]
            user_id = user.get("id", "")
            user_display_name = user.get("displayName", "")
        else:
            user_id = raw_data.get("userId", "")

        # Extract export date
        export_date = raw_data.get("exportDate", datetime.now().isoformat())

        # Extract messages
        messages = raw_data.get("messages", [])

        # Extract conversations
        conversations = {}
        if "conversations" in raw_data:
            if isinstance(raw_data["conversations"], list):
                # Convert list to dict
                for conv in raw_data["conversations"]:
                    if isinstance(conv, dict) and "id" in conv:
                        conv_id = conv["id"]
                        conversations[conv_id] = conv
            else:
                conversations = raw_data["conversations"]
        elif "endpoints" in raw_data:
            # Convert endpoints to conversations
            endpoints = raw_data["endpoints"]
            for endpoint_id, endpoint in endpoints.items():
                conversations[endpoint_id] = {
                    "id": endpoint_id,
                    "displayName": endpoint.get("displayName", ""),
                    "type": endpoint.get("type", ""),
                    "members": endpoint.get("members", []),
                }

        # Combine structured data
        structured_data = {
            "user_id": user_id,
            "user_display_name": user_display_name,
            "export_date": export_date,
            "messages": messages,
            "conversations": conversations,
        }

        return structured_data


class CallableExtractor(IExtractor):
    """Extractor that uses a provided callable."""

    def __init__(self, extractor_callable: Callable[[Dict[str, Any]], Dict[str, Any]]):
        """Initialize the callable extractor.

        Args:
            extractor_callable: Callable that extracts structured data
        """
        self.extractor_callable = extractor_callable

    def extract(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data using the provided callable.

        Args:
            raw_data: Raw data to extract structured data from

        Returns:
            Structured data
        """
        try:
            result = self.extractor_callable(raw_data)
            if not result:
                # Fall back to default extractor if callable returns empty result
                return DefaultExtractor().extract(raw_data)
            return result
        except Exception:
            # Fall back to default extractor if callable raises an exception
            return DefaultExtractor().extract(raw_data)


class ObjectExtractor(IExtractor):
    """Extractor that uses an object with an extract method."""

    def __init__(self, extractor_object: Any):
        """Initialize the object extractor.

        Args:
            extractor_object: Object with an extract method
        """
        self.extractor_object = extractor_object

    def extract(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data using the provided object.

        Args:
            raw_data: Raw data to extract structured data from

        Returns:
            Structured data
        """
        try:
            result = self.extractor_object.extract(raw_data)
            if not result:
                # Fall back to default extractor if object returns empty result
                return DefaultExtractor().extract(raw_data)
            return result
        except Exception:
            # Fall back to default extractor if object raises an exception
            return DefaultExtractor().extract(raw_data)