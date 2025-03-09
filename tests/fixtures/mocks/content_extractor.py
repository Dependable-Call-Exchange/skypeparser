"""
Mock implementation of ContentExtractor for testing.

This module provides a mock implementation of the ContentExtractor class
for use in unit tests.
"""

from typing import Any, Dict, List


class MockContentExtractor:
    """
    Mock implementation of ContentExtractor for testing.

    This class follows the ContentExtractorProtocol interface and provides
    configurable return values for testing different scenarios.
    """

    def __init__(
        self,
        extract_all_return=None,
        clean_content_return=None,
        extract_mentions_return=None,
        extract_links_return=None,
        extract_cleaned_content_return=None
    ):
        """
        Initialize the mock content extractor.

        Args:
            extract_all_return: Value to return from extract_all method
            clean_content_return: Value to return from clean_content method
            extract_mentions_return: Value to return from extract_mentions method
            extract_links_return: Value to return from extract_links method
            extract_cleaned_content_return: Value to return from extract_cleaned_content method
        """
        self.extract_all_return = extract_all_return or {"links": [], "mentions": []}
        self.clean_content_return = clean_content_return or "Cleaned content"
        self.extract_mentions_return = extract_mentions_return or []
        self.extract_links_return = extract_links_return or []
        self.extract_cleaned_content_return = extract_cleaned_content_return or "Cleaned content"

        # Track method calls for verification in tests
        self.extract_all_calls = []
        self.clean_content_calls = []
        self.extract_mentions_calls = []
        self.extract_links_calls = []
        self.extract_cleaned_content_calls = []

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
        self.extract_mentions_calls.append(content)
        return self.extract_mentions_return or self.extract_all_return.get("mentions", [])

    def extract_links(self, content: str) -> List[Dict[str, str]]:
        """
        Mock implementation of extract_links.

        Args:
            content: Message content

        Returns:
            List of links
        """
        self.extract_links_calls.append(content)
        return self.extract_links_return or self.extract_all_return.get("links", [])

    def extract_cleaned_content(self, content: str) -> str:
        """
        Mock implementation of extract_cleaned_content.

        Args:
            content: Message content

        Returns:
            Cleaned content with mentions and links removed
        """
        self.extract_cleaned_content_calls.append(content)
        return self.extract_cleaned_content_return