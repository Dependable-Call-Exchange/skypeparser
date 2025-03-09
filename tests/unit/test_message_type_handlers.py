#!/usr/bin/env python3
"""
Unit tests for message type handlers.

DEPRECATED: Please use test_message_type_handlers_pytest.py instead.
This file is maintained for reference only and will be removed in a future update.
"""

import unittest
from unittest.mock import MagicMock, patch

from src.utils.message_type_handlers import (
    MediaMessageHandler,
    MessageHandlerError,
    PollMessageHandler,
    ScheduledCallHandler,
    SkypeMessageHandlerFactory,
    TextMessageHandler,
    extract_structured_data,
    get_handler_for_message_type,
)

# Import centralized expectations and fixtures
from tests.fixtures.expected_data import (
    get_expected_handler_data,
)

# Define expected data for tests
EXPECTED_MEDIA_DATA = {
    "media_filename": "test_file.jpg",
    "media_filesize": "1024",
    "media_type": "image/jpeg",
    "media_url": "https://example.com/test_file.jpg"
}

EXPECTED_POLL_DATA = {
    "poll_question": "What's your favorite color?",
    "poll_options": ["Red", "Blue", "Green"]
}

class TestMessageTypeHandlers(unittest.TestCase):
    """Test cases for message type handlers."""

    def setUp(self):
        """Set up test fixtures."""
        # Sample messages for testing
        self.text_message = {
            "id": "12345",
            "content": "Hello world",
            "messagetype": "RichText"
        }

        self.media_message = {
            "id": "12345",
            "content": "<URIObject uri='https://example.com/test_file.jpg' type='image/jpeg'>Test file</URIObject>",
            "messagetype": "RichText/Media",
            "properties": {
                "originalName": "test_file.jpg",
                "fileSize": "1024"
            }
        }

        self.poll_message = {
            "id": "12345",
            "content": "<poll question='What&apos;s your favorite color?'><option>Red</option><option>Blue</option><option>Green</option></poll>",
            "messagetype": "RichText/Poll"
        }

        self.scheduled_call_message = {
            "id": "12345",
            "content": "Join meeting",
            "messagetype": "ScheduledCall",
            "properties": {
                "scheduledStartTime": "2023-01-01T12:00:00Z",
                "scheduledEndTime": "2023-01-01T13:00:00Z",
                "title": "Team Meeting"
            }
        }

    def test_get_handler_for_message_type(self):
        """Test getting the appropriate handler for a message type."""
        # Test with text message
        text_handler = get_handler_for_message_type("RichText")
        self.assertIsInstance(text_handler, TextMessageHandler)

        # Test with media message
        media_handler = get_handler_for_message_type("RichText/Media")
        self.assertIsInstance(media_handler, MediaMessageHandler)

        # Test with poll message
        poll_handler = get_handler_for_message_type("RichText/Poll")
        self.assertIsInstance(poll_handler, PollMessageHandler)

        # Test with scheduled call message
        call_handler = get_handler_for_message_type("ScheduledCall")
        self.assertIsInstance(call_handler, ScheduledCallHandler)

        # Test with unknown message type
        unknown_handler = get_handler_for_message_type("Unknown")
        self.assertIsNotNone(unknown_handler)
        self.assertIsInstance(unknown_handler, TextMessageHandler)  # Default handler

    def test_extract_structured_data(self):
        """Test extracting structured data from a message."""
        # Test with poll message
        poll_data = extract_structured_data(self.poll_message)
        self.assertEqual(poll_data["poll_question"], EXPECTED_POLL_DATA["poll_question"])
        self.assertEqual(poll_data["poll_options"], EXPECTED_POLL_DATA["poll_options"])

        # Test with media message
        media_data = extract_structured_data(self.media_message)
        self.assertEqual(media_data["media_filename"], EXPECTED_MEDIA_DATA["media_filename"])
        self.assertEqual(media_data["media_filesize"], EXPECTED_MEDIA_DATA["media_filesize"])

        # Test with text message (should return empty dict)
        text_data = extract_structured_data(self.text_message)
        self.assertEqual(text_data, {})

    def test_handler_error_handling(self):
        """Test error handling in message handlers."""
        # Create a message with invalid content
        invalid_message = {
            "id": "12345",
            "content": "Invalid content",
            "messagetype": "RichText/Media"  # Media type but no media content
        }

        # Create handler
        handler = MediaMessageHandler()

        # Test extraction with invalid content
        invalid_data = handler.extract_data(invalid_message)
        self.assertIn("message_id", invalid_data)
        self.assertEqual(invalid_data["content_type"], "media")
        self.assertEqual(invalid_data["structured_data"], {})  # Empty due to error

        # Test with completely invalid message
        very_invalid_message = {"invalid": "structure"}
        with self.assertRaises(MessageHandlerError):
            handler.extract_data(very_invalid_message)

    def test_media_handler(self):
        """Test MediaMessageHandler."""
        # Create handler
        handler = MediaMessageHandler()

        # Test extraction
        data = handler.extract_data(self.media_message)

        self.assertEqual(data["message_id"], self.media_message["id"])
        self.assertEqual(data["content_type"], "media")
        self.assertEqual(data["structured_data"]["media_filename"], EXPECTED_MEDIA_DATA["media_filename"])
        self.assertEqual(data["structured_data"]["media_filesize"], EXPECTED_MEDIA_DATA["media_filesize"])

    def test_poll_handler(self):
        """Test PollMessageHandler."""
        # Create handler
        handler = PollMessageHandler()

        # Test extraction
        data = handler.extract_data(self.poll_message)

        self.assertEqual(data["message_id"], self.poll_message["id"])
        self.assertEqual(data["content_type"], "poll")
        self.assertEqual(data["structured_data"]["poll_question"], EXPECTED_POLL_DATA["poll_question"])
        self.assertEqual(data["structured_data"]["poll_options"], EXPECTED_POLL_DATA["poll_options"])


if __name__ == "__main__":
    unittest.main()
