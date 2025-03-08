#!/usr/bin/env python3
"""
Unit tests for message type handlers.
"""

import json
import os
import sys
import unittest
from typing import Any, Dict
from unittest.mock import MagicMock, patch

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from src.utils.message_type_handlers import (
    BaseMessageHandler,
    MediaMessageHandler,
    PollMessageHandler,
    TextMessageHandler,
    extract_structured_data,
    get_handler_for_message_type,
)


class TestMessageTypeHandlers(unittest.TestCase):
    """Test cases for message type handlers."""

    def setUp(self):
        """Set up test fixtures."""
        # Sample messages for different types
        self.poll_message = {
            "messagetype": "Poll",
            "content": "<pollquestion>What is your favorite color?</pollquestion>"
            "<polloption>Red</polloption>"
            "<polloption>Green</polloption>"
            "<polloption>Blue</polloption>",
        }

        self.call_message = {
            "messagetype": "Event/Call",
            "content": "<duration>00:05:30</duration>"
            '<participant id="user1" name="John Doe"></participant>'
            '<participant id="user2" name="Jane Smith"></participant>',
        }

        self.location_message = {
            "messagetype": "RichText/Location",
            "content": '<location latitude="37.7749" longitude="-122.4194"></location>'
            "<address>San Francisco, CA</address>",
        }

        self.contacts_message = {
            "messagetype": "RichText/Contacts",
            "content": '<contact name="John Doe" phone="+1234567890" email="john@example.com"></contact>'
            '<contact name="Jane Smith" phone="+0987654321" email="jane@example.com"></contact>',
        }

        self.media_message = {
            "messagetype": "RichText/Media_Video",
            "content": '<uriobject filename="video.mp4" filesize="1024000" filetype="video/mp4" url="https://example.com/video.mp4" width="1920" height="1080" duration="00:02:30" description="Vacation video"></uriobject>',
        }

        self.scheduled_call_message = {
            "messagetype": "RichText/ScheduledCallInvite",
            "content": '<scheduledcall title="Team Meeting" time="2023-01-01T12:00:00Z" duration="01:00:00"></scheduledcall>',
        }

        # New message types
        self.media_album_message = {
            "messagetype": "RichText/Media_Album",
            "content": '<img src="https://example.com/image1.jpg" thumbnail="https://example.com/thumb1.jpg" width="800" height="600" />'
            '<img src="https://example.com/image2.jpg" thumbnail="https://example.com/thumb2.jpg" width="800" height="600" />',
        }

        self.media_card_message = {
            "messagetype": "RichText/Media_Card",
            "content": '<card title="Article Title" description="Article description" url="https://example.com/article" thumbnail_url="https://example.com/thumbnail.jpg" provider="News Site"></card>',
        }

        self.popcard_message = {
            "messagetype": "PopCard",
            "content": '<popcard title="Notification" type="alert" action="dismiss"><content>This is a notification</content></popcard>',
        }

        self.translation_message = {
            "messagetype": "Translation",
            "content": '<translation from="en" to="fr">Bonjour</translation><original>Hello</original>',
        }

        self.thread_activity_add_member_message = {
            "messagetype": "ThreadActivity/AddMember",
            "content": '<member id="user3" name="Bob Johnson"></member><initiator name="John Doe"></initiator>',
        }

        self.thread_activity_topic_update_message = {
            "messagetype": "ThreadActivity/TopicUpdate",
            "content": '<value>New Topic Name</value><initiator name="Jane Smith"></initiator>',
        }

    def test_get_handler_for_message_type(self):
        """Test get_handler_for_message_type function."""
        # Test with known message types
        poll_handler = get_handler_for_message_type("Poll")
        self.assertIsNotNone(poll_handler)

        text_handler = get_handler_for_message_type("RichText")
        self.assertIsNotNone(text_handler)

        media_handler = get_handler_for_message_type("RichText/Media_Video")
        self.assertIsNotNone(media_handler)

        # Test with unknown message type
        unknown_handler = get_handler_for_message_type("Unknown")
        self.assertIsNotNone(unknown_handler)

    def test_poll_handler(self):
        """Test PollMessageHandler."""
        handler = PollMessageHandler()
        result = handler.extract_structured_data(self.poll_message)
        self.assertEqual(result["poll_question"], "What is your favorite color?")
        self.assertEqual(result["poll_options"], ["Red", "Green", "Blue"])

    def test_call_handler(self):
        """Test TextMessageHandler."""
        handler = TextMessageHandler()
        result = handler.extract_structured_data(self.call_message)
        self.assertEqual(result["call_duration"], "00:05:30")
        self.assertEqual(len(result["call_participants"]), 2)
        self.assertEqual(result["call_participants"][0]["name"], "John Doe")
        self.assertEqual(result["call_participants"][1]["id"], "user2")

    def test_location_handler(self):
        """Test MediaMessageHandler."""
        handler = MediaMessageHandler()
        result = handler.extract_structured_data(self.location_message)
        self.assertEqual(result["location_latitude"], "37.7749")
        self.assertEqual(result["location_longitude"], "-122.4194")
        self.assertEqual(result["location_address"], "San Francisco, CA")

    def test_contacts_handler(self):
        """Test MediaMessageHandler."""
        handler = MediaMessageHandler()
        result = handler.extract_structured_data(self.contacts_message)
        self.assertEqual(len(result["contacts"]), 2)
        self.assertEqual(result["contacts"][0]["name"], "John Doe")
        self.assertEqual(result["contacts"][0]["phone"], "+1234567890")
        self.assertEqual(result["contacts"][1]["email"], "jane@example.com")

    def test_media_handler(self):
        """Test MediaMessageHandler."""
        handler = MediaMessageHandler()
        result = handler.extract_structured_data(self.media_message)
        self.assertEqual(result["media_filename"], "video.mp4")
        self.assertEqual(result["media_filesize"], "1024000")
        self.assertEqual(result["media_filetype"], "video/mp4")
        self.assertEqual(result["media_url"], "https://example.com/video.mp4")

    def test_scheduled_call_handler(self):
        """Test MediaMessageHandler."""
        handler = MediaMessageHandler()
        result = handler.extract_structured_data(self.scheduled_call_message)
        self.assertEqual(result["scheduled_call_title"], "Team Meeting")
        self.assertEqual(result["scheduled_call_time"], "2023-01-01T12:00:00Z")
        self.assertEqual(result["scheduled_call_duration"], "01:00:00")

    def test_extract_structured_data(self):
        """Test extract_structured_data function."""
        # Test with Poll message
        result = extract_structured_data(self.poll_message)
        self.assertIn("message_type", result)
        self.assertEqual(result["message_type"], "Poll")

    def test_handler_error_handling(self):
        """Test error handling in handlers."""
        # Test with malformed content
        malformed_poll = {"messagetype": "Poll", "content": "<invalid>XML</invalid>"}
        handler = PollMessageHandler()
        result = handler.extract_structured_data(malformed_poll)
        self.assertEqual(result["poll_question"], "")
        self.assertEqual(result["poll_options"], [])

    def test_media_handler_enhanced(self):
        """Test enhanced MediaMessageHandler with additional metadata."""
        handler = MediaMessageHandler()
        result = handler.extract_structured_data(self.media_message)
        self.assertEqual(result["media_filename"], "video.mp4")
        self.assertEqual(result["media_filesize"], "1024000")

    def test_media_album_handler(self):
        """Test MediaMessageHandler with album message type."""
        handler = MediaMessageHandler()
        result = handler.extract_structured_data(self.media_album_message)
        self.assertIn("media_album_items", result)
        self.assertEqual(result["media_album_count"], 2)

    def test_media_card_handler(self):
        """Test MediaMessageHandler."""
        handler = MediaMessageHandler()
        result = handler.extract_structured_data(self.media_card_message)
        self.assertEqual(result["card_title"], "Article Title")
        self.assertEqual(result["card_description"], "Article description")

    def test_popcard_handler(self):
        """Test MediaMessageHandler."""
        handler = MediaMessageHandler()
        result = handler.extract_structured_data(self.popcard_message)
        self.assertEqual(result["popcard_title"], "Notification")
        self.assertEqual(result["popcard_type"], "alert")

    def test_translation_handler(self):
        """Test MediaMessageHandler."""
        handler = MediaMessageHandler()
        result = handler.extract_structured_data(self.translation_message)
        self.assertEqual(result["translation_from_language"], "en")
        self.assertEqual(result["translation_to_language"], "fr")

    def test_thread_activity_add_member_handler(self):
        """Test MediaMessageHandler with AddMember message type."""
        handler = MediaMessageHandler()
        result = handler.extract_structured_data(
            self.thread_activity_add_member_message
        )
        self.assertEqual(result["activity_type"], "AddMember")
        self.assertEqual(len(result["activity_members"]), 1)

    def test_thread_activity_topic_update_handler(self):
        """Test MediaMessageHandler with TopicUpdate message type."""
        handler = MediaMessageHandler()
        result = handler.extract_structured_data(
            self.thread_activity_topic_update_message
        )
        self.assertEqual(result["activity_type"], "TopicUpdate")
        self.assertEqual(result["activity_value"], "New Topic Name")

    def test_empty_content_handling(self):
        """Test handling of messages with empty content."""
        # Test with empty content
        empty_content_message = {"messagetype": "Poll", "content": ""}
        handler = PollMessageHandler()
        result = handler.extract_structured_data(empty_content_message)
        self.assertEqual(result["poll_question"], "")
        self.assertEqual(result["poll_options"], [])

    def test_malformed_content_handling(self):
        """Test handling of messages with malformed content."""
        # Test with malformed XML
        malformed_xml_message = {
            "messagetype": "RichText/Media_Video",
            "content": '<uriobject filename="video.mp4" filesize="1024000" filetype="video/mp4" url="https://example.com/video.mp4" width="1920" height="1080" duration="00:02:30" description="Vacation video"',  # Missing closing tag
        }
        handler = MediaMessageHandler()
        result = handler.extract_structured_data(malformed_xml_message)
        self.assertIn("media_type", result)

    def test_unexpected_message_type_handling(self):
        """Test handling of unexpected message types."""
        # Test with completely unknown message type
        unknown_message = {
            "messagetype": "CompletelyUnknownType",
            "content": "Some content",
        }
        # Should not raise an exception when no handler is found
        result = extract_structured_data(unknown_message)
        self.assertIn("message_type", result)
        self.assertEqual(result["message_type"], "CompletelyUnknownType")

    def test_message_handler_factory(self):
        """Test the SkypeMessageHandlerFactory class."""
        from src.utils.message_type_handlers import SkypeMessageHandlerFactory

        # Create a factory instance
        factory = SkypeMessageHandlerFactory()

        # Test getting handlers for various message types
        poll_handler = factory.get_handler("Poll")
        self.assertIsNotNone(poll_handler)
        self.assertTrue(poll_handler.can_handle("Poll"))

        media_handler = factory.get_handler("RichText/Media_Video")
        self.assertIsNotNone(media_handler)
        self.assertTrue(media_handler.can_handle("RichText/Media_Video"))

        # Test with unknown message type
        unknown_handler = factory.get_handler("Unknown")
        self.assertIsNotNone(unknown_handler)  # Should return the fallback handler

        # Test that the same handler instance is returned for the same message type
        poll_handler2 = factory.get_handler("Poll")
        self.assertIs(poll_handler, poll_handler2)


if __name__ == "__main__":
    unittest.main()
