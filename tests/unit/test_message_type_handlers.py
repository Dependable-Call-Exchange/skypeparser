#!/usr/bin/env python3
"""
Unit tests for message type handlers.
"""

import unittest
from unittest.mock import patch, MagicMock
import json
import os
import sys
from typing import Dict, Any

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.utils.message_type_handlers import (
    extract_structured_data,
    get_handler_for_message_type,
    PollHandler,
    CallHandler,
    LocationHandler,
    ContactsHandler,
    MediaHandler,
    ScheduledCallHandler,
    MediaCardHandler,
    PopCardHandler,
    TranslationHandler,
    ThreadActivityHandler
)


class TestMessageTypeHandlers(unittest.TestCase):
    """Test cases for message type handlers."""

    def setUp(self):
        """Set up test fixtures."""
        # Sample messages for different types
        self.poll_message = {
            'messagetype': 'Poll',
            'content': '<pollquestion>What is your favorite color?</pollquestion>'
                      '<polloption>Red</polloption>'
                      '<polloption>Green</polloption>'
                      '<polloption>Blue</polloption>'
        }

        self.call_message = {
            'messagetype': 'Event/Call',
            'content': '<duration>00:05:30</duration>'
                      '<participant id="user1" name="John Doe"></participant>'
                      '<participant id="user2" name="Jane Smith"></participant>'
        }

        self.location_message = {
            'messagetype': 'RichText/Location',
            'content': '<location latitude="37.7749" longitude="-122.4194"></location>'
                      '<address>San Francisco, CA</address>'
        }

        self.contacts_message = {
            'messagetype': 'RichText/Contacts',
            'content': '<contact name="John Doe" phone="+1234567890" email="john@example.com"></contact>'
                      '<contact name="Jane Smith" phone="+0987654321" email="jane@example.com"></contact>'
        }

        self.media_message = {
            'messagetype': 'RichText/Media_Video',
            'content': '<uriobject filename="video.mp4" filesize="1024000" filetype="video/mp4" url="https://example.com/video.mp4" width="1920" height="1080" duration="00:02:30" description="Vacation video"></uriobject>'
        }

        self.scheduled_call_message = {
            'messagetype': 'RichText/ScheduledCallInvite',
            'content': '<scheduledcall title="Team Meeting" time="2023-01-01T12:00:00Z" duration="01:00:00"></scheduledcall>'
        }

        # New message types
        self.media_album_message = {
            'messagetype': 'RichText/Media_Album',
            'content': '<img src="https://example.com/image1.jpg" thumbnail="https://example.com/thumb1.jpg" width="800" height="600" />'
                      '<img src="https://example.com/image2.jpg" thumbnail="https://example.com/thumb2.jpg" width="800" height="600" />'
        }

        self.media_card_message = {
            'messagetype': 'RichText/Media_Card',
            'content': '<card title="Article Title" description="Article description" url="https://example.com/article" thumbnail_url="https://example.com/thumbnail.jpg" provider="News Site"></card>'
        }

        self.popcard_message = {
            'messagetype': 'PopCard',
            'content': '<popcard title="Notification" type="alert" action="dismiss"><content>This is a notification</content></popcard>'
        }

        self.translation_message = {
            'messagetype': 'Translation',
            'content': '<translation from="en" to="fr">Bonjour</translation><original>Hello</original>'
        }

        self.thread_activity_add_member_message = {
            'messagetype': 'ThreadActivity/AddMember',
            'content': '<member id="user3" name="Bob Johnson"></member><initiator name="John Doe"></initiator>'
        }

        self.thread_activity_topic_update_message = {
            'messagetype': 'ThreadActivity/TopicUpdate',
            'content': '<value>New Topic Name</value><initiator name="Jane Smith"></initiator>'
        }

    def test_get_handler_for_message_type(self):
        """Test get_handler_for_message_type function."""
        self.assertEqual(get_handler_for_message_type('Poll'), PollHandler)
        self.assertEqual(get_handler_for_message_type('Event/Call'), CallHandler)
        self.assertEqual(get_handler_for_message_type('RichText/Location'), LocationHandler)
        self.assertEqual(get_handler_for_message_type('RichText/Contacts'), ContactsHandler)
        self.assertEqual(get_handler_for_message_type('RichText/Media_Video'), MediaHandler)
        self.assertEqual(get_handler_for_message_type('RichText/Media_AudioMsg'), MediaHandler)
        self.assertEqual(get_handler_for_message_type('RichText/Media_GenericFile'), MediaHandler)
        self.assertEqual(get_handler_for_message_type('RichText/UriObject'), MediaHandler)
        self.assertEqual(get_handler_for_message_type('RichText/ScheduledCallInvite'), ScheduledCallHandler)
        # New message types
        self.assertEqual(get_handler_for_message_type('RichText/Media_Album'), MediaHandler)
        self.assertEqual(get_handler_for_message_type('RichText/Media_Card'), MediaCardHandler)
        self.assertEqual(get_handler_for_message_type('PopCard'), PopCardHandler)
        self.assertEqual(get_handler_for_message_type('Translation'), TranslationHandler)
        self.assertEqual(get_handler_for_message_type('ThreadActivity/AddMember'), ThreadActivityHandler)
        self.assertEqual(get_handler_for_message_type('ThreadActivity/TopicUpdate'), ThreadActivityHandler)
        self.assertIsNone(get_handler_for_message_type('RichText'))
        self.assertIsNone(get_handler_for_message_type('Unknown'))

    def test_poll_handler(self):
        """Test PollHandler."""
        result = PollHandler.extract_data(self.poll_message)
        self.assertEqual(result['poll_question'], 'What is your favorite color?')
        self.assertEqual(result['poll_options'], ['Red', 'Green', 'Blue'])

    def test_call_handler(self):
        """Test CallHandler."""
        result = CallHandler.extract_data(self.call_message)
        self.assertEqual(result['call_duration'], '00:05:30')
        self.assertEqual(len(result['call_participants']), 2)
        self.assertEqual(result['call_participants'][0]['name'], 'John Doe')
        self.assertEqual(result['call_participants'][1]['id'], 'user2')

    def test_location_handler(self):
        """Test LocationHandler."""
        result = LocationHandler.extract_data(self.location_message)
        self.assertEqual(result['location_latitude'], '37.7749')
        self.assertEqual(result['location_longitude'], '-122.4194')
        self.assertEqual(result['location_address'], 'San Francisco, CA')

    def test_contacts_handler(self):
        """Test ContactsHandler."""
        result = ContactsHandler.extract_data(self.contacts_message)
        self.assertEqual(len(result['contacts']), 2)
        self.assertEqual(result['contacts'][0]['name'], 'John Doe')
        self.assertEqual(result['contacts'][0]['phone'], '+1234567890')
        self.assertEqual(result['contacts'][1]['email'], 'jane@example.com')

    def test_media_handler(self):
        """Test MediaHandler."""
        result = MediaHandler.extract_data(self.media_message)
        self.assertEqual(result['media_filename'], 'video.mp4')
        self.assertEqual(result['media_filesize'], '1024000')
        self.assertEqual(result['media_filetype'], 'video/mp4')
        self.assertEqual(result['media_url'], 'https://example.com/video.mp4')

    def test_scheduled_call_handler(self):
        """Test ScheduledCallHandler."""
        result = ScheduledCallHandler.extract_data(self.scheduled_call_message)
        self.assertEqual(result['scheduled_call_title'], 'Team Meeting')
        self.assertEqual(result['scheduled_call_time'], '2023-01-01T12:00:00Z')
        self.assertEqual(result['scheduled_call_duration'], '01:00:00')

    def test_extract_structured_data(self):
        """Test extract_structured_data function."""
        # Test with Poll message
        result = extract_structured_data(self.poll_message)
        self.assertEqual(result['poll_question'], 'What is your favorite color?')

        # Test with unknown message type
        unknown_message = {'messagetype': 'Unknown', 'content': 'test'}
        result = extract_structured_data(unknown_message)
        self.assertEqual(result, {})

        # Test with missing message type
        missing_type_message = {'content': 'test'}
        result = extract_structured_data(missing_type_message)
        self.assertEqual(result, {})

    def test_handler_error_handling(self):
        """Test error handling in handlers."""
        # Test with malformed content
        malformed_poll = {
            'messagetype': 'Poll',
            'content': '<invalid>XML</invalid>'
        }
        result = PollHandler.extract_data(malformed_poll)
        self.assertEqual(result['poll_question'], '')
        self.assertEqual(result['poll_options'], [])

    def test_media_handler_enhanced(self):
        """Test enhanced MediaHandler with additional metadata."""
        result = MediaHandler.extract_data(self.media_message)
        self.assertEqual(result['media_filename'], 'video.mp4')
        self.assertEqual(result['media_filesize'], '1024000')
        self.assertEqual(result['media_filetype'], 'video/mp4')
        self.assertEqual(result['media_url'], 'https://example.com/video.mp4')
        # Test enhanced metadata
        self.assertEqual(result['media_width'], '1920')
        self.assertEqual(result['media_height'], '1080')
        self.assertEqual(result['media_duration'], '00:02:30')
        self.assertEqual(result['media_description'], 'Vacation video')
        self.assertIn('media_filesize_formatted', result)

    def test_media_album_handler(self):
        """Test MediaHandler with album message type."""
        result = MediaHandler.extract_data(self.media_album_message)
        self.assertIn('media_album_items', result)
        self.assertEqual(result['media_album_count'], 2)
        self.assertEqual(len(result['media_album_items']), 2)
        self.assertEqual(result['media_album_items'][0]['url'], 'https://example.com/image1.jpg')
        self.assertEqual(result['media_album_items'][0]['thumbnail'], 'https://example.com/thumb1.jpg')
        self.assertEqual(result['media_album_items'][0]['width'], '800')
        self.assertEqual(result['media_album_items'][0]['height'], '600')

    def test_media_card_handler(self):
        """Test MediaCardHandler."""
        result = MediaCardHandler.extract_data(self.media_card_message)
        self.assertEqual(result['card_title'], 'Article Title')
        self.assertEqual(result['card_description'], 'Article description')
        self.assertEqual(result['card_url'], 'https://example.com/article')
        self.assertEqual(result['card_thumbnail_url'], 'https://example.com/thumbnail.jpg')
        self.assertEqual(result['card_provider'], 'News Site')

    def test_popcard_handler(self):
        """Test PopCardHandler."""
        result = PopCardHandler.extract_data(self.popcard_message)
        self.assertEqual(result['popcard_title'], 'Notification')
        self.assertEqual(result['popcard_type'], 'alert')
        self.assertEqual(result['popcard_action'], 'dismiss')
        self.assertEqual(result['popcard_content'], 'This is a notification')

    def test_translation_handler(self):
        """Test TranslationHandler."""
        result = TranslationHandler.extract_data(self.translation_message)
        self.assertEqual(result['translation_from_language'], 'en')
        self.assertEqual(result['translation_to_language'], 'fr')
        self.assertEqual(result['translation_text'], 'Bonjour')
        self.assertEqual(result['translation_original_text'], 'Hello')

    def test_thread_activity_add_member_handler(self):
        """Test ThreadActivityHandler with AddMember message type."""
        result = ThreadActivityHandler.extract_data(self.thread_activity_add_member_message)
        self.assertEqual(result['activity_type'], 'AddMember')
        self.assertEqual(len(result['activity_members']), 1)
        self.assertEqual(result['activity_members'][0]['id'], 'user3')
        self.assertEqual(result['activity_members'][0]['name'], 'Bob Johnson')
        self.assertEqual(result['activity_initiator'], 'John Doe')

    def test_thread_activity_topic_update_handler(self):
        """Test ThreadActivityHandler with TopicUpdate message type."""
        result = ThreadActivityHandler.extract_data(self.thread_activity_topic_update_message)
        self.assertEqual(result['activity_type'], 'TopicUpdate')
        self.assertEqual(result['activity_value'], 'New Topic Name')
        self.assertEqual(result['activity_initiator'], 'Jane Smith')


if __name__ == '__main__':
    unittest.main()