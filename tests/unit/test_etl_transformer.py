#!/usr/bin/env python3
"""
Unit tests for the ETL Transformer class.

These tests validate the transformation logic of the ETL pipeline,
ensuring that raw Skype data is correctly transformed into a structured format.
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock, Mock
from concurrent.futures import ThreadPoolExecutor

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl.transformer import Transformer
from src.utils.interfaces import (
    ContentExtractorProtocol,
    MessageHandlerFactoryProtocol,
    MessageHandlerProtocol,
    StructuredDataExtractorProtocol
)
from tests.factories import (
    SkypeDataFactory,
    SkypeConversationFactory,
    SkypeMessageFactory,
    BASIC_SKYPE_DATA,
    COMPLEX_SKYPE_DATA
)

class TestTransformer(unittest.TestCase):
    """Test cases for the Transformer class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create mock dependencies
        self.mock_content_extractor = Mock(spec=ContentExtractorProtocol)
        self.mock_content_extractor.extract_cleaned_content.return_value = "Cleaned content"

        self.mock_message_handler = Mock(spec=MessageHandlerProtocol)
        self.mock_message_handler.extract_structured_data.return_value = {
            'id': 'msg1',
            'timestamp': '2023-01-01T12:00:00Z',
            'sender_id': 'user1',
            'sender_name': 'User One',
            'message_type': 'text',
            'is_edited': False
        }

        self.mock_message_handler_factory = Mock(spec=MessageHandlerFactoryProtocol)
        self.mock_message_handler_factory.get_handler.return_value = self.mock_message_handler

        self.mock_structured_data_extractor = Mock(spec=StructuredDataExtractorProtocol)

        # Create a transformer instance with mocked dependencies
        self.transformer = Transformer(
            parallel_processing=False,
            chunk_size=2,
            content_extractor=self.mock_content_extractor,
            message_handler_factory=self.mock_message_handler_factory,
            structured_data_extractor=self.mock_structured_data_extractor
        )

        # Use factory to create sample raw data for testing
        self.raw_data = SkypeDataFactory.build(
            userId='test-user-id',
            exportDate='2023-01-01T00:00:00Z',
            conversations=[
                SkypeConversationFactory.build(
                    id='conv1',
                    displayName='Test Conversation 1',
                    MessageList=[
                        SkypeMessageFactory.build(
                            id='msg1',
                            originalarrivaltime='2023-01-01T12:00:00Z',
                            from_id='user1',
                            messagetype='RichText',
                            content='Hello world',
                            edittime=None
                        ),
                        SkypeMessageFactory.build(
                            id='msg2',
                            originalarrivaltime='2023-01-01T12:05:00Z',
                            from_id='user2',
                            messagetype='RichText',
                            content='Hi there',
                            edittime=None
                        )
                    ]
                ),
                SkypeConversationFactory.build(
                    id='conv2',
                    displayName='Test Conversation 2',
                    MessageList=[
                        SkypeMessageFactory.build(
                            id='msg3',
                            originalarrivaltime='2023-01-02T10:00:00Z',
                            from_id='user1',
                            messagetype='RichText',
                            content='Another message',
                            edittime=None
                        )
                    ]
                )
            ]
        )

        # Expected transformed data structure (partial)
        self.expected_metadata = {
            'user_display_name': 'Test User',
            'total_conversations': 2,
            'total_messages': 3
        }

    def test_transform_basic(self):
        """Test basic transformation of raw data."""
        # Transform the raw data
        transformed_data = self.transformer.transform(self.raw_data)

        # Verify the structure of the transformed data
        self.assertIn('user', transformed_data)
        self.assertIn('conversations', transformed_data)
        self.assertIn('metadata', transformed_data)

        # Verify user data
        self.assertEqual(transformed_data['user']['id'], 'test-user-id')

        # Verify conversation data
        self.assertIn('conv1', transformed_data['conversations'])
        conv = transformed_data['conversations']['conv1']
        self.assertEqual(conv['display_name'], 'Test Conversation 1')
        self.assertEqual(len(conv['messages']), 2)

        # Verify that the mock dependencies were called
        self.mock_content_extractor.extract_cleaned_content.assert_called()
        self.mock_message_handler_factory.get_handler.assert_called()
        self.mock_message_handler.extract_structured_data.assert_called()

    def test_transform_with_parallel_processing(self):
        """Test transformation with parallel processing enabled."""
        # Create a transformer with parallel processing
        parallel_transformer = Transformer(
            parallel_processing=True,
            chunk_size=1,
            max_workers=2,
            content_extractor=self.mock_content_extractor,
            message_handler_factory=self.mock_message_handler_factory,
            structured_data_extractor=self.mock_structured_data_extractor
        )

        # Transform the raw data
        transformed_data = parallel_transformer.transform(self.raw_data)

        # Verify the structure of the transformed data
        self.assertIn('conversations', transformed_data)
        self.assertIn('conv1', transformed_data['conversations'])
        conv = transformed_data['conversations']['conv1']
        self.assertEqual(len(conv['messages']), 2)

        # Verify that the mock dependencies were called
        self.mock_content_extractor.extract_cleaned_content.assert_called()
        self.mock_message_handler_factory.get_handler.assert_called()
        self.mock_message_handler.extract_structured_data.assert_called()

    def test_transform_with_invalid_data(self):
        """Test transformation with invalid data."""
        # Test with None
        with self.assertRaises(ValueError):
            self.transformer.transform(None)

        # Test with non-dict
        with self.assertRaises(ValueError):
            self.transformer.transform([])

        # Test with missing conversations key
        with self.assertRaises(ValueError):
            self.transformer.transform({'userId': 'test-user-id'})

        # Test with non-dict conversations
        with self.assertRaises(ValueError):
            self.transformer.transform({'userId': 'test-user-id', 'conversations': []})

    def test_transform_with_missing_display_name(self):
        """Test transformation with missing display name."""
        # Create raw data with missing display name
        raw_data = SkypeDataFactory.build(
            userId='test-user-id',
            exportDate='2023-01-01T00:00:00Z',
            conversations=[
                SkypeConversationFactory.build(
                    id='conv1',
                    displayName='',  # Empty display name
                    MessageList=[
                        SkypeMessageFactory.build(
                            id='msg1',
                            originalarrivaltime='2023-01-01T12:00:00Z',
                            from_id='user1',
                            messagetype='RichText',
                            content='Hello world',
                            edittime=None
                        )
                    ]
                )
            ]
        )

        # Transform the raw data
        transformed_data = self.transformer.transform(raw_data)

        # Verify the conversation data
        self.assertIn('conv1', transformed_data['conversations'])
        conv = transformed_data['conversations']['conv1']
        self.assertEqual(conv['display_name'], '')  # Should preserve empty display name

    def test_transform_with_user_display_name(self):
        """Test transformation with user display name provided."""
        # Transform the raw data with a user display name
        transformed_data = self.transformer.transform(self.raw_data, user_display_name="Test User")

        # Verify the user data
        self.assertEqual(transformed_data['user']['id'], 'test-user-id')
        self.assertEqual(transformed_data['user']['display_name'], 'Test User')

    def test_transform_with_di_dependencies(self):
        """Test transformation using dependency injection for all dependencies."""
        # Create mock dependencies
        mock_content_extractor = Mock(spec=ContentExtractorProtocol)
        mock_content_extractor.extract_cleaned_content.return_value = "Custom cleaned content"

        mock_message_handler = Mock(spec=MessageHandlerProtocol)
        mock_message_handler.extract_structured_data.return_value = {
            'id': 'custom-msg',
            'timestamp': '2023-01-01T12:00:00Z',
            'sender_id': 'custom-user',
            'sender_name': 'Custom User',
            'message_type': 'custom-type',
            'is_edited': True,
            'custom_field': 'custom value'
        }

        mock_message_handler_factory = Mock(spec=MessageHandlerFactoryProtocol)
        mock_message_handler_factory.get_handler.return_value = mock_message_handler

        # Create a transformer with custom dependencies
        custom_transformer = Transformer(
            content_extractor=mock_content_extractor,
            message_handler_factory=mock_message_handler_factory
        )

        # Transform the raw data
        transformed_data = custom_transformer.transform(self.raw_data)

        # Verify that the custom dependencies were used
        mock_content_extractor.extract_cleaned_content.assert_called()
        mock_message_handler_factory.get_handler.assert_called()
        mock_message_handler.extract_structured_data.assert_called()

        # Verify the custom field in the transformed data
        conv = transformed_data['conversations']['conv1']
        self.assertEqual(conv['messages'][0]['custom_field'], 'custom value')

    def test_transform_with_complex_data(self):
        """Test transformation with complex data."""
        # Create complex raw data
        complex_data = SkypeDataFactory.build_complex()

        # Transform the complex data
        transformed_data = self.transformer.transform(complex_data)

        # Verify the structure of the transformed data
        self.assertIn('user', transformed_data)
        self.assertIn('conversations', transformed_data)
        self.assertIn('metadata', transformed_data)

        # Verify the number of conversations
        self.assertEqual(len(transformed_data['conversations']), len(complex_data['conversations']))

        # Verify metadata
        self.assertIn('conversation_count', transformed_data['metadata'])
        self.assertIn('message_count', transformed_data['metadata'])
        self.assertEqual(transformed_data['metadata']['conversation_count'], len(complex_data['conversations']))

    def test_transform_with_various_message_types(self):
        """Test transformation with various message types."""
        # Create raw data with various message types
        raw_data = SkypeDataFactory.build(
            userId='test-user-id',
            exportDate='2023-01-01T00:00:00Z',
            conversations=[
                SkypeConversationFactory.build(
                    id='conv1',
                    displayName='Test Conversation 1',
                    MessageList=[
                        SkypeMessageFactory.build(
                            id='msg1',
                            originalarrivaltime='2023-01-01T12:00:00Z',
                            from_id='user1',
                            messagetype='RichText',
                            content='Hello world',
                            edittime=None
                        ),
                        SkypeMessageFactory.build(
                            id='msg2',
                            originalarrivaltime='2023-01-01T12:05:00Z',
                            from_id='user2',
                            messagetype='Event/Call',
                            content='<duration>60</duration>',
                            edittime=None
                        ),
                        SkypeMessageFactory.build(
                            id='msg3',
                            originalarrivaltime='2023-01-01T12:10:00Z',
                            from_id='user1',
                            messagetype='RichText/Media_GenericFile',
                            content='<file filename="test.txt" filesize="1024" />',
                            edittime=None
                        )
                    ]
                )
            ]
        )

        # Configure mock handler to return different data based on message type
        def get_mock_handler(message_type):
            handler = Mock(spec=MessageHandlerProtocol)
            if message_type == 'Event/Call':
                handler.extract_structured_data.return_value = {
                    'id': 'msg2',
                    'timestamp': '2023-01-01T12:05:00Z',
                    'sender_id': 'user2',
                    'message_type': 'Event/Call',
                    'call_data': {'duration': 60}
                }
            elif message_type == 'RichText/Media_GenericFile':
                handler.extract_structured_data.return_value = {
                    'id': 'msg3',
                    'timestamp': '2023-01-01T12:10:00Z',
                    'sender_id': 'user1',
                    'message_type': 'RichText/Media_GenericFile',
                    'attachments': [{'name': 'test.txt', 'size': 1024}]
                }
            else:
                handler.extract_structured_data.return_value = {
                    'id': 'msg1',
                    'timestamp': '2023-01-01T12:00:00Z',
                    'sender_id': 'user1',
                    'message_type': 'RichText',
                    'content_text': 'Hello world'
                }
            return handler

        self.mock_message_handler_factory.get_handler.side_effect = get_mock_handler

        # Transform the raw data
        transformed_data = self.transformer.transform(raw_data)

        # Verify the conversation data
        self.assertIn('conv1', transformed_data['conversations'])
        conv = transformed_data['conversations']['conv1']
        self.assertEqual(len(conv['messages']), 3)

        # Verify that the mock factory was called with different message types
        self.mock_message_handler_factory.get_handler.assert_any_call('RichText')
        self.mock_message_handler_factory.get_handler.assert_any_call('Event/Call')
        self.mock_message_handler_factory.get_handler.assert_any_call('RichText/Media_GenericFile')

    def test_transform_with_edited_messages(self):
        """Test transformation with edited messages."""
        # Create raw data with edited messages
        raw_data = SkypeDataFactory.build(
            userId='test-user-id',
            exportDate='2023-01-01T00:00:00Z',
            conversations=[
                SkypeConversationFactory.build(
                    id='conv1',
                    displayName='Test Conversation 1',
                    MessageList=[
                        SkypeMessageFactory.build(
                            id='msg1',
                            originalarrivaltime='2023-01-01T12:00:00Z',
                            from_id='user1',
                            messagetype='RichText',
                            content='Original message',
                            edittime='2023-01-01T12:05:00Z'
                        )
                    ]
                )
            ]
        )

        # Configure mock handler to return data with is_edited=True
        self.mock_message_handler.extract_structured_data.return_value = {
            'id': 'msg1',
            'timestamp': '2023-01-01T12:00:00Z',
            'sender_id': 'user1',
            'sender_name': 'User One',
            'message_type': 'text',
            'is_edited': True,
            'content_text': 'Edited message'
        }

        # Transform the raw data
        transformed_data = self.transformer.transform(raw_data)

        # Verify the message data
        conv = transformed_data['conversations']['conv1']
        self.assertEqual(len(conv['messages']), 1)
        msg = conv['messages'][0]
        self.assertTrue(msg['is_edited'])
        self.assertEqual(msg['content_text'], 'Edited message')

if __name__ == '__main__':
    unittest.main()