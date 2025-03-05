#!/usr/bin/env python3
"""
Unit tests for the ETL Transformer class.

These tests validate the transformation logic of the ETL pipeline,
ensuring that raw Skype data is correctly transformed into a structured format.
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock
from concurrent.futures import ThreadPoolExecutor

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl.transformer import Transformer
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
        # Create a transformer instance with default settings
        self.transformer = Transformer(parallel_processing=False, chunk_size=2)

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
                            edittime='2023-01-01T12:06:00Z'
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
        """Test basic transformation functionality."""
        # Transform the raw data
        transformed_data = self.transformer.transform(self.raw_data, 'Test User')

        # Verify the structure of the transformed data
        self.assertIn('metadata', transformed_data)
        self.assertIn('conversations', transformed_data)

        # Verify metadata
        metadata = transformed_data['metadata']
        self.assertEqual(metadata['user_display_name'], 'Test User')
        self.assertEqual(metadata['total_conversations'], 2)
        self.assertEqual(metadata['total_messages'], 3)

        # Verify conversations
        conversations = transformed_data['conversations']
        self.assertIn('conv1', conversations)
        self.assertIn('conv2', conversations)

        # Verify conversation details
        conv1 = conversations['conv1']
        self.assertEqual(conv1['display_name'], 'Test Conversation 1')
        self.assertEqual(len(conv1['messages']), 2)

        # Verify message details
        msg1 = conv1['messages'][0]
        self.assertEqual(msg1['id'], 'msg1')
        self.assertEqual(msg1['content'], 'Hello world')
        self.assertEqual(msg1['from_id'], 'user1')
        self.assertEqual(msg1['message_type'], 'RichText')
        self.assertFalse(msg1['is_edited'])

        msg2 = conv1['messages'][1]
        self.assertEqual(msg2['id'], 'msg2')
        self.assertEqual(msg2['content'], 'Hi there')
        self.assertEqual(msg2['from_id'], 'user2')
        self.assertEqual(msg2['message_type'], 'RichText')
        self.assertTrue(msg2['is_edited'])

    def test_transform_with_parallel_processing(self):
        """Test transformation with parallel processing enabled."""
        # Create a transformer with parallel processing
        transformer = Transformer(parallel_processing=True, max_workers=2)

        # Transform the raw data
        transformed_data = transformer.transform(self.raw_data, 'Test User')

        # Verify the structure of the transformed data
        self.assertIn('metadata', transformed_data)
        self.assertIn('conversations', transformed_data)

        # Verify metadata
        metadata = transformed_data['metadata']
        self.assertEqual(metadata['user_display_name'], 'Test User')
        self.assertEqual(metadata['total_conversations'], 2)
        self.assertEqual(metadata['total_messages'], 3)

    def test_transform_with_invalid_data(self):
        """Test transformation with invalid data."""
        # Create invalid data using factory
        invalid_data = SkypeDataFactory.build(
            userId=None,  # Missing user ID
            exportDate='invalid-date',  # Invalid date format
            conversations=[]  # Empty conversations list
        )

        # Transform the invalid data
        transformed_data = self.transformer.transform(invalid_data, 'Test User')

        # Verify the structure of the transformed data
        self.assertIn('metadata', transformed_data)
        self.assertIn('conversations', transformed_data)

        # Verify metadata
        metadata = transformed_data['metadata']
        self.assertEqual(metadata['user_display_name'], 'Test User')
        self.assertEqual(metadata['total_conversations'], 0)
        self.assertEqual(metadata['total_messages'], 0)

    def test_transform_with_missing_display_name(self):
        """Test transformation with missing display name."""
        # Create data with a conversation missing display name
        data_with_missing_display_name = SkypeDataFactory.build(
            conversations=[
                SkypeConversationFactory.build(
                    id='conv1',
                    displayName=None,  # Missing display name
                    MessageList=[
                        SkypeMessageFactory.build(
                            id='msg1',
                            content='This message should be skipped'
                        )
                    ]
                ),
                SkypeConversationFactory.build(
                    id='conv2',
                    displayName='Valid Conversation',
                    MessageList=[
                        SkypeMessageFactory.build(
                            id='msg2',
                            content='This message should be processed'
                        )
                    ]
                )
            ]
        )

        # Transform the data
        transformed_data = self.transformer.transform(data_with_missing_display_name, 'Test User')

        # Verify that the conversation with missing display name was skipped
        conversations = transformed_data['conversations']
        self.assertNotIn('conv1', conversations)
        self.assertIn('conv2', conversations)

        # Verify metadata
        metadata = transformed_data['metadata']
        self.assertEqual(metadata['total_conversations'], 1)
        self.assertEqual(metadata['total_messages'], 1)

    def test_transform_with_complex_data(self):
        """Test transformation with complex data."""
        # Use the complex data fixture from factories
        transformed_data = self.transformer.transform(COMPLEX_SKYPE_DATA, 'Test User')

        # Verify the structure of the transformed data
        self.assertIn('metadata', transformed_data)
        self.assertIn('conversations', transformed_data)

        # Verify metadata
        metadata = transformed_data['metadata']
        self.assertEqual(metadata['user_display_name'], 'Test User')

        # Should have 2 conversations (the one with None displayName should be skipped)
        self.assertEqual(metadata['total_conversations'], 2)

        # Should have 5 messages (from the first two conversations)
        self.assertEqual(metadata['total_messages'], 5)

        # Verify conversations
        conversations = transformed_data['conversations']
        self.assertIn('conversation1', conversations)
        self.assertIn('conversation2', conversations)
        self.assertNotIn('conversation3', conversations)  # Should be skipped

        # Verify message types
        conv1 = conversations['conversation1']
        self.assertEqual(len(conv1['messages']), 3)
        self.assertEqual(conv1['messages'][2]['message_type'], 'RichText/HTML')

        conv2 = conversations['conversation2']
        self.assertEqual(len(conv2['messages']), 2)
        self.assertEqual(conv2['messages'][1]['message_type'], 'RichText/Link')

    def test_transform_with_various_message_types(self):
        """Test transformation with various message types."""
        # Create data with various message types using factory traits
        data_with_various_types = SkypeDataFactory.build(
            conversations=[
                SkypeConversationFactory.build(
                    id='conv1',
                    displayName='Message Types Test',
                    MessageList=[
                        SkypeMessageFactory.build(
                            id='msg1',
                            content='Regular message'
                        ),
                        SkypeMessageFactory.build(
                            id='msg2',
                            content='<b>Bold text</b>',
                            messagetype='RichText/HTML'
                        ),
                        SkypeMessageFactory.build(
                            id='msg3',
                            content='https://example.com',
                            messagetype='RichText/Link'
                        ),
                        SkypeMessageFactory.build(
                            id='msg4',
                            content='Call started',
                            messagetype='Event/Call'
                        ),
                        SkypeMessageFactory.build(
                            id='msg5',
                            content='User joined',
                            messagetype='SystemMessage'
                        )
                    ]
                )
            ]
        )

        # Transform the data
        transformed_data = self.transformer.transform(data_with_various_types, 'Test User')

        # Verify that all message types were processed correctly
        conversations = transformed_data['conversations']
        conv1 = conversations['conv1']
        messages = conv1['messages']

        self.assertEqual(len(messages), 5)
        self.assertEqual(messages[0]['message_type'], 'RichText')
        self.assertEqual(messages[1]['message_type'], 'RichText/HTML')
        self.assertEqual(messages[2]['message_type'], 'RichText/Link')
        self.assertEqual(messages[3]['message_type'], 'Event/Call')
        self.assertEqual(messages[4]['message_type'], 'SystemMessage')

    def test_transform_with_edited_messages(self):
        """Test transformation with edited messages."""
        # Create data with edited messages using factory traits
        data_with_edited_messages = SkypeDataFactory.build(
            conversations=[
                SkypeConversationFactory.build(
                    id='conv1',
                    displayName='Edited Messages Test',
                    MessageList=[
                        SkypeMessageFactory.build(
                            id='msg1',
                            content='Original message'
                        ),
                        SkypeMessageFactory.build(
                            id='msg2',
                            content='Edited message',
                            edittime='2023-01-01T13:00:00Z'
                        )
                    ]
                )
            ]
        )

        # Transform the data
        transformed_data = self.transformer.transform(data_with_edited_messages, 'Test User')

        # Verify that edited messages are marked correctly
        conversations = transformed_data['conversations']
        conv1 = conversations['conv1']
        messages = conv1['messages']

        self.assertEqual(len(messages), 2)
        self.assertFalse(messages[0]['is_edited'])
        self.assertTrue(messages[1]['is_edited'])

if __name__ == '__main__':
    unittest.main()