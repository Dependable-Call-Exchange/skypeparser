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

class TestTransformer(unittest.TestCase):
    """Test cases for the Transformer class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a transformer instance with default settings
        self.transformer = Transformer(parallel_processing=False, chunk_size=2)

        # Sample raw data for testing
        self.raw_data = {
            'conversations': [
                {
                    'id': 'conv1',
                    'displayName': 'Test Conversation 1',
                    'MessageList': [
                        {
                            'id': 'msg1',
                            'originalarrivaltime': '2023-01-01T12:00:00Z',
                            'from': 'user1',
                            'messagetype': 'RichText',
                            'content': 'Hello world',
                            'edittime': None
                        },
                        {
                            'id': 'msg2',
                            'originalarrivaltime': '2023-01-01T12:05:00Z',
                            'from': 'user2',
                            'messagetype': 'RichText',
                            'content': 'Hi there',
                            'edittime': '2023-01-01T12:06:00Z'
                        }
                    ]
                },
                {
                    'id': 'conv2',
                    'displayName': 'Test Conversation 2',
                    'MessageList': [
                        {
                            'id': 'msg3',
                            'originalarrivaltime': '2023-01-02T10:00:00Z',
                            'from': 'user1',
                            'messagetype': 'RichText',
                            'content': 'Another message',
                            'edittime': None
                        }
                    ]
                }
            ]
        }

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
        self.assertEqual(conv1['message_count'], 2)
        self.assertEqual(len(conv1['messages']), 2)

        # Verify message details
        msg1 = conv1['messages'][0]
        self.assertEqual(msg1['timestamp'], '2023-01-01T12:00:00Z')
        self.assertEqual(msg1['sender_id'], 'user1')
        self.assertEqual(msg1['message_type'], 'RichText')
        self.assertEqual(msg1['content'], 'Hello world')
        self.assertFalse(msg1['is_edited'])

        msg2 = conv1['messages'][1]
        self.assertEqual(msg2['timestamp'], '2023-01-01T12:05:00Z')
        self.assertEqual(msg2['sender_id'], 'user2')
        self.assertTrue(msg2['is_edited'])

    def test_transform_empty_data(self):
        """Test transformation with empty data."""
        empty_data = {'conversations': []}
        transformed_data = self.transformer.transform(empty_data, 'Test User')

        # Verify the structure
        self.assertIn('metadata', transformed_data)
        self.assertIn('conversations', transformed_data)

        # Verify metadata
        metadata = transformed_data['metadata']
        self.assertEqual(metadata['total_conversations'], 0)
        self.assertEqual(metadata['total_messages'], 0)

        # Verify empty conversations
        self.assertEqual(len(transformed_data['conversations']), 0)

    def test_transform_invalid_data(self):
        """Test transformation with invalid data."""
        # Test with missing 'conversations' key
        invalid_data = {'other_key': 'value'}
        with self.assertRaises(ValueError):
            self.transformer.transform(invalid_data, 'Test User')

        # Test with non-dict data
        with self.assertRaises(ValueError):
            self.transformer.transform("not a dict", 'Test User')

    @patch('src.db.etl.transformer.ContentExtractor')
    def test_content_cleaning(self, mock_content_extractor_class):
        """Test that content is properly cleaned."""
        # Set up the mock
        mock_extractor = MagicMock()
        mock_extractor.clean_content.return_value = "Cleaned content"
        mock_content_extractor_class.return_value = mock_extractor

        # Create a new transformer with the mock
        transformer = Transformer(parallel_processing=False)

        # Transform the data
        transformed_data = transformer.transform(self.raw_data, 'Test User')

        # Verify the content extractor was called
        self.assertTrue(mock_extractor.clean_content.called)

        # Verify the cleaned content was used
        conv1_messages = transformed_data['conversations']['conv1']['messages']
        self.assertEqual(conv1_messages[0]['cleaned_content'], "Cleaned content")

    @patch('src.db.etl.transformer.get_handler_for_message_type')
    def test_structured_data_extraction(self, mock_get_handler):
        """Test that structured data is properly extracted."""
        # Set up the mock
        mock_handler = MagicMock()
        mock_handler.return_value = {"structured": "data"}
        mock_get_handler.return_value = mock_handler

        # Transform the data
        transformed_data = self.transformer.transform(self.raw_data, 'Test User')

        # Verify the handler was called
        self.assertTrue(mock_handler.called)

        # Verify the structured data was used
        conv1_messages = transformed_data['conversations']['conv1']['messages']
        self.assertEqual(conv1_messages[0]['structured_data'], {"structured": "data"})

    def test_parallel_processing(self):
        """Test that parallel processing works correctly."""
        # Create a transformer with parallel processing
        parallel_transformer = Transformer(parallel_processing=True)

        # Mock the ThreadPoolExecutor to verify it's used
        with patch('src.db.etl.transformer.ThreadPoolExecutor', spec=ThreadPoolExecutor) as mock_executor_class:
            # Set up the mock executor
            mock_executor = MagicMock()
            mock_executor.__enter__.return_value = mock_executor
            mock_executor_class.return_value = mock_executor

            # Set up the mock future
            mock_future = MagicMock()
            mock_executor.submit.return_value = mock_future
            mock_future.result.return_value = None

            # Transform the data
            parallel_transformer.transform(self.raw_data, 'Test User')

            # Verify the executor was used
            self.assertTrue(mock_executor_class.called)
            self.assertTrue(mock_executor.submit.called)

    def test_conversation_timespan(self):
        """Test that conversation timespan is correctly calculated."""
        # Transform the data
        transformed_data = self.transformer.transform(self.raw_data, 'Test User')

        # Verify the timespan for conversation 1
        conv1 = transformed_data['conversations']['conv1']
        self.assertEqual(conv1['first_message_time'], '2023-01-01T12:00:00Z')
        self.assertEqual(conv1['last_message_time'], '2023-01-01T12:05:00Z')

        # Verify the timespan for conversation 2
        conv2 = transformed_data['conversations']['conv2']
        self.assertEqual(conv2['first_message_time'], '2023-01-02T10:00:00Z')
        self.assertEqual(conv2['last_message_time'], '2023-01-02T10:00:00Z')

if __name__ == '__main__':
    unittest.main()