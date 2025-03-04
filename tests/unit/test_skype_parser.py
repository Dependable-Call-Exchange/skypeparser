#!/usr/bin/env python3
"""
Tests for the skype_parser module.

This module contains tests for the functionality in src.parser.skype_parser.
"""

import os
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, mock_open

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.parser.skype_parser import (
    SkypeParser,
    parse_skype_export,
    extract_user_info,
    extract_conversation_info,
    format_message
)
from src.parser.exceptions import ParserError


class TestSkypeParser(unittest.TestCase):
    """Test cases for the SkypeParser class and related functions."""

    def setUp(self):
        """Set up test fixtures."""
        # Sample Skype data for testing
        self.sample_skype_data = {
            "userId": "test_user",
            "exportDate": "2023-01-01",
            "conversations": [
                {
                    "id": "conversation1",
                    "displayName": "Test Conversation",
                    "messages": [
                        {
                            "id": "message1",
                            "content": "Hello, world!",
                            "from": "user1",
                            "timestamp": "2023-01-01T12:00:00Z",
                            "messageType": "RichText"
                        }
                    ]
                }
            ],
            "users": [
                {
                    "id": "user1",
                    "displayName": "Test User"
                }
            ]
        }

        # Create a temporary directory for test files
        self.temp_dir = tempfile.TemporaryDirectory()

        # Create a sample JSON file
        self.sample_json_path = os.path.join(self.temp_dir.name, 'sample.json')
        with open(self.sample_json_path, 'w') as f:
            json.dump(self.sample_skype_data, f)

    def tearDown(self):
        """Clean up test fixtures."""
        self.temp_dir.cleanup()

    def test_skype_parser_initialization(self):
        """Test SkypeParser initialization."""
        parser = SkypeParser(self.sample_json_path)
        self.assertEqual(parser.file_path, self.sample_json_path)
        self.assertIsNone(parser.data)

    def test_parse_skype_export(self):
        """Test parse_skype_export function."""
        with patch('src.parser.skype_parser.open', mock_open(read_data=json.dumps(self.sample_skype_data))):
            result = parse_skype_export(self.sample_json_path)
            self.assertIn('user_info', result)
            self.assertIn('conversations', result)

    def test_extract_user_info(self):
        """Test extract_user_info function."""
        user_info = extract_user_info(self.sample_skype_data)
        self.assertEqual(user_info['user_id'], 'test_user')
        self.assertIn('export_date', user_info)

    def test_extract_conversation_info(self):
        """Test extract_conversation_info function."""
        conversation = self.sample_skype_data['conversations'][0]
        users = {user['id']: user for user in self.sample_skype_data['users']}
        conversation_info = extract_conversation_info(conversation, users)
        self.assertEqual(conversation_info['id'], 'conversation1')
        self.assertEqual(conversation_info['display_name'], 'Test Conversation')
        self.assertEqual(len(conversation_info['messages']), 1)

    def test_format_message(self):
        """Test format_message function."""
        message = self.sample_skype_data['conversations'][0]['messages'][0]
        users = {user['id']: user for user in self.sample_skype_data['users']}
        formatted_message = format_message(message, users)
        self.assertEqual(formatted_message['id'], 'message1')
        self.assertEqual(formatted_message['content'], 'Hello, world!')
        self.assertEqual(formatted_message['sender_name'], 'Test User')
        self.assertEqual(formatted_message['message_type'], 'RichText')

    def test_parse(self):
        """Test parse method."""
        parser = SkypeParser(self.sample_json_path)
        result = parser.parse()
        self.assertIn('user_info', result)
        self.assertIn('conversations', result)

    def test_parse_invalid_file(self):
        """Test parse method with invalid file."""
        parser = SkypeParser('nonexistent.json')
        with self.assertRaises(ParserError):
            parser.parse()


if __name__ == '__main__':
    unittest.main()