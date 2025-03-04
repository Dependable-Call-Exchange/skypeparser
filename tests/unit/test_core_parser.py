#!/usr/bin/env python3
"""
Tests for the core_parser module.

This module contains tests for the functionality in src.parser.core_parser.
"""

import os
import json
import tempfile
import unittest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.parser.core_parser import (
    CoreParser,
    parse_skype_data,
    extract_messages,
    extract_users,
    extract_conversations
)
from src.parser.exceptions import ParserError


class TestCoreParser(unittest.TestCase):
    """Test cases for the CoreParser class and related functions."""

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
                            "timestamp": "2023-01-01T12:00:00Z"
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

    def test_core_parser_initialization(self):
        """Test CoreParser initialization."""
        parser = CoreParser(self.sample_json_path)
        self.assertEqual(parser.file_path, self.sample_json_path)
        self.assertIsNone(parser.data)

    def test_parse_skype_data(self):
        """Test parse_skype_data function."""
        data = parse_skype_data(self.sample_skype_data)
        self.assertIn('userId', data)
        self.assertIn('conversations', data)
        self.assertIn('users', data)

    def test_extract_messages(self):
        """Test extract_messages function."""
        conversation = self.sample_skype_data['conversations'][0]
        messages = extract_messages(conversation)
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]['id'], 'message1')

    def test_extract_users(self):
        """Test extract_users function."""
        users = extract_users(self.sample_skype_data)
        self.assertEqual(len(users), 1)
        self.assertEqual(users[0]['id'], 'user1')

    def test_extract_conversations(self):
        """Test extract_conversations function."""
        conversations = extract_conversations(self.sample_skype_data)
        self.assertEqual(len(conversations), 1)
        self.assertEqual(conversations[0]['id'], 'conversation1')

    def test_load_data(self):
        """Test load_data method."""
        parser = CoreParser(self.sample_json_path)
        parser.load_data()
        self.assertIsNotNone(parser.data)
        self.assertEqual(parser.data['userId'], 'test_user')

    def test_load_data_invalid_file(self):
        """Test load_data method with invalid file."""
        parser = CoreParser('nonexistent.json')
        with self.assertRaises(ParserError):
            parser.load_data()

    def test_parse(self):
        """Test parse method."""
        parser = CoreParser(self.sample_json_path)
        result = parser.parse()
        self.assertIn('userId', result)
        self.assertIn('conversations', result)
        self.assertIn('users', result)


if __name__ == '__main__':
    unittest.main()