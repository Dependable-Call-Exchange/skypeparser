#!/usr/bin/env python3
"""
Tests for the file_output module.

This module contains tests for the functionality in src.parser.file_output.
"""

import os
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, mock_open, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.parser.file_output import (
    FileOutput,
    write_json_output,
    write_csv_output,
    write_text_output,
    format_conversation_for_output
)
from src.parser.exceptions import OutputError


class TestFileOutput(unittest.TestCase):
    """Test cases for the FileOutput class and related functions."""

    def setUp(self):
        """Set up test fixtures."""
        # Sample parsed data for testing
        self.sample_parsed_data = {
            'user_info': {
                'user_id': 'test_user',
                'display_name': 'Test User',
                'export_date': '2023-01-01'
            },
            'conversations': [
                {
                    'id': 'conversation1',
                    'display_name': 'Test Conversation',
                    'messages': [
                        {
                            'id': 'message1',
                            'content': 'Hello, world!',
                            'sender_id': 'user1',
                            'sender_name': 'Test User',
                            'timestamp': '2023-01-01T12:00:00Z',
                            'message_type': 'RichText'
                        }
                    ]
                }
            ]
        }

        # Create a temporary directory for test files
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_dir = os.path.join(self.temp_dir.name, 'output')
        os.makedirs(self.output_dir, exist_ok=True)

    def tearDown(self):
        """Clean up test fixtures."""
        self.temp_dir.cleanup()

    def test_file_output_initialization(self):
        """Test FileOutput initialization."""
        output = FileOutput(self.sample_parsed_data, self.output_dir)
        self.assertEqual(output.data, self.sample_parsed_data)
        self.assertEqual(output.output_dir, self.output_dir)

    def test_write_json_output(self):
        """Test write_json_output function."""
        output_file = os.path.join(self.output_dir, 'output.json')
        write_json_output(self.sample_parsed_data, output_file)

        # Verify the file was created
        self.assertTrue(os.path.exists(output_file))

        # Verify the content
        with open(output_file, 'r') as f:
            content = json.load(f)
            self.assertEqual(content, self.sample_parsed_data)

    def test_write_csv_output(self):
        """Test write_csv_output function."""
        output_file = os.path.join(self.output_dir, 'output.csv')
        conversation = self.sample_parsed_data['conversations'][0]

        write_csv_output(conversation, output_file)

        # Verify the file was created
        self.assertTrue(os.path.exists(output_file))

        # Verify the file is not empty
        self.assertGreater(os.path.getsize(output_file), 0)

    def test_write_text_output(self):
        """Test write_text_output function."""
        output_file = os.path.join(self.output_dir, 'output.txt')
        conversation = self.sample_parsed_data['conversations'][0]

        write_text_output(conversation, output_file)

        # Verify the file was created
        self.assertTrue(os.path.exists(output_file))

        # Verify the file is not empty
        self.assertGreater(os.path.getsize(output_file), 0)

    def test_format_conversation_for_output(self):
        """Test format_conversation_for_output function."""
        conversation = self.sample_parsed_data['conversations'][0]
        formatted = format_conversation_for_output(conversation)

        self.assertIsInstance(formatted, list)
        self.assertEqual(len(formatted), 1)  # One message
        self.assertIn('sender_name', formatted[0])
        self.assertIn('content', formatted[0])
        self.assertIn('timestamp', formatted[0])

    def test_write_all_formats(self):
        """Test write_all_formats method."""
        output = FileOutput(self.sample_parsed_data, self.output_dir)
        output.write_all_formats()

        # Check if files were created
        json_file = os.path.join(self.output_dir, 'skype_data.json')
        self.assertTrue(os.path.exists(json_file))

        # Check if conversation directories were created
        conversation_dir = os.path.join(self.output_dir, 'Test Conversation')
        self.assertTrue(os.path.exists(conversation_dir))

        # Check if conversation files were created
        csv_file = os.path.join(conversation_dir, 'messages.csv')
        txt_file = os.path.join(conversation_dir, 'messages.txt')
        self.assertTrue(os.path.exists(csv_file))
        self.assertTrue(os.path.exists(txt_file))

    def test_write_json(self):
        """Test write_json method."""
        output = FileOutput(self.sample_parsed_data, self.output_dir)
        output.write_json()

        json_file = os.path.join(self.output_dir, 'skype_data.json')
        self.assertTrue(os.path.exists(json_file))

    def test_write_conversations(self):
        """Test write_conversations method."""
        output = FileOutput(self.sample_parsed_data, self.output_dir)
        output.write_conversations()

        conversation_dir = os.path.join(self.output_dir, 'Test Conversation')
        self.assertTrue(os.path.exists(conversation_dir))

        csv_file = os.path.join(conversation_dir, 'messages.csv')
        txt_file = os.path.join(conversation_dir, 'messages.txt')
        self.assertTrue(os.path.exists(csv_file))
        self.assertTrue(os.path.exists(txt_file))


if __name__ == '__main__':
    unittest.main()