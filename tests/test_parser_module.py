#!/usr/bin/env python3
"""
Tests for the parser_module module.

This module contains tests for the functionality in src.parser.parser_module.
"""

import os
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, mock_open, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.parser.parser_module import (
    ParserModule,
    get_parser_for_file,
    parse_file,
    write_output
)
from src.parser.exceptions import ParserError, OutputError


class TestParserModule(unittest.TestCase):
    """Test cases for the ParserModule class and related functions."""

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

        # Create output directory
        self.output_dir = os.path.join(self.temp_dir.name, 'output')
        os.makedirs(self.output_dir, exist_ok=True)

    def tearDown(self):
        """Clean up test fixtures."""
        self.temp_dir.cleanup()

    def test_parser_module_initialization(self):
        """Test ParserModule initialization."""
        parser_module = ParserModule()
        self.assertIsNotNone(parser_module)

    def test_get_parser_for_file_json(self):
        """Test get_parser_for_file function with JSON file."""
        parser = get_parser_for_file(self.sample_json_path)
        from src.parser.skype_parser import SkypeParser
        self.assertIsInstance(parser, SkypeParser)

    def test_get_parser_for_file_unsupported(self):
        """Test get_parser_for_file function with unsupported file."""
        unsupported_file = os.path.join(self.temp_dir.name, 'unsupported.txt')
        with open(unsupported_file, 'w') as f:
            f.write('This is not a supported file format')

        with self.assertRaises(ParserError):
            get_parser_for_file(unsupported_file)

    def test_parse_file(self):
        """Test parse_file function."""
        result = parse_file(self.sample_json_path)
        self.assertIn('user_info', result)
        self.assertIn('conversations', result)

    def test_parse_file_invalid(self):
        """Test parse_file function with invalid file."""
        with self.assertRaises(ParserError):
            parse_file('nonexistent.json')

    def test_write_output(self):
        """Test write_output function."""
        parsed_data = parse_file(self.sample_json_path)
        write_output(parsed_data, self.output_dir)

        # Check if output files were created
        json_file = os.path.join(self.output_dir, 'skype_data.json')
        self.assertTrue(os.path.exists(json_file))

        # Check if conversation directories were created
        conversation_dir = os.path.join(self.output_dir, 'Test Conversation')
        self.assertTrue(os.path.exists(conversation_dir))

    def test_parser_module_parse_and_output(self):
        """Test ParserModule parse_and_output method."""
        parser_module = ParserModule()
        parser_module.parse_and_output(self.sample_json_path, self.output_dir)

        # Check if output files were created
        json_file = os.path.join(self.output_dir, 'skype_data.json')
        self.assertTrue(os.path.exists(json_file))

        # Check if conversation directories were created
        conversation_dir = os.path.join(self.output_dir, 'Test Conversation')
        self.assertTrue(os.path.exists(conversation_dir))


if __name__ == '__main__':
    unittest.main()