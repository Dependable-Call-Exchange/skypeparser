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

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.parser.core_parser import (
    timestamp_parser,
    content_parser,
    enhanced_tag_stripper as tag_stripper,
    pretty_quotes,
    parse_skype_data
)
from src.parser.exceptions import SkypeParserError


class TestParserModule(unittest.TestCase):
    """Test cases for the core_parser module and related functions."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.TemporaryDirectory()

        # Create a sample JSON file
        self.json_data = {
            "userId": "user123",
            "exportDate": "2023-01-01T12:00:00.000Z",
            "conversations": [
                {
                    "id": "conversation1",
                    "displayName": "Test Conversation",
                    "messages": [
                        {
                            "id": "1",
                            "originalarrivaltime": "2023-01-01T12:00:00.000Z",
                            "from": "user1",
                            "content": "<p>Hello, world!</p>",
                            "messagetype": "RichText"
                        },
                        {
                            "id": "2",
                            "originalarrivaltime": "2023-01-01T12:01:00.000Z",
                            "from": "user2",
                            "content": "<p>Hi there!</p>",
                            "messagetype": "RichText"
                        }
                    ]
                }
            ]
        }
        self.json_file = os.path.join(self.temp_dir.name, "test.json")
        with open(self.json_file, "w") as f:
            json.dump(self.json_data, f)

        # Create a sample text file
        self.text_file = os.path.join(self.temp_dir.name, "test.txt")
        with open(self.text_file, "w") as f:
            f.write("This is a test file.")

        # Create an output directory
        self.output_dir = os.path.join(self.temp_dir.name, "output")
        os.makedirs(self.output_dir, exist_ok=True)

    def tearDown(self):
        """Clean up test fixtures."""
        self.temp_dir.cleanup()

    def test_timestamp_parser(self):
        """Test timestamp_parser function."""
        date_str, time_str, dt_obj = timestamp_parser("2023-01-01T12:00:00.000Z")
        self.assertEqual(date_str, "2023-01-01")
        self.assertEqual(time_str, "12:00:00")
        self.assertIsNotNone(dt_obj)

    def test_content_parser(self):
        """Test content_parser function."""
        content = "<p>Hello, <b>world</b>!</p>"
        parsed = content_parser(content)
        self.assertIn("Hello", parsed)
        self.assertIn("world", parsed)

    def test_tag_stripper(self):
        """Test tag_stripper function."""
        content = "<p>Hello, <b>world</b>!</p>"
        stripped = tag_stripper(content)
        self.assertIn("Hello", stripped)
        self.assertIn("world", stripped)

    def test_pretty_quotes(self):
        """Test pretty_quotes function."""
        text = 'This is a "test" with \'quotes\'.'
        pretty = pretty_quotes(text)
        self.assertNotEqual(pretty, text)  # Should be different after processing

    def test_parse_skype_data(self):
        """Test parse_skype_data function."""
        with open(self.json_file, 'r') as f:
            raw_data = json.load(f)
        result = parse_skype_data(raw_data, "Test User")
        self.assertIn("conversations", result)
        self.assertIn("user_id", result)
        self.assertEqual(result["user_id"], "user123")


if __name__ == '__main__':
    unittest.main()