#!/usr/bin/env python3
"""
Tests for the core_parser module.

This module contains test cases for the core parser functions in src.parser.core_parser.
"""

import os
import json
import tempfile
import unittest
import shutil
from pathlib import Path
from unittest.mock import patch, mock_open

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.parser.core_parser import (
    timestamp_parser,
    content_parser,
    tag_stripper,
    pretty_quotes,
    type_parser,
    banner_constructor,
    id_selector,
    parse_skype_data
)
from src.parser.exceptions import DataExtractionError


class TestCoreParser(unittest.TestCase):
    """Test cases for core parser functions."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for testing
        self.temp_dir = tempfile.mkdtemp()

        # Sample Skype export data for testing
        self.sample_skype_data = {
            "userId": "test_user",
            "userDisplayName": "Test User",
            "exportDate": "2023-01-01T12:00:00Z",
            "conversations": [
                {
                    "id": "conversation1",
                    "displayName": "Test Conversation",
                    "messageCount": 1,
                    "firstMessageTime": "2023-01-01T12:00:00Z",
                    "lastMessageTime": "2023-01-01T12:00:00Z",
                    "MessageList": [
                        {
                            "originalarrivaltime": "2023-01-01T12:00:00Z",
                            "from": "user1",
                            "content": "Hello, world!",
                            "messagetype": "RichText"
                        }
                    ]
                }
            ]
        }

    def tearDown(self):
        """Tear down test fixtures."""
        # Remove temporary directory and files
        shutil.rmtree(self.temp_dir)

    def test_timestamp_parser(self):
        """Test timestamp_parser function."""
        timestamp = "2023-01-01T12:00:00Z"
        date_str, time_str, dt_obj = timestamp_parser(timestamp)
        self.assertEqual(date_str, "2023-01-01")
        self.assertEqual(time_str, "12:00:00")
        self.assertIsNotNone(dt_obj)

    def test_parse_skype_data(self):
        """Test parse_skype_data function."""
        result = parse_skype_data(self.sample_skype_data, "Test User")
        self.assertIn("user_id", result)
        self.assertIn("export_date", result)
        self.assertIn("export_time", result)
        self.assertIn("conversations", result)
        self.assertEqual(result["user_id"], "test_user")
        self.assertEqual(result["export_date"], "2023-01-01")
        self.assertEqual(result["export_time"], "12:00:00")
        self.assertEqual(len(result["conversations"]), 1)

    def test_content_parser(self):
        """Test content_parser function."""
        content = "<div><p>Hello, world!</p></div>"
        result = content_parser(content)
        self.assertEqual(result, "Hello, world!")

    def test_tag_stripper(self):
        """Test tag_stripper function."""
        content = "<div><p>Hello, world!</p></div>"
        result = tag_stripper(content)
        self.assertEqual(result, "Hello, world!")

    def test_type_parser(self):
        """Test type_parser function."""
        message_type = "RichText"
        result = type_parser(message_type)
        self.assertEqual(result, "***Sent a RichText***")

    def test_banner_constructor(self):
        """Test banner_constructor function."""
        display_name = "Test Conversation"
        person = "test_user"
        export_date = "2023-01-01"
        export_time = "12:00:00"
        timestamps = ["2023-01-01T12:00:00Z", "2023-01-01T12:30:00Z"]

        result = banner_constructor(display_name, person, export_date, export_time, timestamps)

        self.assertIn("Test Conversation", result)
        self.assertIn("test_user", result)
        self.assertIn("2023-01-01", result)
        self.assertIn("12:00:00", result)

    def test_id_selector(self):
        """Test id_selector function."""
        ids = ["conversation1", "conversation2", "conversation3"]
        result = id_selector(ids)
        self.assertEqual(result, ids)  # Should return all IDs when no selection is provided

        # Test with specific selection
        selected_indices = [0, 2]  # Select first and third conversations
        result = id_selector(ids, selected_indices)
        self.assertEqual(result, ["conversation1", "conversation3"])


if __name__ == '__main__':
    unittest.main()