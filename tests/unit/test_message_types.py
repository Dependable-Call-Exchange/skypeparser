#!/usr/bin/env python3
"""
Unit tests for message type handling.
"""

import unittest
import json
import os
import tempfile
from unittest.mock import patch, MagicMock

from src.parser.core_parser import type_parser
from src.utils.config import load_config
from src.parser.exceptions import InvalidInputError

class TestMessageTypes(unittest.TestCase):
    """Test cases for message type handling."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary config file
        self.temp_dir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.temp_dir.name, 'message_types.json')

        # Sample message types configuration
        self.test_config = {
            "message_types": {
                "RichText": "***Text message***",
                "RichText/UriObject": "***Sent a photo or file***",
                "Poll": "***Created a poll***",
                "ThreadActivity/AddMember": "***Added a member to the conversation***",
                "CustomType": "***Custom message type***"
            },
            "default_message_format": "***Sent a {message_type}***"
        }

        # Write config to file
        with open(self.config_path, 'w', encoding='utf-8') as f:
            json.dump(self.test_config, f)

        # Load the config
        self.config = load_config(message_types_file=self.config_path)

    def tearDown(self):
        """Tear down test fixtures."""
        self.temp_dir.cleanup()

    @patch('src.parser.core_parser.config')
    def test_known_message_types(self, mock_config):
        """Test type_parser with known message types."""
        # Set up mock config
        mock_config.get.return_value = self.test_config["default_message_format"]
        mock_config.__getitem__.return_value = self.test_config["message_types"]

        # Test known message types
        self.assertEqual(type_parser("RichText"), "***Text message***")
        self.assertEqual(type_parser("RichText/UriObject"), "***Sent a photo or file***")
        self.assertEqual(type_parser("Poll"), "***Created a poll***")
        self.assertEqual(type_parser("ThreadActivity/AddMember"), "***Added a member to the conversation***")
        self.assertEqual(type_parser("CustomType"), "***Custom message type***")

    @patch('src.parser.core_parser.config')
    @patch('src.parser.core_parser.logger')
    def test_unknown_message_types(self, mock_logger, mock_config):
        """Test type_parser with unknown message types."""
        # Set up mock config
        mock_config.get.return_value = self.test_config["default_message_format"]
        mock_config.__getitem__.side_effect = lambda key: (
            self.test_config["message_types"] if key == "message_types" else None
        )

        # Test unknown message type
        unknown_type = "UnknownType"
        expected = "***Sent a UnknownType***"
        result = type_parser(unknown_type)

        self.assertEqual(result, expected)
        mock_logger.info.assert_called_with(f"Encountered unconfigured message type: {unknown_type}")

    @patch('src.parser.core_parser.config')
    def test_empty_message_type(self, mock_config):
        """Test type_parser with empty message type."""
        # Set up mock config
        mock_config.get.return_value = self.test_config["default_message_format"]

        # Test empty message type
        with self.assertRaises(InvalidInputError):
            type_parser("")

    @patch('src.parser.core_parser.config')
    def test_none_message_type(self, mock_config):
        """Test type_parser with None message type."""
        # Set up mock config
        mock_config.get.return_value = self.test_config["default_message_format"]

        # Test None message type
        with self.assertRaises(InvalidInputError):
            type_parser(None)

if __name__ == '__main__':
    unittest.main()