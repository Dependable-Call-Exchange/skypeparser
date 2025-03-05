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
from src.utils.config import load_config, get_message_type_description
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

    @patch('src.parser.core_parser.get_message_type_description')
    def test_known_message_types(self, mock_get_description):
        """Test type_parser with known message types."""
        # Configure the mock to return appropriate descriptions
        message_types = {
            "RichText": "***Text message***",
            "RichText/UriObject": "***Sent a photo or file***",
            "Poll": "***Created a poll***",
            "ThreadActivity/AddMember": "***Added a member to the conversation***",
            "CustomType": "***Custom message type***"
        }

        mock_get_description.side_effect = lambda config, msg_type: message_types.get(
            msg_type,
            "***Sent a {message_type}***".format(message_type=msg_type)
        )

        # Test known message types
        self.assertEqual(type_parser("RichText"), "***Text message***")
        self.assertEqual(type_parser("RichText/UriObject"), "***Sent a photo or file***")
        self.assertEqual(type_parser("Poll"), "***Created a poll***")
        self.assertEqual(type_parser("ThreadActivity/AddMember"), "***Added a member to the conversation***")
        self.assertEqual(type_parser("CustomType"), "***Custom message type***")

    @patch('src.parser.core_parser.get_message_type_description')
    @patch('src.parser.core_parser.logger')
    def test_unknown_message_types(self, mock_logger, mock_get_description):
        """Test type_parser with unknown message types."""
        # Configure the mock to return the default format for unknown types
        default_format = "***Sent a {message_type}***"

        mock_get_description.side_effect = lambda config, msg_type: default_format.format(message_type=msg_type)

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