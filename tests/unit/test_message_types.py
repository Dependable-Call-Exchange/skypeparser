#!/usr/bin/env python3
"""
Unit tests for message type handling.
"""

import json
import os
import tempfile
from unittest.mock import patch, MagicMock

import pytest

from src.parser.core_parser import type_parser
from src.utils.config import load_config, get_message_type_description
from src.parser.exceptions import InvalidInputError


@pytest.fixture
def message_types_config():
    """Create a temporary message types configuration file."""
    # Create a temporary directory
    temp_dir = tempfile.TemporaryDirectory()
    config_path = os.path.join(temp_dir.name, 'message_types.json')

    # Sample message types configuration
    test_config = {
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
    with open(config_path, 'w', encoding='utf-8') as f:
        json.dump(test_config, f)

    # Yield the config path and config data
    yield {
        'path': config_path,
        'config': test_config,
        'temp_dir': temp_dir
    }

    # Clean up
    temp_dir.cleanup()


def test_known_message_types(message_types_config, message_type_descriptions):
    """Test type_parser with known message types."""
    with patch('src.parser.core_parser.get_message_type_description') as mock_get_description:
        # Configure the mock to return descriptions from the centralized expectations
        mock_get_description.side_effect = lambda config, msg_type: message_type_descriptions.get(
            msg_type,
            "***Sent a {message_type}***".format(message_type=msg_type)
        )

        # Test known message types
        assert type_parser("RichText") == message_type_descriptions["RichText"]
        assert type_parser("RichText/UriObject") == message_type_descriptions["RichText/UriObject"]
        assert type_parser("Poll") == message_type_descriptions["Poll"]
        assert type_parser("ThreadActivity/AddMember") == message_type_descriptions["ThreadActivity/AddMember"]

        # For a type that is in the config but not in the centralized descriptions, use the config value
        if "CustomType" in message_types_config['config']['message_types']:
            assert type_parser("CustomType") == message_types_config['config']['message_types']["CustomType"]


def test_unknown_message_types(message_types_config):
    """Test type_parser with unknown message types."""
    # Configure the mock to return the default format for unknown types
    default_format = message_types_config['config']['default_message_format']

    with patch('src.parser.core_parser.get_message_type_description') as mock_get_description:
        with patch('src.parser.core_parser.logger') as mock_logger:
            mock_get_description.side_effect = lambda config, msg_type: default_format.format(message_type=msg_type)

            # Test unknown message type
            unknown_type = "UnknownType"
            expected = "***Sent a UnknownType***"
            result = type_parser(unknown_type)

            assert result == expected
            mock_logger.info.assert_called_with(f"Encountered unconfigured message type: {unknown_type}")


def test_empty_message_type():
    """Test type_parser with empty message type."""
    with patch('src.parser.core_parser.config') as mock_config:
        # Set up mock config
        mock_config.get.return_value = "***Sent a {message_type}***"

        # Test empty message type
        with pytest.raises(InvalidInputError):
            type_parser("")


def test_none_message_type():
    """Test type_parser with None message type."""
    with patch('src.parser.core_parser.config') as mock_config:
        # Set up mock config
        mock_config.get.return_value = "***Sent a {message_type}***"

        # Test None message type
        with pytest.raises(InvalidInputError):
            type_parser(None)


def test_enhanced_message_types():
    """Test type_parser with enhanced message types."""
    # Test with the new enhanced message types that might have been added
    enhanced_message_types = {
        "RichText/Media_Video": "***Sent a video***",
        "RichText/Media_AudioMsg": "***Sent a voice message***",
        "RichText/Media_GenericFile": "***Sent a file***",
        "RichText/Media_Card": "***Shared a card***",
        "Event/Call": "***Started a call***"
    }

    with patch('src.parser.core_parser.get_message_type_description') as mock_get_description:
        # Configure the mock to return enhanced descriptions
        mock_get_description.side_effect = lambda config, msg_type: enhanced_message_types.get(
            msg_type,
            "***Sent a {message_type}***".format(message_type=msg_type)
        )

        # Test enhanced message types
        assert type_parser("RichText/Media_Video") == "***Sent a video***"
        assert type_parser("RichText/Media_AudioMsg") == "***Sent a voice message***"
        assert type_parser("RichText/Media_GenericFile") == "***Sent a file***"
        assert type_parser("RichText/Media_Card") == "***Shared a card***"
        assert type_parser("Event/Call") == "***Started a call***"