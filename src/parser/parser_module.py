#!/usr/bin/env python3
"""
Skype Parser Module (DEPRECATED)

This module provides functions for parsing Skype export files.
It is designed to be imported by other scripts that need to process Skype data.

DEPRECATION NOTICE:
This module is deprecated and will be removed in a future version.
Please use the functions from core_parser.py instead, which provide
more robust implementations with better timezone handling and error management.
"""

import os
import sys
import json
import logging
import datetime
import re
import html
import warnings

# Show deprecation warning
warnings.warn(
    "The parser_module.py file is deprecated and will be removed in a future version. "
    "Please use the functions from core_parser.py instead.",
    DeprecationWarning,
    stacklevel=2
)

# Import file handling functions from utils
from ..utils.file_handler import read_file, read_tarfile

# Import centralized dependency handling
from ..utils.dependencies import (
    BEAUTIFULSOUP_AVAILABLE,
    BeautifulSoup
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('skype-parser-module')

def safe_filename(name):
    """
    Create a safe filename from a string.

    Args:
        name (str): String to convert to a safe filename

    Returns:
        str: Safe filename
    """
    # Replace invalid characters with underscores
    return re.sub(r'[\\/*?:"<>|]', '_', name)

# Note: read_file and read_tarfile functions are now imported from utils.file_handler

def timestamp_parser(timestamp):
    """
    Parse a timestamp string into datetime object and formatted strings.

    DEPRECATED: Use core_parser.timestamp_parser instead.

    Args:
        timestamp (str): ISO format timestamp string (e.g., '2023-01-01T12:34:56.789Z')

    Returns:
        tuple: (date_str, time_str, datetime_obj) where date_str and time_str are formatted strings
              and datetime_obj is a datetime object with proper timezone information
    """
    warnings.warn(
        "This function is deprecated. Use core_parser.timestamp_parser instead.",
        DeprecationWarning,
        stacklevel=2
    )

    try:
        original_timestamp = timestamp

        # Handle milliseconds - split and keep only the main part
        if '.' in timestamp:
            main_part, _ = timestamp.split('.', 1)
            timestamp = main_part

            # If we have Z after milliseconds, we need to handle it
            if original_timestamp.endswith('Z'):
                # This is UTC time
                dt_obj = datetime.datetime.fromisoformat(timestamp).replace(tzinfo=datetime.timezone.utc)
            else:
                # Check if there's a timezone offset after milliseconds
                offset_match = re.search(r'[+-]\d{2}:\d{2}$', original_timestamp)
                if offset_match:
                    # There is a timezone offset, use the original string but without milliseconds
                    timestamp_with_offset = timestamp + offset_match.group(0)
                    dt_obj = datetime.datetime.fromisoformat(timestamp_with_offset)
                else:
                    # No timezone information, treat as naive
                    dt_obj = datetime.datetime.fromisoformat(timestamp)
        else:
            # No milliseconds
            if timestamp.endswith('Z'):
                # UTC time
                dt_obj = datetime.datetime.fromisoformat(timestamp[:-1]).replace(tzinfo=datetime.timezone.utc)
            else:
                # May have timezone offset or be naive
                dt_obj = datetime.datetime.fromisoformat(timestamp)

        # Format the date and time as strings
        date_str = dt_obj.strftime('%Y-%m-%d')
        time_str = dt_obj.strftime('%H:%M:%S')

        return date_str, time_str, dt_obj
    except Exception as e:
        logger.warning(f"Error parsing timestamp '{timestamp}': {e}")
        return "Unknown date", "Unknown time", None

def content_parser(msg_content):
    """
    Parse message content to extract text.

    DEPRECATED: Use core_parser.content_parser instead.

    Args:
        msg_content (str): Message content to parse

    Returns:
        str: Parsed message content
    """
    warnings.warn(
        "This function is deprecated. Use core_parser.content_parser instead.",
        DeprecationWarning,
        stacklevel=2
    )

    if BEAUTIFULSOUP_AVAILABLE:
        try:
            soup = BeautifulSoup(msg_content, 'html.parser')
            return soup.get_text()
        except Exception as e:
            logger.warning(f"Error parsing content with BeautifulSoup: {e}")
            return tag_stripper(msg_content)
    else:
        return tag_stripper(msg_content)

def tag_stripper(text):
    """
    Strip HTML tags from text using regex and properly decode all HTML entities.

    DEPRECATED: Use core_parser.tag_stripper instead.

    Args:
        text (str): Text to strip HTML tags from

    Returns:
        str: Text with HTML tags removed and entities decoded
    """
    warnings.warn(
        "This function is deprecated. Use core_parser.tag_stripper instead.",
        DeprecationWarning,
        stacklevel=2
    )

    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)

    # Decode all HTML entities using html.unescape
    text = html.unescape(text)

    return text

def pretty_quotes(cleaned_text):
    """
    Replace straight quotes with curly quotes for better readability.

    DEPRECATED: Use core_parser.pretty_quotes instead.

    Args:
        cleaned_text (str): Text to replace quotes in

    Returns:
        str: Text with pretty quotes
    """
    warnings.warn(
        "This function is deprecated. Use core_parser.pretty_quotes instead.",
        DeprecationWarning,
        stacklevel=2
    )

    # Replace straight quotes with curly quotes
    cleaned_text = re.sub(r'(?<!\w)"(?=\w)', '"', cleaned_text)  # Opening double quote
    cleaned_text = re.sub(r'(?<=\w)"(?!\w)', '"', cleaned_text)  # Closing double quote
    cleaned_text = re.sub(r"(?<!\w)'(?=\w)", "'", cleaned_text)  # Opening single quote
    cleaned_text = re.sub(r"(?<=\w)'(?!\w)", "'", cleaned_text)  # Closing single quote

    # Handle quotes that don't match the word boundary patterns
    # For remaining straight double quotes
    cleaned_text = re.sub(r'"', '"', cleaned_text)
    # For remaining straight single quotes
    cleaned_text = re.sub(r"'", "'", cleaned_text)

    return cleaned_text