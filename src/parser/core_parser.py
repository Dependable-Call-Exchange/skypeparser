#!/usr/bin/env python3
"""
Core Parser Module for Skype Export Data

This module provides core functions for parsing Skype export data, including
timestamp parsing, content extraction, and message type handling.
"""

import re
import logging
import datetime
import html
import json
import os
from typing import Dict, List, Tuple, Optional, Any

# Import custom exceptions
from .exceptions import (
    TimestampParsingError,
    ContentParsingError,
    DataExtractionError,
    InvalidInputError
)

# Import centralized dependency handling
from ..utils.dependencies import (
    BEAUTIFULSOUP_AVAILABLE as BEAUTIFULSOUP,
    BeautifulSoup,
    BS_PARSER
)

# Import content extraction module
from .content_extractor import (
    extract_content_data,
    format_content_with_markup,
    ContentExtractor
)

# Import configuration utility
from ..utils.config import load_config, get_message_type_description

# Import message type handlers
from ..utils.message_type_handlers import extract_structured_data, get_handler_for_message_type

# Load configuration
config = load_config(message_types_file='config/message_types.json')

# Set up logging
logger = logging.getLogger(__name__)


def timestamp_parser(timestamp: str) -> Tuple[str, str, Optional[datetime.datetime]]:
    """
    Parse a timestamp string into datetime object and formatted strings.

    Args:
        timestamp (str): ISO format timestamp string (e.g., '2023-01-01T12:34:56.789Z')

    Returns:
        tuple: (date_str, time_str, datetime_obj) where date_str and time_str are formatted strings
              and datetime_obj is a datetime object with proper timezone information

    Raises:
        TimestampParsingError: If the timestamp cannot be parsed correctly
    """
    if not timestamp:
        logger.warning("Empty timestamp provided")
        return "Unknown date", "Unknown time", None

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
    except ValueError as e:
        # Specific error for invalid timestamp format
        error_msg = f"Invalid timestamp format '{timestamp}': {e}"
        logger.warning(error_msg)
        raise TimestampParsingError(error_msg) from e
    except Exception as e:
        # Generic error for other issues
        error_msg = f"Error parsing timestamp '{timestamp}': {e}"
        logger.warning(error_msg)
        raise TimestampParsingError(error_msg) from e


def content_parser(msg_content: str) -> str:
    """
    Parse message content using BeautifulSoup with enhanced handling for Skype-specific elements.

    This function processes HTML/XML content from Skype messages, handling special elements like:
    - @mentions (<at id="user123">User Name</at>)
    - Links (<a href="url">link text</a>)
    - Formatting (bold, italic, etc.)
    - Quotes and replies
    - Emoticons and emojis

    Args:
        msg_content (str): Raw message content with HTML/XML tags

    Returns:
        str: Cleaned and formatted message content with preserved semantic meaning

    Raises:
        ContentParsingError: If the content cannot be parsed correctly
    """
    if not msg_content:
        return ""

    try:
        # Use the new content formatter from the content_extractor module
        return format_content_with_markup(msg_content)
    except Exception as e:
        error_msg = f"Error parsing content: {e}"
        logger.error(error_msg)
        raise ContentParsingError(error_msg) from e


def enhanced_tag_stripper(text: str) -> str:
    """
    Enhanced version of tag_stripper that preserves semantic meaning of special elements.

    This function uses regex patterns to handle:
    - @mentions
    - Links
    - Formatting
    - Quotes

    Args:
        text (str): Text to strip HTML tags from

    Returns:
        str: Text with HTML tags processed and entities decoded

    Raises:
        ContentParsingError: If the HTML tags cannot be processed correctly
    """
    if not text:
        return ""

    try:
        # Process @mentions - <at id="user123">User Name</at> -> @User Name
        text = re.sub(r'<at[^>]*>(.*?)</at>', r'@\1', text)

        # Process links - <a href="url">link text</a> -> link text (url)
        def process_link(match):
            href = re.search(r'href=["\'](.*?)["\']', match.group(0))
            link_text = re.search(r'>(.*?)</a>', match.group(0))

            if href and link_text:
                href_val = href.group(1)
                text_val = link_text.group(1)

                if href_val != text_val:
                    return f"{text_val} ({href_val})"
                else:
                    return href_val
            elif href:
                return href.group(1)
            elif link_text:
                return link_text.group(1)
            else:
                return ""

        text = re.sub(r'<a[^>]*>.*?</a>', process_link, text)

        # Process formatting
        text = re.sub(r'<(?:b|strong)[^>]*>(.*?)</(?:b|strong)>', r'*\1*', text)
        text = re.sub(r'<(?:i|em)[^>]*>(.*?)</(?:i|em)>', r'_\1_', text)
        text = re.sub(r'<u[^>]*>(.*?)</u>', r'_\1_', text)
        text = re.sub(r'<(?:s|strike|del)[^>]*>(.*?)</(?:s|strike|del)>', r'~\1~', text)
        text = re.sub(r'<(?:code|pre)[^>]*>(.*?)</(?:code|pre)>', r'`\1`', text)

        # Process quotes
        def process_quote(match):
            author = re.search(r'author=["\'](.*?)["\']', match.group(0))
            quote_text = re.search(r'>(.*?)</quote>', match.group(0), re.DOTALL)

            if author and quote_text:
                return f"\n> {author.group(1)} wrote:\n> {quote_text.group(1).strip()}\n"
            elif quote_text:
                return f"\n> {quote_text.group(1).strip()}\n"
            else:
                return ""

        text = re.sub(r'<quote[^>]*>.*?</quote>', process_quote, text, flags=re.DOTALL)

        # Process line breaks
        text = re.sub(r'<br[^>]*>', '\n', text)

        # Remove remaining HTML tags
        text = re.sub(r'<[^>]+>', '', text)

        # Decode all HTML entities
        text = html.unescape(text)

        # Apply pretty quotes formatting
        text = pretty_quotes(text)

        # Clean up extra whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        text = re.sub(r'\n\s+', '\n', text)

        return text
    except Exception as e:
        error_msg = f"Error in enhanced tag stripper: {e}"
        logger.warning(error_msg)
        raise ContentParsingError(error_msg) from e


def pretty_quotes(cleaned_text: str) -> str:
    """
    Format quoted messages for better readability and replace straight quotes with curly quotes.

    Args:
        cleaned_text (str): Text with quote markers

    Returns:
        str: Text with formatted quotes

    Raises:
        ContentParsingError: If the quotes cannot be formatted correctly
    """
    if not cleaned_text:
        return ""

    try:
        # Format Skype-specific quote markers
        match = re.compile(r'\[[+-]?\d+(?:\.\d+)?\]')
        cleaned_text = match.sub(r'\n\t*** Quoting the following message: ***\n\t', cleaned_text)
        match = re.compile(r'\<\<\<')
        cleaned_text = match.sub('\t*** And responding with: ***\n\t', cleaned_text)

        # Replace straight quotes with curly quotes for better readability
        cleaned_text = re.sub(r'(?<!\w)"(?=\w)', '"', cleaned_text)  # Opening double quote
        cleaned_text = re.sub(r'(?<=\w)"(?!\w)', '"', cleaned_text)  # Closing double quote
        cleaned_text = re.sub(r"(?<!\w)'(?=\w)", "'", cleaned_text)  # Opening single quote
        cleaned_text = re.sub(r"(?<=\w)'(?!\w)", "'", cleaned_text)  # Closing single quote

        # Handle quotes that don't match the word boundary patterns
        cleaned_text = re.sub(r'"', '"', cleaned_text)  # For remaining straight double quotes
        cleaned_text = re.sub(r"'", "'", cleaned_text)  # For remaining straight single quotes

        return cleaned_text
    except Exception as e:
        error_msg = f"Error formatting quotes: {e}"
        logger.warning(error_msg)
        raise ContentParsingError(error_msg) from e


def type_parser(msg_type: str) -> str:
    """
    Map message types to their human-readable descriptions.

    Args:
        msg_type (str): Skype message type

    Returns:
        str: Human-readable description

    Raises:
        InvalidInputError: If the message type is invalid or empty
    """
    if not msg_type:
        error_msg = "Empty message type provided"
        logger.warning(error_msg)
        raise InvalidInputError(error_msg)

    # Use the configuration utility to get the message type description
    description = get_message_type_description(config, msg_type)

    # Log unknown message types to help identify gaps in our configuration
    default_format = config.get('default_message_format', '***Sent a {message_type}***')
    if description == default_format.format(message_type=msg_type):
        logger.info(f"Encountered unconfigured message type: {msg_type}")

    return description


def banner_constructor(display_name: str, person: str, export_date: str,
                      export_time: str, timestamps: List[str]) -> str:
    """
    Construct a banner for the conversation.

    Args:
        display_name (str): Display name of the conversation
        person (str): ID of the conversation
        export_date (str): Date of export
        export_time (str): Time of export
        timestamps (list): List of timestamps or datetime objects for the conversation

    Returns:
        str: Banner text

    Raises:
        InvalidInputError: If required parameters are missing or invalid
    """
    if not display_name or not person:
        error_msg = "Missing required parameters for banner construction"
        logger.warning(error_msg)
        raise InvalidInputError(error_msg)

    banner = f"Conversation with: {display_name} ({person})\n"
    banner += f"Exported on: {export_date}, at: {export_time}\n"

    if not timestamps:
        banner += "No messages found in this conversation.\n"
        banner += "***** All times are in UTC *****\n"
        return banner

    try:
        # Get first and last timestamps
        if timestamps and all(isinstance(ts, datetime.datetime) for ts in timestamps if ts is not None):
            # If all are datetime objects
            valid_timestamps = [ts for ts in timestamps if ts is not None]
            if valid_timestamps:
                first_timestamp = min(valid_timestamps)
                last_timestamp = max(valid_timestamps)

                first_date = first_timestamp.strftime('%Y-%m-%d')
                first_time = first_timestamp.strftime('%H:%M:%S')
                last_date = last_timestamp.strftime('%Y-%m-%d')
                last_time = last_timestamp.strftime('%H:%M:%S')
            else:
                return banner + "No valid timestamps found.\n***** All times are in UTC *****\n"
        else:
            # Fall back to string handling
            sorted_timestamps = sorted(timestamps)
            first_timestamp = sorted_timestamps[0]
            last_timestamp = sorted_timestamps[-1]

            try:
                first_date, first_time, _ = timestamp_parser(first_timestamp)
                last_date, last_time, _ = timestamp_parser(last_timestamp)
            except TimestampParsingError:
                # If timestamp parsing fails, use default values
                first_date, first_time = "Unknown date", "Unknown time"
                last_date, last_time = "Unknown date", "Unknown time"

        banner += f"Conversations From: {first_date}, at: {first_time}\n"
        banner += f"                To: {last_date}, at: {last_time}\n"
        banner += "***** All times are in UTC *****\n"

        return banner
    except Exception as e:
        error_msg = f"Error processing timestamps for banner: {e}"
        logger.warning(error_msg)
        # Return a basic banner rather than raising an exception
        banner += "***** All times are in UTC *****\n"
        return banner


def id_selector(ids: List[str], selected_indices: Optional[List[int]] = None) -> List[str]:
    """
    Select conversation IDs from a list.

    Args:
        ids (list): List of conversation IDs
        selected_indices (list, optional): List of indices to select. If None, all IDs are returned.

    Returns:
        list: Selected conversation IDs

    Raises:
        InvalidInputError: If the input list is empty or invalid
    """
    if not ids:
        error_msg = "No conversation IDs provided for selection"
        logger.warning(error_msg)
        raise InvalidInputError(error_msg)

    # Log available IDs for reference
    logger.info("Available conversation IDs: ")
    for i, id_val in enumerate(ids):
        logger.info(f'\t{i+1} ----> {id_val}')

    # If no selection provided, return all IDs
    if selected_indices is None:
        logger.info("No selection provided, returning all conversation IDs")
        return ids

    # Validate selected indices
    valid_indices = []
    for idx in selected_indices:
        if isinstance(idx, int) and 0 <= idx < len(ids):
            valid_indices.append(idx)
        else:
            logger.warning(f"Invalid index: {idx}, must be between 0 and {len(ids)-1}")

    if not valid_indices:
        logger.warning("No valid indices provided, returning all conversation IDs")
        return ids

    # Return selected IDs
    selected = [ids[i] for i in valid_indices]
    logger.info(f"Selected {len(selected)} conversation IDs")
    return selected


def parse_skype_data(raw_data: Dict[str, Any], user_display_name: str) -> Dict[str, Any]:
    """
    Parse raw Skype export data into a structured format.

    Args:
        raw_data (dict): Raw Skype export data
        user_display_name (str): Display name of the user

    Returns:
        dict: Structured data with conversations and messages

    Raises:
        InvalidInputError: If required fields are missing in the input data
        DataExtractionError: If data extraction fails
    """
    if not raw_data:
        error_msg = "Empty raw data provided"
        logger.error(error_msg)
        raise InvalidInputError(error_msg)

    if not user_display_name:
        error_msg = "User display name is required"
        logger.error(error_msg)
        raise InvalidInputError(error_msg)

    try:
        # Extract basic metadata
        metadata = _extract_metadata(raw_data)

        # Initialize data structures
        structured_data = {}
        id_to_display_name = {metadata['user_id']: str(user_display_name)}

        # Process conversations
        structured_data = _process_conversations(raw_data, id_to_display_name)

        # Combine all data
        return {
            'user_id': metadata['user_id'],
            'export_date': metadata['export_date_str'],
            'export_time': metadata['export_time_str'],
            'export_datetime': metadata['export_datetime'],
            'conversations': structured_data,
            'id_to_display_name': id_to_display_name
        }

    except Exception as e:
        error_msg = f"Error parsing Skype data: {e}"
        logger.error(error_msg)
        raise DataExtractionError(error_msg) from e


def _extract_metadata(raw_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract basic metadata from raw Skype data.

    Args:
        raw_data (dict): Raw Skype export data

    Returns:
        dict: Extracted metadata

    Raises:
        InvalidInputError: If required fields are missing
    """
    try:
        user_id = raw_data['userId']
        export_date_time = raw_data['exportDate']
        export_date_str, export_time_str, export_datetime = timestamp_parser(export_date_time)
        no_of_conversations = len(raw_data['conversations'])

        return {
            'user_id': user_id,
            'export_date_time': export_date_time,
            'export_date_str': export_date_str,
            'export_time_str': export_time_str,
            'export_datetime': export_datetime,
            'no_of_conversations': no_of_conversations
        }
    except KeyError as e:
        error_msg = f"Missing required field in JSON: {e}"
        logger.error(error_msg)
        raise InvalidInputError(error_msg) from e
    except TimestampParsingError as e:
        # Handle timestamp parsing errors
        logger.warning(f"Error parsing export timestamp: {e}")
        return {
            'user_id': raw_data['userId'],
            'export_date_time': raw_data['exportDate'],
            'export_date_str': "Unknown date",
            'export_time_str': "Unknown time",
            'export_datetime': None,
            'no_of_conversations': len(raw_data['conversations'])
        }


def _process_conversations(raw_data: Dict[str, Any], id_to_display_name: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
    """
    Process all conversations from raw data.

    Args:
        raw_data (dict): Raw Skype export data
        id_to_display_name (dict): Mapping of user IDs to display names

    Returns:
        dict: Structured conversation data
    """
    structured_data = {}

    # Process each conversation
    for i in range(len(raw_data['conversations'])):
        try:
            # Extract conversation metadata
            conversation = raw_data['conversations'][i]
            conv_id = conversation['id']

            # Process display name
            display_name = _get_conversation_display_name(conversation)

            # Store display name mapping
            id_to_display_name[conv_id] = display_name

            # Process messages
            structured_messages = _process_messages(conversation, id_to_display_name)

            # Store conversation data
            structured_data[conv_id] = {
                'display_name': display_name,
                'id': conv_id,
                'export_date': _get_export_date_from_raw(raw_data),
                'export_time': _get_export_time_from_raw(raw_data),
                'messages': structured_messages
            }

        except (KeyError, IndexError) as e:
            logger.warning(f"Error processing conversation {i}: {e}")
            continue

    return structured_data


def _get_conversation_display_name(conversation: Dict[str, Any]) -> str:
    """
    Get the display name for a conversation.

    Args:
        conversation (dict): Conversation data

    Returns:
        str: Display name for the conversation
    """
    d_name = conversation.get('displayName')

    if d_name is None:
        return conversation['id']
    else:
        return d_name


def _get_export_date_from_raw(raw_data: Dict[str, Any]) -> str:
    """
    Get the export date from raw data.

    Args:
        raw_data (dict): Raw Skype export data

    Returns:
        str: Export date string
    """
    try:
        export_date_str, _, _ = timestamp_parser(raw_data['exportDate'])
        return export_date_str
    except (KeyError, TimestampParsingError):
        return "Unknown date"


def _get_export_time_from_raw(raw_data: Dict[str, Any]) -> str:
    """
    Get the export time from raw data.

    Args:
        raw_data (dict): Raw Skype export data

    Returns:
        str: Export time string
    """
    try:
        _, export_time_str, _ = timestamp_parser(raw_data['exportDate'])
        return export_time_str
    except (KeyError, TimestampParsingError):
        return "Unknown time"


def _process_messages(conversation: Dict[str, Any], id_to_display_name: Dict[str, str]) -> List[Dict[str, Any]]:
    """
    Process all messages in a conversation.

    Args:
        conversation (dict): Conversation data
        id_to_display_name (dict): Mapping of user IDs to display names

    Returns:
        list: List of structured message data
    """
    messages = conversation.get('MessageList', [])
    structured_messages = []

    # Process each message
    for msg in messages:
        try:
            structured_message = _process_single_message(msg, id_to_display_name)
            structured_messages.append(structured_message)
        except Exception as e:
            logger.warning(f"Error processing message: {e}")
            continue

    return structured_messages


def _process_single_message(msg: Dict[str, Any], id_to_display_name: Dict[str, str]) -> Dict[str, Any]:
    """
    Process a single message.

    Args:
        msg (dict): Message data
        id_to_display_name (dict): Mapping of user IDs to display names

    Returns:
        dict: Structured message data
    """
    # Extract message data
    msg_timestamp = msg.get('originalarrivaltime', '')
    msg_from = msg.get('from', '')
    msg_content_raw = msg.get('content', '')
    msg_type = msg.get('messagetype', '')

    # Parse timestamp
    msg_date_str, msg_time_str, msg_datetime = _parse_message_timestamp(msg_timestamp)

    # Create message data structure
    msg_data = {
        'timestamp': msg_timestamp,
        'date': msg_date_str,
        'time': msg_time_str,
        'from_id': msg_from,
        'from_name': id_to_display_name.get(msg_from, msg_from),
        'type': msg_type,
        'is_edited': 'skypeeditedid' in msg
    }

    # Process message content and extract structured data
    processed_content, structured_data = _process_message_content(msg, msg_type)

    # Update message data with processed content and structured data
    msg_data['content_raw'] = msg_content_raw
    msg_data['content'] = processed_content

    # Add structured data if available
    if structured_data:
        msg_data['structured_content'] = structured_data

    return msg_data


def _parse_message_timestamp(timestamp: str) -> Tuple[str, str, Optional[datetime.datetime]]:
    """
    Parse a message timestamp.

    Args:
        timestamp (str): Timestamp string

    Returns:
        tuple: (date_str, time_str, datetime_obj)
    """
    try:
        return timestamp_parser(timestamp)
    except TimestampParsingError:
        return "Unknown date", "Unknown time", None


def _process_message_content(message_data, message_type):
    """
    Process message content based on message type.

    Args:
        message_data (dict): Message data dictionary
        message_type (str): Type of message

    Returns:
        tuple: (processed_content, structured_data)
    """
    content = message_data.get('content', '')
    structured_data = {}

    # Skip processing for empty content
    if not content:
        return content, structured_data

    # Use the ContentExtractor to extract structured data
    extractor = ContentExtractor()

    # For RichText messages, use the content extractor to get structured data
    if message_type == 'RichText':
        structured_data = extractor.extract_all(content)

        # Format the content with markup if needed
        if structured_data:
            content = extractor.format_content_with_markup(content, structured_data)

    # For other message types, still try to extract structured data
    # but don't modify the content
    else:
        structured_data = extractor.extract_all(content)

    return content, structured_data
