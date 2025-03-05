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
    Parse message content using BeautifulSoup.

    Args:
        msg_content (str): Raw message content with HTML/XML tags

    Returns:
        str: Cleaned and formatted message content

    Raises:
        ContentParsingError: If the content cannot be parsed correctly
    """
    if not msg_content:
        return ""

    if BEAUTIFULSOUP:
        try:
            # Use the detected parser (lxml or html.parser)
            soup = BeautifulSoup(msg_content, BS_PARSER)
            text = soup.get_text()
            text = pretty_quotes(text)
            return text
        except Exception as e:
            logger.warning(f"Error parsing content with BeautifulSoup: {e}")
            # Fall back to regex if BeautifulSoup fails
            try:
                return tag_stripper(msg_content)
            except Exception as nested_e:
                error_msg = f"Failed to parse content with both BeautifulSoup and regex: {nested_e}"
                logger.error(error_msg)
                raise ContentParsingError(error_msg) from nested_e
    else:
        try:
            return tag_stripper(msg_content)
        except Exception as e:
            error_msg = f"Failed to parse content with regex: {e}"
            logger.error(error_msg)
            raise ContentParsingError(error_msg) from e


def tag_stripper(text: str) -> str:
    """
    Strip HTML tags from text using regex and properly decode all HTML entities.

    Args:
        text (str): Text to strip HTML tags from

    Returns:
        str: Text with HTML tags removed and entities decoded

    Raises:
        ContentParsingError: If the HTML tags cannot be stripped correctly
    """
    if not text:
        return ""

    try:
        # Remove HTML tags with a more precise regex
        text = re.sub(r'<[^>]+>', '', text)

        # Decode all HTML entities using html.unescape
        text = html.unescape(text)

        # Apply pretty quotes formatting
        text = pretty_quotes(text)

        return text
    except Exception as e:
        error_msg = f"Error stripping tags: {e}"
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
    if description == config.get('default_message_format', '***Sent a {message_type}***').format(message_type=msg_type):
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

    # Handle message type and content
    msg_data.update(_process_message_content(msg_type, msg_content_raw, full_message=msg))

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


def _process_message_content(msg_type: str, msg_content_raw: str, full_message: Dict[str, Any] = None) -> Dict[str, str]:
    """
    Process message content based on message type.

    Args:
        msg_type (str): Message type
        msg_content_raw (str): Raw message content
        full_message (Dict[str, Any], optional): The full message object for additional context

    Returns:
        dict: Processed content data
    """
    result = {
        'content_raw': msg_content_raw,
        'content': msg_content_raw
    }

    # Handle message type
    if msg_type != 'RichText':
        try:
            processed_content = type_parser(msg_type)
            result['content'] = processed_content
        except InvalidInputError:
            result['content'] = f"***Unknown message type: {msg_type}***"

    else:
        # Parse content for RichText messages
        try:
            cleaned_content = content_parser(msg_content_raw)
            result['content'] = cleaned_content
        except ContentParsingError:
            # Keep the raw content if parsing fails
            pass

    # Extract structured data for specific message types if full message is provided
    if full_message and get_handler_for_message_type(msg_type):
        structured_data = extract_structured_data(full_message)
        if structured_data:
            result.update(structured_data)

    return result
