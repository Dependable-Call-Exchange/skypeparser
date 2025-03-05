#!/usr/bin/env python3
"""
File Output Module for Skype Export Data

This module provides functions for outputting Skype export data to various file formats,
including text, JSON, and CSV.
"""

import os
import json
import csv
import logging
import datetime
from typing import Dict, Any

from ..utils.file_utils import safe_filename
from .core_parser import timestamp_parser, content_parser, tag_stripper, banner_constructor
from .exceptions import (
    FileOperationError,
    ExportError,
    ContentParsingError
)

# Import centralized dependency handling
from ..utils.dependencies import (
    BEAUTIFULSOUP_AVAILABLE as BEAUTIFULSOUP
)

# Set up logging
logger = logging.getLogger(__name__)


def write_to_file(file_name: str, parsed_content: str) -> None:
    """
    Write parsed content to a file

    Args:
        file_name (str): Name of the file to write to
        parsed_content (str): Content to write to the file

    Raises:
        FileOperationError: If the file cannot be written
    """
    if not file_name:
        error_msg = "No file name provided"
        logger.error(error_msg)
        raise FileOperationError(error_msg)

    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_name) or '.', exist_ok=True)

        with open(file_name, 'w', encoding='utf-8', newline='\n') as f:
            f.write(parsed_content)
        logger.info(f"Successfully wrote to {file_name}")
    except Exception as e:
        error_msg = f"Error writing to file {file_name}: {e}"
        logger.error(error_msg)
        raise FileOperationError(error_msg) from e


def output_structured_data(structured_data: Dict[str, Any], output_format: str,
                          output_dir: str, export_date: str,
                          overwrite: bool = False, skip_existing: bool = False) -> bool:
    """
    Output structured data to files in the specified format.

    Args:
        structured_data (dict): Structured data to output
        output_format (str): Format to output ('json' or 'csv')
        output_dir (str): Directory to output files to
        export_date (str): Date of export for filename
        overwrite (bool): Whether to overwrite existing files without asking
        skip_existing (bool): Whether to skip existing files without asking

    Returns:
        bool: True if successful, False otherwise

    Raises:
        FileOperationError: If there is an error writing to the file
    """
    # Create output directory if it doesn't exist
    try:
        os.makedirs(output_dir, exist_ok=True)
    except Exception as e:
        error_msg = f"Error creating output directory {output_dir}: {e}"
        logger.error(error_msg)
        raise FileOperationError(error_msg) from e

    if output_format == 'json':
        # Create a JSON file with all conversations
        json_filename = os.path.join(output_dir, f"[{export_date}]-skype_conversations.json")

        # Check if file exists and handle according to flags
        if os.path.exists(json_filename):
            if skip_existing:
                logger.info(f"Skipping existing file: {json_filename}")
                return True
            elif not overwrite:
                logger.info(f"File {json_filename} already exists and overwrite is not enabled")
                return False

        try:
            with open(json_filename, 'w', encoding='utf-8', newline='\n') as f:
                json.dump(structured_data, f, indent=4, ensure_ascii=False)
            logger.info(f"Successfully wrote structured JSON data to {json_filename}")
            return True
        except Exception as e:
            error_msg = f"Error writing JSON file {json_filename}: {e}"
            logger.error(error_msg)
            raise FileOperationError(error_msg) from e

    elif output_format == 'csv':
        success = True
        # Create a CSV file for each conversation
        for conv_id, conv_data in structured_data.items():
            # Create a safe filename from the conversation ID
            safe_conv_id = safe_filename(conv_id)
            csv_filename = os.path.join(output_dir, f"[{export_date}]-{safe_conv_id}.csv")

            # Check if file exists and handle according to flags
            if os.path.exists(csv_filename):
                if skip_existing:
                    logger.info(f"Skipping existing file: {csv_filename}")
                    continue
                elif not overwrite:
                    logger.info(f"File {csv_filename} already exists and overwrite is not enabled")
                    continue

            try:
                with open(csv_filename, 'w', encoding='utf-8', newline='\n') as f:
                    # Define CSV writer and headers
                    csv_writer = csv.writer(f)

                    # Enhanced headers with structured data fields
                    headers = [
                        'timestamp', 'date', 'time', 'from_id', 'from_name', 'type', 'is_edited', 'content',
                        # Media fields
                        'media_filename', 'media_filesize', 'media_filetype', 'media_url',
                        # Poll fields
                        'poll_question', 'poll_options',
                        # Location fields
                        'location_latitude', 'location_longitude', 'location_address'
                    ]
                    csv_writer.writerow(headers)

                    # Check if messages is a list (expected structure)
                    if 'messages' in conv_data and isinstance(conv_data['messages'], list):
                        # Write each message as a row
                        for msg in conv_data['messages']:
                            # Extract structured data if available
                            structured_data = msg.get('structuredData', {})

                            # Basic message fields
                            row = [
                                msg.get('timestamp', ''),
                                msg.get('date', ''),
                                msg.get('time', ''),
                                msg.get('from_id', ''),
                                msg.get('from_name', ''),
                                msg.get('type', ''),
                                msg.get('is_edited', False),
                                msg.get('content', '')
                            ]

                            # Media fields
                            row.extend([
                                structured_data.get('media_filename', ''),
                                structured_data.get('media_filesize_formatted', structured_data.get('media_filesize', '')),
                                structured_data.get('media_filetype', ''),
                                structured_data.get('media_url', '')
                            ])

                            # Poll fields
                            poll_options = structured_data.get('poll_options', [])
                            row.extend([
                                structured_data.get('poll_question', ''),
                                '; '.join(poll_options) if poll_options else ''
                            ])

                            # Location fields
                            row.extend([
                                structured_data.get('location_latitude', ''),
                                structured_data.get('location_longitude', ''),
                                structured_data.get('location_address', '')
                            ])

                            csv_writer.writerow(row)
                    else:
                        logger.warning(f"No valid messages found for conversation {conv_id}")

                logger.info(f"Successfully wrote structured CSV data to {csv_filename}")
            except Exception as e:
                error_msg = f"Error writing CSV file {csv_filename}: {e}"
                logger.error(error_msg)
                success = False

        if not success:
            raise FileOperationError("One or more CSV files could not be written")

        return success

    return False


def export_conversations_to_text(structured_data: Dict[str, Any], output_dir: str,
                               export_date: str, overwrite: bool = False,
                               skip_existing: bool = False) -> bool:
    """
    Export conversations to text files.

    Args:
        structured_data (dict): Structured data to export
        output_dir (str): Directory to output files to
        export_date (str): Date of export for filename
        overwrite (bool): Whether to overwrite existing files without asking
        skip_existing (bool): Whether to skip existing files without asking

    Returns:
        bool: True if successful, False otherwise

    Raises:
        FileOperationError: If there is an error writing to the file
    """
    # Create output directory if it doesn't exist
    try:
        os.makedirs(output_dir, exist_ok=True)
    except Exception as e:
        error_msg = f"Error creating output directory {output_dir}: {e}"
        logger.error(error_msg)
        raise FileOperationError(error_msg) from e

    # Get current time for export timestamp
    export_time = datetime.datetime.now().strftime("%H:%M:%S")

    # Process each conversation
    for conv_id, conv_data in structured_data.items():
        try:
            # Get display name and messages
            display_name = conv_data.get('display_name', 'Unknown')
            messages = conv_data.get('messages', [])

            if not messages:
                logger.warning(f"No messages found for conversation with {display_name}")
                continue

            # Create a safe filename
            safe_id = safe_filename(conv_id.split(':')[1] if ':' in conv_id else conv_id)
            safe_display_name = safe_filename(display_name)
            output_file = os.path.join(output_dir, f"[{export_date}]-{safe_display_name}({safe_id}).txt")

            # Check if file exists and handle based on overwrite flag
            if os.path.exists(output_file):
                if skip_existing:
                    logger.info(f"Skipping existing file: {output_file}")
                    continue
                elif not overwrite:
                    logger.info(f"File {output_file} already exists and overwrite is not enabled")
                    continue

            # Extract timestamps for banner
            timestamps = [msg.get('timestamp', '') for msg in messages]

            # Create banner
            try:
                banner = banner_constructor(
                    display_name,
                    conv_id,
                    export_date,
                    export_time,
                    timestamps
                )
            except Exception as e:
                logger.warning(f"Error creating banner: {e}")
                banner = f"Conversation with: {display_name} ({conv_id})\n"
                banner += f"Exported on: {export_date}, at: {export_time}\n"
                banner += "***** All times are in UTC *****\n"

            # Compile messages
            compiled_message = banner
            date_set = set()

            # Sort messages by timestamp
            try:
                sorted_messages = sorted(
                    messages,
                    key=lambda m: timestamp_parser(m.get('timestamp', ''))[2] or datetime.datetime.min
                )
            except Exception as e:
                logger.warning(f"Error sorting messages: {e}")
                sorted_messages = messages

            # Add messages to compiled message
            for msg in sorted_messages:
                date_str = msg.get('date', '')
                time_str = msg.get('time', '')
                from_name = msg.get('from_name', '')
                content_raw = msg.get('content_raw', '')
                msg_type = msg.get('type', '')
                structured_data = msg.get('structuredData', {})

                # Add date header if new date
                if date_str and date_str not in date_set:
                    date_set.add(date_str)
                    compiled_message += f"\n---------- Conversations on {date_str} ----------\n"

                # Add message
                compiled_message += f"[{time_str}] {from_name}: {content_raw}\n"

                # Add structured data for special message types
                if structured_data:
                    # Handle media data
                    if any(key.startswith('media_') for key in structured_data.keys()):
                        media_filename = structured_data.get('media_filename', '')
                        media_filesize = structured_data.get('media_filesize_formatted',
                                                           structured_data.get('media_filesize', ''))
                        media_url = structured_data.get('media_url', '')

                        if media_filename or media_url:
                            compiled_message += f"    File: {media_filename}\n"
                        if media_filesize:
                            compiled_message += f"    Size: {media_filesize}\n"
                        if media_url:
                            compiled_message += f"    URL: {media_url}\n"

                    # Handle poll data
                    if 'poll_question' in structured_data:
                        poll_question = structured_data.get('poll_question', '')
                        poll_options = structured_data.get('poll_options', [])

                        if poll_question:
                            compiled_message += f"    Poll Question: {poll_question}\n"
                        if poll_options:
                            compiled_message += "    Poll Options:\n"
                            for option in poll_options:
                                compiled_message += f"      - {option}\n"

                    # Handle location data
                    if any(key.startswith('location_') for key in structured_data.keys()):
                        location_address = structured_data.get('location_address', '')
                        location_latitude = structured_data.get('location_latitude', '')
                        location_longitude = structured_data.get('location_longitude', '')

                        if location_address:
                            compiled_message += f"    Location: {location_address}\n"
                        if location_latitude and location_longitude:
                            compiled_message += f"    Coordinates: {location_latitude}, {location_longitude}\n"

                    # Handle call data
                    if 'call_duration' in structured_data:
                        call_duration = structured_data.get('call_duration', '')
                        call_participants = structured_data.get('call_participants', [])

                        if call_duration:
                            compiled_message += f"    Call Duration: {call_duration}\n"
                        if call_participants:
                            compiled_message += "    Call Participants:\n"
                            for participant in call_participants:
                                name = participant.get('name', '')
                                compiled_message += f"      - {name}\n"

            # Write to file
            try:
                if BEAUTIFULSOUP:
                    # get rid of the weirder skype XML
                    pretty_parsed_content = content_parser(compiled_message)
                    write_to_file(output_file, pretty_parsed_content)
                else:
                    # no bs4, so we use regex
                    compiled_message = tag_stripper(compiled_message)
                    write_to_file(output_file, compiled_message)
            except (ContentParsingError, FileOperationError) as e:
                logger.error(f"Error writing conversation to file: {e}")
                return False

        except Exception as e:
            logger.error(f"Error processing conversation with {display_name}: {e}")
            return False

    return True


def export_conversations(structured_data: Dict[str, Any], output_format: str,
                        output_dir: str, overwrite: bool = False,
                        skip_existing: bool = False, text_output: bool = False) -> bool:
    """
    Export conversations in the specified format.

    Args:
        structured_data (dict): Dictionary containing structured conversation data
        output_format (str): Format to output (text, json, or csv)
        output_dir (str): Directory to save output files
        overwrite (bool): Whether to overwrite existing files
        skip_existing (bool): Whether to skip existing files
        text_output (bool): Whether to generate text output in addition to structured output

    Returns:
        bool: True if successful, False otherwise

    Raises:
        ExportError: If the conversations cannot be exported
    """
    if not structured_data:
        error_msg = "No structured data provided"
        logger.error(error_msg)
        raise ExportError(error_msg)

    if not output_dir:
        error_msg = "No output directory provided"
        logger.error(error_msg)
        raise ExportError(error_msg)

    export_date = structured_data.get('export_date', 'unknown_date')
    success = True

    try:
        # Handle structured output formats
        if output_format in ['json', 'csv']:
            try:
                output_structured_data(
                    structured_data.get('conversations', {}),
                    output_format,
                    output_dir,
                    export_date,
                    overwrite,
                    skip_existing
                )
            except (FileOperationError, ExportError) as e:
                logger.error(f"Error exporting structured data: {e}")
                success = False

            # If text output is not requested, we're done
            if not text_output:
                return success

        # Handle text output
        if output_format == 'text' or text_output:
            try:
                export_conversations_to_text(
                    structured_data,
                    output_dir,
                    export_date,
                    overwrite,
                    skip_existing
                )
            except (FileOperationError, ExportError) as e:
                logger.error(f"Error exporting conversations to text: {e}")
                success = False

        return success
    except Exception as e:
        error_msg = f"Unexpected error during export: {e}"
        logger.error(error_msg)
        raise ExportError(error_msg) from e