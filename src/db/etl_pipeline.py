#!/usr/bin/env python3
"""
ETL Pipeline for Skype Export Data

This module implements an Extract-Transform-Load (ETL) pipeline for processing
Skype export data. It provides a unified interface for extracting data from
Skype export files, transforming it into a structured format, and loading it
into a PostgreSQL database.
"""

import os
import sys
import json
import logging
import datetime
import re
import psycopg2
from psycopg2.extras import Json, DictCursor
from typing import Dict, List, Union, Optional, Tuple, Any, BinaryIO, Set

from ..utils.file_handler import (
    read_file,
    read_file_object,
    read_tarfile,
    read_tarfile_object,
    extract_tar_contents,
    extract_tar_object
)
from ..parser.parser_module import (
    timestamp_parser,
    content_parser,
    tag_stripper,
    pretty_quotes
)
from ..utils.file_utils import safe_filename
from ..utils.validation import (
    ValidationError,
    validate_file_exists,
    validate_directory,
    validate_file_type,
    validate_json_file,
    validate_tar_file,
    validate_file_object,
    validate_skype_data,
    validate_user_display_name,
    validate_db_config
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Database schema definitions
RAW_EXPORTS_TABLE = """
CREATE TABLE IF NOT EXISTS skype_raw_exports (
    export_id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    export_date TIMESTAMP NOT NULL,
    raw_data JSONB NOT NULL,
    file_source TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CONVERSATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS skype_conversations (
    conversation_id TEXT PRIMARY KEY,
    display_name TEXT,
    export_id INTEGER REFERENCES skype_raw_exports(export_id),
    first_message_time TIMESTAMP,
    last_message_time TIMESTAMP,
    message_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

MESSAGES_TABLE = """
CREATE TABLE IF NOT EXISTS skype_messages (
    message_id SERIAL PRIMARY KEY,
    conversation_id TEXT REFERENCES skype_conversations(conversation_id),
    timestamp TIMESTAMP NOT NULL,
    sender_id TEXT NOT NULL,
    sender_name TEXT,
    message_type TEXT,
    raw_content TEXT,
    cleaned_content TEXT,
    is_edited BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

def timestamp_parser(timestamp: str) -> Tuple[str, str, Optional[datetime.datetime]]:
    """
    Parse a timestamp string into date, time, and datetime components.

    Args:
        timestamp (str): Timestamp string in ISO format

    Returns:
        tuple: (date_str, time_str, datetime_obj)
    """
    try:
        # Replace 'Z' with '+00:00' for ISO format compatibility
        iso_timestamp = timestamp.replace('Z', '+00:00')
        dt = datetime.datetime.fromisoformat(iso_timestamp)

        # Format date and time strings
        date_str = dt.strftime('%Y-%m-%d')
        time_str = dt.strftime('%H:%M:%S')

        return date_str, time_str, dt
    except (ValueError, AttributeError) as e:
        logger.warning(f"Error parsing timestamp {timestamp}: {e}")
        return timestamp, "", None

class SkypeETLPipeline:
    """
    Implements the ETL pipeline for Skype export data.
    """

    def __init__(self, db_config: Dict[str, Any] = None, output_dir: Optional[str] = None):
        """
        Initialize the ETL pipeline.

        Args:
            db_config (dict, optional): Database connection configuration
            output_dir (str, optional): Directory to store extracted files and output
        """
        self.db_config = db_config
        self.output_dir = output_dir
        self.conn = None

        # Validate database configuration if provided
        if db_config:
            try:
                validate_db_config(db_config)
            except ValidationError as e:
                logger.warning(f"Invalid database configuration: {e}")
                self.db_config = None

        # Validate and create output directory if provided
        if output_dir:
            try:
                validate_directory(output_dir, create_if_missing=True)
            except ValidationError as e:
                logger.warning(f"Invalid output directory: {e}")
                self.output_dir = None

    def connect_db(self) -> None:
        """
        Connect to the database using the provided configuration.
        """
        if not self.db_config:
            logger.warning("No database configuration provided. Database operations will be skipped.")
            return

        try:
            self.conn = psycopg2.connect(**self.db_config)
            logger.info("Connected to database successfully")

            # Create tables if they don't exist
            with self.conn.cursor() as cur:
                cur.execute(RAW_EXPORTS_TABLE)
                cur.execute(CONVERSATIONS_TABLE)
                cur.execute(MESSAGES_TABLE)
                self.conn.commit()
                logger.info("Database tables created or verified")
        except Exception as e:
            logger.error(f"Error connecting to database: {e}")
            self.conn = None

    def close_db(self) -> None:
        """
        Close the database connection.
        """
        if self.conn:
            self.conn.close()
            logger.info("Database connection closed")

    def extract(self, file_path: str = None, file_obj: BinaryIO = None) -> Dict[str, Any]:
        """
        Extract raw data from a Skype export file (tar archive or JSON).

        Args:
            file_path (str, optional): Path to the Skype export file
            file_obj (BinaryIO, optional): File-like object containing the Skype export

        Returns:
            dict: The raw data extracted from the file

        Raises:
            ValidationError: If the input is invalid
            ValueError: If neither file_path nor file_obj is provided
        """
        logger.info("Starting extraction phase")

        if not file_path and not file_obj:
            error_msg = "Either file_path or file_obj must be provided"
            logger.error(error_msg)
            raise ValueError(error_msg)

        try:
            # Determine if we're dealing with a file path or file object
            if file_path:
                # Validate file exists and is readable
                try:
                    validate_file_exists(file_path)
                except ValidationError as e:
                    logger.error(f"File validation error: {e}")
                    raise

                # Process based on file type
                if file_path.endswith('.tar'):
                    try:
                        validate_tar_file(file_path)
                        raw_data = read_tarfile(file_path, auto_select=True)
                        logger.info(f"Extracted data from tar file: {file_path}")
                    except ValidationError as e:
                        logger.error(f"TAR file validation error: {e}")
                        raise
                else:
                    try:
                        raw_data = validate_json_file(file_path)
                        logger.info(f"Read data from JSON file: {file_path}")
                    except ValidationError as e:
                        logger.error(f"JSON file validation error: {e}")
                        raise
            elif file_obj:
                # Validate file object
                try:
                    validate_file_object(file_obj, allowed_extensions=['.json', '.tar'])
                except ValidationError as e:
                    logger.error(f"File object validation error: {e}")
                    raise

                # Try to determine file type from name if available
                if hasattr(file_obj, 'name') and file_obj.name.endswith('.tar'):
                    raw_data = read_tarfile_object(file_obj, auto_select=True)
                    logger.info("Extracted data from uploaded tar file")
                else:
                    # Assume JSON if not a tar file
                    raw_data = read_file_object(file_obj)
                    logger.info("Read data from uploaded JSON file")

            # Validate the extracted data structure
            try:
                validate_skype_data(raw_data)
            except ValidationError as e:
                logger.error(f"Skype data validation error: {e}")
                raise

            # Store raw data if output directory is specified
            if self.output_dir and file_path:
                raw_output_path = os.path.join(self.output_dir, 'raw_data.json')
                with open(raw_output_path, 'w', encoding='utf-8') as f:
                    json.dump(raw_data, f, indent=2)
                logger.info(f"Raw data saved to {raw_output_path}")

            return raw_data

        except Exception as e:
            logger.error(f"Error during extraction phase: {e}")
            raise

    def transform(self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Transform the raw data into a structured format.

        Args:
            raw_data (dict): The raw data extracted from the Skype export
            user_display_name (str, optional): The display name to use for the user

        Returns:
            dict: The transformed data

        Raises:
            ValidationError: If the input data is invalid
        """
        logger.info("Starting transformation phase")

        try:
            # Validate raw data
            try:
                validate_skype_data(raw_data)
            except ValidationError as e:
                logger.error(f"Raw data validation error: {e}")
                raise

            # Extract key metadata
            user_id = raw_data['userId']
            export_date_time = raw_data['exportDate']
            export_date_str, export_time_str, export_datetime = timestamp_parser(export_date_time)
            conversations = raw_data['conversations']

            # Validate and sanitize user display name
            if user_display_name:
                try:
                    user_display_name = validate_user_display_name(user_display_name)
                except ValidationError as e:
                    logger.warning(f"User display name validation error: {e}. Using user ID instead.")
                    user_display_name = user_id
            else:
                user_display_name = user_id

            # Initialize the transformed data structure
            transformed_data = {
                'metadata': {
                    'userId': user_id,
                    'userDisplayName': user_display_name,
                    'exportDate': export_date_time,
                    'exportDateFormatted': f"{export_date_str} {export_time_str}",
                    'conversationCount': len(conversations)
                },
                'conversations': {}
            }

            # Map user ID to display name
            id_to_display_name = {user_id: str(user_display_name)}

            # Process each conversation
            for conversation in conversations:
                try:
                    # Extract conversation metadata
                    conv_id = conversation['id']
                    display_name = conversation.get('displayName')

                    # Sanitize display name
                    if display_name is None:
                        safe_id = safe_filename(conv_id.split(':')[1])
                        display_name = safe_id
                    else:
                        safe_display_name = safe_filename(display_name)
                        display_name = safe_display_name

                    # Map conversation ID to display name
                    id_to_display_name[conv_id] = display_name

                    # Initialize conversation data structure
                    transformed_data['conversations'][conv_id] = {
                        'id': conv_id,
                        'displayName': display_name,
                        'messages': []
                    }

                    # Process messages
                    messages = conversation.get('MessageList', [])
                    message_count = len(messages)

                    # Store message count
                    transformed_data['conversations'][conv_id]['messageCount'] = message_count

                    # Track datetime objects for sorting
                    datetime_objects = []

                    # Process each message
                    for i, message in enumerate(messages):
                        try:
                            # Extract message metadata
                            msg_timestamp = message.get('originalarrivaltime')
                            msg_from = message.get('from')
                            msg_content_raw = message.get('content', '')
                            msg_type = message.get('messagetype', 'Unknown')

                            # Parse timestamp
                            msg_date_str, msg_time_str, msg_datetime = timestamp_parser(msg_timestamp)

                            # Initialize message data structure
                            msg_data = {
                                'timestamp': msg_timestamp,
                                'timestampFormatted': f"{msg_date_str} {msg_time_str}",
                                'date': msg_date_str,
                                'time': msg_time_str,
                                'fromId': msg_from,
                                'fromName': id_to_display_name.get(msg_from, msg_from),
                                'type': msg_type,
                                'rawContent': msg_content_raw,
                                'isEdited': False
                            }

                            # Store datetime object for sorting
                            if msg_datetime:
                                datetime_objects.append((i, msg_datetime))

                            # Handle special message types
                            if msg_type != 'RichText':
                                msg_content_raw = self._type_parser(msg_type)
                                msg_data['rawContent'] = msg_content_raw

                            # Check for edited messages
                            if 'skypeeditedid' in message:
                                msg_data['isEdited'] = True

                            # Add message to conversation
                            transformed_data['conversations'][conv_id]['messages'].append(msg_data)

                        except Exception as e:
                            logger.warning(f"Error processing message in conversation {conv_id}: {e}")
                            continue

                    # Sort messages by timestamp if datetime objects are available
                    messages_list = transformed_data['conversations'][conv_id]['messages']
                    if datetime_objects:
                        # Sort by datetime
                        datetime_objects.sort(key=lambda x: x[1])
                        sorted_indices = [x[0] for x in datetime_objects]
                        messages_list = [messages_list[i] for i in sorted_indices]
                        transformed_data['conversations'][conv_id]['messages'] = messages_list

                    # Store first and last message timestamps
                    if messages_list:
                        first_msg = messages_list[0]
                        last_msg = messages_list[-1]

                        transformed_data['conversations'][conv_id]['firstMessageTime'] = first_msg['timestamp']
                        transformed_data['conversations'][conv_id]['lastMessageTime'] = last_msg['timestamp']

                except Exception as e:
                    logger.warning(f"Error processing conversation {conv_id}: {e}")
                    continue

            # Store transformed data if output directory is specified
            if self.output_dir:
                transformed_output_path = os.path.join(self.output_dir, 'transformed_data.json')
                with open(transformed_output_path, 'w', encoding='utf-8') as f:
                    json.dump(transformed_data, f, indent=2)
                logger.info(f"Transformed data saved to {transformed_output_path}")

            return transformed_data

        except Exception as e:
            logger.error(f"Error during transformation phase: {e}")
            raise

    def load(self, raw_data: Dict[str, Any], transformed_data: Dict[str, Any], file_source: Optional[str] = None) -> int:
        """
        Load the data into the database.

        Args:
            raw_data (dict): The raw data extracted from the Skype export
            transformed_data (dict): The transformed data
            file_source (str, optional): The source of the data (e.g., file path)

        Returns:
            int: The export ID in the database

        Raises:
            ValueError: If the database connection is not available
        """
        logger.info("Starting loading phase")

        if not self.conn:
            error_msg = "Database connection not available"
            logger.error(error_msg)
            raise ValueError(error_msg)

        try:
            # Extract metadata
            metadata = transformed_data['metadata']
            user_id = metadata['userId']
            export_date = metadata['exportDate']

            # Parse export date
            _, _, export_datetime = timestamp_parser(export_date)

            # Insert raw export data
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO skype_raw_exports (user_id, export_date, raw_data, file_source)
                    VALUES (%s, %s, %s, %s)
                    RETURNING export_id
                    """,
                    (user_id, export_datetime, json.dumps(raw_data), file_source)
                )
                export_id = cur.fetchone()[0]
                self.conn.commit()
                logger.info(f"Inserted raw export data with ID {export_id}")

                # Insert conversations
                for conv_id, conv_data in transformed_data['conversations'].items():
                    display_name = conv_data['displayName']
                    message_count = conv_data['messageCount']

                    # Parse timestamps
                    first_msg_time = None
                    last_msg_time = None

                    if 'firstMessageTime' in conv_data:
                        _, _, first_msg_time = timestamp_parser(conv_data['firstMessageTime'])

                    if 'lastMessageTime' in conv_data:
                        _, _, last_msg_time = timestamp_parser(conv_data['lastMessageTime'])

                    # Insert conversation
                    cur.execute(
                        """
                        INSERT INTO skype_conversations (
                            conversation_id, display_name, export_id,
                            first_message_time, last_message_time, message_count
                        )
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (conversation_id)
                        DO UPDATE SET
                            display_name = EXCLUDED.display_name,
                            export_id = EXCLUDED.export_id,
                            first_message_time = EXCLUDED.first_message_time,
                            last_message_time = EXCLUDED.last_message_time,
                            message_count = EXCLUDED.message_count,
                            updated_at = CURRENT_TIMESTAMP
                        """,
                        (
                            conv_id, display_name, export_id,
                            first_msg_time, last_msg_time, message_count
                        )
                    )

                    # Insert messages
                    for msg in conv_data['messages']:
                        # Parse timestamp
                        _, _, msg_time = timestamp_parser(msg['timestamp'])

                        # Insert message
                        cur.execute(
                            """
                            INSERT INTO skype_messages
                            (conversation_id, timestamp, sender_id, sender_name, message_type, raw_content, is_edited)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                conv_id, msg_time, msg['fromId'], msg['fromName'],
                                msg['type'], msg['rawContent'], msg['isEdited']
                            )
                        )

                self.conn.commit()
                logger.info(f"Inserted {len(transformed_data['conversations'])} conversations and their messages")

            return export_id

        except Exception as e:
            logger.error(f"Error during loading phase: {e}")
            if self.conn:
                self.conn.rollback()
            raise

    def run_pipeline(self, file_path: str = None, file_obj: BinaryIO = None,
                    user_display_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Run the complete ETL pipeline.

        Args:
            file_path (str, optional): Path to the Skype export file
            file_obj (BinaryIO, optional): File-like object containing the Skype export
            user_display_name (str, optional): The display name to use for the user

        Returns:
            dict: A dictionary containing the results of each phase

        Raises:
            ValueError: If neither file_path nor file_obj is provided
        """
        logger.info("Starting ETL pipeline")

        if not file_path and not file_obj:
            error_msg = "Either file_path or file_obj must be provided"
            logger.error(error_msg)
            raise ValueError(error_msg)

        results = {
            'extraction': None,
            'transformation': None,
            'loading': None
        }

        try:
            # Connect to database if config is provided
            if self.db_config:
                self.connect_db()

            # Extraction phase
            raw_data = self.extract(file_path, file_obj)
            results['extraction'] = {
                'success': True,
                'userId': raw_data['userId'],
                'exportDate': raw_data['exportDate'],
                'conversationCount': len(raw_data['conversations'])
            }

            # Transformation phase
            transformed_data = self.transform(raw_data, user_display_name)
            results['transformation'] = {
                'success': True,
                'conversationCount': len(transformed_data['conversations'])
            }

            # Loading phase
            if self.conn:
                export_id = self.load(raw_data, transformed_data, file_path)
                results['loading'] = {
                    'success': True,
                    'exportId': export_id
                }

            logger.info("ETL pipeline completed successfully")
            return results

        except Exception as e:
            logger.error(f"Error in ETL pipeline: {e}")
            raise

        finally:
            # Close database connection
            self.close_db()

    def _type_parser(self, msg_type: str) -> str:
        """
        Map message types to their human-readable descriptions.

        Args:
            msg_type (str): Skype message type

        Returns:
            str: Human-readable description
        """
        # Map message types to their true meaning
        valid_msg_types = {
            'Event/Call': '***A call started/ended***',
            'Poll': '***Created a poll***',
            'RichText/Media_Album': '***Sent an album of images***',
            'RichText/Media_AudioMsg': '***Sent a voice message***',
            'RichText/Media_CallRecording': '***Sent a call recording***',
            'RichText/Media_Card': '***Sent a media card***',
            'RichText/Media_FlikMsg': '***Sent a moji***',
            'RichText/Media_GenericFile': '***Sent a file***',
            'RichText/Media_Video': '***Sent a video message***',
            'RichText/UriObject': '***Sent a photo***',
            'RichText/ScheduledCallInvite': '***Scheduled a call***',
            'RichText/Location': '***Sent a location***',
            'RichText/Contacts': '***Sent a contact***',
        }

        return valid_msg_types.get(msg_type, f'***Sent a {msg_type}***')


if __name__ == "__main__":
    # This module is not meant to be run directly
    # It should be imported and used as a library
    print("This module is not meant to be run directly.")
    print("Please import it and use the SkypeETLPipeline class instead.")
    print("See the README.md file for usage examples.")