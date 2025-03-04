#!/usr/bin/env python3
"""
Skype Export ETL Pipeline

This module implements a complete Extract-Transform-Load (ETL) pipeline for Skype export data.
It preserves both the raw data (as extracted from the tar archive) and the cleaned,
transformed data used by the SkypeArchive UI.

The pipeline consists of three main stages:
1. Extraction: Retrieve and store the raw export data without any transformation
2. Transformation: Clean, normalize, and structure the raw data
3. Loading: Make both raw and transformed data available to the SkypeArchive UI

This design allows for complete auditability (raw data) while providing users with
a clean and efficient interface (transformed data).
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

        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

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
        """
        logger.info("Starting extraction phase")

        try:
            # Determine if we're dealing with a file path or file object
            if file_path:
                if file_path.endswith('.tar'):
                    raw_data = read_tarfile(file_path, auto_select=True)
                    logger.info(f"Extracted data from tar file: {file_path}")
                else:
                    raw_data = read_file(file_path)
                    logger.info(f"Read data from JSON file: {file_path}")
            elif file_obj:
                # Try to determine file type from name if available
                if hasattr(file_obj, 'name') and file_obj.name.endswith('.tar'):
                    raw_data = read_tarfile_object(file_obj, auto_select=True)
                    logger.info("Extracted data from uploaded tar file")
                else:
                    # Assume JSON if not a tar file
                    raw_data = read_file_object(file_obj)
                    logger.info("Read data from uploaded JSON file")
            else:
                raise ValueError("Either file_path or file_obj must be provided")

            # Basic validation of the extracted data
            if not isinstance(raw_data, dict):
                raise ValueError("Extracted data is not a valid JSON object")

            required_fields = ['userId', 'exportDate', 'conversations']
            for field in required_fields:
                if field not in raw_data:
                    raise ValueError(f"Required field '{field}' missing from extracted data")

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
        """
        logger.info("Starting transformation phase")

        try:
            # Extract key metadata
            user_id = raw_data['userId']
            export_date_time = raw_data['exportDate']
            export_date_str, export_time_str, export_datetime = timestamp_parser(export_date_time)
            conversations = raw_data['conversations']

            # If user_display_name is not provided, use a default
            if not user_display_name:
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
                            if i > 0 and msg_content_raw == messages[i-1].get('content', '') and re.search(r'<e_m.*>', msg_content_raw):
                                edited_msg = f"--This user edited the following message at {msg_time_str}, you are viewing the edited version--"
                                msg_data['isEdited'] = True
                                msg_data['editNote'] = edited_msg

                            # Clean content
                            cleaned_content = content_parser(msg_content_raw)
                            msg_data['cleanedContent'] = cleaned_content

                            # Add message to conversation
                            transformed_data['conversations'][conv_id]['messages'].append(msg_data)

                        except Exception as e:
                            logger.warning(f"Error processing message {i} in conversation {conv_id}: {e}")
                            continue

                    # Sort messages by datetime if we have datetime objects
                    if datetime_objects:
                        # Sort indices by datetime
                        sorted_indices = [idx for idx, dt in sorted(datetime_objects, key=lambda x: x[1])]

                        # Reorder messages
                        messages_list = transformed_data['conversations'][conv_id]['messages']
                        sorted_messages = [messages_list[idx] for idx in sorted_indices]
                        transformed_data['conversations'][conv_id]['messages'] = sorted_messages

                    # Add first and last message timestamps
                    messages_list = transformed_data['conversations'][conv_id]['messages']
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
        Load both raw and transformed data into the database.

        Args:
            raw_data (dict): The raw data extracted from the Skype export
            transformed_data (dict): The transformed data
            file_source (str, optional): The source of the file (e.g., file path or upload info)

        Returns:
            int: The export_id of the inserted raw data
        """
        logger.info("Starting loading phase")

        if not self.conn:
            logger.warning("No database connection. Loading phase skipped.")
            return None

        try:
            # Begin transaction
            with self.conn:
                with self.conn.cursor() as cur:
                    # Insert raw data
                    metadata = transformed_data['metadata']
                    user_id = metadata['userId']
                    export_date = metadata['exportDate']

                    # Check if this export already exists
                    cur.execute(
                        "SELECT export_id FROM skype_raw_exports WHERE user_id = %s AND export_date = %s",
                        (user_id, export_date)
                    )
                    existing = cur.fetchone()

                    if existing:
                        export_id = existing[0]
                        logger.info(f"Export already exists with ID {export_id}. Updating...")

                        # Update existing raw data
                        cur.execute(
                            "UPDATE skype_raw_exports SET raw_data = %s, file_source = %s WHERE export_id = %s",
                            (Json(raw_data), file_source, export_id)
                        )
                    else:
                        # Insert new raw data
                        cur.execute(
                            "INSERT INTO skype_raw_exports (user_id, export_date, raw_data, file_source) VALUES (%s, %s, %s, %s) RETURNING export_id",
                            (user_id, export_date, Json(raw_data), file_source)
                        )
                        export_id = cur.fetchone()[0]
                        logger.info(f"Inserted raw data with export_id {export_id}")

                    # Process conversations and messages
                    for conv_id, conversation in transformed_data['conversations'].items():
                        display_name = conversation['displayName']
                        messages = conversation.get('messages', [])

                        # Get first and last message timestamps
                        first_message_time = conversation.get('firstMessageTime')
                        last_message_time = conversation.get('lastMessageTime')

                        # Insert or update conversation
                        cur.execute(
                            """
                            INSERT INTO skype_conversations
                            (conversation_id, display_name, export_id, first_message_time, last_message_time, message_count)
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
                                conv_id,
                                display_name,
                                export_id,
                                first_message_time,
                                last_message_time,
                                len(messages)
                            )
                        )

                        # Delete existing messages for this conversation
                        cur.execute(
                            "DELETE FROM skype_messages WHERE conversation_id = %s",
                            (conv_id,)
                        )

                        # Insert messages
                        for message in messages:
                            cur.execute(
                                """
                                INSERT INTO skype_messages
                                (conversation_id, timestamp, sender_id, sender_name, message_type, raw_content, cleaned_content, is_edited)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                                """,
                                (
                                    conv_id,
                                    message['timestamp'],
                                    message['fromId'],
                                    message['fromName'],
                                    message['type'],
                                    message['rawContent'],
                                    message['cleanedContent'],
                                    message['isEdited']
                                )
                            )

                    logger.info(f"Loaded {len(transformed_data['conversations'])} conversations and their messages")

            return export_id

        except Exception as e:
            logger.error(f"Error during loading phase: {e}")
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
        """
        logger.info("Starting ETL pipeline")

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