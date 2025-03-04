#!/usr/bin/env python3
"""
ETL Pipeline for Skype Export Data

This module implements an Extract-Transform-Load (ETL) pipeline for processing
Skype export data. It provides a unified interface for extracting data from
Skype export files, transforming it into a structured format, and loading it
into a PostgreSQL database.
"""

import os
import json
import logging
import datetime
import psycopg2
from typing import Dict, List, Optional, Tuple, Any, BinaryIO

from ..utils.file_handler import (
    read_file_object,
    read_tarfile,
    read_tarfile_object
)
from ..parser.parser_module import (
    timestamp_parser
)
from ..utils.file_utils import safe_filename
from ..utils.validation import (
    ValidationError,
    validate_file_exists,
    validate_directory,
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
            db_config (dict, optional): Database configuration
            output_dir (str, optional): Directory to output files to
        """
        # Load configuration if not provided
        if db_config is None:
            from ..utils.config import load_config, get_db_config
            config = load_config()
            db_config = get_db_config(config)

        # Store configuration
        self.db_config = db_config
        self.output_dir = output_dir

        # Load message types configuration
        from ..utils.config import load_config
        self.config = load_config(message_types_file='config/message_types.json')

        # Initialize database connection
        self.conn = None
        self.cursor = None

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
            # Validate and prepare data
            self._validate_raw_data(raw_data)

            # Extract and process metadata
            transformed_data = self._process_metadata(raw_data, user_display_name)

            # Process conversations
            self._process_conversations(raw_data, transformed_data)

            # Store transformed data if output directory is specified
            self._save_transformed_data(transformed_data)

            return transformed_data

        except Exception as e:
            logger.error(f"Error during transformation phase: {e}")
            raise

    def _validate_raw_data(self, raw_data: Dict[str, Any]) -> None:
        """
        Validate the raw data structure.

        Args:
            raw_data (dict): The raw data to validate

        Raises:
            ValidationError: If the input data is invalid
        """
        try:
            validate_skype_data(raw_data)
        except ValidationError as e:
            logger.error(f"Raw data validation error: {e}")
            raise

    def _process_metadata(self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract and process metadata from raw data.

        Args:
            raw_data (dict): The raw data extracted from the Skype export
            user_display_name (str, optional): The display name to use for the user

        Returns:
            dict: The initial transformed data structure with metadata
        """
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

        return transformed_data

    def _process_conversations(self, raw_data: Dict[str, Any], transformed_data: Dict[str, Any]) -> None:
        """
        Process all conversations from the raw data.

        Args:
            raw_data (dict): The raw data extracted from the Skype export
            transformed_data (dict): The transformed data structure to populate
        """
        # Map user ID to display name
        user_id = raw_data['userId']
        user_display_name = transformed_data['metadata']['userDisplayName']
        id_to_display_name = {user_id: str(user_display_name)}

        # Process each conversation
        for conversation in raw_data['conversations']:
            try:
                self._process_single_conversation(conversation, transformed_data, id_to_display_name)
            except Exception as e:
                conv_id = conversation.get('id', 'unknown')
                logger.warning(f"Error processing conversation {conv_id}: {e}")
                continue

    def _process_single_conversation(self, conversation: Dict[str, Any],
                                    transformed_data: Dict[str, Any],
                                    id_to_display_name: Dict[str, str]) -> None:
        """
        Process a single conversation.

        Args:
            conversation (dict): The conversation data
            transformed_data (dict): The transformed data structure to populate
            id_to_display_name (dict): Mapping of user IDs to display names
        """
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

        # Process and sort messages
        self._process_messages(conv_id, messages, transformed_data, id_to_display_name)

    def _process_messages(self, conv_id: str, messages: List[Dict[str, Any]],
                         transformed_data: Dict[str, Any],
                         id_to_display_name: Dict[str, str]) -> None:
        """
        Process all messages in a conversation.

        Args:
            conv_id (str): The conversation ID
            messages (list): List of message data
            transformed_data (dict): The transformed data structure to populate
            id_to_display_name (dict): Mapping of user IDs to display names
        """
        # Track datetime objects for sorting
        datetime_objects = []

        # Process each message
        for i, message in enumerate(messages):
            try:
                self._process_single_message(i, message, conv_id, transformed_data,
                                           id_to_display_name, datetime_objects)
            except Exception as e:
                logger.warning(f"Error processing message in conversation {conv_id}: {e}")
                continue

        # Sort messages by timestamp if datetime objects are available
        self._sort_messages(conv_id, transformed_data, datetime_objects)

        # Store first and last message timestamps
        self._store_conversation_timespan(conv_id, transformed_data)

    def _process_single_message(self, index: int, message: Dict[str, Any],
                               conv_id: str, transformed_data: Dict[str, Any],
                               id_to_display_name: Dict[str, str],
                               datetime_objects: List[Tuple[int, datetime]]) -> None:
        """
        Process a single message.

        Args:
            index (int): The index of the message in the original list
            message (dict): The message data
            conv_id (str): The conversation ID
            transformed_data (dict): The transformed data structure to populate
            id_to_display_name (dict): Mapping of user IDs to display names
            datetime_objects (list): List to store datetime objects for sorting
        """
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
            datetime_objects.append((index, msg_datetime))

        # Handle special message types
        if msg_type != 'RichText':
            msg_content_raw = self._type_parser(msg_type)
            msg_data['rawContent'] = msg_content_raw

        # Check for edited messages
        if 'skypeeditedid' in message:
            msg_data['isEdited'] = True

        # Add message to conversation
        transformed_data['conversations'][conv_id]['messages'].append(msg_data)

    def _sort_messages(self, conv_id: str, transformed_data: Dict[str, Any],
                      datetime_objects: List[Tuple[int, datetime]]) -> None:
        """
        Sort messages by timestamp.

        Args:
            conv_id (str): The conversation ID
            transformed_data (dict): The transformed data structure
            datetime_objects (list): List of datetime objects for sorting
        """
        messages_list = transformed_data['conversations'][conv_id]['messages']
        if datetime_objects:
            # Sort by datetime
            datetime_objects.sort(key=lambda x: x[1])
            sorted_indices = [x[0] for x in datetime_objects]
            messages_list = [messages_list[i] for i in sorted_indices]
            transformed_data['conversations'][conv_id]['messages'] = messages_list

    def _store_conversation_timespan(self, conv_id: str, transformed_data: Dict[str, Any]) -> None:
        """
        Store the first and last message timestamps for a conversation.

        Args:
            conv_id (str): The conversation ID
            transformed_data (dict): The transformed data structure
        """
        messages_list = transformed_data['conversations'][conv_id]['messages']
        if messages_list:
            first_msg = messages_list[0]
            last_msg = messages_list[-1]

            transformed_data['conversations'][conv_id]['firstMessageTime'] = first_msg['timestamp']
            transformed_data['conversations'][conv_id]['lastMessageTime'] = last_msg['timestamp']

    def _save_transformed_data(self, transformed_data: Dict[str, Any]) -> None:
        """
        Save the transformed data to a file if output directory is specified.

        Args:
            transformed_data (dict): The transformed data to save
        """
        if self.output_dir:
            transformed_output_path = os.path.join(self.output_dir, 'transformed_data.json')
            with open(transformed_output_path, 'w', encoding='utf-8') as f:
                json.dump(transformed_data, f, indent=2)
            logger.info(f"Transformed data saved to {transformed_output_path}")

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
            # Insert raw export data and get export ID
            export_id = self._insert_raw_export(raw_data, transformed_data, file_source)

            # Insert conversations and messages
            self._insert_conversations_and_messages(transformed_data, export_id)

            return export_id

        except Exception as e:
            logger.error(f"Error during loading phase: {e}")
            if self.conn:
                self.conn.rollback()
            raise

    def _insert_raw_export(self, raw_data: Dict[str, Any], transformed_data: Dict[str, Any],
                          file_source: Optional[str] = None) -> int:
        """
        Insert raw export data into the database.

        Args:
            raw_data (dict): The raw data extracted from the Skype export
            transformed_data (dict): The transformed data
            file_source (str, optional): The source of the data (e.g., file path)

        Returns:
            int: The export ID in the database
        """
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

        return export_id

    def _insert_conversations_and_messages(self, transformed_data: Dict[str, Any], export_id: int) -> None:
        """
        Insert conversations and their messages into the database.

        Args:
            transformed_data (dict): The transformed data
            export_id (int): The export ID in the database
        """
        with self.conn.cursor() as cur:
            # Insert conversations
            for conv_id, conv_data in transformed_data['conversations'].items():
                self._insert_conversation(cur, conv_id, conv_data, export_id)

                # Insert messages for this conversation
                self._insert_messages(cur, conv_id, conv_data['messages'])

            self.conn.commit()
            logger.info(f"Inserted {len(transformed_data['conversations'])} conversations and their messages")

    def _insert_conversation(self, cursor, conv_id: str, conv_data: Dict[str, Any], export_id: int) -> None:
        """
        Insert a single conversation into the database.

        Args:
            cursor: Database cursor
            conv_id (str): The conversation ID
            conv_data (dict): The conversation data
            export_id (int): The export ID in the database
        """
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
        cursor.execute(
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

    def _insert_messages(self, cursor, conv_id: str, messages: List[Dict[str, Any]]) -> None:
        """
        Insert messages for a conversation into the database.

        Args:
            cursor: Database cursor
            conv_id (str): The conversation ID
            messages (list): List of message data
        """
        for msg in messages:
            # Parse timestamp
            _, _, msg_time = timestamp_parser(msg['timestamp'])

            # Insert message
            cursor.execute(
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

        # Validate input parameters
        self._validate_pipeline_input(file_path, file_obj)

        # Initialize results dictionary
        results = self._initialize_results()

        try:
            # Setup database connection if needed
            self._setup_database_connection()

            # Run extraction phase
            raw_data, results = self._run_extraction_phase(file_path, file_obj, results)

            # Run transformation phase
            transformed_data, results = self._run_transformation_phase(raw_data, user_display_name, results)

            # Run loading phase if database connection is available
            results = self._run_loading_phase(raw_data, transformed_data, file_path, results)

            logger.info("ETL pipeline completed successfully")
            return results

        except Exception as e:
            logger.error(f"Error in ETL pipeline: {e}")
            raise

        finally:
            # Close database connection
            self.close_db()

    def _validate_pipeline_input(self, file_path: str = None, file_obj: BinaryIO = None) -> None:
        """
        Validate that either file_path or file_obj is provided.

        Args:
            file_path (str, optional): Path to the Skype export file
            file_obj (BinaryIO, optional): File-like object containing the Skype export

        Raises:
            ValueError: If neither file_path nor file_obj is provided
        """
        if not file_path and not file_obj:
            error_msg = "Either file_path or file_obj must be provided"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def _initialize_results(self) -> Dict[str, Any]:
        """
        Initialize the results dictionary for the pipeline.

        Returns:
            dict: The initialized results dictionary
        """
        return {
            'extraction': None,
            'transformation': None,
            'loading': None
        }

    def _setup_database_connection(self) -> None:
        """
        Set up the database connection if configuration is provided.
        """
        if self.db_config:
            self.connect_db()

    def _run_extraction_phase(self, file_path: str = None, file_obj: BinaryIO = None,
                             results: Dict[str, Any] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Run the extraction phase of the pipeline.

        Args:
            file_path (str, optional): Path to the Skype export file
            file_obj (BinaryIO, optional): File-like object containing the Skype export
            results (dict, optional): The results dictionary to update

        Returns:
            tuple: A tuple containing the raw data and updated results dictionary
        """
        # Extraction phase
        raw_data = self.extract(file_path, file_obj)

        # Update results
        if results is not None:
            results['extraction'] = {
                'success': True,
                'userId': raw_data['userId'],
                'exportDate': raw_data['exportDate'],
                'conversationCount': len(raw_data['conversations'])
            }

        return raw_data, results

    def _run_transformation_phase(self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None,
                                 results: Dict[str, Any] = None) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Run the transformation phase of the pipeline.

        Args:
            raw_data (dict): The raw data extracted from the Skype export
            user_display_name (str, optional): The display name to use for the user
            results (dict, optional): The results dictionary to update

        Returns:
            tuple: A tuple containing the transformed data and updated results dictionary
        """
        # Transformation phase
        transformed_data = self.transform(raw_data, user_display_name)

        # Update results
        if results is not None:
            results['transformation'] = {
                'success': True,
                'conversationCount': len(transformed_data['conversations'])
            }

        return transformed_data, results

    def _run_loading_phase(self, raw_data: Dict[str, Any], transformed_data: Dict[str, Any],
                          file_path: Optional[str] = None, results: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Run the loading phase of the pipeline if database connection is available.

        Args:
            raw_data (dict): The raw data extracted from the Skype export
            transformed_data (dict): The transformed data
            file_path (str, optional): Path to the Skype export file
            results (dict, optional): The results dictionary to update

        Returns:
            dict: The updated results dictionary
        """
        # Loading phase
        if self.conn:
            export_id = self.load(raw_data, transformed_data, file_path)

            # Update results
            if results is not None:
                results['loading'] = {
                    'success': True,
                    'exportId': export_id
                }

        return results

    def _type_parser(self, msg_type: str) -> str:
        """
        Map message types to their human-readable descriptions.

        Args:
            msg_type (str): Skype message type

        Returns:
            str: Human-readable description
        """
        # Use the configuration utility to get the message type description
        from ..utils.config import get_message_type_description
        return get_message_type_description(self.config, msg_type)


if __name__ == "__main__":
    # This module is not meant to be run directly
    # It should be imported and used as a library
    print("This module is not meant to be run directly.")
    print("Please import it and use the SkypeETLPipeline class instead.")
    print("See the README.md file for usage examples.")