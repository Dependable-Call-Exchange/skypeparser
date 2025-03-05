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
import multiprocessing
import gc
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Any, BinaryIO, Callable

from ..utils.file_handler import (
    read_file_object,
    read_tarfile,
    read_tarfile_object
)
from ..parser.parser_module import (
    timestamp_parser
)
from ..parser.core_parser import (
    parse_skype_data
)
from ..utils.message_type_handlers import (
    extract_structured_data,
    get_handler_for_message_type
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
from .raw_storage.storage import SkypeDataStorage
from .clean_storage.storage import SkypeCleanDataStorage
from .progress_tracker import create_tracker, get_tracker, remove_tracker

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
    structured_data JSONB,
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

class ProgressTracker:
    """
    Tracks progress of ETL operations and provides reporting.
    """

    def __init__(self):
        """Initialize the progress tracker."""
        self.total_conversations = 0
        self.processed_conversations = 0
        self.total_messages = 0
        self.processed_messages = 0
        self.start_time = None
        self.phase = None

    def start_phase(self, phase: str, total_conversations: int = 0, total_messages: int = 0) -> None:
        """
        Start tracking a new phase.

        Args:
            phase (str): Name of the phase (extraction, transformation, loading)
            total_conversations (int): Total number of conversations to process
            total_messages (int): Total number of messages to process
        """
        self.phase = phase
        self.total_conversations = total_conversations
        self.total_messages = total_messages
        self.processed_conversations = 0
        self.processed_messages = 0
        self.start_time = datetime.datetime.now()
        logger.info(f"Starting {phase} phase with {total_conversations} conversations and {total_messages} messages")

    def update_conversation_progress(self, increment: int = 1) -> None:
        """
        Update the conversation progress.

        Args:
            increment (int): Number of conversations processed
        """
        self.processed_conversations += increment
        self._log_progress()

    def update_message_progress(self, increment: int = 1) -> None:
        """
        Update the message progress.

        Args:
            increment (int): Number of messages processed
        """
        self.processed_messages += increment
        # Only log every 1000 messages to avoid log spam
        if self.processed_messages % 1000 == 0:
            self._log_progress()

    def _log_progress(self) -> None:
        """Log the current progress."""
        if self.total_conversations > 0:
            conv_progress = (self.processed_conversations / self.total_conversations) * 100
            logger.info(f"{self.phase} progress: {self.processed_conversations}/{self.total_conversations} conversations ({conv_progress:.1f}%)")

        if self.total_messages > 0:
            msg_progress = (self.processed_messages / self.total_messages) * 100
            logger.info(f"{self.phase} progress: {self.processed_messages}/{self.total_messages} messages ({msg_progress:.1f}%)")

    def finish_phase(self) -> Dict[str, Any]:
        """
        Finish the current phase and return statistics.

        Returns:
            dict: Statistics about the completed phase
        """
        end_time = datetime.datetime.now()
        duration = (end_time - self.start_time).total_seconds()

        stats = {
            'phase': self.phase,
            'total_conversations': self.total_conversations,
            'processed_conversations': self.processed_conversations,
            'total_messages': self.total_messages,
            'processed_messages': self.processed_messages,
            'duration_seconds': duration,
            'messages_per_second': self.processed_messages / duration if duration > 0 else 0
        }

        logger.info(f"Completed {self.phase} phase in {duration:.2f} seconds")
        logger.info(f"Processed {self.processed_conversations}/{self.total_conversations} conversations and {self.processed_messages}/{self.total_messages} messages")
        logger.info(f"Processing rate: {stats['messages_per_second']:.2f} messages per second")

        return stats

class MemoryMonitor:
    """
    Monitors memory usage and triggers garbage collection when needed.
    """

    def __init__(self, memory_limit_mb: int = 1024):
        """
        Initialize the memory monitor.

        Args:
            memory_limit_mb (int): Memory limit in MB before forcing garbage collection
        """
        self.memory_limit_mb = memory_limit_mb
        self.last_gc_time = time.time()
        self.gc_interval = 60  # Minimum time between forced GC in seconds

    def check_memory(self) -> None:
        """
        Check current memory usage and trigger garbage collection if needed.
        """
        # Only check memory usage if psutil is available
        try:
            import psutil
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)

            # Log memory usage every 5 minutes or when approaching limit
            current_time = time.time()
            if current_time - self.last_gc_time > 300 or memory_mb > self.memory_limit_mb * 0.8:
                logger.info(f"Current memory usage: {memory_mb:.2f} MB (limit: {self.memory_limit_mb} MB)")

            # Force garbage collection if memory usage exceeds limit and enough time has passed
            if memory_mb > self.memory_limit_mb and current_time - self.last_gc_time > self.gc_interval:
                logger.warning(f"Memory usage ({memory_mb:.2f} MB) exceeds limit ({self.memory_limit_mb} MB). Forcing garbage collection.")
                self._force_garbage_collection()
                self.last_gc_time = current_time

                # Log memory usage after garbage collection
                memory_info = process.memory_info()
                memory_mb_after = memory_info.rss / (1024 * 1024)
                logger.info(f"Memory usage after garbage collection: {memory_mb_after:.2f} MB (freed {memory_mb - memory_mb_after:.2f} MB)")
        except ImportError:
            # psutil not available, use simpler approach with gc module
            if time.time() - self.last_gc_time > self.gc_interval:
                self._force_garbage_collection()
                self.last_gc_time = time.time()

    def _force_garbage_collection(self) -> None:
        """
        Force garbage collection to free memory.
        """
        # Get counts before collection
        gc_counts_before = gc.get_count()

        # Disable automatic garbage collection
        gc_enabled = gc.isenabled()
        if gc_enabled:
            gc.disable()

        # Run garbage collection multiple times to ensure all cycles are collected
        gc.collect(0)  # Collect generation 0 (youngest objects)
        gc.collect(1)  # Collect generation 1
        gc.collect(2)  # Collect generation 2 (oldest objects)

        # Re-enable automatic garbage collection if it was enabled
        if gc_enabled:
            gc.enable()

        # Get counts after collection
        gc_counts_after = gc.get_count()

        logger.debug(f"Garbage collection: {gc_counts_before} -> {gc_counts_after}")

class SkypeETLPipeline:
    """
    Extract-Transform-Load pipeline for Skype export data.
    """

    def __init__(
        self,
        db_name: Optional[str] = None,
        db_user: Optional[str] = None,
        db_password: Optional[str] = None,
        db_host: str = "localhost",
        db_port: int = 5432,
        output_dir: Optional[str] = None,
        performance_config: Optional[str] = None
    ):
        """
        Initialize the ETL pipeline.

        Args:
            db_name (str, optional): Database name
            db_user (str, optional): Database user
            db_password (str, optional): Database password
            db_host (str, optional): Database host. Defaults to "localhost".
            db_port (int, optional): Database port. Defaults to 5432.
            output_dir (str, optional): Output directory for transformed data
            performance_config (str, optional): Path to performance configuration file
        """
        self.db_config = {
            'dbname': db_name,
            'user': db_user,
            'password': db_password,
            'host': db_host,
            'port': db_port
        }
        self.output_dir = output_dir
        self.conn = None
        self.raw_storage = None
        self.clean_storage = None
        self.progress_tracker = ProgressTracker()

        # Initialize storage if database config is provided
        if all(v is not None for v in [db_name, db_user, db_password]):
            try:
                validate_db_config(self.db_config)
                self.raw_storage = SkypeDataStorage(self.db_config)
                self.clean_storage = SkypeCleanDataStorage(self.db_config)
                logger.info("Storage initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize storage: {e}")

        # Load configuration
        from ..utils.config import load_config
        self.config = load_config(
            config_file=performance_config or 'config/performance.json',
            message_types_file='config/message_types.json'
        )

        # Log performance settings
        self._log_performance_settings()

        # Initialize memory monitor
        self.memory_monitor = MemoryMonitor(memory_limit_mb=self.config.get('memory_limit_mb', 1024))

    def _log_performance_settings(self) -> None:
        """Log the current performance settings."""
        performance_settings = {
            'chunk_size': self.config.get('chunk_size', 1000),
            'db_batch_size': self.config.get('db_batch_size', 100),
            'use_parallel_processing': self.config.get('use_parallel_processing', False),
            'max_workers': self.config.get('max_workers', multiprocessing.cpu_count()),
            'memory_limit_mb': self.config.get('memory_limit_mb', 1024)
        }

        logger.info(f"Performance settings: {performance_settings}")

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

        # Close storage connections if available
        if self.raw_storage:
            self.raw_storage.close()

        if self.clean_storage:
            self.clean_storage.close()

        logger.info("All database connections closed")

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
            # Extract data based on input type
            raw_data = self._extract_data_from_source(file_path, file_obj)

            # Validate the extracted data structure
            self._validate_extracted_data(raw_data)

            # Store raw data if output directory is specified
            self._save_raw_data(raw_data, file_path)

            return raw_data

        except Exception as e:
            logger.error(f"Error during extraction phase: {e}")
            raise

    def _extract_data_from_source(self, file_path: str = None, file_obj: BinaryIO = None) -> Dict[str, Any]:
        """
        Extract data from either a file path or a file object.

        Args:
            file_path (str, optional): Path to the Skype export file
            file_obj (BinaryIO, optional): File-like object containing the Skype export

        Returns:
            dict: The raw data extracted from the source

        Raises:
            ValidationError: If the input is invalid
        """
        if file_path:
            return self._extract_from_file_path(file_path)
        elif file_obj:
            return self._extract_from_file_object(file_obj)

    def _extract_from_file_path(self, file_path: str) -> Dict[str, Any]:
        """
        Extract data from a file path.

        Args:
            file_path (str): Path to the Skype export file

        Returns:
            dict: The raw data extracted from the file

        Raises:
            ValidationError: If the file is invalid
        """
        # Validate file exists and is readable
        try:
            validate_file_exists(file_path)
        except ValidationError as e:
            logger.error(f"File validation error: {e}")
            raise

        # Process based on file type
        if file_path.endswith('.tar'):
            return self._extract_from_tar_file(file_path)
        else:
            return self._extract_from_json_file(file_path)

    def _extract_from_tar_file(self, file_path: str) -> Dict[str, Any]:
        """
        Extract data from a tar file.

        Args:
            file_path (str): Path to the tar file

        Returns:
            dict: The raw data extracted from the file

        Raises:
            ValidationError: If the file is invalid
        """
        try:
            validate_tar_file(file_path)
            raw_data = read_tarfile(file_path, auto_select=True)
            logger.info(f"Extracted data from tar file: {file_path}")
            return raw_data
        except ValidationError as e:
            logger.error(f"TAR file validation error: {e}")
            raise

    def _extract_from_json_file(self, file_path: str) -> Dict[str, Any]:
        """
        Extract data from a JSON file.

        Args:
            file_path (str): Path to the JSON file

        Returns:
            dict: The raw data extracted from the file

        Raises:
            ValidationError: If the file is invalid
        """
        try:
            raw_data = validate_json_file(file_path)
            logger.info(f"Read data from JSON file: {file_path}")
            return raw_data
        except ValidationError as e:
            logger.error(f"JSON file validation error: {e}")
            raise

    def _extract_from_file_object(self, file_obj: BinaryIO) -> Dict[str, Any]:
        """
        Extract data from a file object.

        Args:
            file_obj (BinaryIO): File-like object containing the Skype export

        Returns:
            dict: The raw data extracted from the file object

        Raises:
            ValidationError: If the file object is invalid
        """
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

        return raw_data

    def _validate_extracted_data(self, raw_data: Dict[str, Any]) -> None:
        """
        Validate the extracted data structure.

        Args:
            raw_data (dict): The raw data extracted from the file

        Raises:
            ValidationError: If the data is invalid
        """
        try:
            validate_skype_data(raw_data)
        except ValidationError as e:
            logger.error(f"Skype data validation error: {e}")
            raise

    def _save_raw_data(self, raw_data: Dict[str, Any], file_path: str = None) -> None:
        """
        Save the raw data to a file if output directory is specified.

        Args:
            raw_data (dict): The raw data to save
            file_path (str, optional): The original file path, used for logging
        """
        if self.output_dir and file_path:
            raw_output_path = os.path.join(self.output_dir, 'raw_data.json')
            with open(raw_output_path, 'w', encoding='utf-8') as f:
                json.dump(raw_data, f, indent=2)
            logger.info(f"Raw data saved to {raw_output_path}")

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
            # Count total conversations and messages for progress tracking
            total_conversations = len(raw_data.get('conversations', []))
            total_messages = sum(len(conv.get('MessageList', [])) for conv in raw_data.get('conversations', []))

            # Start progress tracking
            self.progress_tracker.start_phase('transformation', total_conversations, total_messages)

            # Execute the transformation pipeline
            transformed_data = self._execute_transformation_pipeline(raw_data, user_display_name)

            # Finish progress tracking
            stats = self.progress_tracker.finish_phase()
            logger.info(f"Transformation complete. Processed {stats['processed_conversations']} conversations with {stats['processed_messages']} messages")

            return transformed_data
        except ValidationError as e:
            logger.error(f"Validation error during transformation phase: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during transformation phase: {e}")
            raise

    def _execute_transformation_pipeline(self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute the complete transformation pipeline.

        This method orchestrates the entire transformation process, including validation,
        metadata processing, conversation processing, and data storage.

        Args:
            raw_data (dict): The raw data extracted from the Skype export
            user_display_name (str, optional): The display name to use for the user

        Returns:
            dict: The transformed data

        Raises:
            ValidationError: If the input data is invalid
        """
        # Step 1: Validate the raw data
        self._validate_raw_data(raw_data)

        # Step 2: Process metadata and initialize transformed data structure
        transformed_data = self._process_metadata(raw_data, user_display_name)

        # Step 3: Process conversations and messages
        self._process_conversations(raw_data, transformed_data)

        # Step 4: Save the transformed data if output directory is specified
        self._save_transformed_data(transformed_data)

        return transformed_data

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
        # Create ID to display name mapping
        id_to_display_name = self._create_id_display_name_mapping(raw_data, transformed_data)

        # Count total conversations and messages for progress tracking
        conversations = raw_data['conversations']
        total_conversations = len(conversations)
        total_messages = sum(len(conv.get('MessageList', [])) for conv in conversations)

        # Start progress tracking
        self.progress_tracker.start_phase('processing', total_conversations, total_messages)

        # Process all conversations
        self._process_all_conversations(conversations, transformed_data, id_to_display_name)

        # Finish progress tracking
        self.progress_tracker.finish_phase()

    def _create_id_display_name_mapping(self, raw_data: Dict[str, Any],
                                       transformed_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Create a mapping of user IDs to display names.

        Args:
            raw_data (dict): The raw data extracted from the Skype export
            transformed_data (dict): The transformed data structure

        Returns:
            dict: Mapping of user IDs to display names
        """
        user_id = raw_data['userId']
        user_display_name = transformed_data['metadata']['userDisplayName']
        return {user_id: str(user_display_name)}

    def _process_all_conversations(self, conversations: List[Dict[str, Any]],
                                  transformed_data: Dict[str, Any],
                                  id_to_display_name: Dict[str, str]) -> None:
        """
        Process all conversations from the raw data.

        Args:
            conversations (list): List of conversation data
            transformed_data (dict): The transformed data structure to populate
            id_to_display_name (dict): Mapping of user IDs to display names
        """
        # Check if parallel processing is enabled
        use_parallel = self.config.get('use_parallel_processing', False)
        max_workers = self.config.get('max_workers', multiprocessing.cpu_count())

        if use_parallel and len(conversations) > 1:
            logger.info(f"Using parallel processing with {max_workers} workers for {len(conversations)} conversations")
            self._process_conversations_parallel(conversations, transformed_data, id_to_display_name, max_workers)
        else:
            # Sequential processing
            for i, conversation in enumerate(conversations):
                try:
                    self._process_single_conversation(conversation, transformed_data, id_to_display_name)
                    # Update conversation progress
                    self.progress_tracker.update_conversation_progress()
                except Exception as e:
                    conv_id = conversation.get('id', 'unknown')
                    logger.warning(f"Error processing conversation {conv_id}: {e}")
                    continue

    def _process_conversations_parallel(self, conversations: List[Dict[str, Any]],
                                       transformed_data: Dict[str, Any],
                                       id_to_display_name: Dict[str, str],
                                       max_workers: int) -> None:
        """
        Process conversations in parallel using a thread pool.

        Args:
            conversations (list): List of conversation data
            transformed_data (dict): The transformed data structure to populate
            id_to_display_name (dict): Mapping of user IDs to display names
            max_workers (int): Maximum number of worker threads
        """
        # Create a thread-safe dictionary for results
        from threading import Lock
        results_lock = Lock()

        # Function to process a single conversation in a worker thread
        def process_conversation_worker(conversation: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
            try:
                conv_id = conversation.get('id', '')
                if not conv_id:
                    logger.warning(f"Skipping conversation with no ID")
                    return None, None

                # Get conversation display name
                display_name = conversation.get('displayName', conv_id)

                # Create a local copy of id_to_display_name to avoid thread safety issues
                local_id_to_display_name = dict(id_to_display_name)
                local_id_to_display_name[conv_id] = display_name

                # Process messages
                messages = []
                message_list = conversation.get('MessageList', [])

                # Check if chunked processing is needed
                chunk_size = self.config.get('chunk_size', 1000)
                if len(message_list) > chunk_size:
                    # Process in chunks to manage memory
                    for i in range(0, len(message_list), chunk_size):
                        chunk = message_list[i:i + chunk_size]
                        for msg in chunk:
                            if not msg:
                                continue

                            # Transform message
                            transformed_message = self._transform_message(msg, local_id_to_display_name)
                            messages.append(transformed_message)

                            # Update message progress (thread-safe)
                            self.progress_tracker.update_message_progress()

                        # Free memory after processing each chunk
                        del chunk

                        # Check memory usage
                        self.memory_monitor.check_memory()
                else:
                    # Process all messages at once for smaller conversations
                    for msg in message_list:
                        if not msg:
                            continue

                        # Transform message
                        transformed_message = self._transform_message(msg, local_id_to_display_name)
                        messages.append(transformed_message)

                        # Update message progress (thread-safe)
                        self.progress_tracker.update_message_progress()

                # Sort messages by timestamp
                messages = self._sort_messages(messages)

                # Create conversation data structure
                conversation_data = {
                    'display_name': display_name,
                    'id': conv_id,
                    'messages': messages,
                    'timespan': self._get_conversation_timespan(messages)
                }

                return conv_id, conversation_data

            except Exception as e:
                logger.warning(f"Error processing conversation {conversation.get('id', 'unknown')}: {e}")
                return None, None

        # Process conversations in parallel
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks
            future_to_conv = {
                executor.submit(process_conversation_worker, conv): i
                for i, conv in enumerate(conversations)
            }

            # Process results as they complete
            for future in as_completed(future_to_conv):
                conv_idx = future_to_conv[future]
                try:
                    conv_id, conversation_data = future.result()
                    if conv_id and conversation_data:
                        # Thread-safe update of the transformed data
                        with results_lock:
                            # Add export date and time from metadata
                            conversation_data['export_date'] = transformed_data['metadata'].get('exportDate', '')
                            conversation_data['export_time'] = transformed_data['metadata'].get('exportTime', '')

                            # Store conversation data
                            transformed_data['conversations'][conv_id] = conversation_data

                            # Update conversation progress
                            self.progress_tracker.update_conversation_progress()

                            # Update id_to_display_name with any new mappings
                            id_to_display_name[conv_id] = conversation_data['display_name']

                            # Check memory usage
                            self.memory_monitor.check_memory()
                except Exception as e:
                    logger.warning(f"Error processing result for conversation {conv_idx}: {e}")

    def _sort_messages(self, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Sort messages by timestamp.

        Args:
            messages (list): List of message data

        Returns:
            list: Sorted list of message data
        """
        return sorted(messages, key=lambda msg: msg['timestamp'])

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
        # Extract and process conversation metadata
        conv_id, display_name = self._extract_conversation_metadata(conversation)

        # Update ID to display name mapping
        id_to_display_name[conv_id] = display_name

        # Initialize conversation data structure
        self._initialize_conversation_structure(conv_id, display_name, transformed_data)

        # Process messages
        messages = conversation.get('MessageList', [])
        self._update_message_count(conv_id, messages, transformed_data)

        # Process and sort messages
        self._process_messages(conv_id, messages, transformed_data, id_to_display_name)

    def _extract_conversation_metadata(self, conversation: Dict[str, Any]) -> Tuple[str, str]:
        """
        Extract and process conversation metadata.

        Args:
            conversation (dict): The conversation data

        Returns:
            tuple: (conv_id, display_name)
        """
        conv_id = conversation['id']
        display_name = conversation.get('displayName')

        # Sanitize display name
        if display_name is None:
            safe_id = safe_filename(conv_id.split(':')[1])
            display_name = safe_id
        else:
            safe_display_name = safe_filename(display_name)
            display_name = safe_display_name

        return conv_id, display_name

    def _initialize_conversation_structure(self, conv_id: str, display_name: str,
                                          transformed_data: Dict[str, Any]) -> None:
        """
        Initialize the conversation data structure.

        Args:
            conv_id (str): The conversation ID
            display_name (str): The display name for the conversation
            transformed_data (dict): The transformed data structure to populate
        """
        transformed_data['conversations'][conv_id] = {
            'id': conv_id,
            'displayName': display_name,
            'messages': []
        }

    def _update_message_count(self, conv_id: str, messages: List[Dict[str, Any]],
                             transformed_data: Dict[str, Any]) -> None:
        """
        Update the message count for a conversation.

        Args:
            conv_id (str): The conversation ID
            messages (list): List of message data
            transformed_data (dict): The transformed data structure to update
        """
        message_count = len(messages)
        transformed_data['conversations'][conv_id]['messageCount'] = message_count

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
        # Check if chunked processing is needed (for large conversations)
        message_count = len(messages)
        chunk_size = self.config.get('chunk_size', 1000)  # Default chunk size of 1000 messages

        if message_count > chunk_size:
            logger.info(f"Processing large conversation {conv_id} with {message_count} messages in chunks of {chunk_size}")
            # Process messages in chunks
            datetime_objects = self._process_message_chunks(conv_id, messages, transformed_data, id_to_display_name, chunk_size)
        else:
            # Process all messages at once for smaller conversations
            datetime_objects = self._process_message_batch(conv_id, messages, transformed_data, id_to_display_name)

        # Sort messages by timestamp if datetime objects are available
        self._sort_messages(transformed_data['conversations'][conv_id]['messages'])

        # Store first and last message timestamps
        self._store_conversation_timespan(conv_id, transformed_data)

    def _process_message_chunks(self, conv_id: str, messages: List[Dict[str, Any]],
                               transformed_data: Dict[str, Any],
                               id_to_display_name: Dict[str, str],
                               chunk_size: int) -> List[Tuple[int, datetime]]:
        """
        Process messages in chunks to handle large conversations efficiently.

        Args:
            conv_id (str): The conversation ID
            messages (list): List of message data
            transformed_data (dict): The transformed data structure to populate
            id_to_display_name (dict): Mapping of user IDs to display names
            chunk_size (int): Number of messages to process in each chunk

        Returns:
            list: List of tuples (index, datetime) for sorting
        """
        # Track datetime objects for sorting
        all_datetime_objects = []
        total_chunks = (len(messages) + chunk_size - 1) // chunk_size  # Ceiling division

        for chunk_index in range(total_chunks):
            start_idx = chunk_index * chunk_size
            end_idx = min(start_idx + chunk_size, len(messages))
            chunk = messages[start_idx:end_idx]

            # Log progress
            progress = (chunk_index + 1) / total_chunks * 100
            logger.info(f"Processing chunk {chunk_index + 1}/{total_chunks} ({progress:.1f}%) of conversation {conv_id}")

            # Process this chunk of messages
            chunk_datetime_objects = []
            for i, message in enumerate(chunk):
                try:
                    # Calculate the global index in the original messages list
                    global_index = start_idx + i
                    self._process_single_message(global_index, message, conv_id, transformed_data,
                                               id_to_display_name, chunk_datetime_objects)
                except Exception as e:
                    logger.warning(f"Error processing message in conversation {conv_id}: {e}")
                    continue

            # Add datetime objects from this chunk to the overall list
            all_datetime_objects.extend(chunk_datetime_objects)

            # Free up memory by clearing the chunk
            del chunk

            # Check memory usage
            self.memory_monitor.check_memory()

        return all_datetime_objects

    def _process_message_batch(self, conv_id: str, messages: List[Dict[str, Any]],
                              transformed_data: Dict[str, Any],
                              id_to_display_name: Dict[str, str]) -> List[Tuple[int, datetime]]:
        """
        Process a batch of messages and collect datetime objects for sorting.

        Args:
            conv_id (str): The conversation ID
            messages (list): List of message data
            transformed_data (dict): The transformed data structure to populate
            id_to_display_name (dict): Mapping of user IDs to display names

        Returns:
            list: List of tuples (index, datetime) for sorting
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

        return datetime_objects

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
        message_metadata = self._extract_message_metadata(message)

        # Create message data structure
        msg_data = self._create_message_data_structure(message_metadata, id_to_display_name)

        # Store datetime object for sorting if available
        if message_metadata['datetime']:
            datetime_objects.append((index, message_metadata['datetime']))

        # Handle special message types
        self._handle_special_message_types(msg_data, message_metadata['type'])

        # Check for edited messages
        self._check_for_edited_message(msg_data, message)

        # Add message to conversation
        transformed_data['conversations'][conv_id]['messages'].append(msg_data)

        # Update message progress
        self.progress_tracker.update_message_progress()

    def _extract_message_metadata(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract metadata from a message.

        Args:
            message (dict): The message data

        Returns:
            dict: Extracted message metadata
        """
        msg_timestamp = message.get('originalarrivaltime')
        msg_from = message.get('from')
        msg_content_raw = message.get('content', '')
        msg_type = message.get('messagetype', 'Unknown')

        # Parse timestamp
        msg_date_str, msg_time_str, msg_datetime = timestamp_parser(msg_timestamp)

        return {
            'timestamp': msg_timestamp,
            'date_str': msg_date_str,
            'time_str': msg_time_str,
            'datetime': msg_datetime,
            'from': msg_from,
            'content_raw': msg_content_raw,
            'type': msg_type
        }

    def _create_message_data_structure(self, metadata: Dict[str, Any],
                                      id_to_display_name: Dict[str, str]) -> Dict[str, Any]:
        """
        Create a message data structure.

        Args:
            metadata (dict): Message metadata
            id_to_display_name (dict): Mapping of user IDs to display names

        Returns:
            dict: Message data structure
        """
        return {
            'timestamp': metadata['timestamp'],
            'timestampFormatted': f"{metadata['date_str']} {metadata['time_str']}",
            'date': metadata['date_str'],
            'time': metadata['time_str'],
            'fromId': metadata['from'],
            'fromName': id_to_display_name.get(metadata['from'], metadata['from']),
            'type': metadata['type'],
            'rawContent': metadata['content_raw'],
            'isEdited': False
        }

    def _handle_special_message_types(self, msg_data: Dict[str, Any], msg_type: str) -> None:
        """
        Handle special message types.

        Args:
            msg_data (dict): Message data structure to update
            msg_type (str): Message type
        """
        if msg_type != 'RichText':
            msg_content_raw = self._type_parser(msg_type)
            msg_data['rawContent'] = msg_content_raw

    def _check_for_edited_message(self, msg_data: Dict[str, Any], message: Dict[str, Any]) -> None:
        """
        Check if a message has been edited.

        Args:
            msg_data (dict): Message data structure to update
            message (dict): Original message data
        """
        if 'skypeeditedid' in message:
            msg_data['isEdited'] = True

    def _sort_messages(self, messages_list: List[Dict[str, Any]]) -> None:
        """
        Sort messages by timestamp.

        Args:
            messages_list (list): List of message data
        """
        if not messages_list:
            logger.debug(f"No messages to determine timespan for conversation")
            return

        try:
            sorted_messages = self._sort_messages_by_datetime(messages_list)
            messages_list[:] = sorted_messages
        except Exception as e:
            logger.warning(f"Error sorting messages in conversation: {e}")

    def _sort_messages_by_datetime(self, messages_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Sort messages by datetime objects.

        Args:
            messages_list (list): List of message data

        Returns:
            list: Sorted list of message data
        """
        # Sort by datetime
        return sorted(messages_list, key=lambda msg: msg['timestamp'])

    def _store_conversation_timespan(self, conv_id: str, transformed_data: Dict[str, Any]) -> None:
        """
        Store the first and last message timestamps for a conversation.

        Args:
            conv_id (str): The conversation ID
            transformed_data (dict): The transformed data structure
        """
        try:
            messages_list = transformed_data['conversations'][conv_id]['messages']
            if not messages_list:
                logger.debug(f"No messages to determine timespan for conversation {conv_id}")
                return

            self._update_conversation_timespan(conv_id, messages_list, transformed_data)
        except Exception as e:
            logger.warning(f"Error storing conversation timespan for {conv_id}: {e}")

    def _update_conversation_timespan(self, conv_id: str, messages_list: List[Dict[str, Any]],
                                     transformed_data: Dict[str, Any]) -> None:
        """
        Update the conversation timespan with first and last message timestamps.

        Args:
            conv_id (str): The conversation ID
            messages_list (list): List of message data
            transformed_data (dict): The transformed data structure to update
        """
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
        if not self.output_dir:
            return

        try:
            self._write_transformed_data_to_file(transformed_data)
        except Exception as e:
            logger.error(f"Error saving transformed data: {e}")

    def _write_transformed_data_to_file(self, transformed_data: Dict[str, Any]) -> None:
        """
        Write transformed data to a JSON file.

        Args:
            transformed_data (dict): The transformed data to save

        Raises:
            IOError: If there is an error writing to the file
        """
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        # Create output file path
        transformed_output_path = os.path.join(self.output_dir, 'transformed_data.json')

        # Write data to file
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

        if not self.conn and not (self.raw_storage and self.clean_storage):
            error_msg = "Database connection or storage not available"
            logger.error(error_msg)
            raise ValueError(error_msg)

        try:
            # Count total conversations and messages for progress tracking
            total_conversations = len(transformed_data.get('conversations', {}))
            total_messages = sum(len(conv.get('messages', [])) for conv in transformed_data.get('conversations', {}).values())

            # Start progress tracking
            self.progress_tracker.start_phase('loading', total_conversations, total_messages)

            # Use storage classes if available
            if self.raw_storage and self.clean_storage:
                # Store raw data
                file_name = file_source or "skype_export"
                raw_export_id = self.raw_storage.store_raw_data(
                    data=raw_data,
                    file_name=file_name,
                    export_date=transformed_data['metadata'].get('exportDate')
                )

                # Store transformed data
                self.clean_storage.store_transformed_data(
                    transformed_data=transformed_data,
                    raw_export_id=raw_export_id
                )

                # Finish progress tracking
                stats = self.progress_tracker.finish_phase()
                logger.info(f"Loading complete. Processed {stats['processed_conversations']} conversations with {stats['processed_messages']} messages")

                return raw_export_id
            else:
                # Legacy direct database operations
                # Insert raw export data and get export ID
                export_id = self._insert_raw_export(raw_data, transformed_data, file_source)

                # Insert conversations and messages
                self._insert_conversations_and_messages(transformed_data, export_id)

                # Finish progress tracking
                stats = self.progress_tracker.finish_phase()
                logger.info(f"Loading complete. Processed {stats['processed_conversations']} conversations with {stats['processed_messages']} messages")

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
        # Determine batch size from config or use default
        batch_size = self.config.get('db_batch_size', 100)  # Default batch size of 100 messages
        total_messages = len(messages)

        if total_messages == 0:
            logger.warning(f"No messages to insert for conversation {conv_id}")
            return

        logger.info(f"Inserting {total_messages} messages for conversation {conv_id} in batches of {batch_size}")

        # Prepare the SQL statement for batch insert
        insert_sql = """
        INSERT INTO skype_messages
        (conversation_id, timestamp, sender_id, sender_name, message_type, raw_content, is_edited, structured_data)
        VALUES %s
        """

        # Process messages in batches
        for i in range(0, total_messages, batch_size):
            batch = messages[i:i + batch_size]
            batch_values = []

            # Log progress
            progress = min(i + batch_size, total_messages) / total_messages * 100
            logger.info(f"Inserting batch {i // batch_size + 1}/{(total_messages + batch_size - 1) // batch_size} ({progress:.1f}%)")

            # Prepare batch values
            for msg in batch:
                # Parse timestamp
                _, _, msg_time = timestamp_parser(msg['timestamp'])

                # Prepare values for this message
                batch_values.append((
                    conv_id,
                    msg_time,
                    msg['fromId'],
                    msg['fromName'],
                    msg['type'],
                    msg['rawContent'],
                    msg['isEdited'],
                    json.dumps(msg.get('structuredData', {}))
                ))

            # Execute batch insert using psycopg2's execute_values
            try:
                from psycopg2.extras import execute_values
                execute_values(cursor, insert_sql, batch_values)
            except ImportError:
                # Fall back to individual inserts if execute_values is not available
                logger.warning("psycopg2.extras.execute_values not available, falling back to individual inserts")
                for values in batch_values:
                    cursor.execute(
                        """
                        INSERT INTO skype_messages
                        (conversation_id, timestamp, sender_id, sender_name, message_type, raw_content, is_edited, structured_data)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        values
                    )

            # Free up memory
            del batch
            del batch_values

            # Check memory usage
            self.memory_monitor.check_memory()

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

        # Create a unique task ID for tracking progress
        task_id = str(uuid.uuid4())

        # Create a progress tracker with 100 total steps
        tracker = create_tracker(task_id, 100)
        tracker.update(status="starting", message="Initializing ETL pipeline...")

        try:
            # Setup database connection if needed
            self._setup_database_connection()
            tracker.update(increment=5, message="Database connection established")

            # Run extraction phase
            tracker.update(status="extracting", message="Extracting data from Skype export...")
            raw_data, results = self._run_extraction_phase(file_path, file_obj, results)
            tracker.update(increment=30, message=f"Extracted data with {len(raw_data.get('conversations', {}))} conversations")

            # Run transformation phase
            tracker.update(status="transforming", message="Transforming data...")
            transformed_data, results = self._run_transformation_phase(raw_data, user_display_name, results)
            tracker.update(increment=40, message=f"Transformed {results.get('transformation', {}).get('conversationCount', 0)} conversations")

            # Run loading phase if database connection is available
            if self.conn:
                tracker.update(status="loading", message="Loading data into database...")
                results = self._run_loading_phase(raw_data, transformed_data, file_path, results)
                tracker.update(increment=20, message="Data loaded into database")
            else:
                tracker.update(increment=20, message="Skipping database loading (no connection)")

            logger.info("ETL pipeline completed successfully")
            tracker.complete(message="ETL pipeline completed successfully")

            # Add task_id to results
            results['task_id'] = task_id

            return results

        except Exception as e:
            logger.error(f"Error in ETL pipeline: {e}")
            tracker.fail(message=f"Error in ETL pipeline: {str(e)}")
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

    def _transform_message(self, message: Dict[str, Any], id_to_display_name: Dict[str, str]) -> Dict[str, Any]:
        """
        Transform a single message.

        Args:
            message (dict): Raw message data
            id_to_display_name (dict): Mapping of user IDs to display names

        Returns:
            dict: Transformed message data
        """
        # Extract message data
        msg_id = message.get('id', '')
        msg_timestamp = message.get('originalarrivaltime', '')
        msg_from = message.get('from', '')
        msg_content_raw = message.get('content', '')
        msg_type = message.get('messagetype', '')
        is_edited = 'skypeeditedid' in message

        # Parse timestamp
        msg_date_str, msg_time_str, msg_datetime = timestamp_parser(msg_timestamp)

        # Create message data structure
        transformed_message = {
            'id': msg_id,
            'timestamp': msg_timestamp,
            'datetime': msg_datetime,
            'date': msg_date_str,
            'time': msg_time_str,
            'from_id': msg_from,
            'from_name': id_to_display_name.get(msg_from, msg_from),
            'type': msg_type,
            'content': msg_content_raw,  # Will be processed by the parser
            'rawContent': msg_content_raw,
            'isEdited': is_edited
        }

        # Extract structured data if available
        if get_handler_for_message_type(msg_type):
            structured_data = extract_structured_data(message)
            if structured_data:
                transformed_message['structuredData'] = structured_data

        return transformed_message


if __name__ == "__main__":
    # This module is not meant to be run directly
    # It should be imported and used as a library
    print("This module is not meant to be run directly.")
    print("Please import it and use the SkypeETLPipeline class instead.")
    print("See the README.md file for usage examples.")