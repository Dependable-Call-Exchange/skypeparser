"""
Streaming Processor module for the ETL pipeline.

This module provides functionality for streaming processing of very large datasets,
allowing the ETL pipeline to handle millions of messages efficiently.
"""

import logging
import json
import os
import gc
import time
from typing import Dict, Any, Optional, List, Iterator, Tuple, BinaryIO
from datetime import datetime
import ijson

from src.utils.interfaces import DatabaseConnectionProtocol
from .context import ETLContext

logger = logging.getLogger(__name__)

class StreamingProcessor:
    """
    Processes very large datasets using streaming techniques.

    This class provides methods for streaming extraction, transformation, and loading
    of very large Skype export files, allowing the ETL pipeline to handle millions
    of messages without loading the entire dataset into memory.
    """

    def __init__(
        self,
        context: ETLContext,
        db_connection: Optional[DatabaseConnectionProtocol] = None
    ):
        """
        Initialize the streaming processor.

        Args:
            context: ETL context for sharing state between components
            db_connection: Optional database connection
        """
        self.context = context
        self.db_connection = db_connection
        self.memory_monitor = self.context.memory_monitor
        self.progress_tracker = self.context.progress_tracker

        logger.info("Streaming processor initialized")

    def stream_extract(self, file_path: str) -> Iterator[Dict[str, Any]]:
        """
        Stream extract data from a Skype export file.

        This method reads the file in chunks, yielding individual conversations
        without loading the entire file into memory.

        Args:
            file_path: Path to the Skype export file

        Yields:
            Individual conversations from the export file
        """
        logger.info(f"Starting streaming extraction from {file_path}")

        # Check if file exists
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        # Get file extension
        file_ext = os.path.splitext(file_path)[1].lower()

        # Process based on file type
        if file_ext == '.json':
            yield from self._stream_json_file(file_path)
        elif file_ext == '.tar':
            yield from self._stream_tar_file(file_path)
        else:
            raise ValueError(f"Unsupported file extension: {file_ext}")

    def _stream_json_file(self, file_path: str) -> Iterator[Dict[str, Any]]:
        """
        Stream data from a JSON file.

        Args:
            file_path: Path to the JSON file

        Yields:
            Individual conversations from the JSON file
        """
        logger.info(f"Streaming JSON file: {file_path}")

        # Extract metadata first
        metadata = {}
        with open(file_path, 'rb') as f:
            # Parse userId and exportDate
            parser = ijson.parse(f)
            for prefix, event, value in parser:
                if prefix == 'userId':
                    metadata['userId'] = value
                elif prefix == 'exportDate':
                    metadata['exportDate'] = value

                # Break once we have both values
                if 'userId' in metadata and 'exportDate' in metadata:
                    break

        # Update context with metadata
        if 'userId' in metadata:
            self.context.user_id = metadata['userId']
        if 'exportDate' in metadata:
            self.context.export_date = metadata['exportDate']

        # Stream conversations
        with open(file_path, 'rb') as f:
            # Get conversation count first for progress tracking
            conversation_count = 0
            for _ in ijson.items(f, 'conversations.item'):
                conversation_count += 1

            # Reset file pointer
            f.seek(0)

            # Initialize progress tracking
            if self.progress_tracker:
                self.progress_tracker.start_phase('extract', total_conversations=conversation_count)

            # Stream conversations
            for conversation in ijson.items(f, 'conversations.item'):
                # Check memory usage and force garbage collection if needed
                if self.memory_monitor:
                    self.memory_monitor.check_memory()

                # Update progress
                if self.progress_tracker:
                    self.progress_tracker.update_conversation_progress()

                # Yield conversation
                yield conversation

    def _stream_tar_file(self, file_path: str) -> Iterator[Dict[str, Any]]:
        """
        Stream data from a TAR file.

        Args:
            file_path: Path to the TAR file

        Yields:
            Individual conversations from the TAR file
        """
        import tarfile

        logger.info(f"Streaming TAR file: {file_path}")

        with tarfile.open(file_path, 'r') as tar:
            # Find the messages.json file
            messages_file = None
            for member in tar.getmembers():
                if member.name.endswith('messages.json'):
                    messages_file = member
                    break

            if not messages_file:
                raise ValueError("No messages.json file found in TAR archive")

            # Extract messages.json to a temporary file
            temp_dir = self.context.get_temp_dir()
            tar.extract(messages_file, path=temp_dir)
            temp_file_path = os.path.join(temp_dir, messages_file.name)

            # Stream from the extracted JSON file
            yield from self._stream_json_file(temp_file_path)

            # Clean up temporary file
            os.remove(temp_file_path)

    def stream_transform_load(
        self,
        conversation_iterator: Iterator[Dict[str, Any]],
        batch_size: int = 1000
    ) -> Dict[str, Any]:
        """
        Stream transform and load conversations.

        This method transforms and loads conversations in batches without
        keeping the entire dataset in memory.

        Args:
            conversation_iterator: Iterator of conversations
            batch_size: Number of messages to process in each batch

        Returns:
            Dictionary with processing statistics
        """
        logger.info(f"Starting streaming transform and load with batch size {batch_size}")

        # Initialize statistics
        stats = {
            'conversations_processed': 0,
            'messages_processed': 0,
            'start_time': datetime.now(),
            'end_time': None,
            'duration_seconds': 0
        }

        # Initialize progress tracking
        if self.progress_tracker:
            self.progress_tracker.start_phase('transform_load')

        # Process conversations
        for conversation in conversation_iterator:
            # Get conversation ID and message list
            conversation_id = conversation.get('id')
            message_list = conversation.get('MessageList', [])

            # Update message count for progress tracking
            if self.progress_tracker:
                self.progress_tracker.total_messages += len(message_list)

            # Process messages in batches
            for i in range(0, len(message_list), batch_size):
                # Get batch of messages
                message_batch = message_list[i:i+batch_size]

                # Transform and load batch
                self._process_message_batch(conversation_id, conversation, message_batch)

                # Update statistics
                stats['messages_processed'] += len(message_batch)

                # Update progress
                if self.progress_tracker:
                    self.progress_tracker.update_message_progress(len(message_batch))

                # Check memory usage and force garbage collection if needed
                if self.memory_monitor:
                    self.memory_monitor.check_memory()

            # Update statistics
            stats['conversations_processed'] += 1

            # Update progress
            if self.progress_tracker:
                self.progress_tracker.update_conversation_progress()

        # Update statistics
        stats['end_time'] = datetime.now()
        stats['duration_seconds'] = (stats['end_time'] - stats['start_time']).total_seconds()

        # Finish progress tracking
        if self.progress_tracker:
            self.progress_tracker.finish_phase()

        logger.info(f"Streaming transform and load completed: {stats['conversations_processed']} conversations, {stats['messages_processed']} messages in {stats['duration_seconds']:.2f} seconds")

        return stats

    def _process_message_batch(
        self,
        conversation_id: str,
        conversation: Dict[str, Any],
        message_batch: List[Dict[str, Any]]
    ) -> None:
        """
        Process a batch of messages.

        This method transforms and loads a batch of messages for a specific conversation.

        Args:
            conversation_id: ID of the conversation
            conversation: Conversation data
            message_batch: Batch of messages to process
        """
        # Transform messages
        transformed_messages = []
        for message in message_batch:
            try:
                # Basic transformation
                transformed_message = {
                    'id': message.get('id'),
                    'conversation_id': conversation_id,
                    'sender_id': message.get('from'),
                    'sender_name': message.get('displayName'),
                    'timestamp': self._parse_timestamp(message.get('originalarrivaltime')),
                    'message_type': message.get('messagetype'),
                    'content': self._extract_content(message),
                    'raw_content': json.dumps(message),
                    'is_edited': 'skypeeditedid' in message
                }

                transformed_messages.append(transformed_message)
            except Exception as e:
                logger.error(f"Error transforming message {message.get('id')}: {str(e)}")

        # Load messages to database
        if self.db_connection and transformed_messages:
            try:
                # Insert messages
                self._batch_insert_messages(transformed_messages)
            except Exception as e:
                logger.error(f"Error loading messages to database: {str(e)}")

    def _parse_timestamp(self, timestamp_str: Optional[str]) -> Optional[datetime]:
        """
        Parse a timestamp string to a datetime object.

        Args:
            timestamp_str: Timestamp string

        Returns:
            Datetime object or None if parsing fails
        """
        if not timestamp_str:
            return None

        try:
            return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        except (ValueError, TypeError):
            logger.warning(f"Failed to parse timestamp: {timestamp_str}")
            return None

    def _extract_content(self, message: Dict[str, Any]) -> str:
        """
        Extract content from a message.

        Args:
            message: Message data

        Returns:
            Extracted content
        """
        content = message.get('content', '')

        # Simple HTML tag removal
        # In a real implementation, this would use the ContentExtractor service
        import re
        content = re.sub(r'<[^>]+>', '', content)

        return content

    def _batch_insert_messages(self, messages: List[Dict[str, Any]]) -> None:
        """
        Insert a batch of messages into the database.

        Args:
            messages: List of transformed messages
        """
        if not self.db_connection:
            return

        # Prepare query and parameters
        query = """
            INSERT INTO messages (
                message_id, conversation_id, sender_id, sender_name,
                timestamp, message_type, content, raw_content, is_edited
            ) VALUES (
                %(id)s, %(conversation_id)s, %(sender_id)s, %(sender_name)s,
                %(timestamp)s, %(message_type)s, %(content)s, %(raw_content)s, %(is_edited)s
            )
            ON CONFLICT (message_id) DO NOTHING
        """

        # Execute batch insert
        self.db_connection.execute_batch(query, messages)