"""
Transformer module for the ETL pipeline.

This module handles the transformation of raw Skype data into a structured format,
including conversation and message processing.
"""

import logging
import datetime
import os
import json
from typing import Dict, List, Any, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from src.utils.message_type_handlers import extract_structured_data, get_handler_for_message_type
from src.parser.content_extractor import ContentExtractor
from .context import ETLContext

logger = logging.getLogger(__name__)

class Transformer:
    """Handles transformation of raw Skype data into structured format."""

    def __init__(
        self,
        context: ETLContext = None,
        parallel_processing: bool = True,
        chunk_size: int = 1000,
        max_workers: Optional[int] = None
    ):
        """Initialize the Transformer.

        Args:
            context: Shared ETL context object
            parallel_processing: Whether to use parallel processing for conversations (used if context not provided)
            chunk_size: Size of message chunks for batch processing (used if context not provided)
            max_workers: Maximum number of worker threads (used if context not provided)
        """
        self.context = context

        # Use context settings if available, otherwise use parameters
        if context:
            self.parallel_processing = context.parallel_processing
            self.chunk_size = context.chunk_size
            self.max_workers = context.max_workers
        else:
            self.parallel_processing = parallel_processing
            self.chunk_size = chunk_size
            self.max_workers = max_workers

        self.content_extractor = ContentExtractor()

        # Thread-safe storage for parallel processing
        self.results_lock = Lock()

    def transform(self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None) -> Dict[str, Any]:
        """Transform raw Skype data into structured format.

        Args:
            raw_data: Raw data from the extractor
            user_display_name: Display name of the user

        Returns:
            Dict containing the transformed data
        """
        logger.info("Starting data transformation")

        # Validate the raw data
        self._validate_raw_data(raw_data)

        # Validate user display name
        user_display_name = self._validate_user_display_name(user_display_name, raw_data)

        # Count total conversations and messages for progress tracking
        total_conversations = len(raw_data.get('conversations', []))
        total_messages = sum(len(conv.get('MessageList', [])) for conv in raw_data.get('conversations', []))

        # Update context if available
        if self.context:
            # Context phase is managed by the pipeline manager, but we can update counts
            self.context.progress_tracker.total_conversations = total_conversations
            self.context.progress_tracker.total_messages = total_messages

        logger.info(f"Transforming {total_conversations} conversations with {total_messages} messages")

        # Initialize transformed data structure with metadata
        transformed_data = self._process_metadata(raw_data, user_display_name)

        # Create mapping of IDs to display names
        id_to_display_name = self._create_id_display_name_mapping(raw_data, transformed_data)

        # Process conversations
        self._process_conversations(raw_data, transformed_data, id_to_display_name)

        # Validate the transformed data
        self._validate_transformed_data(transformed_data)

        # Save transformed data if output directory is specified
        self._save_transformed_data(transformed_data)

        # Update context if available
        if self.context:
            self.context.transformed_data = transformed_data
            self.context.check_memory()

        logger.info(f"Transformation complete: {len(transformed_data['conversations'])} conversations processed")

        return transformed_data

    def _validate_raw_data(self, raw_data: Dict[str, Any]) -> None:
        """Validate the raw data structure with enhanced checks.

        Args:
            raw_data: Raw data to validate

        Raises:
            ValueError: If data structure is invalid
        """
        if not isinstance(raw_data, dict):
            raise ValueError("Raw data must be a dictionary")

        if 'conversations' not in raw_data:
            raise ValueError("Raw data must contain 'conversations' key")

        if not isinstance(raw_data.get('conversations', []), list):
            raise ValueError("Conversations must be a list")

        if 'userId' not in raw_data:
            raise ValueError("Raw data must contain 'userId' key")

        if 'exportDate' not in raw_data:
            raise ValueError("Raw data must contain 'exportDate' key")

        # Check for required fields in conversations
        for i, conv in enumerate(raw_data.get('conversations', [])):
            if not isinstance(conv, dict):
                raise ValueError(f"Conversation at index {i} must be a dictionary")

            if 'id' not in conv:
                logger.warning(f"Conversation at index {i} is missing 'id' field")

            if 'MessageList' not in conv:
                logger.warning(f"Conversation at index {i} is missing 'MessageList' field")
            elif not isinstance(conv['MessageList'], list):
                raise ValueError(f"MessageList for conversation at index {i} must be a list")

        logger.info("Raw data validation completed successfully")

    def _validate_user_display_name(self, user_display_name: Optional[str], raw_data: Dict[str, Any]) -> str:
        """Validate and sanitize user display name.

        Args:
            user_display_name: User display name to validate
            raw_data: Raw data containing user information

        Returns:
            str: Sanitized user display name
        """
        user_id = raw_data.get('userId', '')

        if user_display_name is None or user_display_name.strip() == '':
            logger.info(f"No user display name provided, using user ID: {user_id}")
            return user_id

        # Import validation function here to avoid circular imports
        from src.utils.validation import validate_user_display_name

        try:
            sanitized_name = validate_user_display_name(user_display_name)
            if sanitized_name != user_display_name:
                logger.info(f"Sanitized user display name from '{user_display_name}' to '{sanitized_name}'")
            return sanitized_name
        except Exception as e:
            logger.warning(f"User display name validation error: {e}. Using user ID instead.")
            return user_id

    def _validate_transformed_data(self, transformed_data: Dict[str, Any]) -> None:
        """Validate the transformed data structure.

        Args:
            transformed_data: Transformed data to validate

        Raises:
            ValueError: If transformed data structure is invalid
        """
        if not isinstance(transformed_data, dict):
            raise ValueError("Transformed data must be a dictionary")

        if 'metadata' not in transformed_data:
            raise ValueError("Transformed data must contain 'metadata' key")

        if 'conversations' not in transformed_data:
            raise ValueError("Transformed data must contain 'conversations' key")

        if not isinstance(transformed_data['conversations'], dict):
            raise ValueError("Transformed conversations must be a dictionary")

        # Check for required fields in metadata
        metadata = transformed_data.get('metadata', {})
        required_metadata = ['user_display_name', 'export_time', 'total_conversations', 'total_messages']
        missing_metadata = [field for field in required_metadata if field not in metadata]
        if missing_metadata:
            logger.warning(f"Missing metadata fields: {', '.join(missing_metadata)}")

        # Check for required fields in conversations
        for conv_id, conv in transformed_data.get('conversations', {}).items():
            if not isinstance(conv, dict):
                raise ValueError(f"Conversation '{conv_id}' must be a dictionary")

            required_conv_fields = ['display_name', 'messages']
            missing_conv_fields = [field for field in required_conv_fields if field not in conv]
            if missing_conv_fields:
                logger.warning(f"Conversation '{conv_id}' is missing fields: {', '.join(missing_conv_fields)}")

            if 'messages' in conv and not isinstance(conv['messages'], list):
                raise ValueError(f"Messages for conversation '{conv_id}' must be a list")

            # Check for empty conversations
            if 'messages' in conv and len(conv['messages']) == 0:
                logger.warning(f"Conversation '{conv_id}' has no messages")

        # Verify conversation count matches
        if len(transformed_data.get('conversations', {})) != metadata.get('total_conversations', 0):
            logger.warning(f"Metadata conversation count ({metadata.get('total_conversations', 0)}) " +
                          f"doesn't match actual count ({len(transformed_data.get('conversations', {}))})")

        logger.info("Transformed data validation completed successfully")

    def _process_metadata(self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None) -> Dict[str, Any]:
        """Process and structure the metadata.

        Args:
            raw_data: Raw data to process
            user_display_name: Display name of the user

        Returns:
            Dict containing initialized transformed data structure
        """
        return {
            'metadata': {
                'user_display_name': user_display_name,
                'export_time': datetime.datetime.now().isoformat(),
                'total_conversations': len(raw_data.get('conversations', [])),
                'total_messages': sum(len(c.get('MessageList', []))
                                   for c in raw_data.get('conversations', []))
            },
            'conversations': {}
        }

    def _create_id_display_name_mapping(self, raw_data: Dict[str, Any],
                                      transformed_data: Dict[str, Any]) -> Dict[str, str]:
        """Create mapping of user IDs to display names.

        Args:
            raw_data: Raw data containing user information
            transformed_data: Transformed data structure

        Returns:
            Dict mapping user IDs to display names
        """
        id_to_display_name = {}

        # Add the main user's display name
        user_display_name = transformed_data['metadata'].get('user_display_name')
        if user_display_name:
            id_to_display_name['user'] = user_display_name

        return id_to_display_name

    def _process_conversations(self, raw_data: Dict[str, Any],
                             transformed_data: Dict[str, Any],
                             id_to_display_name: Dict[str, str]) -> None:
        """Process all conversations.

        Args:
            raw_data: Raw data containing conversations
            transformed_data: Transformed data structure to update
            id_to_display_name: Mapping of user IDs to display names
        """
        if self.parallel_processing:
            self._process_conversations_parallel(
                raw_data['conversations'],
                transformed_data,
                id_to_display_name
            )
        else:
            self._process_all_conversations(
                raw_data['conversations'],
                transformed_data,
                id_to_display_name
            )

    def _process_all_conversations(self, conversations: List[Dict[str, Any]],
                                 transformed_data: Dict[str, Any],
                                 id_to_display_name: Dict[str, str]) -> None:
        """Process all conversations sequentially.

        Args:
            conversations: List of conversations to process
            transformed_data: Transformed data structure to update
            id_to_display_name: Mapping of user IDs to display names
        """
        for conversation in conversations:
            self._process_single_conversation(conversation, transformed_data, id_to_display_name)

    def _process_conversations_parallel(self, conversations: List[Dict[str, Any]],
                                     transformed_data: Dict[str, Any],
                                     id_to_display_name: Dict[str, str],
                                     max_workers: int = None) -> None:
        """Process conversations in parallel.

        Args:
            conversations: List of conversations to process
            transformed_data: Transformed data structure to update
            id_to_display_name: Mapping of user IDs to display names
            max_workers: Maximum number of worker threads
        """
        if max_workers is None:
            max_workers = min(32, (len(conversations) + 3) // 4)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_conv = {
                executor.submit(self._process_single_conversation,
                              conv, transformed_data, id_to_display_name): conv
                for conv in conversations
            }

            for future in as_completed(future_to_conv):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing conversation: {e}")

    def _process_single_conversation(self, conversation: Dict[str, Any],
                                  transformed_data: Dict[str, Any],
                                  id_to_display_name: Dict[str, str]) -> None:
        """Process a single conversation.

        Args:
            conversation: Conversation data to process
            transformed_data: Transformed data structure to update
            id_to_display_name: Mapping of user IDs to display names
        """
        conv_id, display_name = self._extract_conversation_metadata(conversation)

        # Initialize conversation structure
        self._initialize_conversation_structure(conv_id, display_name, transformed_data)

        # Process messages
        messages = conversation.get('MessageList', [])
        if messages:
            self._process_messages(conv_id, messages, transformed_data, id_to_display_name)
            self._update_conversation_timespan(conv_id, messages, transformed_data)

    def _extract_conversation_metadata(self, conversation: Dict[str, Any]) -> Tuple[str, str]:
        """Extract conversation ID and display name.

        Args:
            conversation: Conversation data

        Returns:
            Tuple of (conversation_id, display_name)
        """
        conv_id = conversation.get('id', '')
        display_name = conversation.get('displayName', '')
        return conv_id, display_name

    def _initialize_conversation_structure(self, conv_id: str, display_name: str,
                                       transformed_data: Dict[str, Any]) -> None:
        """Initialize the conversation structure in transformed data.

        Args:
            conv_id: Conversation ID
            display_name: Display name of the conversation
            transformed_data: Transformed data structure to update
        """
        transformed_data['conversations'][conv_id] = {
            'display_name': display_name,
            'first_message_time': None,
            'last_message_time': None,
            'message_count': 0,
            'messages': []
        }

    def _process_messages(self, conv_id: str, messages: List[Dict[str, Any]],
                        transformed_data: Dict[str, Any],
                        id_to_display_name: Dict[str, str]) -> None:
        """Process messages for a conversation.

        Args:
            conv_id: Conversation ID
            messages: List of messages to process
            transformed_data: Transformed data structure to update
            id_to_display_name: Mapping of user IDs to display names
        """
        # Sort messages by timestamp
        sorted_messages = sorted(messages, key=lambda x: x.get('originalarrivaltime', ''))

        # Process in chunks
        for i in range(0, len(sorted_messages), self.chunk_size):
            chunk = sorted_messages[i:i + self.chunk_size]
            self._process_message_batch(conv_id, chunk, transformed_data, id_to_display_name)

    def _process_message_batch(self, conv_id: str, messages: List[Dict[str, Any]],
                            transformed_data: Dict[str, Any],
                            id_to_display_name: Dict[str, str]) -> None:
        """Process a batch of messages.

        Args:
            conv_id: Conversation ID
            messages: List of messages to process
            transformed_data: Transformed data structure to update
            id_to_display_name: Mapping of user IDs to display names
        """
        for message in messages:
            processed_message = self._transform_message(message, id_to_display_name)
            if processed_message:
                transformed_data['conversations'][conv_id]['messages'].append(processed_message)
                transformed_data['conversations'][conv_id]['message_count'] += 1

    def _transform_message(self, message: Dict[str, Any], id_to_display_name: Dict[str, str]) -> Optional[Dict[str, Any]]:
        """Transform a single message.

        Args:
            message: Message to transform
            id_to_display_name: Mapping of user IDs to display names

        Returns:
            Transformed message or None if message should be skipped
        """
        try:
            msg_data = {
                'timestamp': message.get('originalarrivaltime', ''),
                'sender_id': message.get('from', ''),
                'sender_name': id_to_display_name.get(message.get('from', ''), ''),
                'message_type': message.get('messagetype', ''),
                'content': message.get('content', ''),
                'is_edited': bool(message.get('edittime')),
            }

            # Extract structured data based on message type
            msg_type = msg_data['message_type']
            handler = get_handler_for_message_type(msg_type)
            if handler:
                structured_data = handler(message)
                if structured_data:
                    msg_data['structured_data'] = structured_data

            # Clean content if present
            if msg_data['content']:
                msg_data['cleaned_content'] = self.content_extractor.clean_content(
                    msg_data['content']
                )

            return msg_data
        except Exception as e:
            logger.error(f"Error transforming message: {e}")
            return None

    def _update_conversation_timespan(self, conv_id: str, messages: List[Dict[str, Any]],
                                   transformed_data: Dict[str, Any]) -> None:
        """Update the first and last message times for a conversation.

        Args:
            conv_id: Conversation ID
            messages: List of messages
            transformed_data: Transformed data structure to update
        """
        if not messages:
            return

        # Sort messages by timestamp
        sorted_messages = sorted(messages, key=lambda x: x.get('originalarrivaltime', ''))

        # Update conversation timespan
        transformed_data['conversations'][conv_id]['first_message_time'] = sorted_messages[0].get('originalarrivaltime', '')
        transformed_data['conversations'][conv_id]['last_message_time'] = sorted_messages[-1].get('originalarrivaltime', '')

    def _save_transformed_data(self, transformed_data: Dict[str, Any]) -> None:
        """Save transformed data to file.

        Args:
            transformed_data: Transformed data to save
        """
        if self.context and self.context.output_dir:
            filename = f"{self.context.output_dir}/transformed_data.json"
            with open(filename, 'w') as f:
                json.dump(transformed_data, f)
