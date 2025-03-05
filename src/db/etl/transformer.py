"""
Transformer module for the ETL pipeline.

This module provides the Transformer class that transforms raw Skype export data
into a structured format suitable for database loading.
"""

import logging
import json
from typing import Dict, Any, Optional, List, Callable, Union
import concurrent.futures
from datetime import datetime

from src.utils.interfaces import (
    TransformerProtocol,
    ContentExtractorProtocol,
    MessageHandlerFactoryProtocol,
    StructuredDataExtractorProtocol
)
from src.utils.di import get_service
from .context import ETLContext

logger = logging.getLogger(__name__)

class Transformer(TransformerProtocol):
    """Transforms raw Skype export data into a structured format."""

    def __init__(
        self,
        context: Optional[ETLContext] = None,
        parallel_processing: bool = True,
        chunk_size: int = 1000,
        max_workers: Optional[int] = None,
        content_extractor: Optional[ContentExtractorProtocol] = None,
        message_handler_factory: Optional[MessageHandlerFactoryProtocol] = None,
        structured_data_extractor: Optional[StructuredDataExtractorProtocol] = None
    ):
        """Initialize the transformer.

        Args:
            context: ETL context for sharing state between components
            parallel_processing: Whether to use parallel processing for transformations
            chunk_size: Size of message chunks for batch processing
            max_workers: Maximum number of worker threads/processes
            content_extractor: Optional custom content extractor
            message_handler_factory: Optional custom message handler factory
            structured_data_extractor: Optional custom structured data extractor
        """
        self.context = context
        self.parallel_processing = parallel_processing
        self.chunk_size = chunk_size
        self.max_workers = max_workers

        # Use provided dependencies or get from service container
        self.content_extractor = content_extractor or get_service(ContentExtractorProtocol)
        self.message_handler_factory = message_handler_factory or get_service(MessageHandlerFactoryProtocol)
        self.structured_data_extractor = structured_data_extractor or get_service(StructuredDataExtractorProtocol)

        logger.info("Transformer initialized")

    def transform(self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None) -> Dict[str, Any]:
        """Transform raw Skype export data into a structured format.

        Args:
            raw_data: Raw data from the extraction phase
            user_display_name: Display name of the user

        Returns:
            Transformed data in a structured format

        Raises:
            ValueError: If raw_data is invalid
            Exception: If an error occurs during transformation
        """
        # Validate input data
        self._validate_input_data(raw_data)

        # Update context if available
        if self.context:
            self.context.set_phase('transform')
            self.context.update_progress(0, "Starting transformation")

        # Extract user information
        user_id = raw_data.get('userId', '')
        if not user_display_name and self.context:
            user_display_name = self.context.user_display_name

        # Initialize result structure
        result = {
            'user': {
                'id': user_id,
                'display_name': user_display_name or ''
            },
            'conversations': {},
            'metadata': {
                'transformed_at': datetime.now().isoformat(),
                'conversation_count': 0,
                'message_count': 0
            }
        }

        # Get conversations from raw data
        conversations = raw_data.get('conversations', {})
        total_conversations = len(conversations)

        if total_conversations == 0:
            logger.warning("No conversations found in raw data")
            return result

        # Process conversations
        logger.info(f"Transforming {total_conversations} conversations")

        # Track progress
        processed_conversations = 0
        total_messages = 0

        # Process each conversation
        for conversation_id, conversation_data in conversations.items():
            try:
                # Transform conversation
                transformed_conversation = self._transform_conversation(
                    conversation_id,
                    conversation_data,
                    user_id,
                    user_display_name
                )

                # Add to result
                result['conversations'][conversation_id] = transformed_conversation

                # Update counts
                processed_conversations += 1
                conversation_message_count = len(transformed_conversation.get('messages', []))
                total_messages += conversation_message_count

                # Update progress
                if self.context:
                    progress = (processed_conversations / total_conversations) * 100
                    self.context.update_progress(
                        progress,
                        f"Processed {processed_conversations}/{total_conversations} conversations"
                    )

                logger.debug(f"Transformed conversation {conversation_id} with {conversation_message_count} messages")

            except Exception as e:
                logger.error(f"Error transforming conversation {conversation_id}: {e}")
                if self.context:
                    self.context.record_error('transform', f"Error transforming conversation {conversation_id}: {e}")

        # Update metadata
        result['metadata']['conversation_count'] = processed_conversations
        result['metadata']['message_count'] = total_messages

        # Update context
        if self.context:
            self.context.update_progress(100, f"Transformation complete: {processed_conversations} conversations, {total_messages} messages")
            self.context.set_transformed_data(result)

        logger.info(f"Transformation complete: {processed_conversations} conversations, {total_messages} messages")
        return result

    def _validate_input_data(self, raw_data: Dict[str, Any]) -> None:
        """Validate the input data for transformation.

        Args:
            raw_data: Raw data to validate

        Raises:
            ValueError: If the data is invalid
        """
        if not isinstance(raw_data, dict):
            error_msg = f"Raw data must be a dictionary, got {type(raw_data).__name__}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        if 'conversations' not in raw_data:
            error_msg = "Raw data must contain a 'conversations' key"
            logger.error(error_msg)
            raise ValueError(error_msg)

        if not isinstance(raw_data['conversations'], dict):
            error_msg = f"Conversations must be a dictionary, got {type(raw_data['conversations']).__name__}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def _transform_conversation(
        self,
        conversation_id: str,
        conversation_data: Dict[str, Any],
        user_id: str,
        user_display_name: Optional[str]
    ) -> Dict[str, Any]:
        """Transform a single conversation.

        Args:
            conversation_id: ID of the conversation
            conversation_data: Raw conversation data
            user_id: ID of the user
            user_display_name: Display name of the user

        Returns:
            Transformed conversation data
        """
        # Extract conversation properties
        properties = conversation_data.get('Properties', {})
        message_list = conversation_data.get('MessageList', [])

        # Get conversation type and participants
        conversation_type = properties.get('conversationType', 'unknown')
        display_name = properties.get('displayName', '')
        participants = self._extract_participants(properties.get('participants', []), user_id, user_display_name)

        # Create transformed conversation structure
        transformed_conversation = {
            'id': conversation_id,
            'display_name': display_name,
            'type': conversation_type,
            'participants': participants,
            'created_at': properties.get('creationTime', ''),
            'last_message_at': properties.get('lastUpdatedTime', ''),
            'messages': [],
            'metadata': {
                'message_count': len(message_list),
                'participant_count': len(participants)
            }
        }

        # Transform messages
        if message_list:
            if self.parallel_processing and len(message_list) > self.chunk_size:
                # Process messages in parallel
                transformed_messages = self._transform_messages_parallel(message_list, conversation_id)
            else:
                # Process messages sequentially
                transformed_messages = self._transform_messages_sequential(message_list, conversation_id)

            # Sort messages by timestamp
            transformed_conversation['messages'] = sorted(
                transformed_messages,
                key=lambda m: m.get('timestamp', '0')
            )

        return transformed_conversation

    def _extract_participants(
        self,
        participants_data: List[Dict[str, Any]],
        user_id: str,
        user_display_name: Optional[str]
    ) -> List[Dict[str, Any]]:
        """Extract and transform participant information.

        Args:
            participants_data: Raw participant data
            user_id: ID of the user
            user_display_name: Display name of the user

        Returns:
            List of transformed participant data
        """
        transformed_participants = []

        for participant in participants_data:
            # Extract participant properties
            mri = participant.get('mri', '')
            display_name = participant.get('displayName', '')

            # Check if this is the current user
            is_self = mri == user_id

            # Use provided user display name if this is the current user and no display name is available
            if is_self and not display_name and user_display_name:
                display_name = user_display_name

            # Create transformed participant
            transformed_participant = {
                'id': mri,
                'display_name': display_name,
                'is_self': is_self
            }

            transformed_participants.append(transformed_participant)

        return transformed_participants

    def _transform_messages_parallel(self, messages: List[Dict[str, Any]], conversation_id: str) -> List[Dict[str, Any]]:
        """Transform messages in parallel using multiple workers.

        Args:
            messages: List of raw messages
            conversation_id: ID of the conversation

        Returns:
            List of transformed messages
        """
        # Split messages into chunks
        chunks = [messages[i:i + self.chunk_size] for i in range(0, len(messages), self.chunk_size)]
        transformed_messages = []

        # Process chunks in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit tasks
            future_to_chunk = {
                executor.submit(self._transform_messages_sequential, chunk, conversation_id): i
                for i, chunk in enumerate(chunks)
            }

            # Collect results
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk_index = future_to_chunk[future]
                try:
                    chunk_result = future.result()
                    transformed_messages.extend(chunk_result)
                    logger.debug(f"Processed message chunk {chunk_index+1}/{len(chunks)} for conversation {conversation_id}")
                except Exception as e:
                    logger.error(f"Error processing message chunk {chunk_index+1}/{len(chunks)} for conversation {conversation_id}: {e}")
                    if self.context:
                        self.context.record_error('transform', f"Error processing message chunk for conversation {conversation_id}: {e}")

        return transformed_messages

    def _transform_messages_sequential(self, messages: List[Dict[str, Any]], conversation_id: str) -> List[Dict[str, Any]]:
        """Transform messages sequentially.

        Args:
            messages: List of raw messages
            conversation_id: ID of the conversation

        Returns:
            List of transformed messages
        """
        transformed_messages = []

        for message in messages:
            try:
                # Get message type
                message_type = message.get('messagetype', 'unknown')

                # Get appropriate handler for this message type
                handler = self.message_handler_factory.get_handler(message_type)

                if handler:
                    # Extract content
                    content_html = message.get('content', '')
                    content_text = self.content_extractor.extract_cleaned_content(content_html)

                    # Transform message using handler
                    transformed_message = handler.extract_structured_data(message)

                    # Add extracted content
                    transformed_message['content_html'] = content_html
                    transformed_message['content_text'] = content_text

                    # Add to result
                    transformed_messages.append(transformed_message)
                else:
                    logger.warning(f"No handler found for message type: {message_type}")
            except Exception as e:
                logger.error(f"Error transforming message in conversation {conversation_id}: {e}")
                if self.context:
                    self.context.record_error('transform', f"Error transforming message in conversation {conversation_id}: {e}")

        return transformed_messages
