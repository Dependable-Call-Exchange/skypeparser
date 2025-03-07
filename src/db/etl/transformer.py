"""
Transformer module for the ETL pipeline.

This module provides the Transformer class that transforms raw Skype export data
into a structured format suitable for database loading.
"""

import concurrent.futures
import json
import logging
import os
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Union

from src.utils.attachment_handler import AttachmentHandler
from src.utils.di import get_service
from src.utils.interfaces import (
    ContentExtractorProtocol,
    MessageHandlerFactoryProtocol,
    StructuredDataExtractorProtocol,
    TransformerProtocol,
)

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
        structured_data_extractor: Optional[StructuredDataExtractorProtocol] = None,
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
        self.content_extractor = content_extractor or get_service(
            ContentExtractorProtocol
        )
        self.message_handler_factory = message_handler_factory or get_service(
            MessageHandlerFactoryProtocol
        )
        self.structured_data_extractor = structured_data_extractor or get_service(
            StructuredDataExtractorProtocol
        )

        logger.info("Transformer initialized")

    def transform(
        self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Transform raw Skype export data into a structured format.

        Args:
            raw_data: Raw Skype export data
            user_display_name: Optional display name of the user

        Returns:
            Transformed data
        """
        if self.context:
            self.context.start_phase("transform")
            if user_display_name:
                self.context.user_display_name = user_display_name

        logger.info("Starting transformation")
        start_time = datetime.now()

        # Initialize result structure
        result = {
            "metadata": {
                "transformed_at": datetime.now().isoformat(),
                "conversations_count": 0,
                "messages_count": 0,
            },
            "conversations": {},
        }

        # Extract conversations
        conversations = raw_data.get("conversations", {})
        result["metadata"]["conversations_count"] = len(conversations)

        # Process each conversation
        for conversation_id, conversation_data in conversations.items():
            try:
                # Transform conversation
                transformed_conversation = self._transform_conversation(
                    conversation_id, conversation_data
                )
                result["conversations"][conversation_id] = transformed_conversation
                result["metadata"]["messages_count"] += len(
                    transformed_conversation.get("messages", [])
                )
            except Exception as e:
                logger.error(f"Error transforming conversation {conversation_id}: {e}")
                if self.context:
                    self.context.record_error(
                        "transform",
                        f"Error transforming conversation {conversation_id}: {e}",
                    )

        # Add user information
        if user_display_name:
            result["user"] = {
                "display_name": user_display_name,
                "id": self.context.user_id
                if self.context and hasattr(self.context, "user_id")
                else f"user_{hash(user_display_name) % 10000}",
            }

        # Add required keys for loader
        result["user_id"] = (
            self.context.user_id
            if self.context and hasattr(self.context, "user_id")
            else result.get("user", {}).get("id", "unknown_user")
        )
        result["export_date"] = (
            self.context.export_date
            if self.context and hasattr(self.context, "export_date")
            else result["metadata"]["transformed_at"]
        )

        # Log completion
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        logger.info(
            f"Transformation complete: {result['metadata']['conversations_count']} conversations, {result['metadata']['messages_count']} messages"
        )
        logger.debug(f"Transformation completed in {duration:.2f} seconds")

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

        if "conversations" not in raw_data:
            error_msg = "Raw data must contain a 'conversations' key"
            logger.error(error_msg)
            raise ValueError(error_msg)

        if not isinstance(raw_data["conversations"], dict):
            error_msg = f"Conversations must be a dictionary, got {type(raw_data['conversations']).__name__}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def _transform_conversation(
        self,
        conversation_id: str,
        conversation_data: Dict[str, Any],
        user_id: Optional[str] = None,
        user_display_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Transform a single conversation.

        Args:
            conversation_id: ID of the conversation
            conversation_data: Raw conversation data
            user_id: Optional ID of the user
            user_display_name: Optional display name of the user

        Returns:
            Transformed conversation data
        """
        # Get user ID from context if not provided
        if user_id is None and self.context and hasattr(self.context, "user_id"):
            user_id = self.context.user_id
        elif (
            user_id is None
            and self.context
            and hasattr(self.context, "user_display_name")
        ):
            # Generate a user ID from the display name if available
            user_id = f"user_{hash(self.context.user_display_name) % 10000}"
        elif user_id is None:
            # Default user ID if nothing else is available
            user_id = "unknown_user"

        # Get user display name from context if not provided
        if (
            user_display_name is None
            and self.context
            and hasattr(self.context, "user_display_name")
        ):
            user_display_name = self.context.user_display_name

        # Extract conversation properties
        properties = conversation_data.get("Properties", {})
        message_list = conversation_data.get("MessageList", [])

        # Get conversation type and participants
        conversation_type = properties.get("conversationType", "unknown")
        display_name = properties.get("displayName", "")
        participants = self._extract_participants(
            properties.get("participants", []), user_id, user_display_name
        )

        # Create transformed conversation structure
        transformed_conversation = {
            "id": conversation_id,
            "display_name": display_name,
            "type": conversation_type,
            "participants": participants,
            "created_at": properties.get("creationTime", ""),
            "last_message_at": properties.get("lastUpdatedTime", ""),
            "messages": [],
            "metadata": {
                "message_count": len(message_list),
                "participant_count": len(participants),
            },
        }

        # Transform messages
        if message_list:
            if self.parallel_processing and len(message_list) > self.chunk_size:
                # Process messages in parallel
                transformed_messages = self._transform_messages_parallel(
                    message_list, conversation_id
                )
            else:
                # Process messages sequentially
                transformed_messages = self._transform_messages_sequential(
                    message_list, conversation_id
                )

            # Sort messages by timestamp
            transformed_conversation["messages"] = sorted(
                transformed_messages, key=lambda m: m.get("timestamp", "0")
            )

        return transformed_conversation

    def _extract_participants(
        self,
        participants_data: List[Dict[str, Any]],
        user_id: str,
        user_display_name: Optional[str],
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
            mri = participant.get("mri", "")
            display_name = participant.get("displayName", "")

            # Check if this is the current user
            is_self = mri == user_id

            # Use provided user display name if this is the current user and no display name is available
            if is_self and not display_name and user_display_name:
                display_name = user_display_name

            # Create transformed participant
            transformed_participant = {
                "id": mri,
                "display_name": display_name,
                "is_self": is_self,
            }

            transformed_participants.append(transformed_participant)

        return transformed_participants

    def _transform_messages_parallel(
        self, messages: List[Dict[str, Any]], conversation_id: str
    ) -> List[Dict[str, Any]]:
        """Transform messages in parallel using multiple workers.

        Args:
            messages: List of raw messages
            conversation_id: ID of the conversation

        Returns:
            List of transformed messages
        """
        # Split messages into chunks
        chunks = [
            messages[i : i + self.chunk_size]
            for i in range(0, len(messages), self.chunk_size)
        ]
        transformed_messages = []

        # Process chunks in parallel
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers
        ) as executor:
            # Submit tasks
            future_to_chunk = {
                executor.submit(
                    self._transform_messages_sequential, chunk, conversation_id
                ): i
                for i, chunk in enumerate(chunks)
            }

            # Collect results
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk_index = future_to_chunk[future]
                try:
                    chunk_result = future.result()
                    transformed_messages.extend(chunk_result)
                    logger.debug(
                        f"Processed message chunk {chunk_index+1}/{len(chunks)} for conversation {conversation_id}"
                    )
                except Exception as e:
                    logger.error(
                        f"Error processing message chunk {chunk_index+1}/{len(chunks)} for conversation {conversation_id}: {e}"
                    )
                    if self.context:
                        self.context.record_error(
                            "transform",
                            f"Error processing message chunk for conversation {conversation_id}: {e}",
                        )

        return transformed_messages

    def _transform_messages_sequential(
        self, messages: List[Dict[str, Any]], conversation_id: str
    ) -> List[Dict[str, Any]]:
        """Transform messages sequentially.

        Args:
            messages: List of raw messages
            conversation_id: ID of the conversation

        Returns:
            List of transformed messages
        """
        transformed_messages = []

        # Initialize attachment handler if needed
        attachment_handler = None
        if self.context and self.context.download_attachments:
            # Create attachments directory if it doesn't exist
            if self.context.attachments_dir and not os.path.exists(
                self.context.attachments_dir
            ):
                os.makedirs(self.context.attachments_dir, exist_ok=True)

            # Initialize attachment handler
            attachment_handler = AttachmentHandler(
                storage_dir=self.context.attachments_dir,
            )
            logger.info(
                f"Initialized attachment handler with storage directory: {self.context.attachments_dir}"
            )

        for message in messages:
            try:
                # Get message type
                message_type = message.get("messagetype", "unknown")

                # Get appropriate handler for this message type
                handler = self.message_handler_factory.get_handler(message_type)

                if handler:
                    # Extract content
                    content_html = message.get("content", "")
                    content_text = self.content_extractor.extract_cleaned_content(
                        content_html
                    )

                    # Transform message using handler
                    transformed_message = handler.extract_structured_data(message)

                    # Add extracted content
                    transformed_message["content_html"] = content_html
                    transformed_message["content_text"] = content_text

                    # Process attachments if enabled
                    if attachment_handler and "attachments" in transformed_message:
                        try:
                            # Process attachments
                            processed_message = (
                                attachment_handler.process_message_attachments(
                                    transformed_message
                                )
                            )
                            transformed_message = processed_message
                            logger.debug(
                                f"Processed {len(transformed_message.get('attachments', []))} attachments for message in conversation {conversation_id}"
                            )
                        except Exception as e:
                            logger.warning(
                                f"Error processing attachments for message in conversation {conversation_id}: {e}"
                            )
                            if self.context:
                                self.context.record_error(
                                    "transform",
                                    f"Error processing attachments for message in conversation {conversation_id}: {e}",
                                )

                    # Add to result
                    transformed_messages.append(transformed_message)
                else:
                    logger.warning(f"No handler found for message type: {message_type}")
            except Exception as e:
                logger.error(
                    f"Error transforming message in conversation {conversation_id}: {e}"
                )
                if self.context:
                    self.context.record_error(
                        "transform",
                        f"Error transforming message in conversation {conversation_id}: {e}",
                    )

        return transformed_messages
