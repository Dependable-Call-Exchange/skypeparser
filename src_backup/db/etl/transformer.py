"""
Transformer module for the ETL pipeline.

This module provides the Transformer class that transforms raw Skype export data
into a structured format suitable for database loading.
"""

import json
import logging
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from src.utils.attachment_handler import AttachmentHandler
from src.utils.conversation_processor import ConversationProcessor
from src.utils.data_validator import DataValidator
from src.utils.di import get_service
from src.utils.extractors import IExtractor, DefaultExtractor, CallableExtractor, ObjectExtractor
from src.utils.interfaces import (
    ContentExtractorProtocol,
    MessageHandlerFactoryProtocol,
    StructuredDataExtractorProtocol,
    TransformerProtocol,
)
from src.utils.message_processor import MessageProcessor
from src.utils.new_structured_logging import (
    get_logger,
    log_execution_time,
    log_call,
    handle_errors,
    with_context,
    LogContext,
)

from .context import ETLContext

logger = get_logger(__name__)


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
        # Initialize metrics
        self._metrics = {
            "start_time": None,
            "end_time": None,
            "transformation_time_ms": 0,
            "message_count": 0,
            "conversation_count": 0,
            "chunk_count": 0,
            "processing_times": [],
            "chunk_sizes": [],
        }

        # Set context
        self.context = context

        # Set performance parameters
        if context is not None:
            if hasattr(context, "parallel_processing"):
                parallel_processing = context.parallel_processing
            if hasattr(context, "chunk_size"):
                chunk_size = context.chunk_size
            if hasattr(context, "max_workers"):
                max_workers = context.max_workers

        self.parallel_processing = parallel_processing
        self.chunk_size = chunk_size
        self.max_workers = max_workers

        # Set component dependencies
        self.content_extractor = content_extractor
        if self.content_extractor is None:
            # Get content extractor from service registry if available
            try:
                self.content_extractor = get_service("content_extractor")
            except (ImportError, KeyError):
                self.content_extractor = None

        self.message_handler_factory = message_handler_factory
        if self.message_handler_factory is None:
            # Get message handler factory from service registry if available
            try:
                self.message_handler_factory = get_service("message_handler_factory")
            except (ImportError, KeyError):
                self.message_handler_factory = None

        # Create extractor based on the provided structured_data_extractor
        if structured_data_extractor is None:
            # Get structured data extractor from service registry if available
            try:
                structured_data_extractor = get_service("structured_data_extractor")
            except (ImportError, KeyError):
                structured_data_extractor = None

        if structured_data_extractor is None:
            self.extractor = DefaultExtractor()
        elif callable(structured_data_extractor) and not hasattr(structured_data_extractor, 'extract'):
            self.extractor = CallableExtractor(structured_data_extractor)
        else:
            self.extractor = ObjectExtractor(structured_data_extractor)

        # Create processors
        self.validator = DataValidator(strict_mode=False)
        self.message_processor = MessageProcessor(
            parallel_processing=self.parallel_processing,
            chunk_size=self.chunk_size,
            max_workers=self.max_workers,
        )
        self.conversation_processor = ConversationProcessor()

        # Log initialization
        logger.info(
            "Initialized Transformer",
            extra={
                "parallel_processing": self.parallel_processing,
                "chunk_size": self.chunk_size,
                "max_workers": self.max_workers,
                "has_content_extractor": self.content_extractor is not None,
                "has_message_handler_factory": self.message_handler_factory is not None,
                "has_structured_data_extractor": structured_data_extractor is not None,
            }
        )

    @log_execution_time(level=logging.INFO)
    @log_call(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error transforming data")
    def transform(
        self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Transform raw data into structured data.

        Args:
            raw_data: Raw data from the extractor
            user_display_name: Display name of the user (optional)

        Returns:
            Transformed data
        """
        logger.info("Transforming data")

        # Validate input data
        if not self.validator.validate(raw_data):
            logger.warning(
                "Input data validation failed",
                extra={"errors": self.validator.get_errors()}
            )
            # Continue with best effort transformation

        # Default empty structured data in case of extraction failure
        structured_data = {
            "user_id": "",
            "export_date": "",
            "conversations": {}
        }

        try:
            # Use the structured data extractor to handle the raw data
            # This will handle both the new format with 'messages' array and the direct format
            structured_data = self.extractor.extract(raw_data)
            logger.debug(f"Extracted structured data with keys: {list(structured_data.keys())}")

            # Fallback to direct extraction if structured_data_extractor didn't handle it
            if not structured_data.get("user_id") and not structured_data.get("conversations"):
                logger.debug("Structured data extractor didn't provide complete data, falling back to direct extraction")
                # Normalize the raw data to support different formats
                if "messages" in raw_data and isinstance(raw_data["messages"], list):
                    # This appears to be the raw Skype export format
                    # It contains a 'messages' array with user, export date, and conversations info
                    if len(raw_data["messages"]) > 0:
                        first_message_obj = raw_data["messages"][0]

                        # Check if this is a Skype export format
                        if "userId" in first_message_obj and "exportDate" in first_message_obj and "conversations" in first_message_obj:
                            # This is the expected format
                            logger.debug("Found Skype export format with userId, exportDate, and conversations")
                            structured_data = {
                                "user_id": first_message_obj.get("userId", ""),
                                "export_date": first_message_obj.get("exportDate", ""),
                                "conversations": first_message_obj.get("conversations", [])
                            }
                        else:
                            # If it's just a list of messages, wrap it
                            logger.debug("Found array of messages, assuming these are the conversations")
                            structured_data = {
                                "user_id": user_display_name or "",
                                "export_date": datetime.now().isoformat(),
                                "conversations": raw_data["messages"]
                            }
        except Exception as e:
            logger.error(f"Error extracting structured data: {e}")
            # Continue with default empty structured data

        # Add user display name if provided
        if user_display_name:
            structured_data["user_display_name"] = user_display_name

        # Transform conversations
        transformed_conversations = self._transform_conversations(structured_data)

        # Count total messages across all conversations
        total_messages = 0
        for conv_id, conv in transformed_conversations.items():
            total_messages += len(conv.get("messages", []))

        # Combine transformed data
        transformed_data = {
            "metadata": {
                "user_id": structured_data.get("user_id", ""),
                "user_display_name": structured_data.get("user_display_name", ""),
                "export_date": structured_data.get("export_date", ""),
                "total_conversations": len(transformed_conversations),
                "total_messages": total_messages,
            },
            "conversations": transformed_conversations,
            "messages": {},  # For backward compatibility
        }

        # Log transformation metrics
        logger.debug(f"Transformation metrics: {self._metrics}")

        logger.info("Data transformed successfully")
        return transformed_data

    def _transform_conversations(self, structured_data: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Transform conversations from structured data.

        Args:
            structured_data: Structured data from the extractor

        Returns:
            Dictionary of transformed conversations
        """
        logger.debug("Transforming conversations")
        logger.debug(f"Structured data keys: {list(structured_data.keys())}")

        # Check if we have conversations in the structured data
        if "conversations" in structured_data and structured_data["conversations"]:
            conversations = structured_data["conversations"]

            # Handle both dict and list formats
            if isinstance(conversations, dict):
                return self.conversation_processor.transform_conversations(
                    conversations, self._transform_conversation
                )
            elif isinstance(conversations, list):
                return self.conversation_processor.transform_conversation_list(
                    conversations, self._transform_conversation
                )

        # If we don't have conversations, return empty dict
        logger.debug("No conversations found in structured data")
        return {}

    def _transform_conversation(self, conv_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a conversation.

        Args:
            conv_data: Conversation data

        Returns:
            Transformed conversation
        """
        logger.debug(f"Transforming conversation: {conv_data.get('id', 'unknown')}")

        # Extract conversation properties
        display_name = conv_data.get("displayName", "")
        if not display_name:
            logger.debug(f"Skipping conversation with no display name: {conv_data.get('id', 'unknown')}")
            return {}

        # Extract other properties
        conv_type = conv_data.get("type", "")
        version = conv_data.get("version", "")
        properties = conv_data.get("properties", {})
        thread_properties = conv_data.get("threadProperties", {})
        members = conv_data.get("members", [])

        # Extract messages
        messages = []
        message_list = conv_data.get("MessageList", [])
        if message_list:
            logger.debug(f"Found {len(message_list)} messages in MessageList")

            # Use message processor to transform messages
            messages = self.message_processor.transform_messages(
                message_list,
                lambda msg: self._transform_message(msg, conv_data.get("id", "")),
            )

        # Transform conversation
        transformed_conversation = {
            "id": conv_data.get("id", ""),
            "display_name": display_name,
            "type": conv_type,
            "version": version,
            "properties": properties,
            "thread_properties": thread_properties,
            "members": members,
            "messages": messages,
        }

        logger.debug(f"Conversation transformed: {conv_data.get('id', 'unknown')} with {len(messages)} messages")
        return transformed_conversation

    def _transform_message(self, msg_data: Dict[str, Any], conversation_id: str) -> Dict[str, Any]:
        """Transform a message.

        Args:
            msg_data: Message data
            conversation_id: ID of the conversation the message belongs to

        Returns:
            Transformed message
        """
        # Extract message properties
        msg_id = msg_data.get("id", "")
        if not msg_id:
            logger.debug(f"Skipping message with no ID in conversation {conversation_id}")
            return {}

        # Extract message content
        content = msg_data.get("content", "")

        # Extract message type
        message_type = msg_data.get("messagetype", "")

        # Extract sender information
        from_id = msg_data.get("from_id", "")
        from_name = msg_data.get("from_name", "")

        # Extract timestamp
        timestamp = msg_data.get("originalarrivaltime", "")

        # Check if message is edited
        is_edited = msg_data.get("edittime") is not None

        # Clean content using content extractor
        cleaned_content = ""
        if self.content_extractor and content:
            cleaned_content = self.content_extractor.extract_cleaned_content(content)

        # Extract structured data using message handler factory
        structured_data = {}
        if self.message_handler_factory and message_type:
            try:
                handler = self.message_handler_factory.get_handler(message_type)
                if handler:
                    structured_data = handler.extract_structured_data(msg_data)
                    logger.debug(f"Extracted structured data for message type {message_type}")
            except Exception as e:
                logger.warning(f"Error extracting structured data for message {msg_id} of type {message_type}: {str(e)}")

        # Process attachments if present
        attachments = []
        if structured_data and 'attachments' in structured_data:
            attachments = structured_data['attachments']
        elif 'properties' in msg_data and 'attachments' in msg_data['properties']:
            attachments = msg_data['properties']['attachments']

        # Use the extractor for additional data extraction
        extracted_data = {}
        try:
            extracted_data = self.extractor.extract(msg_data) or {}
        except Exception as e:
            logger.warning(f"Error using structured data extractor for message {msg_id}: {str(e)}")

        # Transform message
        transformed_message = {
            "id": msg_id,
            "conversation_id": conversation_id,
            "content": content,
            "cleaned_content": cleaned_content,
            "message_type": message_type,
            "from_id": from_id,
            "from_name": from_name,
            "timestamp": timestamp,
            "is_edited": is_edited,
            "structured_data": structured_data,
            "extracted_data": extracted_data,
            "attachments": attachments
        }

        return transformed_message

    @handle_errors(log_level="ERROR", default_message="Error saving transformed data")
    def save_transformed_data(self, transformed_data: Dict[str, Any], output_path: Optional[str] = None) -> str:
        """Save transformed data to a file.

        Args:
            transformed_data: Transformed data
            output_path: Path to save the data (generated if not provided)

        Returns:
            Path to the saved file
        """
        # Generate output path if not provided
        if output_path is None:
            if not hasattr(self, "output_dir") or not self.output_dir:
                if self.context and hasattr(self.context, "output_dir"):
                    output_dir = self.context.output_dir
                else:
                    output_dir = "output"
            else:
                output_dir = self.output_dir

            # Create output directory if it doesn't exist
            os.makedirs(output_dir, exist_ok=True)

            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(output_dir, f"transformed_data_{timestamp}.json")

        # Log save start
        logger.info(
            f"Saving transformed data to: {output_path}",
            extra={"output_path": output_path}
        )

        # Save data
        start_time = time.time()

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Write data to file
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(transformed_data, f, indent=2)

        # Calculate save time
        save_time_ms = (time.time() - start_time) * 1000

        # Log save completion
        logger.info(
            f"Transformed data saved successfully to: {output_path}",
            extra={
                "output_path": output_path,
                "save_time_ms": save_time_ms,
                "file_size_bytes": os.path.getsize(output_path),
            }
        )

        return output_path
