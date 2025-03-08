"""
Transformer module for the ETL pipeline.

This module provides the Transformer class that transforms raw Skype export data
into a structured format suitable for database loading.
"""

import concurrent.futures
import json
import logging
import os
import time
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
from src.utils.new_structured_logging import (
    get_logger,
    log_execution_time,
    log_call,
    handle_errors,
    with_context,
    LogContext,
    log_metrics
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

        self.structured_data_extractor = structured_data_extractor
        if self.structured_data_extractor is None:
            # Get structured data extractor from service registry if available
            try:
                self.structured_data_extractor = get_service("structured_data_extractor")
            except (ImportError, KeyError):
                self.structured_data_extractor = None

        # Log initialization
        logger.info(
            "Initialized Transformer",
            extra={
                "parallel_processing": self.parallel_processing,
                "chunk_size": self.chunk_size,
                "max_workers": self.max_workers,
                "has_content_extractor": self.content_extractor is not None,
                "has_message_handler_factory": self.message_handler_factory is not None,
                "has_structured_data_extractor": self.structured_data_extractor is not None,
            }
        )

    @log_execution_time(level=logging.INFO)
    @with_context(operation="transform")
    def transform(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform raw Skype export data into a structured format.

        Args:
            raw_data: Raw data from the extractor

        Returns:
            Transformed data
        """
        # Start timer
        self._metrics["start_time"] = time.time()

        try:
            # Validate input data
            self._validate_input_data(raw_data)

            # Extract structured data
            structured_data = self._extract_structured_data(raw_data)

            # Transform conversations
            conversations = self._transform_conversations(structured_data)

            # Transform messages
            messages_by_conversation = self._transform_messages(structured_data)

            # Combine transformed data
            transformed_data = {
                "conversations": conversations,
                "messages": messages_by_conversation,
                "user_id": structured_data.get("user_id", ""),
                "user_display_name": structured_data.get("user_display_name", ""),
                "export_date": structured_data.get("export_date", datetime.now().isoformat()),
            }

            # Update context if available
            if self.context is not None:
                self.context.transformed_data = transformed_data
                if hasattr(self.context, "user_id") and not self.context.user_id:
                    self.context.user_id = transformed_data["user_id"]
                if hasattr(self.context, "user_display_name") and not self.context.user_display_name:
                    self.context.user_display_name = transformed_data["user_display_name"]
                if hasattr(self.context, "export_date") and not self.context.export_date:
                    self.context.export_date = transformed_data["export_date"]

            # End timer
            self._metrics["end_time"] = time.time()
            self._metrics["transformation_time_ms"] = (self._metrics["end_time"] - self._metrics["start_time"]) * 1000

            # Update metrics
            self._metrics["conversation_count"] = len(conversations)
            self._metrics["message_count"] = sum(len(msgs) for msgs in messages_by_conversation.values())

            # Log success
            logger.info(
                "Data transformed successfully",
                extra={
                    "metrics": {
                        "transformation_time_ms": self._metrics["transformation_time_ms"],
                        "conversation_count": self._metrics["conversation_count"],
                        "message_count": self._metrics["message_count"],
                        "chunk_count": self._metrics["chunk_count"],
                        "avg_chunk_size": sum(self._metrics["chunk_sizes"]) / len(self._metrics["chunk_sizes"]) if self._metrics["chunk_sizes"] else 0,
                        "avg_chunk_time_ms": sum(self._metrics["processing_times"]) / len(self._metrics["processing_times"]) if self._metrics["processing_times"] else 0,
                    }
                }
            )

            return transformed_data

        except Exception as e:
            # Log error
            logger.error(
                f"Error transforming data: {e}",
                exc_info=True,
                extra={"error": str(e)}
            )
            raise

    @handle_errors(log_level="ERROR", default_message="Error validating input data")
    def _validate_input_data(self, raw_data: Dict[str, Any]) -> None:
        """Validate input data.

        Args:
            raw_data: Raw data from the extractor

        Raises:
            ValueError: If input data is invalid
        """
        # Basic validation
        if not isinstance(raw_data, dict):
            raise ValueError("Raw data must be a dictionary")

        # Check for required keys
        if "messages" not in raw_data:
            raise ValueError("Raw data must contain 'messages' key")

        # Log validation success
        logger.debug(
            "Input data validated",
            extra={
                "raw_data_keys": list(raw_data.keys()),
                "message_count": len(raw_data.get("messages", [])),
            }
        )

    @handle_errors(log_level="ERROR", default_message="Error extracting structured data")
    def _extract_structured_data(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from raw data.

        Args:
            raw_data: Raw data from the extractor

        Returns:
            Structured data
        """
        # Log extraction start
        logger.debug("Extracting structured data")

        # Use structured data extractor if available
        start_time = time.time()
        if self.structured_data_extractor:
            structured_data = self.structured_data_extractor.extract(raw_data)
        else:
            # Fallback to direct extraction
            structured_data = self._extract_structured_data_fallback(raw_data)

        # Calculate extraction time
        extraction_time_ms = (time.time() - start_time) * 1000

        # Log extraction completion
        logger.debug(
            "Structured data extracted",
            extra={
                "extraction_time_ms": extraction_time_ms,
                "structured_data_keys": list(structured_data.keys()),
            }
        )

        return structured_data

    def _extract_structured_data_fallback(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from raw data (fallback implementation).

        Args:
            raw_data: Raw data from the extractor

        Returns:
            Structured data
        """
        # Extract user information
        user_id = ""
        user_display_name = ""

        # Try to extract user information from various sources
        if "userProfile" in raw_data:
            user_profile = raw_data["userProfile"]
            user_id = user_profile.get("id", "")
            user_display_name = user_profile.get("displayName", "")
        elif "user" in raw_data:
            user = raw_data["user"]
            user_id = user.get("id", "")
            user_display_name = user.get("displayName", "")

        # Extract export date
        export_date = raw_data.get("exportDate", datetime.now().isoformat())

        # Extract messages
        messages = raw_data.get("messages", [])

        # Extract conversations
        conversations = {}
        if "conversations" in raw_data:
            conversations = raw_data["conversations"]
        elif "endpoints" in raw_data:
            # Convert endpoints to conversations
            endpoints = raw_data["endpoints"]
            for endpoint_id, endpoint in endpoints.items():
                conversations[endpoint_id] = {
                    "id": endpoint_id,
                    "displayName": endpoint.get("displayName", ""),
                    "type": endpoint.get("type", ""),
                    "members": endpoint.get("members", []),
                }

        # Combine structured data
        structured_data = {
            "user_id": user_id,
            "user_display_name": user_display_name,
            "export_date": export_date,
            "messages": messages,
            "conversations": conversations,
        }

        return structured_data

    @handle_errors(log_level="ERROR", default_message="Error transforming conversations")
    def _transform_conversations(self, structured_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform conversations.

        Args:
            structured_data: Structured data

        Returns:
            Transformed conversations
        """
        # Log transformation start
        logger.debug("Transforming conversations")

        # Get conversations from structured data
        conversations = structured_data.get("conversations", {})

        # Transform each conversation
        transformed_conversations = {}
        start_time = time.time()

        for conv_id, conv_data in conversations.items():
            transformed_conversations[conv_id] = self._transform_conversation(conv_id, conv_data)

        # Calculate transformation time
        transformation_time_ms = (time.time() - start_time) * 1000

        # Log transformation completion
        logger.debug(
            "Conversations transformed",
            extra={
                "transformation_time_ms": transformation_time_ms,
                "conversation_count": len(transformed_conversations),
            }
        )

        return transformed_conversations

    @handle_errors(log_level="ERROR", default_message="Error transforming conversation")
    def _transform_conversation(self, conv_id: str, conv_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform a conversation.

        Args:
            conv_id: Conversation ID
            conv_data: Conversation data

        Returns:
            Transformed conversation
        """
        # Extract conversation properties
        display_name = conv_data.get("displayName", "")
        conv_type = conv_data.get("type", "")
        version = conv_data.get("version", 0)
        properties = conv_data.get("properties", {})
        thread_properties = conv_data.get("threadProperties", {})
        members = conv_data.get("members", [])

        # Transform conversation
        transformed_conversation = {
            "id": conv_id,
            "displayName": display_name,
            "type": conv_type,
            "version": version,
            "properties": properties,
            "threadProperties": thread_properties,
            "members": members,
        }

        return transformed_conversation

    @handle_errors(log_level="ERROR", default_message="Error transforming messages")
    def _transform_messages(self, structured_data: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        """Transform messages.

        Args:
            structured_data: Structured data

        Returns:
            Transformed messages by conversation
        """
        # Log transformation start
        logger.info("Transforming messages")

        # Get messages from structured data
        messages = structured_data.get("messages", [])

        # Group messages by conversation
        messages_by_conversation = {}
        for message in messages:
            conv_id = message.get("conversationId", "default")
            if conv_id not in messages_by_conversation:
                messages_by_conversation[conv_id] = []
            messages_by_conversation[conv_id].append(message)

        # Transform messages for each conversation
        transformed_messages = {}
        for conv_id, conv_messages in messages_by_conversation.items():
            with LogContext(conversation_id=conv_id):
                transformed_messages[conv_id] = self._transform_messages_for_conversation(conv_id, conv_messages)

        # Log transformation completion
        logger.info(
            "Messages transformed",
            extra={
                "conversation_count": len(transformed_messages),
                "message_count": sum(len(msgs) for msgs in transformed_messages.values()),
            }
        )

        return transformed_messages

    @handle_errors(log_level="ERROR", default_message="Error transforming messages for conversation")
    def _transform_messages_for_conversation(self, conv_id: str, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform messages for a conversation.

        Args:
            conv_id: Conversation ID
            messages: Messages for the conversation

        Returns:
            Transformed messages
        """
        # Log transformation start
        logger.debug(
            f"Transforming {len(messages)} messages for conversation: {conv_id}",
            extra={
                "conversation_id": conv_id,
                "message_count": len(messages),
            }
        )

        # Process messages in chunks
        transformed_messages = []

        if self.parallel_processing and len(messages) > self.chunk_size:
            # Process in parallel
            transformed_messages = self._transform_messages_parallel(conv_id, messages)
        else:
            # Process sequentially
            transformed_messages = self._transform_messages_sequential(conv_id, messages)

        # Log transformation completion
        logger.debug(
            f"Transformed {len(transformed_messages)} messages for conversation: {conv_id}",
            extra={
                "conversation_id": conv_id,
                "message_count": len(transformed_messages),
            }
        )

        return transformed_messages

    @handle_errors(log_level="ERROR", default_message="Error transforming messages in parallel")
    def _transform_messages_parallel(self, conv_id: str, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform messages in parallel.

        Args:
            conv_id: Conversation ID
            messages: Messages to transform

        Returns:
            Transformed messages
        """
        # Log parallel processing start
        logger.debug(
            f"Processing {len(messages)} messages in parallel for conversation: {conv_id}",
            extra={
                "conversation_id": conv_id,
                "message_count": len(messages),
                "chunk_size": self.chunk_size,
                "max_workers": self.max_workers,
            }
        )

        # Split messages into chunks
        chunks = [
            messages[i:i + self.chunk_size]
            for i in range(0, len(messages), self.chunk_size)
        ]

        # Update metrics
        self._metrics["chunk_count"] += len(chunks)
        self._metrics["chunk_sizes"].extend([len(chunk) for chunk in chunks])

        # Process chunks in parallel
        transformed_messages = []
        start_time = time.time()

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit tasks
            future_to_chunk = {
                executor.submit(self._transform_message_chunk, conv_id, chunk, i): (chunk, i)
                for i, chunk in enumerate(chunks)
            }

            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk, chunk_index = future_to_chunk[future]
                try:
                    result = future.result()
                    transformed_messages.extend(result)

                    # Log chunk completion
                    logger.debug(
                        f"Processed chunk {chunk_index + 1}/{len(chunks)} for conversation: {conv_id}",
                        extra={
                            "conversation_id": conv_id,
                            "chunk_index": chunk_index,
                            "chunk_size": len(chunk),
                            "total_chunks": len(chunks),
                        }
                    )
                except Exception as e:
                    # Log chunk error
                    logger.error(
                        f"Error processing chunk {chunk_index + 1}/{len(chunks)} for conversation {conv_id}: {e}",
                        exc_info=True,
                        extra={
                            "conversation_id": conv_id,
                            "chunk_index": chunk_index,
                            "chunk_size": len(chunk),
                            "total_chunks": len(chunks),
                            "error": str(e),
                        }
                    )
                    raise

        # Calculate processing time
        processing_time_ms = (time.time() - start_time) * 1000
        self._metrics["processing_times"].append(processing_time_ms)

        # Log parallel processing completion
        logger.debug(
            f"Parallel processing completed for conversation: {conv_id}",
            extra={
                "conversation_id": conv_id,
                "message_count": len(messages),
                "transformed_count": len(transformed_messages),
                "chunk_count": len(chunks),
                "processing_time_ms": processing_time_ms,
            }
        )

        return transformed_messages

    @handle_errors(log_level="ERROR", default_message="Error transforming messages sequentially")
    def _transform_messages_sequential(self, conv_id: str, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Transform messages sequentially.

        Args:
            conv_id: Conversation ID
            messages: Messages to transform

        Returns:
            Transformed messages
        """
        # Log sequential processing start
        logger.debug(
            f"Processing {len(messages)} messages sequentially for conversation: {conv_id}",
            extra={
                "conversation_id": conv_id,
                "message_count": len(messages),
            }
        )

        # Process messages
        start_time = time.time()
        transformed_messages = self._transform_message_chunk(conv_id, messages, 0)

        # Calculate processing time
        processing_time_ms = (time.time() - start_time) * 1000

        # Update metrics
        self._metrics["chunk_count"] += 1
        self._metrics["chunk_sizes"].append(len(messages))
        self._metrics["processing_times"].append(processing_time_ms)

        # Log sequential processing completion
        logger.debug(
            f"Sequential processing completed for conversation: {conv_id}",
            extra={
                "conversation_id": conv_id,
                "message_count": len(messages),
                "transformed_count": len(transformed_messages),
                "processing_time_ms": processing_time_ms,
            }
        )

        return transformed_messages

    @handle_errors(log_level="ERROR", default_message="Error transforming message chunk")
    def _transform_message_chunk(self, conv_id: str, messages: List[Dict[str, Any]], chunk_index: int) -> List[Dict[str, Any]]:
        """Transform a chunk of messages.

        Args:
            conv_id: Conversation ID
            messages: Messages to transform
            chunk_index: Index of the chunk

        Returns:
            Transformed messages
        """
        # Transform each message in the chunk
        transformed_messages = []

        for message in messages:
            transformed_message = self._transform_message(conv_id, message)
            if transformed_message:
                transformed_messages.append(transformed_message)

        return transformed_messages

    @handle_errors(log_level="ERROR", default_message="Error transforming message")
    def _transform_message(self, conv_id: str, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Transform a message.

        Args:
            conv_id: Conversation ID
            message: Message to transform

        Returns:
            Transformed message or None if the message should be skipped
        """
        # Extract message properties
        message_id = message.get("id", "")
        if not message_id:
            message_id = message.get("clientmessageid", "")

        sender_id = message.get("from", "")
        if isinstance(sender_id, dict):
            sender_id = sender_id.get("id", "")

        sender_display_name = message.get("senderDisplayName", "")
        if not sender_display_name and "from" in message and isinstance(message["from"], dict):
            sender_display_name = message["from"].get("displayName", "")

        timestamp = message.get("originalarrivaltime", "")
        if not timestamp:
            timestamp = message.get("timestamp", "")
            if not timestamp:
                timestamp = message.get("composetime", "")

        content = message.get("content", "")
        if not content and "text" in message:
            content = message["text"]

        message_type = message.get("messagetype", "")
        if not message_type:
            message_type = message.get("type", "")
            if not message_type:
                # Try to determine message type from content
                if "<URIObject" in content:
                    message_type = "RichText/UriObject"
                elif "<a href" in content:
                    message_type = "RichText/Html"
                else:
                    message_type = "Text"

        # Extract additional properties
        properties = {}
        for key, value in message.items():
            if key not in ["id", "clientmessageid", "from", "senderDisplayName",
                          "originalarrivaltime", "timestamp", "composetime",
                          "content", "text", "messagetype", "type", "conversationId"]:
                properties[key] = value

        # Use content extractor if available
        if self.content_extractor and content:
            try:
                extracted_content = self.content_extractor.extract_content(content, message_type)
                if extracted_content:
                    properties["extracted_content"] = extracted_content
            except Exception as e:
                logger.warning(
                    f"Error extracting content for message {message_id}: {e}",
                    extra={
                        "message_id": message_id,
                        "message_type": message_type,
                        "error": str(e),
                    }
                )

        # Use message handler if available
        if self.message_handler_factory:
            try:
                handler = self.message_handler_factory.get_handler(message_type)
                if handler:
                    try:
                        handler_result = handler.handle(message)
                        if handler_result:
                            properties["handler_result"] = handler_result
                    except Exception as e:
                        logger.warning(
                            f"Error handling message {message_id} with type {message_type}: {e}",
                            extra={
                                "message_id": message_id,
                                "message_type": message_type,
                                "error": str(e),
                            }
                        )
            except Exception as e:
                logger.warning(
                    f"Error getting message handler for message {message_id}: {e}",
                    extra={
                        "message_id": message_id,
                        "error": str(e),
                    }
                )

        # Create transformed message
        transformed_message = {
            "id": message_id,
            "conversationId": conv_id,
            "senderId": sender_id,
            "senderDisplayName": sender_display_name,
            "timestamp": timestamp,
            "content": content,
            "messageType": message_type,
            "properties": properties,
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
