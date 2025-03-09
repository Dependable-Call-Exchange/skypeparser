"""
Message processor for transforming messages.

This module provides the MessageProcessor class that handles
transformation of messages using sequential or parallel processing.
"""

import concurrent.futures
import logging
from typing import List, Dict, Any, Callable, Optional

from src.logging.new_structured_logging import get_logger

logger = get_logger(__name__)


class MessageProcessor:
    """Handles transformation of messages using sequential or parallel processing."""

    def __init__(
        self,
        parallel_processing: bool = True,
        chunk_size: int = 1000,
        max_workers: Optional[int] = None,
    ):
        """Initialize the message processor.

        Args:
            parallel_processing: Whether to use parallel processing
            chunk_size: Size of chunks for parallel processing
            max_workers: Maximum number of worker threads
        """
        self.parallel_processing = parallel_processing
        self.chunk_size = chunk_size
        self.max_workers = max_workers
        self.metrics = {
            "chunk_count": 0,
            "processing_times": [],
            "chunk_sizes": [],
        }

    def transform_messages(
        self,
        messages: List[Dict[str, Any]],
        transform_func: Callable[[Dict[str, Any]], Dict[str, Any]],
        context: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Transform messages using the provided function.

        Args:
            messages: Messages to transform
            transform_func: Function to transform each message
            context: Optional context to pass to the transform function

        Returns:
            Transformed messages
        """
        if not messages:
            logger.debug("No messages to transform")
            return []

        logger.debug(f"Transforming {len(messages)} messages")

        if self.parallel_processing and len(messages) > self.chunk_size:
            logger.debug(f"Using parallel processing with chunk size {self.chunk_size}")
            return self._transform_parallel(messages, transform_func, context)

        logger.debug("Using sequential processing")
        return self._transform_sequential(messages, transform_func, context)

    def _transform_parallel(
        self,
        messages: List[Dict[str, Any]],
        transform_func: Callable[[Dict[str, Any]], Dict[str, Any]],
        context: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Transform messages in parallel.

        Args:
            messages: Messages to transform
            transform_func: Function to transform each message
            context: Optional context to pass to the transform function

        Returns:
            Transformed messages
        """
        # Split messages into chunks
        chunks = [
            messages[i : i + self.chunk_size]
            for i in range(0, len(messages), self.chunk_size)
        ]

        # Update metrics
        self.metrics["chunk_count"] += len(chunks)
        self.metrics["chunk_sizes"].extend([len(chunk) for chunk in chunks])

        transformed_messages = []

        # Process chunks in parallel
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit tasks
            future_to_chunk = {
                executor.submit(
                    self._transform_chunk, chunk, transform_func, i, context
                ): (chunk, i)
                for i, chunk in enumerate(chunks)
            }

            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_chunk):
                chunk, chunk_index = future_to_chunk[future]
                try:
                    result = future.result()
                    transformed_messages.extend(result)
                    logger.debug(f"Processed chunk {chunk_index + 1}/{len(chunks)}")
                except Exception as e:
                    logger.error(f"Error processing chunk {chunk_index + 1}/{len(chunks)}: {e}")
                    raise

        return transformed_messages

    def _transform_sequential(
        self,
        messages: List[Dict[str, Any]],
        transform_func: Callable[[Dict[str, Any]], Dict[str, Any]],
        context: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Transform messages sequentially.

        Args:
            messages: Messages to transform
            transform_func: Function to transform each message
            context: Optional context to pass to the transform function

        Returns:
            Transformed messages
        """
        # Update metrics
        self.metrics["chunk_count"] += 1
        self.metrics["chunk_sizes"].append(len(messages))

        return self._transform_chunk(messages, transform_func, 0, context)

    def _transform_chunk(
        self,
        chunk: List[Dict[str, Any]],
        transform_func: Callable[[Dict[str, Any]], Dict[str, Any]],
        chunk_index: int,
        context: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """Transform a chunk of messages.

        Args:
            chunk: Chunk of messages to transform
            transform_func: Function to transform each message
            chunk_index: Index of the chunk
            context: Optional context to pass to the transform function

        Returns:
            Transformed messages
        """
        transformed_messages = []

        # Handle None chunks gracefully
        if chunk is None:
            logger.warning(f"Received None chunk at index {chunk_index}")
            return transformed_messages

        # Validate chunk is iterable
        try:
            iterator = iter(chunk)
        except TypeError:
            logger.error(f"Chunk at index {chunk_index} is not iterable: {type(chunk)}")
            return transformed_messages

        for message in chunk:
            # Skip non-dict messages
            if not isinstance(message, dict):
                logger.debug(f"Skipping non-dict message in chunk {chunk_index}: {type(message)}")
                continue

            try:
                # Apply transform function with or without context
                transformed_message = None
                if context:
                    transformed_message = transform_func(message, **context)
                else:
                    transformed_message = transform_func(message)

                # Only append non-empty results
                if transformed_message:
                    transformed_messages.append(transformed_message)
                else:
                    logger.debug(f"Transform function returned empty result for message in chunk {chunk_index}")
            except Exception as e:
                logger.error(f"Error transforming message in chunk {chunk_index}: {e}")
                # Continue processing other messages
                continue

        return transformed_messages