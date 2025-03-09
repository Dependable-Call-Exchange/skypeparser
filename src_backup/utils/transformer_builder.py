"""
Transformer builder for creating transformer instances.

This module provides the TransformerBuilder class that simplifies
the creation of Transformer instances with various configurations.
"""

from typing import Any, Optional, Dict

from src.db.etl.context import ETLContext
from src.utils.extractors import IExtractor, DefaultExtractor
from src.utils.interfaces import (
    ContentExtractorProtocol,
    MessageHandlerFactoryProtocol,
)


class TransformerBuilder:
    """Builder for creating Transformer instances with various configurations."""

    def __init__(self):
        """Initialize the transformer builder with default values."""
        self._context = None
        self._parallel_processing = True
        self._chunk_size = 1000
        self._max_workers = None
        self._content_extractor = None
        self._message_handler_factory = None
        self._extractor = None
        self._output_dir = None

    def with_context(self, context: ETLContext) -> 'TransformerBuilder':
        """Set the ETL context.

        Args:
            context: ETL context

        Returns:
            Self for chaining
        """
        self._context = context
        return self

    def with_parallel_processing(self, enabled: bool = True) -> 'TransformerBuilder':
        """Set whether to use parallel processing.

        Args:
            enabled: Whether to enable parallel processing

        Returns:
            Self for chaining
        """
        self._parallel_processing = enabled
        return self

    def with_chunk_size(self, size: int) -> 'TransformerBuilder':
        """Set the chunk size for parallel processing.

        Args:
            size: Chunk size

        Returns:
            Self for chaining
        """
        self._chunk_size = size
        return self

    def with_max_workers(self, workers: int) -> 'TransformerBuilder':
        """Set the maximum number of worker threads.

        Args:
            workers: Maximum number of worker threads

        Returns:
            Self for chaining
        """
        self._max_workers = workers
        return self

    def with_content_extractor(self, extractor: ContentExtractorProtocol) -> 'TransformerBuilder':
        """Set the content extractor.

        Args:
            extractor: Content extractor

        Returns:
            Self for chaining
        """
        self._content_extractor = extractor
        return self

    def with_message_handler_factory(self, factory: MessageHandlerFactoryProtocol) -> 'TransformerBuilder':
        """Set the message handler factory.

        Args:
            factory: Message handler factory

        Returns:
            Self for chaining
        """
        self._message_handler_factory = factory
        return self

    def with_extractor(self, extractor: IExtractor) -> 'TransformerBuilder':
        """Set the structured data extractor.

        Args:
            extractor: Structured data extractor

        Returns:
            Self for chaining
        """
        self._extractor = extractor
        return self

    def with_output_dir(self, output_dir: str) -> 'TransformerBuilder':
        """Set the output directory.

        Args:
            output_dir: Output directory

        Returns:
            Self for chaining
        """
        self._output_dir = output_dir
        return self

    def build(self) -> Any:
        """Build a Transformer instance with the configured options.

        Returns:
            Transformer instance
        """
        # Import here to avoid circular imports
        from src.db.etl.transformer import Transformer

        # Create the transformer
        transformer = Transformer(
            context=self._context,
            parallel_processing=self._parallel_processing,
            chunk_size=self._chunk_size,
            max_workers=self._max_workers,
            content_extractor=self._content_extractor,
            message_handler_factory=self._message_handler_factory,
            structured_data_extractor=self._extractor,
        )

        # Set output directory if provided
        if self._output_dir:
            transformer.output_dir = self._output_dir

        return transformer