"""
Mock implementations for testing.

This package provides organized mock implementations of various components
used in the Skype Parser, making it easier to test components in isolation.
The mocks are organized by component type to follow the Single Responsibility Principle.
"""

# Import and re-export all mocks for easy access
from .content_extractor import MockContentExtractor
from .database import MockDatabase
from .file_handler import MockFileHandler
from .message_handler import (
    MockMessageHandler,
    MockMessageHandlerFactory,
    mock_message_handler_factory,
)
from .message_processor import MockMessageProcessor
from .structured_data_extractor import (
    MockStructuredDataExtractor,
    mock_structured_data_extractor,
)
from .etl_components import MockExtractor, MockTransformer, MockLoader
from .progress_tracker import MockProgressTracker
from .validation_service import MockValidationService

__all__ = [
    # Content extraction mocks
    "MockContentExtractor",

    # Database mocks
    "MockDatabase",

    # File handling mocks
    "MockFileHandler",

    # Message handling mocks
    "MockMessageHandler",
    "MockMessageHandlerFactory",
    "mock_message_handler_factory",

    # Message processing mocks
    "MockMessageProcessor",

    # Structured data extraction mocks
    "MockStructuredDataExtractor",
    "mock_structured_data_extractor",

    # ETL component mocks
    "MockExtractor",
    "MockTransformer",
    "MockLoader",

    # Progress tracking mocks
    "MockProgressTracker",

    # Validation service mocks
    "MockValidationService",
]