"""
Skype Parser Module

This module provides functionality for parsing Skype export files and extracting
meaningful data from them. It includes tools for cleaning and formatting message
content, handling different message types, and exporting conversations in various formats.

The module is organized into four main components:
- core_parser: Core functions for parsing Skype export data
- content_extractor: Functions for extracting structured data from message content
- file_output: Functions for outputting data to various file formats
- skype_parser: Command-line interface for parsing Skype export files
- exceptions: Custom exceptions for consistent error handling

Note: The parser_module.py file is deprecated and will be removed in a future version.
      Please use the functions from core_parser.py instead.
"""

import warnings

# Show deprecation warning for parser_module
warnings.warn(
    "The parser_module.py file is deprecated and will be removed in a future version. "
    "Please use the functions from core_parser.py instead.",
    DeprecationWarning,
    stacklevel=2
)

# Import core parsing functions
from .core_parser import (
    timestamp_parser,
    content_parser,
    pretty_quotes,
    type_parser,
    banner_constructor,
    parse_skype_data,
    id_selector
)

# Import content extraction functions
from .content_extractor import (
    extract_content_data,
    format_content_with_markup,
    format_content_with_regex,
    ContentExtractor
)

# Import file output functions
from .file_output import (
    write_to_file,
    output_structured_data,
    export_conversations_to_text,
    export_conversations
)

# Import custom exceptions
from .exceptions import (
    SkypeParserError,
    TimestampParsingError,
    ContentParsingError,
    FileOperationError,
    DataExtractionError,
    InvalidInputError,
    DatabaseOperationError,
    ExportError
)

# Define public API
__all__ = [
    # Core parsing functions
    'timestamp_parser',
    'content_parser',
    'pretty_quotes',
    'type_parser',
    'banner_constructor',
    'parse_skype_data',
    'id_selector',

    # Content extraction functions
    'extract_content_data',
    'format_content_with_markup',
    'format_content_with_regex',
    'ContentExtractor',

    # File output functions
    'write_to_file',
    'output_structured_data',
    'export_conversations_to_text',
    'export_conversations',

    # Custom exceptions
    'SkypeParserError',
    'TimestampParsingError',
    'ContentParsingError',
    'FileOperationError',
    'DataExtractionError',
    'InvalidInputError',
    'DatabaseOperationError',
    'ExportError'
]
