#!/usr/bin/env python3
"""
Custom Exceptions for the Skype Parser Module

This module defines custom exception classes for the Skype Parser module to provide
consistent error handling and more specific error information.
"""


class SkypeParserError(Exception):
    """Base exception class for all Skype Parser errors."""
    pass


class TimestampParsingError(SkypeParserError):
    """Raised when a timestamp cannot be parsed correctly."""
    pass


class ContentParsingError(SkypeParserError):
    """Raised when message content cannot be parsed correctly."""
    pass


class FileOperationError(SkypeParserError):
    """Raised when a file operation (read/write) fails."""
    pass


class DataExtractionError(SkypeParserError):
    """Raised when data cannot be extracted from the Skype export file."""
    pass


class InvalidInputError(SkypeParserError):
    """Raised when input data is invalid or missing required fields."""
    pass


class DatabaseOperationError(SkypeParserError):
    """Raised when a database operation fails."""
    pass


class ExportError(SkypeParserError):
    """Raised when exporting conversations fails."""
    pass