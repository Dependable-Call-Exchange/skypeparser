#!/usr/bin/env python3
"""
Standardized Error Handling Module

This module provides a centralized approach to error handling across the SkypeParser
project. It includes functions for consistent error reporting, error context
management, and standardized error response generation.
"""

import logging
import sys
import traceback
from datetime import datetime
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Type, TypeVar, Union

from src.parser.exceptions import SkypeParserError

# Set up logging
logger = logging.getLogger(__name__)

# Type variable for generic function type
F = TypeVar("F", bound=Callable[..., Any])

# Error severity levels
ERROR_SEVERITY = {"CRITICAL": 50, "ERROR": 40, "WARNING": 30, "INFO": 20, "DEBUG": 10}


class ErrorContext:
    """
    Context manager for tracking error context information.

    This class allows you to add context information to errors that occur within
    a block of code. It can be used as a context manager or as a decorator.
    """

    _current_context: Dict[str, Any] = {}
    _context_stack: List[Dict[str, Any]] = []

    @classmethod
    def get_current_context(cls) -> Dict[str, Any]:
        """
        Get the current error context.

        Returns:
            Dict[str, Any]: Current error context
        """
        return cls._current_context.copy()

    @classmethod
    def add_context(cls, **kwargs) -> None:
        """
        Add context information to the current error context.

        Args:
            **kwargs: Key-value pairs to add to the context
        """
        cls._current_context.update(kwargs)

    @classmethod
    def reset_context(cls) -> None:
        """Reset the current error context."""
        cls._current_context = {}
        cls._context_stack = []

    def __init__(self, **context):
        """
        Initialize the error context.

        Args:
            **context: Context information to add
        """
        self.context = context

    def __enter__(self):
        """Enter the context manager."""
        # Save current context to the stack
        ErrorContext._context_stack.append(ErrorContext._current_context.copy())
        # Update current context with new context
        ErrorContext._current_context.update(self.context)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit the context manager."""
        # Restore previous context
        if ErrorContext._context_stack:
            ErrorContext._current_context = ErrorContext._context_stack.pop()
        return False  # Don't suppress exceptions

    def __call__(self, func: F) -> F:
        """Use as a decorator."""

        @wraps(func)
        def wrapper(*args, **kwargs):
            with self:
                return func(*args, **kwargs)

        return wrapper  # type: ignore


def handle_errors(
    error_types: Union[Type[Exception], List[Type[Exception]]] = Exception,
    reraise: bool = True,
    log_level: str = "ERROR",
    default_message: str = "An error occurred",
    include_traceback: bool = True,
) -> Callable[[F], F]:
    """
    Decorator for standardized error handling.

    Args:
        error_types: Exception type(s) to catch
        reraise: Whether to reraise the caught exception
        log_level: Logging level for errors
        default_message: Default error message
        include_traceback: Whether to include traceback in the log

    Returns:
        Decorator function
    """
    if not isinstance(error_types, list):
        error_types = [error_types]  # type: ignore

    def decorator(func: F) -> F:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except tuple(error_types) as e:  # type: ignore
                # Get the error context
                context = ErrorContext.get_current_context()

                # Create error details
                error_details = {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "function": f"{func.__module__}.{func.__name__}",
                    "timestamp": datetime.now().isoformat(),
                    "context": context,
                }

                # Add traceback if requested
                if include_traceback:
                    error_details["traceback"] = traceback.format_exc()

                # Log the error
                log_func = getattr(logger, log_level.lower(), logger.error)
                log_message = f"{default_message}: {str(e)}"

                if include_traceback:
                    log_func(
                        log_message,
                        exc_info=True,
                        extra={"error_details": error_details},
                    )
                else:
                    log_func(log_message, extra={"error_details": error_details})

                # Reraise if requested
                if reraise:
                    raise

                # Return None if not reraising
                return None

        return wrapper  # type: ignore

    return decorator


def generate_error_response(
    error: Exception,
    status: str = "failed",
    include_traceback: bool = False,
    include_context: bool = True,
) -> Dict[str, Any]:
    """
    Generate a standardized error response dictionary.

    Args:
        error: The exception that occurred
        status: Status to include in the response
        include_traceback: Whether to include traceback in the response
        include_context: Whether to include error context in the response

    Returns:
        Dictionary containing standardized error information
    """
    response = {
        "status": status,
        "error": {
            "type": type(error).__name__,
            "message": str(error),
            "timestamp": datetime.now().isoformat(),
        },
    }

    # Add traceback if requested
    if include_traceback:
        response["error"]["traceback"] = traceback.format_exc()

    # Add context if requested
    if include_context:
        response["error"]["context"] = ErrorContext.get_current_context()

    return response


def report_error(
    error: Exception,
    log_level: str = "ERROR",
    include_traceback: bool = True,
    additional_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Report an error with standardized format and information.

    Args:
        error: The exception that occurred
        log_level: Logging level for the error
        include_traceback: Whether to include traceback in the log and response
        additional_context: Additional context information to include

    Returns:
        Dictionary containing error information
    """
    # Get current context and update with additional context
    context = ErrorContext.get_current_context()
    if additional_context:
        context.update(additional_context)

    # Create error details
    error_details = {
        "error_type": type(error).__name__,
        "error_message": str(error),
        "timestamp": datetime.now().isoformat(),
        "context": context,
    }

    # Add traceback if requested
    if include_traceback:
        error_details["traceback"] = traceback.format_exc()

    # Log the error
    log_func = getattr(logger, log_level.lower(), logger.error)
    log_message = f"Error reported: {str(error)}"

    if include_traceback:
        log_func(log_message, exc_info=True, extra={"error_details": error_details})
    else:
        log_func(log_message, extra={"error_details": error_details})

    return error_details


def is_fatal_error(error: Exception) -> bool:
    """
    Determine if an error should be considered fatal.

    Args:
        error: The exception to check

    Returns:
        True if the error is fatal, False otherwise
    """
    # System errors are always fatal
    if isinstance(error, (SystemExit, KeyboardInterrupt, MemoryError)):
        return True

    # Check for specific fatal error types from our custom exceptions
    fatal_error_types = ["DatabaseOperationError", "FileOperationError"]

    return type(error).__name__ in fatal_error_types


def get_error_severity(error: Exception) -> int:
    """
    Get the severity level for an error.

    Args:
        error: The exception to check

    Returns:
        Severity level (higher is more severe)
    """
    # System errors are the most severe
    if isinstance(error, (SystemExit, KeyboardInterrupt, MemoryError)):
        return ERROR_SEVERITY["CRITICAL"]

    # Map custom errors to severity levels
    error_severity_map = {
        "DatabaseOperationError": ERROR_SEVERITY["ERROR"],
        "FileOperationError": ERROR_SEVERITY["ERROR"],
        "DataExtractionError": ERROR_SEVERITY["ERROR"],
        "TimestampParsingError": ERROR_SEVERITY["WARNING"],
        "ContentParsingError": ERROR_SEVERITY["WARNING"],
        "InvalidInputError": ERROR_SEVERITY["WARNING"],
        "ValidationError": ERROR_SEVERITY["WARNING"],
    }

    return error_severity_map.get(type(error).__name__, ERROR_SEVERITY["ERROR"])


def safe_execute(func: Callable, *args, default: Any = None, **kwargs) -> Any:
    """
    Execute a function safely, returning a default value if an exception occurs.

    Args:
        func: Function to execute
        *args: Positional arguments to pass to the function
        default: Default value to return if an exception occurs
        **kwargs: Keyword arguments to pass to the function

    Returns:
        Result of the function or default value if an exception occurs
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        logger.debug(f"Error in safe_execute: {e}")
        return default
