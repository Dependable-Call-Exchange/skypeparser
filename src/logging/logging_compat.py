"""
Logging Compatibility Module

This module provides backward compatibility with the old logging approach,
making it easier to migrate to the new centralized logging system.
"""

import logging
import functools
from typing import Any, Dict, Optional, Callable, TypeVar, cast

from src.logging.new_structured_logging import (
    get_logger as get_structured_logger,
    log_execution_time as structured_log_execution_time,
    log_call as structured_log_call,
    handle_errors as structured_handle_errors,
    set_context,
    get_context_data,
    clear_context,
    LogContext
)

# Type variables for decorators
F = TypeVar('F', bound=Callable[..., Any])


def get_logger(name: str) -> logging.Logger:
    """
    Get a logger with the given name.

    This function provides backward compatibility with code
    that uses the old logging approach.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    return get_structured_logger(name)


def with_context(**context):
    """
    Context manager for setting logging context.

    This function provides backward compatibility with the old
    context management approach.

    Args:
        **context: Context key-value pairs

    Returns:
        Context manager
    """
    return LogContext(**context)


def log_execution_time(logger: Optional[logging.Logger] = None, level: int = logging.INFO):
    """
    Decorator for logging function execution time.

    This function provides backward compatibility with the old
    log_execution_time decorator.

    Args:
        logger: Logger to use
        level: Logging level

    Returns:
        Decorator function
    """
    return structured_log_execution_time(logger, level)


def log_call(logger: Optional[logging.Logger] = None, level: int = logging.DEBUG):
    """
    Decorator for logging function calls.

    This function provides backward compatibility with the old
    log_call decorator.

    Args:
        logger: Logger to use
        level: Logging level

    Returns:
        Decorator function
    """
    return structured_log_call(logger, level)


def handle_errors(
    logger: Optional[logging.Logger] = None,
    log_level: str = "ERROR",
    default_message: str = "An error occurred",
    reraise: bool = True,
    default_return: Any = None
):
    """
    Decorator for handling and logging errors.

    This function provides backward compatibility with the old
    error handling approach.

    Args:
        logger: Logger to use
        log_level: Logging level for errors
        default_message: Default error message
        reraise: Whether to re-raise the exception
        default_return: Default return value if exception is caught and not re-raised

    Returns:
        Decorator function
    """
    return structured_handle_errors(
        logger=logger,
        log_level=log_level,
        default_message=default_message,
        reraise=reraise,
        default_return=default_return
    )


class StructuredLoggerAdapter:
    """
    Adapter for the old StructuredLogger class.

    This class provides backward compatibility with code that
    uses the old StructuredLogger class.
    """

    def __init__(self, logger: logging.Logger):
        """
        Initialize the adapter.

        Args:
            logger: Logger to adapt
        """
        self.logger = logger

    def with_context(self, **context):
        """
        Return a logger with the specified context.

        Args:
            **context: Context key-value pairs

        Returns:
            Logger with context
        """
        # Set context
        for key, value in context.items():
            set_context(key, value)

        return self

    def structured(self, level, msg, *args, **kwargs):
        """
        Log a structured message.

        Args:
            level: Log level
            msg: Log message
            *args: Args for message formatting
            **kwargs: Additional structured data
        """
        # Extract structured data
        structured_data = {}
        for key, value in kwargs.items():
            if key not in ["exc_info", "stack_info", "stacklevel"]:
                structured_data[key] = value

        # Add structured data to extra
        extra = kwargs.pop("extra", {})
        if structured_data:
            extra["structured_data"] = structured_data

        # Log message
        self.logger.log(level, msg, *args, exc_info=kwargs.get("exc_info"), extra=extra)

    def debug_s(self, msg, *args, **kwargs):
        """Structured debug log."""
        self.structured(logging.DEBUG, msg, *args, **kwargs)

    def info_s(self, msg, *args, **kwargs):
        """Structured info log."""
        self.structured(logging.INFO, msg, *args, **kwargs)

    def warning_s(self, msg, *args, **kwargs):
        """Structured warning log."""
        self.structured(logging.WARNING, msg, *args, **kwargs)

    def error_s(self, msg, *args, **kwargs):
        """Structured error log."""
        self.structured(logging.ERROR, msg, *args, **kwargs)

    def critical_s(self, msg, *args, **kwargs):
        """Structured critical log."""
        self.structured(logging.CRITICAL, msg, *args, **kwargs)

    def log_exception(self, msg, exc_info=None, **kwargs):
        """
        Log an exception with additional context.

        Args:
            msg: Log message
            exc_info: Exception info tuple
            **kwargs: Additional context information
        """
        self.logger.error(msg, exc_info=exc_info or True, extra=kwargs)

    def __getattr__(self, name):
        """
        Forward attribute access to the underlying logger.

        Args:
            name: Attribute name

        Returns:
            Attribute value
        """
        return getattr(self.logger, name)


def get_structured_logger(name: str):
    """
    Get a structured logger with the given name.

    This function provides backward compatibility with code
    that uses the old get_structured_logger function.

    Args:
        name: Logger name

    Returns:
        StructuredLogger instance
    """
    logger = get_logger(name)
    return StructuredLoggerAdapter(logger)


# Patch the logging module to use our compatibility layer
def patch_logging():
    """
    Patch the logging module to use our compatibility layer.

    This function replaces the standard logging.getLogger function
    with our compatibility function.
    """
    logging.getLogger = get_logger

    # Log the patching
    logger = get_logger(__name__)
    logger.info("Logging module patched for compatibility")


# Automatically patch logging when this module is imported
# patch_logging()  # Uncomment to enable automatic patching