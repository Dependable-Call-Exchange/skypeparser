#!/usr/bin/env python3
"""
Structured Logging Module

This module provides enhanced logging capabilities for the SkypeParser project,
including structured logging, customizable formatters, and consistent logging
patterns across the application.
"""

import json
import logging
import os
import sys
import time
from datetime import datetime
from logging.handlers import RotatingFileHandler, TimedRotatingFileHandler
from typing import Any, Dict, List, Optional, Union

# Default log format
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Default structured log format (includes more details)
DEFAULT_STRUCTURED_FORMAT = (
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s - "
    "file: %(pathname)s:%(lineno)d - func: %(funcName)s"
)

# Default JSON format for structured logging
DEFAULT_JSON_FORMAT = {
    "timestamp": "%(asctime)s",
    "name": "%(name)s",
    "level": "%(levelname)s",
    "message": "%(message)s",
    "module": "%(module)s",
    "function": "%(funcName)s",
    "line": "%(lineno)d",
    "path": "%(pathname)s",
    "process": "%(process)d",
    "thread": "%(thread)d",
}


class StructuredLogRecord(logging.LogRecord):
    """
    Enhanced LogRecord that adds support for structured data and context.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.context = {}
        self.error_details = {}
        self.structured_data = {}
        self.execution_id = None


class StructuredLogger(logging.Logger):
    """
    Enhanced Logger class that adds support for structured logging.
    """

    def makeRecord(
        self,
        name,
        level,
        fn,
        lno,
        msg,
        args,
        exc_info,
        func=None,
        extra=None,
        sinfo=None,
    ):
        """
        Create a LogRecord with additional structured data support.
        """
        record = StructuredLogRecord(
            name, level, fn, lno, msg, args, exc_info, func, sinfo
        )
        if extra is not None:
            for key in extra:
                if key in [
                    "context",
                    "error_details",
                    "structured_data",
                    "execution_id",
                ]:
                    setattr(record, key, extra[key])
                elif key in logging._nameToLevel:
                    # Skip level names to avoid confusion
                    pass
                else:
                    setattr(record, key, extra[key])
        return record

    def with_context(self, **context):
        """
        Return a logger with the specified context.

        Args:
            **context: Context key-value pairs

        Returns:
            StructuredLogger: Logger with context
        """
        # Create a new logger with the same name
        logger = logging.getLogger(self.name)

        # Add a filter that adds the context to all records
        class ContextFilter(logging.Filter):
            def filter(self, record):
                if not hasattr(record, "context"):
                    record.context = {}
                record.context.update(context)
                return True

        # Add the filter to the logger
        logger.addFilter(ContextFilter())

        return logger

    def structured(self, level, msg, *args, **kwargs):
        """
        Log a structured message.

        Args:
            level: Log level
            msg: Log message
            *args: Args for message formatting
            **kwargs: Additional structured data
        """
        if self.isEnabledFor(level):
            # Extract any non-logging kwargs to include as structured data
            structured_data = {}
            extra = kwargs.pop("extra", {})

            for key, value in kwargs.items():
                if key not in ["exc_info", "stack_info", "stacklevel"]:
                    structured_data[key] = value

            if structured_data:
                if "extra" not in kwargs:
                    kwargs["extra"] = {}
                if not isinstance(kwargs["extra"], dict):
                    kwargs["extra"] = {}

                kwargs["extra"]["structured_data"] = structured_data

            # Add any existing extra data
            if extra:
                if "extra" not in kwargs:
                    kwargs["extra"] = {}
                kwargs["extra"].update(extra)

            self._log(level, msg, args, **kwargs)

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
            exc_info: Exception info tuple (type, value, traceback)
            **kwargs: Additional context information
        """
        if exc_info is None:
            exc_info = sys.exc_info()

        # Add exception info to structured data
        exception_data = {
            "exception_type": exc_info[0].__name__ if exc_info[0] else None,
            "exception_message": str(exc_info[1]) if exc_info[1] else None,
        }

        # Update kwargs with exception data
        for key, value in exception_data.items():
            kwargs[key] = value

        # Log as error with exception info
        self.error_s(msg, exc_info=exc_info, **kwargs)


class JSONFormatter(logging.Formatter):
    """
    Formatter that outputs log records as JSON.
    """

    def __init__(self, fmt=None, datefmt=None, style="%", json_format=None):
        """
        Initialize the formatter.

        Args:
            fmt: Format string
            datefmt: Date format string
            style: Style of fmt (%, {, or $)
            json_format: JSON format dictionary
        """
        super().__init__(fmt, datefmt, style)
        self.json_format = json_format or DEFAULT_JSON_FORMAT

    def format(self, record):
        """
        Format the log record as JSON.

        Args:
            record: Log record to format

        Returns:
            str: Formatted JSON string
        """
        # Create a copy of the record to avoid modifying the original
        record_dict = self.get_record_dict(record)

        # Add structured data if present
        if hasattr(record, "structured_data"):
            record_dict.update(record.structured_data)

        # Add context if present
        if hasattr(record, "context") and record.context:
            record_dict["context"] = record.context

        # Add error details if present
        if hasattr(record, "error_details") and record.error_details:
            record_dict["error"] = record.error_details

        # Add execution ID if present
        if hasattr(record, "execution_id") and record.execution_id:
            record_dict["execution_id"] = record.execution_id

        # Convert to JSON
        return json.dumps(record_dict)

    def get_record_dict(self, record):
        """
        Convert a log record to a dictionary.

        Args:
            record: Log record to convert

        Returns:
            dict: Record as a dictionary
        """
        result = {}
        for key, value in self.json_format.items():
            try:
                # Format the value using the record
                result[key] = value % record.__dict__
            except (KeyError, TypeError):
                # If formatting fails, use the raw value
                result[key] = value

        return result


class StructuredFormatter(logging.Formatter):
    """
    Formatter that adds structured data to log records.
    """

    def format(self, record):
        """
        Format the log record with additional structured data.

        Args:
            record: Log record to format

        Returns:
            str: Formatted log message
        """
        # Format the record using the parent formatter
        formatted = super().format(record)

        # Add structured data if present
        if hasattr(record, "structured_data") and record.structured_data:
            formatted += f" - data: {json.dumps(record.structured_data)}"

        # Add context if present
        if hasattr(record, "context") and record.context:
            formatted += f" - context: {json.dumps(record.context)}"

        # Add execution ID if present
        if hasattr(record, "execution_id") and record.execution_id:
            formatted += f" - execution_id: {record.execution_id}"

        return formatted


def setup_logging(
    level: Union[int, str] = logging.INFO,
    log_file: Optional[str] = None,
    json_format: bool = False,
    structured: bool = True,
    rotation: str = "size",  # 'size', 'time', or None
    max_bytes: int = 10 * 1024 * 1024,  # 10 MB
    backup_count: int = 5,
    when: str = "midnight",  # For TimedRotatingFileHandler
) -> None:
    """
    Set up logging with enhanced features.

    Args:
        level: Logging level
        log_file: Path to log file
        json_format: Whether to use JSON format
        structured: Whether to use structured logging
        rotation: Log rotation type ('size', 'time', or None)
        max_bytes: Maximum log file size for size-based rotation
        backup_count: Number of backup files to keep
        when: When to rotate for time-based rotation
    """
    # Register our custom logger class
    logging.setLoggerClass(StructuredLogger)

    # Convert string level to int if needed
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)

    # Configure the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers to avoid duplicates
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)

    # Configure formatter based on format type
    if json_format:
        formatter = JSONFormatter()
    elif structured:
        formatter = StructuredFormatter(DEFAULT_STRUCTURED_FORMAT)
    else:
        formatter = logging.Formatter(DEFAULT_LOG_FORMAT)

    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Add file handler if log_file is specified
    if log_file:
        # Create directory for log file if it doesn't exist
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)

        # Configure file handler based on rotation type
        if rotation == "size":
            file_handler = RotatingFileHandler(
                log_file, maxBytes=max_bytes, backupCount=backup_count
            )
        elif rotation == "time":
            file_handler = TimedRotatingFileHandler(
                log_file, when=when, backupCount=backup_count
            )
        else:
            file_handler = logging.FileHandler(log_file)

        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Log setup completion
    logging.getLogger(__name__).info(
        f"Logging initialized (level={logging.getLevelName(level)}, "
        f"file={log_file}, json={json_format}, structured={structured})"
    )


def get_logger(name: str) -> StructuredLogger:
    """
    Get a structured logger with the specified name.

    Args:
        name: Logger name

    Returns:
        StructuredLogger: Logger instance
    """
    return logging.getLogger(name)


def log_execution_time(logger: logging.Logger, level: int = logging.DEBUG):
    """
    Decorator to log function execution time.

    Args:
        logger: Logger to use
        level: Logging level

    Returns:
        Decorator function
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            execution_time = end_time - start_time
            logger.log(
                level,
                f"Function {func.__name__} executed in {execution_time:.4f}s",
                extra={
                    "structured_data": {
                        "function": func.__name__,
                        "execution_time": execution_time,
                        "execution_time_ms": int(execution_time * 1000),
                    }
                },
            )
            return result

        return wrapper

    return decorator


def log_call(logger: logging.Logger, level: int = logging.DEBUG):
    """
    Decorator to log function calls with parameters.

    Args:
        logger: Logger to use
        level: Logging level

    Returns:
        Decorator function
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            # Log function call
            arg_str = ", ".join([repr(arg) for arg in args])
            kwarg_str = ", ".join(
                [f"{key}={repr(value)}" for key, value in kwargs.items()]
            )
            params = ", ".join(filter(None, [arg_str, kwarg_str]))
            logger.log(
                level,
                f"Calling {func.__name__}({params})",
                extra={
                    "structured_data": {
                        "function": func.__name__,
                        "args_count": len(args),
                        "kwargs_count": len(kwargs),
                    }
                },
            )

            # Execute function
            try:
                result = func(*args, **kwargs)
                return result
            except Exception as e:
                # Log exception
                logger.exception(
                    f"Exception in {func.__name__}: {str(e)}",
                    extra={
                        "structured_data": {
                            "function": func.__name__,
                            "exception": str(e),
                        }
                    },
                )
                raise

        return wrapper

    return decorator
