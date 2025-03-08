#!/usr/bin/env python3
"""
Structured Logging Module

This module provides a centralized, structured logging system for the application.
It includes JSON formatting, context tracking, and helper functions for common
logging patterns.
"""

import datetime
import functools
import json
import logging
import os
import sys
import threading
import time
import traceback
import uuid
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union, cast

import psutil

# Thread-local storage for request context
_context = threading.local()

# Type variables for decorators
F = TypeVar("F", bound=Callable[..., Any])


class JsonFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON."""
        log_data = {
            "timestamp": datetime.datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName or "",  # Ensure function is always a string
            "line": record.lineno,
        }

        # Add context data if available
        context_data = get_context_data()
        if context_data:
            log_data["context"] = context_data

        # Add exception info if available
        if record.exc_info:
            exc_type, exc_value, exc_traceback = record.exc_info
            log_data["exception"] = {
                "type": exc_type.__name__ if exc_type else "",
                "message": str(exc_value) if exc_value else "",
                "traceback": traceback.format_exception(
                    exc_type, exc_value, exc_traceback
                )
                if exc_traceback
                else [],
            }

        # Add extra fields from record
        for key, value in record.__dict__.items():
            if key not in [
                "args",
                "asctime",
                "created",
                "exc_info",
                "exc_text",
                "filename",
                "funcName",
                "id",
                "levelname",
                "levelno",
                "lineno",
                "module",
                "msecs",
                "message",
                "msg",
                "name",
                "pathname",
                "process",
                "processName",
                "relativeCreated",
                "stack_info",
                "thread",
                "threadName",
            ] and not key.startswith("_"):
                log_data[key] = value

        return json.dumps(log_data, default=str)


def initialize_logging(
    log_level: str = "INFO",
    log_format: Optional[str] = None,
    log_file: Optional[str] = None,
    enable_console: bool = True,
    enable_json: bool = True,
    max_file_size_mb: int = 10,
    backup_count: int = 5,
) -> None:
    """Initialize logging configuration for the application.

    Args:
        log_level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_format: Custom log format (if None, uses structured JSON format)
        log_file: Path to log file (if None, logs to console only)
        enable_console: Whether to log to console
        enable_json: Whether to use JSON formatting
        max_file_size_mb: Maximum log file size in MB before rotation
        backup_count: Number of backup log files to keep
    """
    handlers: List[logging.Handler] = []

    # Create formatter
    if enable_json:
        formatter = JsonFormatter()
    else:
        if log_format is None:
            log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(log_format)

    # Console handler
    if enable_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        handlers.append(console_handler)

    # File handler
    if log_file:
        from logging.handlers import RotatingFileHandler

        file_handler = RotatingFileHandler(
            log_file, maxBytes=max_file_size_mb * 1024 * 1024, backupCount=backup_count
        )
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level))

    # Remove existing handlers
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    # Add new handlers
    for handler in handlers:
        root_logger.addHandler(handler)

    # Log initialization
    logger = get_logger("logging")
    logger.info(f"Logging initialized at level {log_level}")


def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)
    return logger


def set_context(key: str, value: Any) -> None:
    """Set a value in the current context.

    Args:
        key: Context key
        value: Context value
    """
    if not hasattr(_context, "data"):
        _context.data = {}
    _context.data[key] = value


def get_context_data() -> Dict[str, Any]:
    """Get all context data.

    Returns:
        Dictionary of context data
    """
    if not hasattr(_context, "data"):
        _context.data = {}
    return _context.data.copy()


def get_context_value(key: str, default: Any = None) -> Any:
    """Get a value from the current context.

    Args:
        key: Context key
        default: Default value if key not found

    Returns:
        Context value or default
    """
    if not hasattr(_context, "data"):
        return default
    return _context.data.get(key, default)


def clear_context() -> None:
    """Clear all context data."""
    if hasattr(_context, "data"):
        _context.data = {}


def update_context(**kwargs) -> None:
    """Update context with multiple values.

    Args:
        **kwargs: Context key-value pairs
    """
    if not hasattr(_context, "data"):
        _context.data = {}
    _context.data.update(kwargs)


class LogContext:
    """Context manager for setting logging context."""

    def __init__(self, **kwargs):
        """Initialize with context values.

        Args:
            **kwargs: Context key-value pairs
        """
        self.kwargs = kwargs
        self.previous_context = {}

    def __enter__(self):
        """Enter context and set values."""
        # Save previous context values
        self.previous_context = {}
        for key in self.kwargs:
            if hasattr(_context, "data") and key in _context.data:
                self.previous_context[key] = _context.data[key]

        # Set new context values
        update_context(**self.kwargs)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context and restore previous values."""
        if hasattr(_context, "data"):
            # Remove keys that weren't in the previous context
            for key in self.kwargs:
                if key not in self.previous_context and key in _context.data:
                    del _context.data[key]

            # Restore previous values
            for key, value in self.previous_context.items():
                _context.data[key] = value


def with_context(**kwargs):
    """Decorator for setting logging context for a function.

    Args:
        **kwargs: Context key-value pairs or callables that return key-value pairs

    Returns:
        Decorated function
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **func_kwargs):
            # Evaluate callable kwargs
            context_values = {}
            for key, value in kwargs.items():
                if callable(value):
                    try:
                        context_values[key] = value(*args, **func_kwargs)
                    except Exception as e:
                        logger = get_logger(__name__)
                        logger.warning(f"Error evaluating context value for {key}: {e}")
                else:
                    context_values[key] = value

            # Set context values
            with LogContext(**context_values):
                return func(*args, **func_kwargs)

        return cast(F, wrapper)

    return decorator


def log_execution_time(
    logger: Optional[logging.Logger] = None, level: int = logging.INFO
):
    """Decorator for logging function execution time.

    Args:
        logger: Logger to use (defaults to module logger)
        level: Log level to use

    Returns:
        Decorated function
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get logger
            nonlocal logger
            if logger is None:
                logger = get_logger(func.__module__)

            # Get function name with class if method
            if (
                args
                and hasattr(args[0], "__class__")
                and hasattr(args[0].__class__, func.__name__)
            ):
                func_name = f"{args[0].__class__.__name__}.{func.__name__}"
            else:
                func_name = f"{func.__module__}.{func.__name__}"

            # Measure execution time
            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                return result
            finally:
                end_time = time.time()
                duration_ms = (end_time - start_time) * 1000
                logger.log(
                    level,
                    f"{func_name} completed in {duration_ms:.2f} ms",
                    extra={
                        "function_name": func_name,
                        "duration_ms": duration_ms,
                        "call_module": func.__module__,
                    },
                )

        return cast(F, wrapper)

    return decorator


def log_call(logger: Optional[logging.Logger] = None, level: int = logging.DEBUG):
    """Decorator for logging function calls.

    Args:
        logger: Logger to use (defaults to module logger)
        level: Log level to use

    Returns:
        Decorated function
    """

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get logger
            nonlocal logger
            if logger is None:
                logger = get_logger(func.__module__)

            # Format args and kwargs for logging
            args_str = [repr(arg) for arg in args]
            kwargs_str = [f"{key}={repr(value)}" for key, value in kwargs.items()]
            all_args = args_str + kwargs_str
            args_formatted = ", ".join(all_args)

            # Get function name with class if method
            if (
                args
                and hasattr(args[0], "__class__")
                and hasattr(args[0].__class__, func.__name__)
            ):
                func_name = f"{args[0].__class__.__name__}.{func.__name__}"
            else:
                func_name = f"{func.__module__}.{func.__name__}"

            # Log the call
            logger.log(
                level,
                f"Calling {func_name}({args_formatted})",
                extra={
                    "function_name": func_name,
                    "args_str": args_str,
                    "kwargs_str": kwargs_str,
                    "call_module": func.__module__,
                },
            )

            # Call the function
            result = func(*args, **kwargs)

            # Log the return
            logger.log(
                level,
                f"{func_name} returned {repr(result)}",
                extra={
                    "function_name": func_name,
                    "result": repr(result),
                    "call_module": func.__module__,
                },
            )

            return result

        return cast(F, wrapper)

    return decorator


def handle_errors(
    logger=None,
    log_level="ERROR",
    default_message="An error occurred",
    reraise=True,
    default_return=None,
):
    """Decorator for handling and logging errors.

    Args:
        logger: Logger to use (defaults to module logger)
        log_level: Log level for error messages
        default_message: Default message to log
        reraise: Whether to reraise the exception
        default_return: Default return value if exception is caught and not reraised

    Returns:
        Decorated function
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get logger
            nonlocal logger
            if logger is None:
                logger = get_logger(func.__module__)

            # Get log level
            level = (
                getattr(logging, log_level) if isinstance(log_level, str) else log_level
            )

            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Format error message
                error_message = f"{default_message}: {str(e)}"

                # Get traceback
                exc_info = sys.exc_info()

                # Log error with exception info
                logger.log(level, error_message, exc_info=exc_info)

                # Add error details to context
                update_context(
                    error=str(e),
                    error_type=e.__class__.__name__,
                    error_traceback=traceback.format_exception(*exc_info),
                )

                # Reraise or return default
                if reraise:
                    raise
                return default_return

        return wrapper

    return decorator


def get_structured_logger(name: str) -> logging.Logger:
    """Get a structured logger with the given name.

    This is an alias for get_logger for backward compatibility.

    Args:
        name: Logger name

    Returns:
        Logger instance
    """
    return get_logger(name)


def log_metrics(
    logger: logging.Logger,
    metrics: Dict[str, Any],
    level: int = logging.INFO,
    message: str = "Metrics",
):
    """Log metrics with the given logger.

    Args:
        logger: Logger to use
        metrics: Dictionary of metrics to log
        level: Logging level
        message: Log message
    """
    logger.log(level, message, extra={"metrics": metrics})


def get_system_metrics() -> Dict[str, Any]:
    """Get system metrics for logging.

    Returns:
        Dictionary of system metrics
    """
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()

    return {
        "memory_rss_mb": memory_info.rss / (1024 * 1024),
        "memory_vms_mb": memory_info.vms / (1024 * 1024),
        "cpu_percent": process.cpu_percent(),
        "thread_count": process.num_threads(),
        "open_files": len(process.open_files()),
        "system_cpu_percent": psutil.cpu_percent(),
        "system_memory_percent": psutil.virtual_memory().percent,
    }


def log_system_metrics(logger: logging.Logger, level: int = logging.DEBUG):
    """Log system metrics with the given logger.

    Args:
        logger: Logger to use
        level: Logging level
    """
    metrics = get_system_metrics()
    log_metrics(logger, metrics, level, "System metrics")


def measure_execution_time(func: Callable, *args, **kwargs) -> Tuple[Any, float]:
    """Measure the execution time of a function.

    Args:
        func: Function to measure
        *args: Arguments to pass to the function
        **kwargs: Keyword arguments to pass to the function

    Returns:
        Tuple of (result, duration_ms)
    """
    start_time = time.time()
    result = func(*args, **kwargs)
    end_time = time.time()
    duration_ms = (end_time - start_time) * 1000
    return result, duration_ms


def create_request_context(request_id: Optional[str] = None) -> Dict[str, Any]:
    """Create a request context dictionary.

    Args:
        request_id: Request ID (generated if not provided)

    Returns:
        Request context dictionary
    """
    if request_id is None:
        request_id = str(uuid.uuid4())

    return {
        "request_id": request_id,
        "start_time": datetime.datetime.now().isoformat(),
        "source": "api"
        if "flask" in sys.modules or "fastapi" in sys.modules
        else "cli",
    }


def with_request_context(func: F) -> F:
    """Decorator for setting a request context.

    Args:
        func: Function to decorate

    Returns:
        Decorated function
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # Create request context
        request_context = create_request_context()

        # Execute function with context
        with LogContext(**request_context):
            return func(*args, **kwargs)

    return cast(F, wrapper)


def log_database_query(
    logger: logging.Logger,
    query: str,
    params: Optional[Dict[str, Any]] = None,
    duration_ms: Optional[float] = None,
    rows_affected: Optional[int] = None,
    level: int = logging.DEBUG,
):
    """Log a database query.

    Args:
        logger: Logger to use
        query: SQL query
        params: Query parameters
        duration_ms: Query duration in milliseconds
        rows_affected: Number of rows affected
        level: Logging level
    """
    # Truncate query if too long
    if len(query) > 1000:
        query = query[:997] + "..."

    # Build metrics
    metrics = {}
    if duration_ms is not None:
        metrics["duration_ms"] = duration_ms
    if rows_affected is not None:
        metrics["rows_affected"] = rows_affected

    # Log query
    logger.log(
        level,
        f"Database query: {query}",
        extra={"query": query, "params": params, "metrics": metrics},
    )
