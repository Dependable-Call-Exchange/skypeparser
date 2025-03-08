#!/usr/bin/env python3
"""
Structured Logging Module

This module provides a centralized, structured logging system for the application.
It includes JSON formatting, context tracking, and helper functions for common
logging patterns.
"""

import logging
import json
import datetime
import uuid
import threading
import functools
import time
import traceback
import os
import sys
import psutil
from typing import Any, Dict, Optional, Callable, TypeVar, cast, List, Union, Tuple

# Thread-local storage for request context
_context = threading.local()

# Type variables for decorators
F = TypeVar('F', bound=Callable[..., Any])


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
            "function": record.funcName,
            "line": record.lineno
        }

        # Add context data if available
        context_data = get_context_data()
        if context_data:
            log_data["context"] = context_data

        # Add exception info if available
        if record.exc_info:
            log_data["exception"] = {
                "type": record.exc_info[0].__name__,
                "message": str(record.exc_info[1]),
                "traceback": self.formatException(record.exc_info)
            }

        # Add extra fields from record
        if hasattr(record, 'extra'):
            log_data["extra"] = record.extra

        # Add metrics if available
        if hasattr(record, 'metrics'):
            log_data["metrics"] = record.metrics

        # Add any other custom attributes
        for key, value in record.__dict__.items():
            if key not in {
                "args", "asctime", "created", "exc_info", "exc_text", "filename",
                "funcName", "id", "levelname", "levelno", "lineno", "module",
                "msecs", "message", "msg", "name", "pathname", "process",
                "processName", "relativeCreated", "stack_info", "thread", "threadName",
                "extra", "metrics", "context"
            }:
                log_data[key] = value

        return json.dumps(log_data)


def initialize_logging(
    log_level: str = "INFO",
    log_format: Optional[str] = None,
    log_file: Optional[str] = None,
    enable_console: bool = True,
    enable_json: bool = True,
    max_file_size_mb: int = 10,
    backup_count: int = 5
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
            log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
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
            log_file,
            maxBytes=max_file_size_mb * 1024 * 1024,
            backupCount=backup_count
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
    """Set a value in the current logging context.

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
    return getattr(_context, "data", {})


def get_context_value(key: str, default: Any = None) -> Any:
    """Get a value from the current logging context.

    Args:
        key: Context key
        default: Default value if key not found

    Returns:
        Context value or default
    """
    context_data = get_context_data()
    return context_data.get(key, default)


def clear_context() -> None:
    """Clear all context data."""
    if hasattr(_context, "data"):
        _context.data = {}


def update_context(**kwargs) -> None:
    """Update multiple context values at once.

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
        """Set context values on enter."""
        # Save previous context values
        for key in self.kwargs:
            if hasattr(_context, "data") and key in _context.data:
                self.previous_context[key] = _context.data[key]

        # Set new context values
        for key, value in self.kwargs.items():
            set_context(key, value)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Restore previous context values on exit."""
        if hasattr(_context, "data"):
            # Remove keys that weren't in the previous context
            for key in self.kwargs:
                if key not in self.previous_context:
                    if key in _context.data:
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


def log_execution_time(logger: Optional[logging.Logger] = None, level: int = logging.INFO):
    """Decorator for logging function execution time.

    Args:
        logger: Logger to use (if None, uses the module logger)
        level: Logging level for the execution time message

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

            # Get function details for context
            module_name = func.__module__
            function_name = func.__qualname__

            # Start timer
            start_time = time.time()

            # Execute function
            try:
                result = func(*args, **kwargs)
                success = True
            except Exception as e:
                success = False
                raise
            finally:
                # Calculate execution time
                end_time = time.time()
                duration_ms = (end_time - start_time) * 1000

                # Log execution time
                status = "completed" if success else "failed"
                logger.log(
                    level,
                    f"{function_name} {status} in {duration_ms:.2f} ms",
                    extra={
                        "metrics": {
                            "duration_ms": duration_ms,
                            "function": function_name,
                            "module": module_name,
                            "success": success
                        }
                    }
                )

            return result
        return cast(F, wrapper)
    return decorator


def log_call(logger: Optional[logging.Logger] = None, level: int = logging.DEBUG):
    """Decorator for logging function calls.

    Args:
        logger: Logger to use (if None, uses the module logger)
        level: Logging level for the call message

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

            # Format arguments for logging
            args_repr = [repr(a) for a in args]
            kwargs_repr = [f"{k}={repr(v)}" for k, v in kwargs.items()]
            signature = ", ".join(args_repr + kwargs_repr)

            # Log function call
            logger.log(
                level,
                f"Calling {func.__qualname__}({signature})",
                extra={
                    "function": func.__qualname__,
                    "module": func.__module__,
                    "args": args_repr,
                    "kwargs": kwargs_repr
                }
            )

            # Execute function
            return func(*args, **kwargs)
        return cast(F, wrapper)
    return decorator


def handle_errors(
    logger=None,
    log_level="ERROR",
    default_message="An error occurred",
    reraise=True,
    default_return=None,
):
    """
    Decorator for handling and logging errors.

    Args:
        logger: Logger to use (if None, a logger will be created based on the module name)
        log_level: Log level to use for error messages
        default_message: Default message to log if no specific message is provided
        reraise: Whether to re-raise the exception after logging
        default_return: Default value to return if an exception occurs and reraise is False

    Returns:
        Decorator function
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get logger
            nonlocal logger
            if logger is None:
                logger = get_logger(func.__module__)

            # Get log level
            level = getattr(logging, log_level)

            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Get function name
                func_name = func.__qualname__

                # Get exception details
                exc_type = type(e).__name__
                exc_message = str(e)
                exc_traceback = traceback.format_exc()

                # Log error
                logger.log(
                    level,
                    f"{default_message}: {exc_message}",
                    exc_info=True,
                    extra={
                        "exception_type": exc_type,
                        "exception_message": exc_message,
                        "error_function": func_name,
                        "error_module": func.__module__,
                    }
                )

                # Record metrics
                if hasattr(logger, "metrics"):
                    logger.metrics["errors"] = logger.metrics.get("errors", 0) + 1
                    logger.metrics["last_error"] = {
                        "type": exc_type,
                        "message": exc_message,
                        "function": func_name,
                        "timestamp": datetime.datetime.now().isoformat(),
                    }

                # Log execution time
                end_time = time.time()
                if hasattr(wrapper, "_start_time"):
                    duration_ms = (end_time - wrapper._start_time) * 1000
                    logger.info(
                        f"{func_name} failed in {duration_ms:.2f} ms",
                        extra={
                            "metrics": {
                                "duration_ms": duration_ms,
                                "success": False,
                            }
                        }
                    )

                # Re-raise or return default
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


def log_metrics(logger: logging.Logger, metrics: Dict[str, Any], level: int = logging.INFO, message: str = "Metrics"):
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
        "system_memory_percent": psutil.virtual_memory().percent
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
        "source": "api" if "flask" in sys.modules or "fastapi" in sys.modules else "cli"
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
    level: int = logging.DEBUG
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
        extra={
            "query": query,
            "params": params,
            "metrics": metrics
        }
    )
