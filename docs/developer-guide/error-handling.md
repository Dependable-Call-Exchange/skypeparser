# Error Handling in SkypeParser

This document outlines the comprehensive error handling architecture implemented in the SkypeParser project, including exception hierarchy, error context tracking, handling patterns, and logging integration.

## Error Handling Architecture Overview

SkypeParser implements a robust error handling architecture that provides:

1. **Structured Error Information**: Standardized error classification and reporting
2. **Contextual Error Data**: Rich context for debugging and user feedback
3. **Consistent Error Behavior**: Predictable error handling across the application
4. **Recovery Mechanisms**: Strategies for graceful recovery from errors
5. **User-Friendly Messages**: Clear, actionable error information for users

The architecture follows this high-level design:

```
┌─────────────────────────────────────────────────────────────┐
│                  Application Components                      │
└───────────────────────────┬─────────────────────────────────┘
                            │
                            │ throws/handles
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                     Error Framework                          │
├─────────────────┬─────────────────────┬─────────────────────┤
│ Custom Exception│     ErrorContext    │   Error Handling    │
│    Hierarchy    │                     │     Decorators      │
└─────────┬───────┴──────────┬──────────┴──────────┬──────────┘
          │                  │                     │
          │                  │                     │
          ▼                  ▼                     ▼
┌────────────────┐  ┌────────────────┐   ┌────────────────────┐
│     Logger     │  │  Error Report  │   │ Recovery Strategies │
└────────────────┘  └────────────────┘   └────────────────────┘
```

## Custom Exception Hierarchy

All exceptions in SkypeParser inherit from a common `SkypeParserError` base class, enabling consistent handling and classification of different error types.

### Exception Class Hierarchy

```
Exception
└── SkypeParserError
    ├── ValidationError
    │   ├── TimestampParsingError
    │   ├── SchemaValidationError
    │   └── InvalidInputError
    ├── ParseError
    │   ├── ContentParsingError
    │   └── DataExtractionError
    ├── IOError
    │   ├── FileOperationError
    │   └── ExportError
    ├── DatabaseError
    │   ├── ConnectionError
    │   ├── QueryError
    │   └── TransactionError
    └── ApplicationError
        ├── ConfigurationError
        └── DependencyError
```

### Implementation Details

The exception hierarchy is implemented in `src/utils/exceptions.py`:

```python
"""Custom exceptions for the SkypeParser project."""

class SkypeParserError(Exception):
    """Base exception for all SkypeParser errors."""

    def __init__(self, message=None, cause=None, **context):
        """
        Initialize the exception with a message, cause, and additional context.

        Args:
            message: Error message
            cause: Original exception that caused this error
            **context: Additional context information for debugging
        """
        self.message = message or "An error occurred in SkypeParser"
        self.cause = cause
        self.context = context

        # Format the message with context if available
        if context:
            context_str = ", ".join(f"{k}={v!r}" for k, v in context.items())
            full_message = f"{self.message} [{context_str}]"
        else:
            full_message = self.message

        # Include the cause's message if available
        if cause and not isinstance(cause, str):
            full_message = f"{full_message} caused by: {str(cause)}"

        super().__init__(full_message)

# Validation Errors
class ValidationError(SkypeParserError):
    """Base class for validation errors."""

class TimestampParsingError(ValidationError):
    """Raised when a timestamp cannot be parsed correctly."""

class SchemaValidationError(ValidationError):
    """Raised when data fails schema validation."""

    def __init__(self, message=None, errors=None, **context):
        """
        Initialize with validation errors.

        Args:
            message: Error message
            errors: List of validation error details
            **context: Additional context information
        """
        self.errors = errors or []
        super().__init__(message or "Schema validation failed", **context)

class InvalidInputError(ValidationError):
    """Raised when input data is invalid or missing required fields."""

# Parse Errors
class ParseError(SkypeParserError):
    """Base class for parsing errors."""

class ContentParsingError(ParseError):
    """Raised when message content cannot be parsed correctly."""

class DataExtractionError(ParseError):
    """Raised when data cannot be extracted from the Skype export file."""

# IO Errors
class IOError(SkypeParserError):
    """Base class for input/output errors."""

class FileOperationError(IOError):
    """Raised when a file operation (read/write) fails."""

class ExportError(IOError):
    """Raised when exporting conversations fails."""

# Database Errors
class DatabaseError(SkypeParserError):
    """Base class for database errors."""

class ConnectionError(DatabaseError):
    """Raised when a database connection fails."""

class QueryError(DatabaseError):
    """Raised when a database query fails."""

class TransactionError(DatabaseError):
    """Raised when a database transaction fails."""

# Application Errors
class ApplicationError(SkypeParserError):
    """Base class for application-level errors."""

class ConfigurationError(ApplicationError):
    """Raised when there is an issue with application configuration."""

class DependencyError(ApplicationError):
    """Raised when a required dependency is missing or incompatible."""
```

### Using Custom Exceptions

Custom exceptions should be used throughout the application to provide specific error information:

```python
def parse_timestamp(timestamp_str):
    """Parse a timestamp string into a datetime object."""
    if not timestamp_str:
        raise InvalidInputError("Empty timestamp provided", field="timestamp")

    try:
        return datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
    except ValueError as e:
        raise TimestampParsingError(
            f"Invalid timestamp format: {timestamp_str}",
            cause=e,
            timestamp=timestamp_str
        ) from e
```

## Error Context Management

The `ErrorContext` class provides a powerful way to attach contextual information to errors, enhancing debugging and user feedback.

### ErrorContext Implementation

```python
"""Error context management for the SkypeParser project."""

import contextlib
import functools
import inspect
import logging
import os
import sys
import traceback
from typing import Any, Callable, Dict, Optional, Type, TypeVar, cast

from .exceptions import SkypeParserError

logger = logging.getLogger(__name__)

F = TypeVar('F', bound=Callable[..., Any])

class ErrorContext:
    """
    Context manager and decorator for adding context to errors.

    This can be used as a context manager:

    ```python
    with ErrorContext(user_id="123", operation="parse"):
        # Code that might raise an exception
        parse_data(data)
    ```

    Or as a decorator:

    ```python
    @ErrorContext(component="parser")
    def parse_data(data):
        # Implementation
    ```
    """

    # Class-level context that applies to all instances
    _global_context: Dict[str, Any] = {}

    @classmethod
    def set_global_context(cls, **kwargs) -> None:
        """Set context values that apply to all ErrorContext instances."""
        cls._global_context.update(kwargs)

    @classmethod
    def clear_global_context(cls) -> None:
        """Clear all global context values."""
        cls._global_context.clear()

    def __init__(self, **context):
        """
        Initialize with context key-value pairs.

        Args:
            **context: Context key-value pairs to attach to any error raised
                      within this context.
        """
        self.context = context
        self.combined_context: Dict[str, Any] = {}

    def __enter__(self):
        """Enter the context manager."""
        # Combine global and local context
        self.combined_context = {**self._global_context, **self.context}
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the context manager.

        If an exception was raised, attach the context to it.
        """
        if exc_val is not None:
            self._attach_context(exc_val)
        return False  # Don't suppress the exception

    def __call__(self, func: F) -> F:
        """
        Use as a decorator to attach context to errors raised in a function.

        Args:
            func: The function to decorate

        Returns:
            The decorated function
        """
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with self:
                return func(*args, **kwargs)
        return cast(F, wrapper)

    def _attach_context(self, exc: Exception) -> None:
        """
        Attach context to an exception.

        Args:
            exc: The exception to attach context to
        """
        if isinstance(exc, SkypeParserError):
            # For our custom exceptions, update their context
            exc.context.update(self.combined_context)
        else:
            # For other exceptions, log the context
            context_str = ", ".join(f"{k}={v!r}" for k, v in self.combined_context.items())
            logger.debug(f"Context for {type(exc).__name__}: {context_str}")
```

### Using ErrorContext

The `ErrorContext` can be used in two ways:

#### As a Context Manager

```python
def process_user_data(user_id, data):
    """Process user data with error context."""
    with ErrorContext(user_id=user_id, action="process_data", data_size=len(data)):
        # If an error occurs here, it will include the context information
        validate_data(data)
        transform_data(data)
        save_data(data)
```

#### As a Decorator

```python
@ErrorContext(component="data_processor", priority="high")
def process_data(data):
    """Process data with attached error context from decorator."""
    # This function will have the component context attached to any errors
    validate_data(data)
    transform_data(data)
    save_data(data)
```

#### Setting Global Context

For application-wide context:

```python
# In application startup
ErrorContext.set_global_context(
    app_version="1.2.3",
    environment="production"
)

# This context will be included in all errors
```

## Standardized Error Handling

### handle_errors Decorator

The `handle_errors` decorator provides a standardized approach to catching and handling errors:

```python
def handle_errors(
    error_types=None,
    reraise=True,
    log_level="ERROR",
    default_message="An error occurred",
    include_traceback=False,
    error_handler=None
):
    """
    Decorator for standardized error handling.

    Args:
        error_types: List of exception types to catch (defaults to Exception)
        reraise: Whether to reraise the exception after handling
        log_level: Level to log the error at
        default_message: Default message if none is provided
        include_traceback: Whether to include the traceback in the log
        error_handler: Optional function to call with the error and context

    Returns:
        Decorator function
    """
    if error_types is None:
        error_types = (Exception,)
    elif not isinstance(error_types, (list, tuple)):
        error_types = (error_types,)

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except error_types as e:
                # Get function info for context
                module = func.__module__
                qualname = func.__qualname__

                # Get calling info
                frame = inspect.currentframe().f_back
                filename = frame.f_code.co_filename
                lineno = frame.f_lineno

                # Create error report
                error_report = {
                    "error_type": type(e).__name__,
                    "error_message": str(e),
                    "function": f"{module}.{qualname}",
                    "arguments": {
                        "args": args,
                        "kwargs": kwargs
                    },
                    "location": {
                        "file": os.path.basename(filename),
                        "line": lineno
                    }
                }

                # Include traceback if requested
                if include_traceback:
                    error_report["traceback"] = traceback.format_exc()

                # Get context from custom exception
                if isinstance(e, SkypeParserError) and hasattr(e, "context"):
                    error_report["context"] = e.context

                # Log the error
                log_message = f"{default_message}: {e}"
                getattr(logger, log_level.lower())(
                    log_message,
                    extra={"error_report": error_report}
                )

                # Call custom error handler if provided
                if error_handler:
                    error_handler(e, error_report)

                # Reraise or return None
                if reraise:
                    raise
                return None

        return wrapper

    return decorator
```

### Using the handle_errors Decorator

```python
@handle_errors(
    error_types=[ValidationError, DataExtractionError],
    reraise=True,
    log_level="ERROR",
    default_message="Error processing Skype data",
    include_traceback=True
)
def process_skype_data(file_path, user_name):
    """Process Skype data with standardized error handling."""
    validate_input(file_path, user_name)
    data = extract_data(file_path)
    return transform_data(data, user_name)
```

## Error Reporting

The `report_error` function provides a standardized way to report errors with additional context:

```python
def report_error(
    error,
    log_level="ERROR",
    include_traceback=False,
    additional_context=None
):
    """
    Report an error with standardized format and additional context.

    Args:
        error: The exception to report
        log_level: Level to log the error at
        include_traceback: Whether to include the traceback
        additional_context: Additional context to include

    Returns:
        Dict with error details
    """
    # Create base error report
    error_report = {
        "error_type": type(error).__name__,
        "error_message": str(error),
        "timestamp": datetime.now().isoformat()
    }

    # Add traceback if requested
    if include_traceback:
        error_report["traceback"] = traceback.format_exc()

    # Add context from SkypeParserError
    if isinstance(error, SkypeParserError) and hasattr(error, "context"):
        error_report["context"] = error.context

    # Add additional context
    if additional_context:
        if "context" not in error_report:
            error_report["context"] = {}
        error_report["context"].update(additional_context)

    # Log the error
    getattr(logger, log_level.lower())(
        f"Error: {error}",
        extra={"error_report": error_report}
    )

    return error_report
```

### Using report_error

```python
try:
    process_data(data)
except Exception as e:
    error_details = report_error(
        error=e,
        log_level="ERROR",
        include_traceback=True,
        additional_context={"data_size": len(data), "user_id": user_id}
    )
    # error_details contains structured information about the error
    # Can be used for user feedback, monitoring, etc.
```

## Integration with Structured Logging

Error handling is integrated with the structured logging system to provide comprehensive error information in logs.

### Structured Logger Implementation

```python
"""Structured logging for the SkypeParser project."""

import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict, Optional

class StructuredLogger(logging.Logger):
    """
    Logger that supports structured logging.

    Extends the standard Python logger with methods for structured logging.
    """

    def debug_s(self, msg, *args, **kwargs):
        """Debug log with structured data."""
        self._log_structured(logging.DEBUG, msg, args, kwargs)

    def info_s(self, msg, *args, **kwargs):
        """Info log with structured data."""
        self._log_structured(logging.INFO, msg, args, kwargs)

    def warning_s(self, msg, *args, **kwargs):
        """Warning log with structured data."""
        self._log_structured(logging.WARNING, msg, args, kwargs)

    def error_s(self, msg, *args, **kwargs):
        """Error log with structured data."""
        self._log_structured(logging.ERROR, msg, args, kwargs)

    def critical_s(self, msg, *args, **kwargs):
        """Critical log with structured data."""
        self._log_structured(logging.CRITICAL, msg, args, kwargs)

    def _log_structured(self, level, msg, args, kwargs):
        """
        Log a message with structured data.

        Args:
            level: Logging level
            msg: Log message
            args: Positional arguments for message formatting
            kwargs: Keyword arguments for structured data
        """
        # Extract standard logging kwargs
        exc_info = kwargs.pop('exc_info', None)
        extra = kwargs.pop('extra', {})
        stack_info = kwargs.pop('stack_info', False)

        # Add remaining kwargs as structured data
        if not extra:
            extra = {}
        extra['structured_data'] = kwargs

        # Log with standard logger
        self.log(level, msg, *args, exc_info=exc_info,
                 extra=extra, stack_info=stack_info)

# Replace the default logger class
logging.setLoggerClass(StructuredLogger)

def get_logger(name):
    """
    Get a structured logger.

    Args:
        name: Logger name (usually __name__)

    Returns:
        StructuredLogger instance
    """
    return logging.getLogger(name)
```

### Setting Up Logging Integration

The `setup_logging` function configures structured logging with error handling:

```python
def setup_logging(
    level="INFO",
    log_file=None,
    json_format=False,
    structured=True,
    rotation="size",
    max_bytes=10 * 1024 * 1024,  # 10 MB
    backup_count=5
):
    """
    Set up logging for the application.

    Args:
        level: Logging level
        log_file: Path to log file (None for console only)
        json_format: Whether to use JSON format
        structured: Whether to enable structured logging
        rotation: Rotation type ('size' or 'time')
        max_bytes: Maximum size of log file before rotation
        backup_count: Number of backup log files to keep
    """
    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, level.upper()))

    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create formatter
    if json_format:
        formatter = JsonFormatter()
    else:
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Create file handler if log_file is specified
    if log_file:
        if rotation == "size":
            from logging.handlers import RotatingFileHandler
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=max_bytes,
                backupCount=backup_count
            )
        else:  # time
            from logging.handlers import TimedRotatingFileHandler
            file_handler = TimedRotatingFileHandler(
                log_file,
                when="midnight",
                backupCount=backup_count
            )

        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Enable structured logging
    if structured:
        logging.setLoggerClass(StructuredLogger)

    # Log that logging has been set up
    root_logger.info(f"Logging initialized with level={level}, "
                    f"json_format={json_format}, structured={structured}")
```

## Schema Validation

Data validation is an important part of error prevention. The SkypeParser uses JSON Schema for validating data.

### Schema Validation Implementation

```python
"""Schema validation utilities."""

import json
import os
from typing import Any, Dict, List, Optional

import jsonschema
from jsonschema.exceptions import ValidationError as JsonSchemaValidationError

from .exceptions import SchemaValidationError

def validate_data(
    data: Dict[str, Any],
    schema_name: str,
    fill_defaults: bool = True,
    schema_dir: Optional[str] = None,
    raise_exception: bool = True
) -> Dict[str, Any]:
    """
    Validate data against a JSON schema.

    Args:
        data: Data to validate
        schema_name: Name of the schema file (without .json extension)
        fill_defaults: Whether to fill in default values
        schema_dir: Directory containing schema files (default: schemas/)
        raise_exception: Whether to raise an exception on validation failure

    Returns:
        Validated data (with defaults if fill_defaults=True)

    Raises:
        SchemaValidationError: If validation fails and raise_exception=True
    """
    # Determine schema directory
    if schema_dir is None:
        schema_dir = os.path.join(os.path.dirname(__file__), "../schemas")

    # Load schema
    schema_path = os.path.join(schema_dir, f"{schema_name}.json")
    with open(schema_path, 'r') as f:
        schema = json.load(f)

    # Create validator
    validator_cls = jsonschema.validators.validator_for(schema)
    validator = validator_cls(schema)

    # Validate
    errors = []
    for error in validator.iter_errors(data):
        errors.append({
            'path': '.'.join(str(p) for p in error.path),
            'message': error.message,
            'schema_path': '.'.join(str(p) for p in error.schema_path)
        })

    # Handle validation errors
    if errors:
        if raise_exception:
            raise SchemaValidationError(
                f"Validation failed for schema {schema_name}",
                errors=errors,
                schema=schema_name
            )
        return data

    # Fill defaults if requested
    if fill_defaults:
        return validator_cls(schema).validate(data)

    return data
```

### Using Schema Validation

```python
def process_config(config):
    """Process application configuration."""
    try:
        # Validate configuration against schema
        validated_config = validate_data(
            data=config,
            schema_name="app_config",
            fill_defaults=True
        )

        # Use validated configuration
        return setup_application(validated_config)

except SchemaValidationError as e:
        # Handle validation failure with specific error details
    print(f"Configuration validation failed: {e}")
        for error in e.errors:
            print(f"- {error['path']}: {error['message']}")

        # Exit with error or use default configuration
        sys.exit(1)
```

## Error Handling Best Practices

### Function-Level Error Handling

Functions in the SkypeParser project should follow these error handling patterns:

1. **Input Validation**: Functions should validate their inputs at the beginning and raise appropriate exceptions if the inputs are invalid.

2. **Specific Exception Handling**: Functions should catch specific exceptions and re-raise them as custom exceptions with more context.

3. **Comprehensive Documentation**: Function docstrings should include information about the exceptions they might raise.

Example:

```python
def extract_skype_data(file_path: str) -> Dict[str, Any]:
    """
    Extract data from a Skype export file.

    Args:
        file_path: Path to the Skype export file (JSON or TAR)

    Returns:
        Dict containing extracted Skype data

    Raises:
        FileOperationError: If the file cannot be read
        DataExtractionError: If data cannot be extracted from the file
        InvalidInputError: If the file path is invalid
    """
    # Validate input
    if not file_path:
        raise InvalidInputError("File path cannot be empty")

    # Wrap file operations in try/except
    try:
        with ErrorContext(file_path=file_path, operation="extract"):
            # Check file extension
            if file_path.endswith('.json'):
                return _extract_from_json(file_path)
            elif file_path.endswith('.tar'):
                return _extract_from_tar(file_path)
            else:
                raise InvalidInputError(
                    f"Unsupported file type: {file_path}",
                    supported_types=['.json', '.tar']
                )
    except FileNotFoundError as e:
        # Convert to our custom exception
        raise FileOperationError(
            f"File not found: {file_path}",
            cause=e,
            file_path=file_path
        ) from e
    except PermissionError as e:
        # Convert to our custom exception
        raise FileOperationError(
            f"Permission denied: {file_path}",
            cause=e,
            file_path=file_path
        ) from e
    except json.JSONDecodeError as e:
        # Convert to our custom exception
        raise DataExtractionError(
            f"Invalid JSON format in file: {file_path}",
            cause=e,
            file_path=file_path
        ) from e
    except Exception as e:
        # Generic fallback
        raise DataExtractionError(
            f"Failed to extract data from file: {file_path}",
            cause=e,
            file_path=file_path
        ) from e
```

### Module-Level Error Handling

Modules should apply consistent error handling patterns:

1. **Consistent Logging**: All modules should use the same logging approach.
2. **Exception Propagation**: Exceptions should be propagated up the call stack with added context.
3. **User-Friendly Error Messages**: Error messages should be informative and actionable.

### Application-Level Error Handling

The application should have a top-level error handler to catch uncaught exceptions:

```python
def main():
    """Main entry point for the application."""
    try:
        # Setup error handling and logging
        setup_logging(level="INFO", log_file="app.log")

        # Set global error context
        ErrorContext.set_global_context(
            app_version="1.2.3",
            start_time=datetime.now().isoformat()
        )

        # Run application
        run_application()

    except Exception as e:
        # Report the error
        error_report = report_error(
            error=e,
            log_level="CRITICAL",
            include_traceback=True
        )

        # Display user-friendly message
        if isinstance(e, SkypeParserError):
            print(f"Application error: {e.message}")
        else:
            print("An unexpected error occurred. Check the log for details.")

        # Exit with error code
    sys.exit(1)
    finally:
        # Clean up
        ErrorContext.clear_global_context()
```

## Recovery Strategies

The SkypeParser implements several recovery strategies for different error scenarios:

### 1. Retry Logic

For transient errors (e.g., network issues):

```python
def perform_with_retry(func, *args, max_retries=3, backoff_factor=2, **kwargs):
    """
    Perform a function with retry logic.

    Args:
        func: Function to call
        *args: Positional arguments for func
        max_retries: Maximum number of retries
        backoff_factor: Factor for exponential backoff
        **kwargs: Keyword arguments for func

    Returns:
        Result of the function call

    Raises:
        The last exception if all retries fail
    """
    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_exception = e

            # Check if we should retry
            if attempt < max_retries:
                # Log the retry
                logger.warning(
                    f"Attempt {attempt + 1}/{max_retries + 1} failed, retrying in "
                    f"{backoff_factor ** attempt} seconds. Error: {e}"
                )

                # Wait with exponential backoff
                time.sleep(backoff_factor ** attempt)
            else:
                # Log the final failure
                logger.error(
                    f"All {max_retries + 1} attempts failed. Error: {e}"
                )

    # Reraise the last exception
    raise last_exception
```

### 2. Partial Recovery

For errors that affect only part of the data:

```python
def process_skype_data_with_partial_recovery(data):
    """
    Process Skype data with partial recovery for individual conversations.

    Args:
        data: Skype data to process

    Returns:
        Dictionary with results and errors
    """
    results = {
        "success_count": 0,
        "error_count": 0,
        "conversations": {},
        "errors": {}
    }

    for conv_id, conversation in data.get("conversations", {}).items():
        try:
            with ErrorContext(conversation_id=conv_id):
                # Process the conversation
                results["conversations"][conv_id] = process_conversation(conversation)
                results["success_count"] += 1
except Exception as e:
            # Record the error but continue processing
            results["errors"][conv_id] = report_error(
                error=e,
                log_level="WARNING",
                additional_context={"conversation_id": conv_id}
            )
            results["error_count"] += 1

    # Log summary
    logger.info(
        f"Processed {results['success_count']} conversations successfully, "
        f"{results['error_count']} failed"
    )

    return results
```

### 3. Graceful Degradation

For non-critical features:

```python
def render_message(message, include_formatting=True, include_attachments=True):
    """
    Render a message with graceful degradation for non-critical features.

    Args:
        message: Message to render
        include_formatting: Whether to include formatting
        include_attachments: Whether to include attachments

    Returns:
        Rendered message
    """
    result = {
        "id": message["id"],
        "content": message["content"],
        "timestamp": message["timestamp"],
        "sender": message["sender"],
    }

    # Try to add formatting, but degrade gracefully if it fails
    if include_formatting:
        try:
            result["formatted_content"] = format_message_content(message["content"])
        except Exception as e:
            logger.warning(f"Failed to format message content: {e}")
            result["formatted_content"] = message["content"]

    # Try to add attachments, but degrade gracefully if it fails
    if include_attachments and "attachments" in message:
        try:
            result["attachment_previews"] = generate_attachment_previews(
                message["attachments"]
            )
        except Exception as e:
            logger.warning(f"Failed to generate attachment previews: {e}")
            result["attachment_previews"] = []

    return result
```

## Conclusion

The error handling architecture in SkypeParser provides a comprehensive framework for dealing with errors in a structured, consistent, and user-friendly way. By following the patterns and best practices outlined in this document, developers can ensure that errors are properly handled, reported, and recovered from throughout the application.

Key benefits of this approach include:

1. **Better Debugging**: Rich contextual information helps diagnose issues
2. **Improved User Experience**: Clearer error messages and graceful recovery
3. **Consistent Behavior**: Standardized approach to error handling
4. **Enhanced Reliability**: Recovery strategies for different error scenarios
5. **Comprehensive Logging**: Integration with structured logging for better monitoring
