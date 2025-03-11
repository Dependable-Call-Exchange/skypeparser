# Centralized Logging Implementation Plan

This document outlines the plan for implementing a centralized, structured logging system for the SkypeParser project. The goal is to standardize logging across all components, improve observability, and make debugging easier.

## Table of Contents

1. [Current State](#current-state)
2. [Goals](#goals)
3. [Implementation Plan](#implementation-plan)
4. [Logging Architecture](#logging-architecture)
5. [Migration Strategy](#migration-strategy)
6. [Best Practices](#best-practices)
7. [Testing](#testing)

## Current State

The current logging implementation has several issues:

- **Inconsistent Initialization**: Some files use `logging.getLogger(__name__)` directly, while others use a custom `get_logger` function from `src.utils.structured_logging`.
- **Varied Configuration**: Different components configure logging differently, leading to inconsistent log formats and levels.
- **Limited Context**: Logs lack contextual information that would make them more useful for debugging and monitoring.
- **No Structured Format**: Logs are primarily text-based, making them difficult to parse and analyze programmatically.
- **Scattered Error Handling**: Error handling and logging are not consistently integrated.

## Goals

The centralized logging system aims to achieve the following:

1. **Consistency**: Ensure uniform log format, levels, and handling across the application.
2. **Structured Logging**: Implement JSON-formatted logs for easier parsing and analysis.
3. **Context Tracking**: Add request IDs and operation context to logs for better traceability.
4. **Configurability**: Make it easy to change logging behavior application-wide.
5. **Performance**: Optimize logging to minimize overhead.
6. **Integration**: Seamlessly integrate with error handling and monitoring systems.

## Implementation Plan

### Phase 1: Core Logging Infrastructure

1. **Enhance Structured Logging Utility**
   - Update `src/utils/structured_logging.py` with improved functionality
   - Add JSON formatting capability
   - Implement context tracking
   - Add helper functions for common logging patterns

2. **Create Logging Configuration Module**
   - Create `src/utils/logging_config.py` for centralized configuration
   - Support different environments (development, testing, production)
   - Configure third-party library loggers
   - Add rotation and size limits for log files

3. **Update Interfaces and Base Classes**
   - Ensure all base classes and interfaces use the centralized logging
   - Add logging-related methods to core interfaces where appropriate

### Phase 2: Component Migration

1. **Update ETL Components**
   - Migrate the Loader component to use centralized logging
   - Update Extractor and Transformer components
   - Add context tracking to ETL pipeline operations

2. **Update Database Components**
   - Migrate connection and pool implementations
   - Add performance logging for database operations

3. **Update Parser Components**
   - Migrate parser modules to use centralized logging
   - Add detailed logging for parsing operations

4. **Update Utility Modules**
   - Migrate remaining utility modules
   - Ensure consistent logging across all utilities

### Phase 3: Advanced Features

1. **Add Performance Monitoring**
   - Implement timing metrics for key operations
   - Add resource usage logging

2. **Implement Log Aggregation**
   - Add support for sending logs to external systems
   - Implement log correlation for distributed operations

3. **Create Monitoring Dashboard**
   - Develop a simple dashboard for log visualization
   - Add alerting for critical errors

## Logging Architecture

### Core Components

1. **Structured Logger**
   ```python
   # src/utils/structured_logging.py
   import logging
   import json
   import datetime
   import uuid
   import threading
   from typing import Any, Dict, Optional

   # Thread-local storage for request context
   _context = threading.local()

   def get_logger(name: str) -> logging.Logger:
       """Get a logger with the given name."""
       logger = logging.getLogger(name)
       return logger

   def set_context(key: str, value: Any) -> None:
       """Set a value in the current logging context."""
       if not hasattr(_context, "data"):
           _context.data = {}
       _context.data[key] = value

   # Additional functions for context management and formatting
   ```

2. **Logging Configuration**
   ```python
   # src/utils/logging_config.py
   import os
   import logging
   from typing import Dict, Any, Optional

   from src.utils.structured_logging import initialize_logging

   def configure_logging(
       app_name: str = "skype_parser",
       log_level: Optional[str] = None,
       log_file: Optional[str] = None,
       config: Optional[Dict[str, Any]] = None
   ) -> None:
       """Configure logging for the application."""
       # Implementation details
   ```

3. **JSON Formatter**
   ```python
   class JsonFormatter(logging.Formatter):
       """JSON formatter for structured logging."""

       def format(self, record: logging.LogRecord) -> str:
           """Format log record as JSON."""
           # Implementation details
   ```

### Log Format

The structured logs will follow this JSON format:

```json
{
  "timestamp": "2025-03-08T12:34:56.789Z",
  "level": "INFO",
  "logger": "src.db.etl.loader",
  "message": "Data loaded successfully with export ID: 123",
  "module": "loader",
  "function": "load",
  "line": 245,
  "context": {
    "request_id": "550e8400-e29b-41d4-a716-446655440000",
    "operation": "load",
    "file_source": "skype_export.tar",
    "batch_size": 100
  },
  "metrics": {
    "duration_ms": 1234,
    "records_processed": 500
  }
}
```

## Migration Strategy

To ensure a smooth transition to the centralized logging system, we'll follow this migration strategy:

### 1. Branch Strategy

Create the following branches:
- `feature/logging-infrastructure`: Core logging infrastructure
- `feature/etl-logging`: ETL component logging updates
- `feature/db-logging`: Database component logging updates
- `feature/parser-logging`: Parser component logging updates
- `feature/utils-logging`: Utility module logging updates

### 2. Incremental Migration

1. **Start with Core Infrastructure**
   - Implement the core logging infrastructure first
   - Create utility functions that make migration easier
   - Add backward compatibility for existing logging code

2. **Component-by-Component Migration**
   - Migrate one component at a time
   - Start with the most critical components (ETL, Database)
   - Add comprehensive tests for each migrated component

3. **Gradual Rollout**
   - Merge each branch back to main only after thorough testing
   - Monitor for any issues after each merge
   - Be prepared to roll back if necessary

### 3. Compatibility Layer

To ease migration, implement a compatibility layer:

```python
# src/utils/logging_compat.py
import logging
from src.utils.structured_logging import get_logger as get_structured_logger

def get_logger(name: str) -> logging.Logger:
    """Compatibility function for getting a logger.

    This function provides backward compatibility with code
    that uses the old logging approach.
    """
    return get_structured_logger(name)
```

## Best Practices

When implementing and using the centralized logging system, follow these best practices:

### 1. Log Level Guidelines

- **ERROR**: Use for errors that prevent normal operation
- **WARNING**: Use for unexpected conditions that don't prevent operation
- **INFO**: Use for significant events in normal operation
- **DEBUG**: Use for detailed information useful for debugging

### 2. Contextual Information

Always include relevant context in logs:

```python
# Good
logger.info(f"Processing file {filename} with {record_count} records")

# Better
logger.info(
    "Processing file",
    extra={
        "filename": filename,
        "record_count": record_count,
        "file_size_mb": file_size / (1024 * 1024)
    }
)
```

### 3. Error Logging

When logging exceptions, include the exception information:

```python
try:
    # Operation that might fail
    process_file(filename)
except Exception as e:
    logger.error(
        f"Failed to process file: {str(e)}",
        exc_info=True,
        extra={"filename": filename}
    )
```

### 4. Performance Considerations

- Avoid expensive operations in log messages unless the log level is enabled
- Use lazy evaluation for complex log messages
- Consider sampling for high-volume logs

```python
# Avoid this
logger.debug(f"User data: {get_expensive_user_data()}")

# Do this instead
if logger.isEnabledFor(logging.DEBUG):
    logger.debug(f"User data: {get_expensive_user_data()}")
```

## Testing

To ensure the logging system works correctly, implement the following tests:

### 1. Unit Tests

- Test logger initialization and configuration
- Test context tracking
- Test JSON formatting
- Test log level filtering

### 2. Integration Tests

- Test logging across component boundaries
- Test logging in error scenarios
- Test logging performance

### 3. Log Verification

Create a test utility to verify logs:

```python
# tests/utils/test_logging.py
import json
import logging
from io import StringIO

class LogCapture:
    """Capture logs for testing."""

    def __init__(self):
        self.log_output = StringIO()
        self.handler = logging.StreamHandler(self.log_output)
        self.handler.setFormatter(JsonFormatter())

    def __enter__(self):
        root_logger = logging.getLogger()
        self.old_level = root_logger.level
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(self.handler)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        root_logger = logging.getLogger()
        root_logger.setLevel(self.old_level)
        root_logger.removeHandler(self.handler)

    def get_logs(self):
        """Get captured logs as a list of dictionaries."""
        logs = []
        for line in self.log_output.getvalue().splitlines():
            if line.strip():
                logs.append(json.loads(line))
        return logs
```

Example test:

```python
def test_loader_logging():
    """Test that the loader logs correctly."""
    with LogCapture() as logs:
        # Create a loader and perform operations
        loader = Loader(context=mock_context, db_connection=mock_db)
        loader.load(mock_raw_data, mock_transformed_data, "test.json")

        # Get captured logs
        captured_logs = logs.get_logs()

        # Verify logs
        assert any(
            log["message"] == "Data loaded successfully with export ID: 1"
            for log in captured_logs
        )
        assert any(
            log["context"].get("operation") == "load"
            for log in captured_logs
        )
```

By following this implementation plan, we'll create a robust, centralized logging system that improves the maintainability and observability of the SkypeParser project.

## Troubleshooting and Error Resolution

During the migration to the centralized logging system, several types of errors may be encountered. This section outlines common issues and provides guidance on how to address them.

### Common Errors and Solutions

#### 1. Import Errors

Import errors are the most common issues encountered during the migration process:

```
ImportError: cannot import name 'read_file_object' from 'src.utils.file_handler'
ImportError: cannot import name 'get_file_extension' from 'src.utils.file_utils'
ImportError: cannot import name 'PollHandler' from 'src.utils.message_type_handlers'
```

**Resolution Strategy:**
- **Check Function Existence**: Verify if the function exists in the specified module
- **Update Import Paths**: If the function has been moved, update the import path
- **Implement Missing Functions**: If the function is needed but doesn't exist, implement it
- **Use Compatibility Layer**: For functions that have been renamed or replaced, use a compatibility layer

Example compatibility implementation:
```python
# In src/utils/file_handler.py
def read_file_object(file_obj, **kwargs):
    """Compatibility function for reading file objects."""
    from src.utils.new_file_handler import process_file_object
    return process_file_object(file_obj, **kwargs)
```

#### 2. Context Tracking Issues

Tests related to context tracking may fail when migrating to the new logging system:

```
KeyError: 'key1'
AssertionError: None != ''
```

**Resolution Strategy:**
- **Check Context Initialization**: Ensure context is properly initialized before use
- **Verify Decorator Implementation**: Make sure the `with_context` decorator correctly sets and clears context
- **Add Context Validation**: Add validation to prevent accessing non-existent context keys
- **Update Tests**: Update tests to match the new context behavior

Example fix for context tracking:
```python
def get_context_data() -> Dict[str, Any]:
    """Get the current context data safely."""
    if not hasattr(_context, "data"):
        _context.data = {}
    return _context.data.copy()  # Return a copy to prevent modification
```

#### 3. ETL Pipeline Integration Issues

The ETL pipeline may have issues integrating with the new logging system:

```
TypeError: TestableETLPipeline.__init__() got an unexpected keyword argument 'output_dir'
AttributeError: <module 'src.db.etl.pipeline_manager'> does not have the attribute 'MemoryMonitor'
```

**Resolution Strategy:**
- **Update Constructor Parameters**: Ensure constructors accept the same parameters or provide defaults
- **Implement Missing Components**: Add missing classes or methods referenced by tests
- **Use Dependency Injection**: Refactor to use dependency injection for better testability
- **Create Mock Objects**: Provide mock implementations for testing

Example fix for ETL pipeline:
```python
# In src/db/etl/pipeline_manager.py
class MemoryMonitor:
    """Monitor memory usage during ETL operations."""

    def __init__(self, threshold_mb=1000):
        self.threshold_mb = threshold_mb
        self.peak_usage = 0

    def check_memory(self):
        """Check current memory usage."""
        # Implementation
        pass
```

#### 4. Configuration Mismatches

Tests may fail due to configuration mismatches:

```
AssertionError: 'aws-0-us-west-1.pooler.supabase.com' != 'test-host'
```

**Resolution Strategy:**
- **Use Test-Specific Configurations**: Create separate configurations for tests
- **Mock Configuration Loading**: Use mocks to override configuration loading in tests
- **Add Environment Detection**: Make configuration aware of test environments
- **Update Test Expectations**: Update tests to match the actual configuration behavior

Example configuration fix:
```python
def get_db_config(env=None):
    """Get database configuration with environment awareness."""
    if env == 'test' or (env is None and os.environ.get('TESTING') == '1'):
        return {
            'host': 'test-host',
            'port': 5432,
            # Other test-specific settings
        }
    # Regular configuration loading
```

### Systematic Approach to Error Resolution

To systematically address the errors encountered during the logging migration:

1. **Categorize Errors**: Group errors by type (import, context, ETL, configuration)
2. **Prioritize Fixes**: Address fundamental issues first (imports, core functionality)
3. **Create Compatibility Layers**: Implement compatibility functions for backward compatibility
4. **Update Tests**: Modify tests to work with the new logging system
5. **Document Changes**: Keep track of all changes made during the migration
6. **Incremental Testing**: Test each fix individually before moving to the next issue

### Preventing Future Issues

To prevent similar issues in future updates:

1. **Comprehensive Test Coverage**: Ensure all logging functionality is covered by tests
2. **Versioned Interfaces**: Use versioned interfaces for major changes
3. **Deprecation Warnings**: Add deprecation warnings before removing or changing functionality
4. **Migration Scripts**: Provide scripts to automate migration of logging code
5. **Documentation**: Keep documentation up-to-date with all logging changes

By following these troubleshooting guidelines, the migration to the centralized logging system can be completed successfully with minimal disruption to the existing codebase.