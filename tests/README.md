# Tests for Skype Parser

This directory contains tests for the Skype Parser project. The tests are organized into different categories to improve maintainability and clarity.

## Test Organization

The tests are organized into the following directories:

- **Unit Tests** (`tests/unit/`): Tests for individual components in isolation
- **Integration Tests** (`tests/integration/`): Tests for components working together with real dependencies
- **Fixtures** (`tests/fixtures/`): Reusable test data and utilities
- **Examples** (`tests/examples/`): Example tests demonstrating best practices

## Key Features

### TestableETLPipeline

The `TestableETLPipeline` class extends `SkypeETLPipeline` to make it more testable through dependency injection. This allows for cleaner tests without extensive patching.

```python
from src.db.testable_etl_pipeline import TestableETLPipeline
from tests.fixtures import MockFileReader, MockDatabase

# Create testable pipeline with injected dependencies
pipeline = TestableETLPipeline(
    read_file_func=mock_reader.read_file,
    db_connection=mock_db.conn
)
```

### Fixtures

The `tests.fixtures` module provides reusable test data and utilities:

- **Skype Data Fixtures**: Predefined data structures for testing
- **Database Fixtures**: Utilities for database testing
- **Mock Fixtures**: Mock objects for unit testing

```python
from tests.fixtures import (
    BASIC_SKYPE_DATA,
    test_db_connection,
    MockFileReader
)
```

## Running Tests

You can run the tests using pytest:

```bash
# Run all tests
pytest

# Run unit tests only
pytest tests/unit/

# Run integration tests only
pytest tests/integration/

# Run a specific test file
pytest tests/unit/test_etl_pipeline.py

# Run a specific test
pytest tests/unit/test_etl_pipeline.py::TestETLPipeline::test_transform
```

## Test Categories

### Unit Tests

Unit tests focus on testing individual components in isolation, without dependencies on external systems like databases. They use mock objects to simulate dependencies.

See [Unit Tests README](unit/README.md) for more details.

### Integration Tests

Integration tests focus on testing components working together with real external dependencies like databases. They verify end-to-end functionality.

See [Integration Tests README](integration/README.md) for more details.

### Examples

Example tests demonstrate best practices for testing, including refactoring from extensive patching to dependency injection.

See [Examples README](examples/README.md) for more details.

## Best Practices

1. **Use TestableETLPipeline**: Prefer dependency injection over extensive patching
2. **Use Fixtures**: Import test data and mock objects from `tests.fixtures`
3. **Descriptive Assertions**: Include descriptive messages in assertions
4. **Test Edge Cases**: Include tests for error conditions and edge cases
5. **Separate Unit and Integration Tests**: Keep unit tests fast and isolated