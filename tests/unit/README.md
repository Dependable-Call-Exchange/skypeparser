# Unit Tests for Skype Parser

This directory contains unit tests for the Skype Parser project. Unit tests focus on testing individual components in isolation, without dependencies on external systems like databases.

## Key Principles

1. **Isolation**: Tests should not depend on external systems or other components
2. **Speed**: Unit tests should run quickly to provide fast feedback
3. **Dependency Injection**: Use TestableETLPipeline for dependency injection instead of extensive patching
4. **Mocking**: Use mock objects to simulate dependencies
5. **Focused Testing**: Each test should focus on a specific behavior or edge case

## Test Organization

The unit tests are organized by module:

- `test_etl_pipeline.py`: Tests for the ETL pipeline using TestableETLPipeline
- `test_validation.py`: Tests for validation functions
- `test_file_handler.py`: Tests for file handling functions
- `test_parser_module.py`: Tests for parsing functions
- `test_content_extractor.py`: Tests for content extraction functionality, including HTML cleaning
- `test_message_type_handlers.py`: Tests for message type handlers, including edge cases
- `test_db_connection.py`: Tests for database connection methods and batch operations
- `test_etl_context.py`: Tests for ETL context initialization and attribute handling
- `test_etl_di_integration.py`: Tests for DI-specific service resolution with ETL components
- etc.

## Running Unit Tests

You can run the unit tests using pytest:

```bash
# Run all unit tests
pytest tests/unit/

# Run a specific unit test file
pytest tests/unit/test_etl_pipeline.py

# Run a specific test
pytest tests/unit/test_etl_pipeline.py::TestETLPipeline::test_transform
```

## Using TestableETLPipeline

The `TestableETLPipeline` class allows for dependency injection, making it easier to test without extensive patching. Here's how to use it in unit tests:

```python
from src.db.testable_etl_pipeline import TestableETLPipeline
from tests.fixtures import MockFileReader, MockDatabase

# Create mock objects
mock_reader = MockFileReader(sample_data)
mock_db = MockDatabase()

# Create testable pipeline with injected dependencies
pipeline = TestableETLPipeline(
    read_file_func=mock_reader.read_file,
    db_connection=mock_db.conn
)

# Test the pipeline
result = pipeline.run_pipeline(file_path='test.json')
assert result['extraction']['success']
```

## Dependency Injection Testing

The `test_etl_di_integration.py` file demonstrates how to test the dependency injection framework with ETL components:

```python
from src.utils.di import get_service_provider, get_service
from src.utils.service_registry import register_all_services
from src.utils.interfaces import ContentExtractorProtocol, LoaderProtocol

# Register all services
register_all_services(db_config, output_dir=output_dir)

# Resolve services
content_extractor = get_service(ContentExtractorProtocol)
loader = get_service(LoaderProtocol)

# Verify dependencies
assert loader.db_connection is not None
```

## Best Practices

1. **Use Fixtures**: Import test data and mock objects from `tests.fixtures`
2. **Descriptive Assertions**: Include descriptive messages in assertions
3. **Test Edge Cases**: Include tests for error conditions and edge cases
4. **Avoid Side Effects**: Tests should not affect each other or leave behind artifacts
5. **Clear Setup/Teardown**: Use setUp and tearDown methods to manage test state
6. **Test Interface Implementations**: Ensure all interface methods are properly tested
7. **Test Error Handling**: Verify that components handle errors gracefully