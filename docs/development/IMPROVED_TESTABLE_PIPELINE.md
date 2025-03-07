# Improved Testable ETL Pipeline

This document provides an overview of the `ImprovedTestableETLPipeline` class, which is a simplified and more consistent version of the `TestableETLPipeline` class.

## Overview

The `ImprovedTestableETLPipeline` class is designed to make testing the ETL pipeline easier by following better dependency injection practices and providing a more consistent interface. It follows the same pattern as the `SOLIDSkypeETLPipeline` class, making it easier to understand and use.

## Key Features

- **Clean Dependency Injection**: All dependencies are injected through the constructor, making it easy to replace them with mocks.
- **Consistent Interface**: The interface is consistent with the `SOLIDSkypeETLPipeline` class, making it easier to switch between them.
- **Simplified Initialization**: The initialization is simplified by removing complex logic for handling mock objects.
- **Factory Function**: A factory function `create_testable_etl_pipeline` is provided for easier creation.
- **Mock Services**: Mock implementations of `FileHandler` and `ValidationService` are provided for testing.

## Usage

### Basic Usage

```python
from src.db.testable_etl_pipeline import (
    ImprovedTestableETLPipeline,
    create_testable_etl_pipeline,
    MockFileHandler,
    MockValidationService
)
from tests.fixtures import (
    BASIC_SKYPE_DATA,
    MockDatabase,
    MockServiceFactory
)

# Create mock services
file_handler = MockFileHandler(
    read_file_func=lambda path: BASIC_SKYPE_DATA
)

validation_service = MockValidationService(
    validate_file_exists_func=lambda path: True,
    validate_user_display_name_func=lambda name: name
)

# Create mock database
mock_db = MockDatabase()

# Create mock content extractor
content_extractor = MockServiceFactory.create_content_extractor()

# Create mock structured data extractor
structured_data_extractor = MockServiceFactory.create_structured_data_extractor()

# Create mock message handler factory
message_handler_factory = MockServiceFactory.create_message_handler_factory()

# Create pipeline with all dependencies injected
pipeline = ImprovedTestableETLPipeline(
    db_config={'host': 'localhost', 'port': 5432, 'dbname': 'test_db'},
    file_handler=file_handler,
    validation_service=validation_service,
    db_connection=mock_db.conn,
    content_extractor=content_extractor,
    structured_data_extractor=structured_data_extractor,
    message_handler_factory=message_handler_factory
)

# Run the pipeline
result = pipeline.run_pipeline(
    file_path='test.json',
    user_display_name='Test User'
)
```

### Using the Factory Function

```python
from src.db.testable_etl_pipeline import create_testable_etl_pipeline
from tests.fixtures import MockDatabase, BASIC_SKYPE_DATA

# Create mock database
mock_db = MockDatabase()

# Create pipeline using factory function
pipeline = create_testable_etl_pipeline(
    db_config={'host': 'localhost', 'port': 5432, 'dbname': 'test_db'},
    db_connection=mock_db.conn,
    read_file_func=lambda path: BASIC_SKYPE_DATA,
    validate_file_exists_func=lambda path: True,
    validate_user_display_name_func=lambda name: name
)

# Run the pipeline
result = pipeline.run_pipeline(
    file_path='test.json',
    user_display_name='Test User'
)
```

### Using the MockServiceFactory

```python
from src.db.testable_etl_pipeline import ImprovedTestableETLPipeline
from tests.fixtures import MockDatabase, BASIC_SKYPE_DATA, MockServiceFactory

# Create mock services using MockServiceFactory
file_handler = MockServiceFactory.create_file_handler(
    read_file_return=BASIC_SKYPE_DATA
)

validation_service = MockServiceFactory.create_validation_service(
    file_exists_return=True,
    user_display_name_return='Test User'
)

content_extractor = MockServiceFactory.create_content_extractor()
structured_data_extractor = MockServiceFactory.create_structured_data_extractor()
message_handler_factory = MockServiceFactory.create_message_handler_factory()

# Create mock database
mock_db = MockDatabase()

# Create pipeline with all dependencies injected
pipeline = ImprovedTestableETLPipeline(
    db_config={'host': 'localhost', 'port': 5432, 'dbname': 'test_db'},
    file_handler=file_handler,
    validation_service=validation_service,
    db_connection=mock_db.conn,
    content_extractor=content_extractor,
    structured_data_extractor=structured_data_extractor,
    message_handler_factory=message_handler_factory
)

# Run the pipeline
result = pipeline.run_pipeline(
    file_path='test.json',
    user_display_name='Test User'
)
```

## Mock Services

### MockFileHandler

The `MockFileHandler` class is a mock implementation of the `FileHandlerProtocol` that allows you to specify custom functions for reading files and extracting tar files.

```python
from src.db.testable_etl_pipeline import MockFileHandler

# Create a mock file handler
file_handler = MockFileHandler(
    read_file_func=lambda path: {'userId': 'test-user', 'conversations': []},
    read_file_object_func=lambda file_obj: {'userId': 'test-user', 'conversations': []},
    tar_extract_func=lambda path: {'userId': 'test-user', 'conversations': []}
)
```

### MockValidationService

The `MockValidationService` class is a mock implementation of the `ValidationServiceProtocol` that allows you to specify custom functions for validating files and user display names.

```python
from src.db.testable_etl_pipeline import MockValidationService

# Create a mock validation service
validation_service = MockValidationService(
    validate_file_exists_func=lambda path: True,
    validate_file_object_func=lambda file_obj: True,
    validate_json_file_func=lambda path: {'userId': 'test-user', 'conversations': []},
    validate_user_display_name_func=lambda name: name
)
```

## Backward Compatibility

The original `TestableETLPipeline` class is still available for backward compatibility, but it is deprecated and will be removed in a future version. It now uses the `ImprovedTestableETLPipeline` internally, so it should behave the same way.

```python
from src.db.testable_etl_pipeline import TestableETLPipeline

# Create a testable pipeline (deprecated)
pipeline = TestableETLPipeline(
    db_config={'host': 'localhost', 'port': 5432, 'dbname': 'test_db'},
    read_file_func=lambda path: {'userId': 'test-user', 'conversations': []},
    validate_file_exists_func=lambda path: True,
    validate_user_display_name_func=lambda name: name
)
```

## Example Tests

For complete examples of how to use the `ImprovedTestableETLPipeline` class, see the following files:

- `tests/examples/improved_testable_pipeline_example.py`: Examples of using the `ImprovedTestableETLPipeline` class.
- `tests/examples/factory_patterns_example.py`: Examples of using the `MockServiceFactory` and `MockBuilderFactory` classes.

## See Also

- [ETL Fixtures README](../fixtures/ETL_FIXTURES_README.md): Documentation for ETL fixtures.
- [Factory Patterns README](../../tests/FACTORY_PATTERNS_README.md): Documentation for factory patterns.
