# Test Factory Patterns

This document provides an overview of the factory patterns used for testing in the Skype Parser project.

## Overview

The test factory patterns are designed to simplify the creation of test data and mock objects in a flexible and maintainable way. These patterns help reduce duplication, improve test readability, and make it easier to create complex test scenarios.

## Available Factory Patterns

### Data Factories

- `SkypeMessageFactory`: Creates message data with sensible defaults
- `SkypeConversationFactory`: Creates conversation data with messages
- `SkypeDataFactory`: Creates complete Skype export data
- `DatabaseRecordFactory`: Creates database records

### Mock Service Factories

- `MockServiceFactory`: Creates mock service objects with configurable behavior
- `MockBuilderFactory`: Creates complex mock objects using a builder pattern

## Using Data Factories

### SkypeMessageFactory

```python
from tests.fixtures import SkypeMessageFactory

# Create a basic message
message = SkypeMessageFactory.build()

# Create a message with specific fields
message = SkypeMessageFactory.build(
    id='custom-id',
    content='Custom content',
    from_id='user123'
)

# Create a message with a trait
html_message = SkypeMessageFactory.build(html=True)
link_message = SkypeMessageFactory.build(link=True)
edited_message = SkypeMessageFactory.build(edited=True)
```

### SkypeConversationFactory

```python
from tests.fixtures import SkypeConversationFactory, SkypeMessageFactory

# Create a conversation with default messages
conversation = SkypeConversationFactory.build()

# Create a conversation with specific fields
conversation = SkypeConversationFactory.build(
    id='conv123',
    displayName='My Conversation'
)

# Create a conversation with a specific number of messages
conversation = SkypeConversationFactory.build(with_message_count=5)

# Create a conversation with custom messages
conversation = SkypeConversationFactory.build(
    with_messages=[
        {'id': 'msg1', 'content': 'Hello'},
        {'id': 'msg2', 'content': 'World', 'edited': True}
    ]
)
```

### SkypeDataFactory

```python
from tests.fixtures import SkypeDataFactory

# Create basic Skype data
data = SkypeDataFactory.build()

# Create Skype data with specific fields
data = SkypeDataFactory.build(
    userId='user123',
    exportDate='2023-01-01T00:00:00Z'
)

# Create Skype data with a specific number of conversations
data = SkypeDataFactory.build(with_conversation_count=5)
```

## Using Mock Service Factories

### MockServiceFactory

```python
from tests.fixtures import MockServiceFactory

# Create a mock ContentExtractor
content_extractor = MockServiceFactory.create_content_extractor(
    extract_content_return="Custom content",
    extract_html_content_return="<p>Custom HTML content</p>",
    extract_cleaned_content_return="Custom cleaned content"
)

# Create a mock ValidationService
validation_service = MockServiceFactory.create_validation_service(
    file_exists_return=True,
    user_display_name_return="Test User"
)

# Create a mock FileHandler
file_handler = MockServiceFactory.create_file_handler(
    read_file_return=SkypeDataFactory.build()
)

# Create a mock StructuredDataExtractor
structured_data_extractor = MockServiceFactory.create_structured_data_extractor(
    extract_structured_data_return={
        "type": "custom",
        "mentions": ["@user1"],
        "links": ["https://example.com"],
        "formatted": True
    }
)

# Create a mock MessageHandlerFactory
message_handler_factory = MockServiceFactory.create_message_handler_factory(
    can_handle_return=True,
    extract_data_return={"type": "custom", "content": "Custom content"}
)
```

### MockBuilderFactory

```python
from tests.fixtures import MockBuilderFactory

# Create a mock ContentExtractor using builder pattern
content_extractor = MockBuilderFactory.content_extractor() \
    .with_content("Builder content") \
    .with_html_content("<p>Builder HTML content</p>") \
    .with_cleaned_content("Builder cleaned content") \
    .build()

# Create a mock ValidationService using builder pattern
validation_service = MockBuilderFactory.validation_service() \
    .with_file_exists(True) \
    .with_user_display_name("Test User") \
    .build()

# Create a mock ValidationService with an error
validation_service = MockBuilderFactory.validation_service() \
    .with_error('validate_file_exists', ValueError("File not found")) \
    .build()
```

## Using Factory Patterns with ETL Pipeline

```python
from tests.fixtures import (
    MockServiceFactory,
    SkypeDataFactory,
    MockDatabase
)
from src.db.etl_pipeline import SOLIDSkypeETLPipeline

# Create mock services
content_extractor = MockServiceFactory.create_content_extractor()
validation_service = MockServiceFactory.create_validation_service()
file_handler = MockServiceFactory.create_file_handler()
structured_data_extractor = MockServiceFactory.create_structured_data_extractor()
message_handler_factory = MockServiceFactory.create_message_handler_factory()

# Create mock database
mock_db = MockDatabase()

# Create pipeline with all dependencies injected
pipeline = SOLIDSkypeETLPipeline(
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

## Benefits of Using Factory Patterns

1. **Reduced Duplication**: Common test data is defined once and reused
2. **Improved Readability**: Tests focus on assertions rather than setup
3. **Better Maintainability**: Changes to data structures only need to be made in one place
4. **Flexibility**: Easy to create variations of test data
5. **Composability**: Factory patterns can be combined to create complex test scenarios

## Best Practices

1. **Use the most specific factory**: Choose the factory that most closely matches your test needs
2. **Customize only what's necessary**: Use the default values for fields that don't matter for your test
3. **Use traits for variations**: Define common variations as traits to make tests more readable
4. **Use builders for complex objects**: Use the builder pattern for objects with many configurable properties
5. **Document factory behavior**: Add comments explaining non-obvious factory logic

## See Also

- [ETL Fixtures README](fixtures/ETL_FIXTURES_README.md): Documentation for ETL fixtures
- [Example Tests](examples/factory_patterns_example.py): Example tests using factory patterns
