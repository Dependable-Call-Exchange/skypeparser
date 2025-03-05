# Test Data Factory Guidelines

This document provides guidelines for using `factory_boy` and `faker` to generate test data in the Skype Parser project.

## Overview

We use `factory_boy` and `faker` to create test data in a consistent, maintainable, and flexible way. These libraries help reduce duplication, improve test readability, and make it easier to create complex test scenarios.

## Installation

The required packages are included in `requirements-dev.txt`. To install them:

```bash
pip install -r requirements-dev.txt
```

## When to Use Factories

- **Use factories** when creating complex nested data structures
- **Use factories** when you need multiple variations of similar data
- **Use factories** for testing edge cases and validation
- **Consider hardcoded fixtures** for simple, static test data that rarely changes

## Available Factories

### Message Factory

`SkypeMessageFactory` creates message data with sensible defaults:

```python
from tests.factories import SkypeMessageFactory

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

### Conversation Factory

`SkypeConversationFactory` creates conversation data with messages:

```python
from tests.factories import SkypeConversationFactory, SkypeMessageFactory

# Create a conversation with default messages
conversation = SkypeConversationFactory.build()

# Create a conversation with specific messages
conversation = SkypeConversationFactory.build(
    id='conv123',
    displayName='My Conversation',
    MessageList=[
        SkypeMessageFactory.build(content='Message 1'),
        SkypeMessageFactory.build(content='Message 2')
    ]
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

### Skype Data Factory

`SkypeDataFactory` creates complete Skype export data:

```python
from tests.factories import SkypeDataFactory

# Create basic Skype data
data = SkypeDataFactory.build()

# Create Skype data with specific fields
data = SkypeDataFactory.build(
    userId='user123',
    exportDate='2023-01-01T00:00:00Z'
)

# Create Skype data with a specific number of conversations
data = SkypeDataFactory.build(with_conversation_count=5)

# Create Skype data with specific conversations
data = SkypeDataFactory.build(
    conversations=[
        SkypeConversationFactory.build(displayName='Conversation 1'),
        SkypeConversationFactory.build(displayName='Conversation 2')
    ]
)
```

### Database Record Factory

`DatabaseRecordFactory` creates database records:

```python
from tests.factories import DatabaseRecordFactory

# Create a basic record
record = DatabaseRecordFactory.build()

# Create a conversation record
conversation_record = DatabaseRecordFactory.build(conversation=True)

# Create a message record
message_record = DatabaseRecordFactory.build(message=True)
```

## Factory Best Practices

1. **Use `build()` instead of `create()`**: In most cases, use `build()` to create objects without persisting them to a database. This is faster and avoids side effects.

2. **Keep factories focused**: Each factory should represent one data type and have sensible defaults.

3. **Use traits for variations**: Define common variations as traits to make tests more readable.

4. **Balance randomization and determinism**: Use fixed values for fields that tests depend on, and random values for fields that don't matter.

5. **Set a seed for reproducible tests**: Use `Faker.seed(12345)` to ensure that random data is reproducible.

6. **Document factory behavior**: Add comments explaining non-obvious factory logic.

7. **Use post-generation hooks**: Use `@factory.post_generation` to customize objects after they're created.

## Common Patterns

### Creating Related Data

```python
# Create a conversation with messages
conversation = SkypeConversationFactory.build(
    MessageList=[
        SkypeMessageFactory.build() for _ in range(3)
    ]
)

# Create complete Skype data
data = SkypeDataFactory.build(
    conversations=[
        SkypeConversationFactory.build() for _ in range(2)
    ]
)
```

### Testing Edge Cases

```python
# Test missing required fields
invalid_data = SkypeDataFactory.build(userId=None)

# Test invalid data types
invalid_data = SkypeDataFactory.build(exportDate='not-a-date')

# Test empty collections
empty_data = SkypeDataFactory.build(conversations=[])
```

### Creating Large Datasets

```python
# Create a large dataset for performance testing
large_data = SkypeDataFactory.build(
    with_conversation_count=100
)

# Create conversations with many messages
conversation = SkypeConversationFactory.build(
    with_message_count=1000
)
```

## Integration with Pytest

If you're using pytest, you can create fixtures for common factory patterns:

```python
import pytest
from tests.factories import SkypeDataFactory

@pytest.fixture
def basic_skype_data():
    return SkypeDataFactory.build()

@pytest.fixture
def complex_skype_data():
    return SkypeDataFactory.build(
        conversations=[
            SkypeConversationFactory.build(with_message_count=5),
            SkypeConversationFactory.build(with_message_count=10)
        ]
    )
```

## Troubleshooting

### Random Data Issues

If tests are failing due to random data, set a fixed seed:

```python
from faker import Faker
Faker.seed(12345)
```

### Performance Issues

If factories are slowing down tests:

1. Use `build()` instead of `create()`
2. Cache complex factory instances
3. Use pytest fixtures with `scope="session"` for large datasets

## Migration Guide

If you're migrating existing tests to use factories:

1. Identify repetitive test data patterns
2. Create factories for common data structures
3. Replace hardcoded data with factory calls
4. Use traits for variations
5. Refactor tests to use the new factories

## Example: Before and After

### Before

```python
def setUp(self):
    self.raw_data = {
        'userId': 'test-user-id',
        'exportDate': '2023-01-01T00:00:00Z',
        'conversations': [
            {
                'id': 'conv1',
                'displayName': 'Test Conversation 1',
                'MessageList': [
                    {
                        'id': 'msg1',
                        'originalarrivaltime': '2023-01-01T12:00:00Z',
                        'from': 'user1',
                        'messagetype': 'RichText',
                        'content': 'Hello world',
                        'edittime': None
                    }
                ]
            }
        ]
    }
```

### After

```python
def setUp(self):
    self.raw_data = SkypeDataFactory.build(
        userId='test-user-id',
        exportDate='2023-01-01T00:00:00Z',
        conversations=[
            SkypeConversationFactory.build(
                id='conv1',
                displayName='Test Conversation 1',
                MessageList=[
                    SkypeMessageFactory.build(
                        id='msg1',
                        originalarrivaltime='2023-01-01T12:00:00Z',
                        from_id='user1',
                        content='Hello world'
                    )
                ]
            )
        ]
    )
```