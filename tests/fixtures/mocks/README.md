# Mock Implementations for Testing

This directory contains organized mock implementations of various components used in the Skype Parser, making it easier to test components in isolation. The mocks are organized by component type to follow the Single Responsibility Principle.

## Directory Structure

The mocks are organized into separate files by component type:

- `content_extractor.py`: Mock implementation of ContentExtractor
- `database.py`: Mock implementation of database connection
- `etl_components.py`: Mock implementations of ETL components (Extractor, Transformer, Loader)
- `file_handler.py`: Mock implementation of FileHandler
- `message_handler.py`: Mock implementations of message handlers and factories
- `message_processor.py`: Mock implementation of message processor
- `progress_tracker.py`: Mock implementation of progress tracker
- `structured_data_extractor.py`: Mock implementations of structured data extractors
- `validation_service.py`: Mock implementation of validation service

## Usage

All mocks are imported and re-exported through the `__init__.py` file, so you can import them directly from the `tests.fixtures.mocks` package:

```python
from tests.fixtures.mocks import (
    MockContentExtractor,
    MockDatabase,
    MockExtractor,
    MockFileHandler,
    MockLoader,
    MockMessageHandler,
    MockMessageHandlerFactory,
    MockMessageProcessor,
    MockProgressTracker,
    MockStructuredDataExtractor,
    MockTransformer,
    MockValidationService,
)
```

For convenience, all mocks are also re-exported through the `tests.fixtures` package:

```python
from tests.fixtures import (
    MockContentExtractor,
    MockDatabase,
    MockExtractor,
    # ...
)
```

## Design Principles

The mock implementations follow these design principles:

1. **Single Responsibility Principle**: Each mock class has a single responsibility and is defined in a separate file.
2. **Interface Compliance**: Mocks implement the same interfaces as the real components they replace.
3. **Configurable Behavior**: Mocks can be configured to return specific values or exhibit specific behaviors.
4. **Testability**: Mocks track method calls and parameters for verification in tests.
5. **Consistency**: Mocks follow a consistent naming convention and API design.

## Examples

### Basic Usage

```python
from tests.fixtures import MockFileHandler

# Create a mock file handler with default behavior
mock_file_handler = MockFileHandler()

# Use the mock in a test
result = mock_file_handler.read_file("test.json")
assert mock_file_handler.read_file_calls == ["test.json"]
```

### Configuring Return Values

```python
from tests.fixtures import MockContentExtractor

# Create a mock content extractor with custom return values
mock_content_extractor = MockContentExtractor(
    extract_all_return={"links": ["https://example.com"], "mentions": ["@user1"]},
    clean_content_return="Custom cleaned content"
)

# Use the mock in a test
result = mock_content_extractor.extract_all("Some content")
assert result["links"] == ["https://example.com"]
assert result["mentions"] == ["@user1"]
```

### Testing Error Conditions

```python
from tests.fixtures import MockDatabase

# Create a mock database that fails on execute
mock_db = MockDatabase(should_fail=True)

# Use the mock in a test
with pytest.raises(Exception, match="Mock database error"):
    mock_db.execute("SELECT * FROM table")
```

## Contributing

When adding new mock implementations:

1. Create a new file for the mock class if it doesn't fit into an existing category.
2. Follow the existing naming conventions and API design.
3. Add the mock class to the `__init__.py` file.
4. Update this README.md file with information about the new mock.
5. Add tests for the new mock implementation.