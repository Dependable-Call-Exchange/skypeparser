# Test Fixtures for Skype Parser

This directory contains test fixtures for the Skype Parser project. These fixtures are designed to make tests more maintainable, reduce duplication, and improve test clarity.

## Available Fixtures

### Skype Data Fixtures (`skype_data.py`)

Contains predefined data structures that match the Skype export format:

- `BASIC_SKYPE_DATA`: Simple valid Skype data with one conversation and one message
- `COMPLEX_SKYPE_DATA`: More complex data with multiple conversations and message types
- `INVALID_SKYPE_DATA`: Data with invalid fields to test error handling
- `MINIMAL_SKYPE_DATA`: Minimal valid data structure
- `CONVERSATION_SKIP_TEST_DATA`: Data for testing conversation skipping behavior

### Database Fixtures (`db_fixtures.py`)

Utilities for database testing:

- `get_test_db_config()`: Get database configuration from environment variables
- `test_db_connection()`: Context manager that creates a test database connection with a unique schema
- `is_db_available()`: Check if the test database is available

### Mock Fixtures (`mock_fixtures.py`)

Mock objects and functions to simplify unit testing:

- `MockFileReader`: Mock file reader that returns predefined data for file paths
- `MockDatabase`: Mock database for testing ETL pipeline loading
- `create_mock_file_environment()`: Create a set of mock functions for file operations

## Usage Examples

### Using Skype Data Fixtures

```python
from tests.fixtures import BASIC_SKYPE_DATA, COMPLEX_SKYPE_DATA

def test_transform():
    result = pipeline.transform(BASIC_SKYPE_DATA)
    # assertions...
```

### Using Database Fixtures

```python
from tests.fixtures import test_db_connection, is_db_available

def test_integration():
    if not is_db_available():
        pytest.skip("Database not available")

    with test_db_connection() as conn:
        # Test with real database in isolated schema
        # ...
```

### Using Mock Fixtures

```python
from tests.fixtures import create_mock_file_environment

def test_extract():
    mock_env = create_mock_file_environment()

    with mock_env["read_file"], mock_env["validate_file_exists"]:
        result = pipeline.extract(file_path='test.json')
        # assertions...
```

### Using TestableETLPipeline with Fixtures

The `TestableETLPipeline` class allows for dependency injection, making it easier to test without extensive patching. Here's how to use it with our fixtures:

```python
from src.db.testable_etl_pipeline import TestableETLPipeline
from tests.fixtures import BASIC_SKYPE_DATA, MockFileReader, MockDatabase

# Create mock objects
mock_reader = MockFileReader(BASIC_SKYPE_DATA)
mock_db = MockDatabase()

# Create testable pipeline with injected dependencies
pipeline = TestableETLPipeline(
    read_file_func=mock_reader.read_file,
    read_file_object_func=mock_reader.read_file_object,
    db_connection=mock_db.conn
)

# Run the pipeline
results = pipeline.run_pipeline(file_path='test.json')

# Verify results
assert results['extraction']['success']
assert results['transformation']['success']
assert results['loading']['success']

# Check database queries
executed_queries = mock_db.get_executed_queries()
assert any("INSERT INTO skype_raw_exports" in query for query in executed_queries)
```

## Benefits of Using These Fixtures

1. **Reduced Duplication**: Common test data is defined once and reused
2. **Improved Maintainability**: Changes to data structures only need to be made in one place
3. **Better Test Isolation**: Each test uses a clean database schema
4. **Simplified Mocking**: Fewer patches needed in individual tests
5. **More Descriptive Tests**: Focus on what's being tested, not on setup
6. **Dependency Injection**: With TestableETLPipeline, tests are cleaner and more focused

## Adding New Fixtures

When adding new fixtures:

1. Place them in the appropriate file based on their type
2. Add them to the `__init__.py` exports
3. Document their purpose and structure
4. Consider adding usage examples to this README