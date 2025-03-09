# SkypeParser Test Suite

This directory contains the test suite for the SkypeParser project. The tests are organized by type (unit, integration, performance) and use pytest for test execution.

## Test Organization

The test suite is organized into the following directories:

```
tests/
├── conftest.py             # Shared pytest fixtures
├── fixtures/               # Test fixtures and data
│   ├── __init__.py
│   ├── etl_fixtures.py
│   ├── mock_fixtures.py
│   ├── mocks/              # Consolidated mock implementations
│   │   ├── __init__.py
│   │   ├── README.md
│   │   ├── content_extractor.py
│   │   ├── database.py
│   │   └── ...
│   ├── skype_data.py       # Test data
│   └── test_helpers.py
├── unit/                   # Unit tests
│   ├── test_etl_pipeline_pytest.py
│   ├── test_etl_loader.py
│   └── ...
├── integration/            # Integration tests
│   ├── test_db_connection_integration.py
│   ├── test_etl_validation_integration.py
│   └── ...
└── performance/            # Performance tests
    ├── test_etl_performance.py
    ├── test_streaming_processor.py
    └── ...
```

## Setup

### Install Test Dependencies

```bash
# Install the project with test dependencies
pip install -e ".[test]"

# Or install the test dependencies separately
pip install -r requirements-dev.txt
```

### Configure Database for Integration Tests

Some integration tests require a database connection. You can set up a test database using:

```bash
# Set up test database
python scripts/setup_test_db.py
```

Or set the `POSTGRES_TEST_DB` environment variable to `true` to use a mock database:

```bash
export POSTGRES_TEST_DB=true
```

## Running Tests

### Using the Enhanced Test Runner

The project includes an enhanced test runner script with detailed reporting and coverage analysis:

```bash
# Run all tests with coverage and reporting
python run_pytest_tests.py --coverage --report

# Run only unit tests
python run_pytest_tests.py --run-type unit

# Run only integration tests
python run_pytest_tests.py --run-type integration

# Run tests matching a specific pattern
python run_pytest_tests.py --pattern "test_etl_*.py"

# Run tests with a specific pytest marker
python run_pytest_tests.py --markers "etl_pipeline"
```

### Using pytest Directly

You can also run the tests directly with pytest:

```bash
# Run all tests
pytest

# Run unit tests only
pytest tests/unit/

# Run a specific test file
pytest tests/unit/test_etl_pipeline_pytest.py

# Run with coverage
pytest --cov=src tests/
```

## Test Fixtures

The test suite uses pytest fixtures for dependency injection and test data setup. Key fixtures include:

### Mock Components

```python
@pytest.fixture
def mock_file_handler():
    """Create a mock file handler."""
    return MockFileHandler()

@pytest.fixture
def mock_transformer():
    """Create a mock transformer."""
    return MockTransformer()
```

### Expected Data

```python
@pytest.fixture
def expected_transformed_message() -> TransformedMessage:
    """Fixture for a basic expected transformed message."""
    return BASIC_TRANSFORMED_MESSAGE

@pytest.fixture
def custom_expected_message() -> Callable[..., TransformedMessage]:
    """Fixture that allows customizing an expected message."""
    def _custom_expected_message(**kwargs: Any) -> TransformedMessage:
        return get_expected_transformed_message(**kwargs)
    return _custom_expected_message
```

### Test Data

```python
@pytest.fixture
def basic_skype_data():
    """Fixture for basic Skype data with one conversation and one message."""
    return SkypeDataFactory.build(...)
```

## Writing Tests

### Test Style

The test suite uses pytest-style tests with fixtures for dependency injection:

```python
def test_transform_basic(transformer, basic_skype_data, expected_transformed_data):
    """Test basic transformation functionality."""
    # Transform the raw data
    transformed_data = transformer.transform(basic_skype_data, 'Test User')

    # Verify the result using centralized expectations
    assert transformed_data['metadata']['user_display_name'] == 'Test User'
    assert len(transformed_data['conversations']) == 1
```

### Using Centralized Expectations

Instead of hardcoding expected values, use the centralized expectations:

```python
# Don't do this:
assert result["status"] == "success"
assert result["message"] == "Operation completed successfully"

# Do this instead:
expected = get_expected_api_response("success")
assert result["status"] == expected["status"]
assert result["message"] == expected["message"]
```

### Using Factory Patterns

Use factory patterns to generate test data:

```python
# Generate test data with factory patterns
test_data = SkypeDataFactory.build(
    with_conversation_count=2,
    with_message_count=5
)

# Generate expected data with factory patterns
expected_message = ExpectedTransformedMessageFactory.build(
    message_type="RichText/HTML",
    content="<b>Bold text</b>"
)
```

## Best Practices

1. **Use Dependency Injection**: Pass dependencies via pytest fixtures instead of patching.

2. **Centralize Expectations**: Use the centralized expectations in `tests/fixtures/expected_data.py` for common expected values.

3. **Isolate Tests**: Each test should be independent and not rely on global state or other tests.

4. **Test One Thing**: Each test should focus on testing one specific aspect of the code.

5. **Use Descriptive Names**: Test names should describe what is being tested and what the expected outcome is.

6. **Add Type Hints**: Use type hints in test code to improve IDE support and catch errors early.

7. **Use Factory Patterns**: Use factory patterns to generate test data with controlled randomness.

8. **Document Tests**: Add docstrings to test functions explaining what is being tested.

## Adding New Tests

1. Create a new test file in the appropriate directory (unit, integration, performance).

2. Import the necessary fixtures from conftest.py and any mock components needed.

3. Write pytest-style tests using dependency injection via fixtures.

4. Use centralized expectations for expected values.

5. Run the tests using the enhanced test runner:

```bash
python run_pytest_tests.py --pattern "test_new_feature.py"
```

## Documentation

For more details on the test suite improvements, see the following documents:

- [TEST_IMPROVEMENTS_REPORT.md](../TEST_IMPROVEMENTS_REPORT.md) - Summary of improvements made to the test suite
- [TEST_SUITE_OPTIMIZATION_REPORT.md](../TEST_SUITE_OPTIMIZATION_REPORT.md) - Comprehensive report on test suite optimization
- [tests/fixtures/mocks/README.md](fixtures/mocks/README.md) - Documentation for mock implementations