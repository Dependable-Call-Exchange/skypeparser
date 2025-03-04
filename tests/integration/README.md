# Integration Tests for Skype Parser

This directory contains integration tests for the Skype Parser project. Integration tests focus on testing components working together with real external dependencies like databases.

## Key Principles

1. **Real Dependencies**: Tests use real database connections and file systems
2. **End-to-End Testing**: Tests verify that components work together correctly
3. **Isolation Between Tests**: Each test runs in its own database schema
4. **Realistic Scenarios**: Tests simulate real-world usage scenarios
5. **Comprehensive Verification**: Tests verify both results and side effects

## Test Organization

The integration tests are organized by module:

- `test_etl_pipeline_integration.py`: Tests for the ETL pipeline with real database connections
- `test_web_integration.py`: Tests for web API integration
- `test_postgres_connection.py`: Tests for database connection and operations
- etc.

## Running Integration Tests

Integration tests require a database connection. You can run them using pytest:

```bash
# Set environment variable to enable database tests
export POSTGRES_TEST_DB=true

# Run all integration tests
pytest tests/integration/

# Run a specific integration test file
pytest tests/integration/test_etl_pipeline_integration.py

# Run a specific test
pytest tests/integration/test_etl_pipeline_integration.py::TestETLPipelineIntegration::test_integration_run_pipeline
```

## Using TestableETLPipeline with Real Dependencies

The `TestableETLPipeline` class can be used with real dependencies for integration testing:

```python
from src.db.testable_etl_pipeline import TestableETLPipeline
from tests.fixtures import test_db_connection

# Create a pipeline with a real database connection
with test_db_connection() as conn:
    pipeline = TestableETLPipeline(
        output_dir=test_dir,
        db_connection=conn
    )

    # Test the pipeline with real dependencies
    result = pipeline.run_pipeline(file_path=real_file_path)
    assert result['loading']['success']
```

## Database Fixtures

The `tests.fixtures` module provides utilities for database testing:

- `test_db_connection()`: Creates a test database connection with a unique schema
- `is_db_available()`: Checks if the test database is available
- `get_test_db_config()`: Gets database configuration from environment variables

## Best Practices

1. **Skip When Unavailable**: Skip tests when external dependencies are not available
2. **Clean Up**: Always clean up test data and connections
3. **Isolated Schemas**: Use unique schemas for each test run
4. **Realistic Data**: Use realistic test data that matches production scenarios
5. **Comprehensive Checks**: Verify both results and side effects (e.g., database state)