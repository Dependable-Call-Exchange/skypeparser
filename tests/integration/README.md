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
- `test_etl_components_integration.py`: Tests for ETL components working together
- `test_etl_validation_integration.py`: Tests for validation with real data
- `test_modular_etl_integration.py`: Tests for modular ETL components
- `test_extractor_integration.py`: Tests for the extractor with real files
- `test_transformer_integration.py`: Tests for the transformer with real data
- `test_loader_integration.py`: Tests for the loader with real database
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

## Error Handling and Recovery Testing

The integration tests include scenarios for testing error handling and recovery mechanisms:

```python
# Test checkpoint creation and resumption
pipeline = ETLPipeline(db_config=db_config, output_dir=test_dir)

try:
    # Simulate an error during transformation
    with patch.object(pipeline.transformer, 'transform', side_effect=ValueError("Test error")):
        pipeline.run_pipeline(file_path=test_file)
except ValueError:
    pass

# Verify checkpoint was created
checkpoints = pipeline.get_available_checkpoints()
assert len(checkpoints) > 0

# Test resumption from checkpoint
resume_pipeline = ETLPipeline.load_from_checkpoint(checkpoints[0], db_config=db_config)
result = resume_pipeline.run_pipeline(resume_from_checkpoint=True)
assert result['status'] == 'completed'
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
6. **Test Error Recovery**: Verify that the system can recover from errors using checkpoints
7. **Test Various Data Formats**: Test with different Skype export formats and sizes
8. **Validate Data Integrity**: Ensure that data is correctly stored in the database