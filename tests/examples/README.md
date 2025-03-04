# Test Examples for Skype Parser

This directory contains example tests that demonstrate best practices and techniques for testing the Skype Parser project.

## Available Examples

### Refactored Test Example (`refactored_test_example.py`)

This example demonstrates how to refactor a test that uses extensive patching to a cleaner version that uses dependency injection with `TestableETLPipeline`. It includes:

- An original test with 9 patch decorators and complex setup
- A refactored test using dependency injection
- A comparison of the two approaches

## Key Benefits of Using TestableETLPipeline

1. **Reduced Patching**: No need for extensive patch decorators
2. **Cleaner Tests**: Dependency injection makes tests more readable
3. **Better Maintainability**: Changes to implementation don't require test changes
4. **Focused Testing**: Tests focus on behavior, not implementation details
5. **Realistic Testing**: More realistic testing of interactions between components

## How to Run the Examples

You can run the examples using pytest:

```bash
# Run all examples
pytest tests/examples/

# Run a specific example
pytest tests/examples/refactored_test_example.py
```

## How to Apply These Techniques

To apply these techniques to your own tests:

1. Import `TestableETLPipeline` instead of `SkypeETLPipeline`
2. Use fixtures from `tests.fixtures` to create mock objects
3. Inject dependencies into `TestableETLPipeline` instead of patching
4. Focus assertions on behavior and results, not implementation details

## Example Usage

```python
from src.db.testable_etl_pipeline import TestableETLPipeline
from tests.fixtures import create_mock_file_environment, MockDatabase

# Create mock environment
mock_env = create_mock_file_environment()
mock_db = MockDatabase()

# Create testable pipeline with injected dependencies
pipeline = TestableETLPipeline(
    read_file_func=mock_env['read_file'],
    validate_file_exists_func=mock_env['validate_file_exists'],
    db_connection=mock_db.conn
)

# Run the pipeline
result = pipeline.run_pipeline(file_path='test.json')

# Assertions
assert result['extraction']['success']
assert result['transformation']['success']
assert result['loading']['success']
```