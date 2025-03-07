# ETL Pipeline Test Fixtures

This document provides an overview of the specialized test fixtures for ETL pipeline components in the Skype Parser project.

## Overview

The ETL fixtures are designed to simplify testing of ETL pipeline components by providing pre-configured instances and test scenarios. These fixtures help reduce test setup code, improve test readability, and ensure consistent testing patterns across the codebase.

## Available Fixtures

### ETL Context Fixtures

- `etl_context`: A basic ETLContext instance with default configuration
- `etl_context_with_phases`: An ETLContext with pre-configured phases (extraction completed, transformation started)
- `etl_context_with_error`: An ETLContext with an error condition in the extraction phase

### Component Mock Fixtures

- `mock_file_handler`: A mock FileHandler instance
- `mock_validation_service`: A mock ValidationService instance
- `mock_content_extractor`: A mock ContentExtractor instance
- `mock_structured_data_extractor`: A mock StructuredDataExtractor instance
- `mock_message_handler_factory`: A mock MessageHandlerFactory instance

### Configured Component Fixtures

- `configured_extractor`: A fully configured Extractor instance with mock dependencies
- `configured_transformer`: A fully configured Transformer instance with mock dependencies
- `configured_loader`: A fully configured Loader instance with mock dependencies

### Test Data File Fixtures

- `temp_json_file`: Creates a temporary JSON file with valid Skype data
- `temp_invalid_json_file`: Creates a temporary file with invalid JSON content

### Error Scenario Fixtures

- `extraction_error_scenario`: Sets up a scenario for testing extraction errors
- `transformation_error_scenario`: Sets up a scenario for testing transformation errors
- `loading_error_scenario`: Sets up a scenario for testing loading errors

### Complete Pipeline Fixture

- `pipeline_test_environment`: Sets up a complete environment for testing the ETL pipeline

## Usage Examples

### Testing ETL Components

```python
def test_extractor(configured_extractor, mock_file_handler):
    """Test the Extractor using specialized fixtures."""
    # Setup
    test_file_path = 'test.json'

    # Execute
    result = configured_extractor.extract(file_path=test_file_path)

    # Verify
    assert result is not None
    mock_file_handler.read_file.assert_called_with(test_file_path)
```

### Testing Error Scenarios

```python
def test_extraction_error(extraction_error_scenario):
    """Test extraction error handling."""
    # Setup
    extractor = extraction_error_scenario['extractor']
    context = extraction_error_scenario['context']
    expected_error = extraction_error_scenario['expected_error']

    # Execute and verify
    with pytest.raises(ValueError) as exc_info:
        extractor.extract(file_path='test.json')

    assert expected_error in str(exc_info.value)
    assert context.get_phase_status('extraction') == 'failed'
```

### Testing the Complete Pipeline

```python
def test_pipeline(pipeline_test_environment):
    """Test the complete ETL pipeline."""
    # Setup
    env = pipeline_test_environment

    # Create pipeline with dependencies
    pipeline = SOLIDSkypeETLPipeline(
        db_config={'host': 'localhost', 'port': 5432, 'dbname': 'test_db'},
        file_handler=env['file_handler'],
        validation_service=env['validation_service'],
        db_connection=env['db_connection'],
        content_extractor=env['content_extractor'],
        structured_data_extractor=env['structured_data_extractor'],
        message_handler_factory=env['message_handler_factory']
    )

    # Execute
    result = pipeline.run_pipeline(
        file_path=env['test_file_path'],
        user_display_name=env['user_display_name']
    )

    # Verify
    assert result['extraction']['success']
    assert result['transformation']['success']
    assert result['loading']['success']
```

## Benefits of Using ETL Fixtures

1. **Reduced Duplication**: Common test setup code is defined once and reused
2. **Improved Readability**: Tests focus on assertions rather than setup
3. **Better Isolation**: Each test uses clean, isolated components
4. **Consistent Testing Patterns**: Standardized approach to testing ETL components
5. **Easier Maintenance**: Changes to component interfaces only need to be updated in one place

## Best Practices

1. **Use the most specific fixture**: Choose the fixture that most closely matches your test needs
2. **Combine fixtures**: Use multiple fixtures together to create complex test scenarios
3. **Override defaults**: Modify fixture attributes when needed for specific test cases
4. **Add cleanup**: Use pytest's yield fixtures for resources that need cleanup
5. **Document usage**: Add examples when creating new fixtures

## See Also

- [Mock Fixtures README](README.md): Documentation for mock fixtures
- [Example Tests](../examples/etl_fixtures_example.py): Example tests using ETL fixtures
