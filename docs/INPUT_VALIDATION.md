# Input Validation in SkypeParser

This document describes the input validation system implemented in the SkypeParser project, explaining how it works and how to use it effectively.

## Overview

The SkypeParser project now includes a comprehensive input validation system that ensures all data processed by the application is valid and safe. This system is implemented in the `src/utils/validation.py` module and is integrated throughout the codebase.

The validation system provides:

- Validation for file paths, directories, and file types
- Validation for Skype data structure
- Validation for user input and configuration
- Centralized error handling for validation failures
- Detailed error messages for debugging

## Validation Functions

### File System Validation

- `validate_path_safety(file_path, base_dir=None, allow_absolute=False, allow_symlinks=False)`: Performs strict validation on file paths to prevent security issues
- `validate_file_exists(file_path, base_dir=None, allow_absolute=False, allow_symlinks=False)`: Ensures a file exists and is accessible
- `validate_directory(directory, create_if_missing=False, base_dir=None, allow_absolute=False, allow_symlinks=False)`: Validates a directory exists or creates it
- `validate_file_type(file_path, allowed_extensions, base_dir=None, allow_absolute=False, allow_symlinks=False)`: Checks if a file has an allowed extension
- `validate_json_file(file_path, base_dir=None, allow_absolute=False, allow_symlinks=False)`: Validates and parses a JSON file
- `validate_tar_file(file_path, base_dir=None, allow_absolute=False, allow_symlinks=False)`: Validates a TAR archive
- `validate_file_object(file_obj, allowed_extensions)`: Validates a file-like object

### Data Validation

- `validate_skype_data(data)`: Validates the structure of Skype export data
- `validate_user_display_name(name)`: Validates and sanitizes a user display name

### Configuration Validation

- `validate_db_config(config)`: Validates database configuration
- `validate_config(config)`: Validates the application configuration

## Error Handling

All validation functions raise a `ValidationError` exception when validation fails. This exception includes a detailed error message that explains what went wrong and how to fix it.

Example:

```python
try:
    validate_file_exists("nonexistent.json")
except ValidationError as e:
    print(f"Validation error: {e}")
    # Handle the error appropriately
```

## Integration with ETL Pipeline

The ETL pipeline has been enhanced to use the validation system at each stage:

1. **Extraction**: Validates input files before processing
2. **Transformation**: Validates data structure before transformation
3. **Loading**: Validates database configuration before loading

This ensures that invalid data is caught early in the process, preventing errors and data corruption.

## Integration with Web Application

The web application has been enhanced to validate:

- File uploads (type, size, content)
- User input (display names, configuration)
- API requests (parameters, authentication)

This improves security and provides better error messages for users.

## Best Practices

When working with the SkypeParser project, follow these best practices for input validation:

1. **Always validate user input**: Use the appropriate validation function for any user-provided data
2. **Handle validation errors gracefully**: Catch `ValidationError` exceptions and provide helpful feedback
3. **Validate early**: Validate input as soon as it's received to prevent errors later in the process
4. **Use specific validation functions**: Choose the most specific validation function for your needs
5. **Add custom validation**: Extend the validation system with your own functions if needed

## Example Usage

### Validating a File Path

```python
from src.utils.validation import validate_file_exists, ValidationError

try:
    validate_file_exists("path/to/file.json")
    # File exists, proceed with processing
except ValidationError as e:
    logger.error(f"File validation error: {e}")
    # Handle the error (e.g., show error message to user)
```

### Validating Skype Data

```python
from src.utils.validation import validate_skype_data, ValidationError

try:
    validate_skype_data(data)
    # Data is valid, proceed with processing
except ValidationError as e:
    logger.error(f"Data validation error: {e}")
    # Handle the error (e.g., show error message to user)
```

### Validating Configuration

```python
from src.utils.validation import validate_config, ValidationError

try:
    validate_config(config)
    # Configuration is valid, proceed with processing
except ValidationError as e:
    logger.error(f"Configuration validation error: {e}")
    # Handle the error (e.g., show error message to user)
```

## Testing

The validation system includes comprehensive tests in `tests/test_validation.py`. These tests ensure that the validation functions work correctly and handle edge cases appropriately.

To run the tests:

```bash
cd tests
python -m unittest test_validation.py
```

## Path Safety Validation

The SkypeParser project now includes strict path validation to prevent security issues such as path traversal attacks. This validation is performed by the `validate_path_safety` function and is integrated into all file-related validation functions.

### Features

- **Path Traversal Prevention**: Detects and prevents path traversal attacks (e.g., `../../../etc/passwd`)
- **Absolute Path Control**: Allows you to restrict operations to relative paths only
- **Symbolic Link Control**: Allows you to prevent following symbolic links
- **Base Directory Restriction**: Ensures all file operations are within a specified base directory
- **Path Normalization**: Normalizes paths to resolve `.` and `..` components

### Usage Examples

#### Basic Path Validation

```python
from src.utils.validation import validate_path_safety, ValidationError

try:
    # Validate a path (relative paths only by default)
    safe_path = validate_path_safety("data/file.txt")

    # Now use the safe path for file operations
    with open(safe_path, 'r') as f:
        data = f.read()
except ValidationError as e:
    print(f"Path validation error: {e}")
```

#### Restricting to a Base Directory

```python
from src.utils.validation import validate_path_safety, ValidationError

try:
    # Ensure the path is within the uploads directory
    safe_path = validate_path_safety(
        user_provided_path,
        base_dir="uploads",
        allow_absolute=False,
        allow_symlinks=False
    )

    # Now use the safe path for file operations
    with open(safe_path, 'r') as f:
        data = f.read()
except ValidationError as e:
    print(f"Path validation error: {e}")
```

#### Allowing Absolute Paths

```python
from src.utils.validation import validate_path_safety, ValidationError

try:
    # Allow absolute paths but still validate for other security issues
    safe_path = validate_path_safety(
        "/path/to/file.txt",
        allow_absolute=True
    )

    # Now use the safe path for file operations
    with open(safe_path, 'r') as f:
        data = f.read()
except ValidationError as e:
    print(f"Path validation error: {e}")
```

### Best Practices

1. **Always validate user-provided paths**: Never use paths from untrusted sources without validation
2. **Use a base directory**: When possible, restrict file operations to a specific directory
3. **Avoid absolute paths**: Use relative paths when possible to limit access to the file system
4. **Disable symbolic links**: Prevent following symbolic links to avoid accessing files outside the expected directory
5. **Handle validation errors gracefully**: Provide clear error messages to users when path validation fails

## Conclusion

The input validation system in SkypeParser provides a robust foundation for ensuring data integrity and security. By validating all input data, the application can prevent errors, improve security, and provide better feedback to users.