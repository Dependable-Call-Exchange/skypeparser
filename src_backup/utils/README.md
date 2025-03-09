# Utilities Module

This module provides utility functions for file handling, extraction, and other common operations used throughout the Skype Parser project.

## Overview

The utilities module contains several components that provide essential functionality for the Skype Parser project:

- **File Handler**: Functions for reading and extracting data from various file formats
- **File Utilities**: General file operations and helper functions
- **TAR Extractor**: Command-line tool for extracting and listing contents of TAR files

## Key Components

### File Handler (`file_handler.py`)

The file handler module provides functions for reading and extracting data from various file formats, including JSON files and TAR archives. It serves as the foundation for the Extraction phase of the ETL pipeline.

Key functions:

- `read_file(file_path)`: Read a JSON file from a file path
- `read_file_object(file_obj)`: Read a JSON file from a file-like object
- `read_tarfile(tar_path, json_index=0)`: Read a JSON file from a TAR archive
- `extract_tar_contents(tar_path, output_dir)`: Extract all contents from a TAR archive

### File Utilities (`file_utils.py`)

General file utility functions used throughout the project.

Key functions:

- `safe_filename(s)`: Sanitize a string to be used as a filename

### TAR Extractor (`tar_extractor.py`)

A command-line tool for extracting and listing contents of TAR files. It demonstrates the use of the `file_handler` module and includes argument parsing for various options.

## Usage

### File Handler

```python
from src.utils.file_handler import read_file, read_tarfile, extract_tar_contents

# Read a JSON file
data = read_file('path/to/file.json')

# Read a JSON file from a TAR archive
data = read_tarfile('path/to/archive.tar')

# Extract all contents from a TAR archive
extract_tar_contents('path/to/archive.tar', 'output_dir')
```

### File Utilities

```python
from src.utils.file_utils import safe_filename

# Sanitize a string to be used as a filename
safe_name = safe_filename('Unsafe/File:Name?')
# Result: 'Unsafe_File_Name_'
```

### TAR Extractor

```bash
# List contents of a TAR file
python -m src.utils.tar_extractor path/to/archive.tar --list

# Extract all contents from a TAR file
python -m src.utils.tar_extractor path/to/archive.tar --extract --output-dir output_dir

# Extract a specific file from a TAR file
python -m src.utils.tar_extractor path/to/archive.tar --extract --file-name specific_file.json
```

## Integration with ETL Pipeline

The utilities module, particularly the `file_handler` module, is a key component of the ETL pipeline. It provides the functionality for the Extraction phase, reading and validating data from Skype export files.

## Error Handling

All functions in the utilities module include comprehensive error handling. Errors are logged with appropriate context, and exceptions are raised with descriptive messages to help diagnose issues.

## Dependencies

- Python 3.6+
- tarfile (standard library)
- json (standard library)
- logging (standard library)

# Dependency Injection Framework

This directory contains a lightweight dependency injection (DI) framework for the Skype Parser project. The framework provides a simple way to manage dependencies and improve testability, maintainability, and flexibility of the codebase.

## Overview

The DI framework consists of the following components:

- **Interfaces**: Protocol classes that define the contracts for dependencies
- **Service Provider**: A container for registering and resolving dependencies
- **Service Registry**: A centralized place for registering all services

## Files

- `interfaces.py`: Contains protocol definitions for all dependencies
- `di.py`: Contains the `ServiceProvider` class and related functions
- `service_registry.py`: Contains functions for registering services

## Using the Framework

### Defining Interfaces

Interfaces are defined as Protocol classes in `interfaces.py`. For example:

```python
class FileHandlerProtocol(Protocol):
    def read_file(self, file_path: str) -> Dict[str, Any]:
        ...

    def read_file_obj(self, file_obj: BinaryIO, file_name: Optional[str] = None) -> Dict[str, Any]:
        ...
```

### Implementing Interfaces

Implementations should inherit from the corresponding protocol:

```python
class FileHandler(FileHandlerProtocol):
    def read_file(self, file_path: str) -> Dict[str, Any]:
        # Implementation
        ...

    def read_file_obj(self, file_obj: BinaryIO, file_name: Optional[str] = None) -> Dict[str, Any]:
        # Implementation
        ...
```

### Registering Services

Services are registered in `service_registry.py`:

```python
def register_core_services() -> None:
    provider = get_service_provider()

    # Register a singleton service
    provider.register_singleton(
        FileHandlerProtocol,
        FileHandler
    )

    # Register a factory function
    provider.register_factory(
        DatabaseConnectionProtocol,
        lambda: DatabaseConnection(db_config)
    )
```

### Resolving Services

Services can be resolved using the `get_service` function:

```python
from src.utils.di import get_service
from src.utils.interfaces import FileHandlerProtocol

# Get a service by its protocol
file_handler = get_service(FileHandlerProtocol)

# Use the service
data = file_handler.read_file("path/to/file.json")
```

### Constructor Injection

Services can be injected into constructors:

```python
def __init__(
    self,
    context: Optional[ETLContext] = None,
    file_handler: Optional[FileHandlerProtocol] = None
):
    self.context = context

    # Use provided dependency or get from service container
    self.file_handler = file_handler or get_service(FileHandlerProtocol)
```

## Service Lifetime

The framework supports three types of service lifetime:

1. **Singleton**: The same instance is returned for all requests
2. **Transient**: A new instance is created for each request
3. **Factory**: A factory function is called to create the instance

## Best Practices

1. **Always define interfaces**: Define interfaces for all dependencies to make the contracts explicit
2. **Use constructor injection**: Inject dependencies through constructors
3. **Provide default implementations**: Allow for default implementations to be used if no custom implementation is provided
4. **Register services centrally**: Use the service registry to register all services in one place
5. **Use interfaces in type hints**: Use protocol classes in type hints instead of concrete implementations

## Testing with the Framework

The DI framework makes testing easier by allowing dependencies to be mocked:

```python
def test_extractor():
    # Create mock dependencies
    mock_file_handler = Mock(spec=FileHandlerProtocol)
    mock_file_handler.read_file.return_value = {"test": "data"}

    # Create instance with mock dependencies
    extractor = Extractor(file_handler=mock_file_handler)

    # Test the extractor
    result = extractor.extract("test.json")

    # Verify mock was called
    mock_file_handler.read_file.assert_called_once_with("test.json")
```

## Migrating Existing Code

When migrating existing code to use the DI framework:

1. Define interfaces for the dependencies
2. Update implementations to inherit from the interfaces
3. Update constructors to accept dependencies
4. Register services in the service registry
5. Update code to resolve dependencies from the container

## Conclusion

The DI framework provides a simple way to manage dependencies and improve the quality of the codebase. By using interfaces and constructor injection, the code becomes more testable, maintainable, and flexible.