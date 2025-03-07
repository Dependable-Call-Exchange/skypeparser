# Centralized Dependency Handling in Skype Parser

This document outlines the centralized dependency handling approach implemented in the Skype Parser project.

## Overview

The Skype Parser project uses two complementary approaches to handle dependencies:

1. **Centralized dependency handling** for optional external libraries (via `src/utils/dependencies.py`)
2. **Dependency Injection (DI) framework** for internal components (via `src/utils/di.py` and `src/utils/service_registry.py`)

## External Dependency Handling

The Skype Parser project now uses a centralized approach to handle external dependencies across all modules. This approach ensures consistent behavior when dealing with optional dependencies like BeautifulSoup and psycopg2.

## Dependency Utility Module

The centralized dependency handling is implemented in the `src/utils/dependencies.py` module. This module provides:

1. Functions to check for the availability of dependencies
2. Pre-initialized module variables for common dependencies
3. Utility functions for requiring dependencies
4. Consistent error messages and logging

## Key Features

### Centralized Dependency Checking

The module checks for the availability of dependencies once at import time, ensuring consistent behavior across all modules:

```python
# Initialize BeautifulSoup once at module import time
BEAUTIFULSOUP_AVAILABLE, BeautifulSoup, BS_PARSER = get_beautifulsoup()

# Initialize database dependencies
PSYCOPG2_AVAILABLE, psycopg2 = get_psycopg2()
```

### Consistent Error Messages

The module provides consistent error messages for missing dependencies:

```python
logger.warning("lxml parser not found, falling back to html.parser. For best results, install lxml with: pip install lxml")
```

### Dependency Requirement Functions

The module includes utility functions for checking and requiring dependencies:

```python
def check_dependency(dependency_name: str) -> bool:
    """Check if a dependency is available."""
    # Implementation...

def require_dependency(dependency_name: str) -> None:
    """Require a dependency to be available. Exits if not available."""
    # Implementation...
```

## Supported Dependencies

### BeautifulSoup

BeautifulSoup is used for HTML parsing in the Skype Parser project. The dependency module provides:

- `BEAUTIFULSOUP_AVAILABLE`: Boolean indicating if BeautifulSoup is available
- `BeautifulSoup`: The BeautifulSoup class (or None if not available)
- `BS_PARSER`: The parser to use ('lxml', 'html.parser', or '' if not available)

### psycopg2

psycopg2 is used for PostgreSQL database operations in the Skype Parser project. The dependency module provides:

- `PSYCOPG2_AVAILABLE`: Boolean indicating if psycopg2 is available
- `psycopg2`: The psycopg2 module (or None if not available)

## Usage in Modules

### Importing Dependencies

Modules should import dependencies from the centralized module:

```python
from ..utils.dependencies import (
    BEAUTIFULSOUP_AVAILABLE as BEAUTIFULSOUP,
    BeautifulSoup,
    BS_PARSER
)
```

### Checking Dependency Availability

Modules should check for dependency availability before using a dependency:

```python
if BEAUTIFULSOUP:
    # Use BeautifulSoup
else:
    # Use fallback
```

### Requiring Dependencies

Modules can require dependencies when they are essential:

```python
from ..utils.dependencies import require_dependency

def function_that_needs_beautifulsoup():
    require_dependency('beautifulsoup')
    # Use BeautifulSoup...
```

## Benefits of Centralized Dependency Handling

The centralized dependency handling approach provides several benefits:

1. **Consistency**: All modules handle dependencies in the same way, ensuring consistent behavior.
2. **Maintainability**: Changes to dependency handling only need to be made in one place.
3. **Clarity**: Clear indication of which dependencies are optional and which are required.
4. **User Experience**: Consistent error messages and fallbacks for missing dependencies.
5. **Testing**: Easier to mock dependencies for testing.

## Best Practices for Dependency Handling

When working with the Skype Parser project, follow these best practices for dependency handling:

1. **Use the Centralized Module**: Always import dependencies from the centralized module.
2. **Check Availability**: Always check if a dependency is available before using it.
3. **Provide Fallbacks**: When possible, provide fallbacks for missing dependencies.
4. **Document Dependencies**: Document which dependencies are required and which are optional.
5. **Handle Gracefully**: Handle missing dependencies gracefully, providing useful error messages.

## Adding New Dependencies

To add a new dependency to the centralized module:

1. Add a function to check for the dependency:

```python
def get_new_dependency() -> Tuple[bool, Optional[Any]]:
    """Get new dependency."""
    try:
        import new_dependency
        return True, new_dependency
    except ImportError:
        logger.warning("new_dependency is not installed.")
        return False, None
```

2. Initialize the dependency at module import time:

```python
NEW_DEPENDENCY_AVAILABLE, new_dependency = get_new_dependency()
```

3. Add the dependency to the `check_dependency` function:

```python
def check_dependency(dependency_name: str) -> bool:
    """Check if a dependency is available."""
    if dependency_name.lower() == 'new_dependency':
        return NEW_DEPENDENCY_AVAILABLE
    # Other dependencies...
```

4. Update the module exports in `__init__.py`:

```python
from .dependencies import (
    # Existing exports...
    NEW_DEPENDENCY_AVAILABLE,
    new_dependency
)

__all__ = [
    # Existing exports...
    'NEW_DEPENDENCY_AVAILABLE',
    'new_dependency'
]
```

## Dependency Injection Framework

The Skype Parser project implements a dependency injection framework to promote SOLID principles, particularly the Dependency Inversion Principle. This framework helps make components more testable, maintainable, and loosely coupled.

### Key Components

1. **Service Provider (`src/utils/di.py`)**: A container that manages service registrations and instantiations.
2. **Service Registry (`src/utils/service_registry.py`)**: Centralizes the registration of services with the DI container.
3. **Interfaces (`src/utils/interfaces.py`)**: Defines protocols (interfaces) that establish contracts for services.

### Types of Service Registration

The DI framework supports three types of service registration:

1. **Singleton**: A single instance shared throughout the application
   ```python
   provider.register_singleton(DatabaseConnectionProtocol, db_connection)
   provider.register_singleton_class(FileHandlerProtocol, FileHandler)
   ```

2. **Transient**: A new instance created each time the service is requested
   ```python
   provider.register_transient(TransformerProtocol, Transformer)
   ```

3. **Factory**: A custom function that creates the service
   ```python
   provider.register_factory(ComplexServiceProtocol, create_complex_service)
   ```

### Resolving Dependencies

Services can be resolved from the DI container using the `get_service` function:

```python
from src.utils.di import get_service
from src.utils.interfaces import DatabaseConnectionProtocol

# Get the registered database connection
db_connection = get_service(DatabaseConnectionProtocol)
```

### Best Practices for Dependency Injection

When working with the Skype Parser project, follow these best practices for dependency injection:

1. **Program to Interfaces**: Use the protocols defined in `interfaces.py` rather than concrete implementations.

2. **Constructor Injection**: Accept dependencies through the constructor.
   ```python
   # Good
   def __init__(self, db_connection: DatabaseConnectionProtocol, file_handler: FileHandlerProtocol):
       self.db_connection = db_connection
       self.file_handler = file_handler

   # Avoid
   def __init__(self):
       self.db_connection = DatabaseConnection()  # Direct instantiation
   ```

3. **Use the Service Registry**: Register services through the service registry rather than directly instantiating them.

4. **Resolve Through DI Container**: Obtain dependencies from the DI container rather than creating them directly.

5. **Isolate Third-Party Dependencies**: Wrap third-party libraries in your own interfaces.

### Example: Using Dependency Injection

Here's an example of properly using dependency injection in a class:

```python
from src.utils.interfaces import DatabaseConnectionProtocol, FileHandlerProtocol

class ExampleService:
    def __init__(
        self,
        db_connection: DatabaseConnectionProtocol,
        file_handler: FileHandlerProtocol
    ):
        self.db_connection = db_connection
        self.file_handler = file_handler

    def process_file(self, file_path: str) -> None:
        # Use injected dependencies
        data = self.file_handler.read_file(file_path)
        self.db_connection.execute("INSERT INTO table VALUES (%s)", {"data": data})
```

To use this service with the DI container:

```python
from src.utils.di import get_service
from src.utils.service_registry import register_all_services
from src.utils.interfaces import DatabaseConnectionProtocol, FileHandlerProtocol

# Register services
register_all_services(db_config=db_config)

# Resolve dependencies from the container
db_connection = get_service(DatabaseConnectionProtocol)
file_handler = get_service(FileHandlerProtocol)

# Create the service with dependencies
service = ExampleService(db_connection, file_handler)
```

### Service Lifetime Management

Consider the appropriate lifetime for your services:

- **Singleton**: Use for services that maintain state that should be shared (e.g., database connections)
- **Transient**: Use for stateless services that perform operations but don't store state
- **Factory**: Use for services with complex creation logic or those that depend on non-service parameters

## SOLID Principles and Dependency Injection

The dependency injection framework helps enforce SOLID principles:

### Single Responsibility Principle (SRP)

Each class should have only one reason to change. DI helps by:
- Separating creation logic from business logic
- Allowing classes to focus on their core functionality

### Open/Closed Principle (OCP)

Classes should be open for extension but closed for modification. DI helps by:
- Allowing new implementations to be injected without changing existing code
- Supporting plugin-like architecture through interfaces

### Liskov Substitution Principle (LSP)

Subtypes should be substitutable for their base types. DI helps by:
- Working with interfaces rather than concrete implementations
- Ensuring implementations adhere to contracts defined by protocols

### Interface Segregation Principle (ISP)

Clients should not be forced to depend on methods they do not use. DI helps by:
- Promoting focused, specific interfaces
- Allowing clients to depend only on the interfaces they need

### Dependency Inversion Principle (DIP)

High-level modules should not depend on low-level modules; both should depend on abstractions. DI helps by:
- Explicitly inverting control flow
- Ensuring high-level modules depend on abstractions (interfaces)
- Allowing low-level modules to be swapped out easily

## Testing with Dependency Injection

One of the primary benefits of DI is improved testability. Here's how to use the DI framework for testing:

```python
# Create mock dependencies
mock_db_connection = MagicMock(spec=DatabaseConnectionProtocol)
mock_file_handler = MagicMock(spec=FileHandlerProtocol)

# Register mocks with the DI container
provider = ServiceProvider()
provider.register_singleton(DatabaseConnectionProtocol, mock_db_connection)
provider.register_singleton(FileHandlerProtocol, mock_file_handler)

# Setup mock behavior
mock_file_handler.read_file.return_value = {"test": "data"}

# Create the service to test
service = ExampleService(mock_db_connection, mock_file_handler)

# Test the service
service.process_file("test.json")

# Verify mock interactions
mock_file_handler.read_file.assert_called_once_with("test.json")
mock_db_connection.execute.assert_called_once()
```

## Common Anti-Patterns to Avoid

1. **Service Locator**: Avoid calling `get_service` within methods. Instead, use constructor injection.

2. **Direct Instantiation**: Don't create dependencies directly within a class. This creates tight coupling.

3. **Static Dependencies**: Avoid using static or global state. This makes testing difficult.

4. **Circular Dependencies**: Be careful not to create circular references between services.

5. **Overuse of DI**: Not everything needs to be injected; use DI where it adds value in terms of testing and maintainability.

## Conclusion

The centralized dependency handling approach in the Skype Parser project ensures consistent, maintainable, and user-friendly handling of external dependencies. By following this approach, developers can create more robust and maintainable code, and users can better understand and resolve issues related to missing dependencies.