# Dependency Injection in SkypeParser

This document explains the dependency management architecture in the SkypeParser project, including both centralized dependency handling for external libraries and the internal dependency injection framework.

## Introduction to Dependency Management

SkypeParser employs a two-pronged approach to dependency management:

1. **Centralized Dependency Handling**: For managing optional external libraries in a consistent way
2. **Dependency Injection Framework**: For wiring together internal components through abstractions

These approaches work together to create a system that is flexible, testable, and maintainable.

```
┌─────────────────────────────────────────┐
│            Application Code             │
└───────────────┬─────────────────────────┘
                │
                │ depends on
                ▼
┌───────────────────────────┐     ┌───────────────────────────┐
│  Dependency Injection     │     │  Centralized Dependency   │
│       Framework          │     │        Handling           │
└───────────────┬───────────┘     └───────────────┬───────────┘
                │                                 │
                │ manages                         │ manages
                ▼                                 ▼
┌───────────────────────────┐     ┌───────────────────────────┐
│    Internal Components    │     │    External Libraries     │
└───────────────────────────┘     └───────────────────────────┘
```

## Centralized Dependency Handling

### Purpose and Benefits

The centralized dependency handling approach provides several benefits:

1. **Consistent Error Handling**: All modules handle missing dependencies in the same way
2. **Improved User Experience**: Clear error messages and fallback behaviors for optional dependencies
3. **Simplified Dependency Checks**: A single point of reference for dependency availability
4. **Enhanced Testability**: Easier to mock dependencies in tests
5. **Reduced Code Duplication**: Centralized imports eliminate repetitive try/except blocks

### Implementation Details

The centralized dependency handling is implemented in the `src/utils/dependencies.py` module:

```python
"""Centralized dependency handling for optional external libraries."""

import importlib
import logging
from typing import Any, Optional, Tuple, Dict

# Set up logging
logger = logging.getLogger(__name__)

def get_beautifulsoup() -> Tuple[bool, Optional[Any], str]:
    """Get BeautifulSoup and preferred parser."""
    try:
        from bs4 import BeautifulSoup
        try:
            import lxml
            parser = 'lxml'
        except ImportError:
            logger.warning("lxml parser not found, falling back to html.parser. "
                          "For best results, install lxml with: pip install lxml")
            parser = 'html.parser'
        return True, BeautifulSoup, parser
    except ImportError:
        logger.warning("BeautifulSoup is not installed. HTML content will not be parsed. "
                      "Install with: pip install beautifulsoup4")
        return False, None, ''

def get_psycopg2() -> Tuple[bool, Optional[Any]]:
    """Get psycopg2 database adapter."""
    try:
        import psycopg2
        return True, psycopg2
    except ImportError:
        logger.warning("psycopg2 is not installed. Database operations will not be available. "
                      "Install with: pip install psycopg2-binary")
        return False, None

# Initialize dependencies at module import time
BEAUTIFULSOUP_AVAILABLE, BeautifulSoup, BS_PARSER = get_beautifulsoup()
PSYCOPG2_AVAILABLE, psycopg2 = get_psycopg2()

# Dictionary mapping dependency names to their availability
DEPENDENCY_STATUS: Dict[str, bool] = {
    'beautifulsoup': BEAUTIFULSOUP_AVAILABLE,
    'psycopg2': PSYCOPG2_AVAILABLE,
}

def check_dependency(dependency_name: str) -> bool:
    """
    Check if a dependency is available.

    Args:
        dependency_name: Name of the dependency to check

    Returns:
        bool: True if the dependency is available, False otherwise
    """
    dependency_name = dependency_name.lower()
    if dependency_name not in DEPENDENCY_STATUS:
        logger.warning(f"Unknown dependency: {dependency_name}")
        return False
    return DEPENDENCY_STATUS[dependency_name]

def require_dependency(dependency_name: str) -> None:
    """
    Require a dependency to be available.

    Args:
        dependency_name: Name of the dependency to require

    Raises:
        ImportError: If the dependency is not available
    """
    if not check_dependency(dependency_name):
        raise ImportError(
            f"Required dependency {dependency_name} is not available. "
            f"Please install it and try again."
        )
```

### Supported Dependencies

The following external dependencies are centrally managed:

#### BeautifulSoup

Used for HTML parsing in message content:

```python
# Import from centralized module
from ..utils.dependencies import BEAUTIFULSOUP_AVAILABLE, BeautifulSoup, BS_PARSER

def parse_html_content(html_content: str) -> str:
    """Parse HTML content to extract text."""
    if BEAUTIFULSOUP_AVAILABLE:
        soup = BeautifulSoup(html_content, BS_PARSER)
        return soup.get_text()
    else:
        # Fallback to basic HTML tag stripping
        return re.sub(r'<[^>]+>', '', html_content)
```

#### psycopg2

Used for PostgreSQL database operations:

```python
# Import from centralized module
from ..utils.dependencies import PSYCOPG2_AVAILABLE, psycopg2, require_dependency

def connect_to_database(db_config: Dict[str, Any]) -> Any:
    """Connect to the PostgreSQL database."""
    # This function requires psycopg2, so we explicitly require it
    require_dependency('psycopg2')

    # Since we required the dependency, we know it's available
    return psycopg2.connect(**db_config)
```

### Best Practices for Dependency Handling

When working with optional dependencies in the SkypeParser project:

1. **Always import from the centralized module**:
   ```python
   from ..utils.dependencies import BEAUTIFULSOUP_AVAILABLE, BeautifulSoup
   ```

2. **Check availability before using**:
   ```python
   if BEAUTIFULSOUP_AVAILABLE:
    # Use BeautifulSoup
else:
    # Use fallback
```

3. **Require dependencies when necessary**:
```python
from ..utils.dependencies import require_dependency

def function_that_needs_beautifulsoup():
    require_dependency('beautifulsoup')
    # Use BeautifulSoup...
```

4. **Provide fallbacks when possible**:
   ```python
   def parse_content(content):
       if BEAUTIFULSOUP_AVAILABLE:
           # Parse with BeautifulSoup
           return parse_with_beautifulsoup(content)
       else:
           # Use a simpler fallback parser
           return simple_fallback_parser(content)
   ```

### Adding New Dependencies

To add a new dependency to the centralized system:

1. Add a getter function in `dependencies.py`:
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

2. Initialize at module import time:
```python
NEW_DEPENDENCY_AVAILABLE, new_dependency = get_new_dependency()
```

3. Add to the `DEPENDENCY_STATUS` dictionary:
```python
   DEPENDENCY_STATUS['new_dependency'] = NEW_DEPENDENCY_AVAILABLE
   ```

4. Update exports in `__init__.py`:
```python
from .dependencies import (
    # Existing exports...
    NEW_DEPENDENCY_AVAILABLE,
    new_dependency
)
```

## Dependency Injection Framework

### Purpose and Benefits

The Dependency Injection (DI) framework in SkypeParser provides several benefits:

1. **Loose Coupling**: Components depend on interfaces, not implementations
2. **Enhanced Testability**: Dependencies can be easily mocked for testing
3. **Flexible Configuration**: Components can be swapped out or reconfigured
4. **Separation of Concerns**: Component creation is separated from business logic
5. **Explicit Dependencies**: Dependencies are clearly declared in constructors

### Architecture Overview

The DI framework consists of three main components:

```
┌─────────────────────┐
│  Service Provider   │◄──────┐
├─────────────────────┤       │
│ - services          │       │
│ + register()        │       │ configures
│ + get_service()     │       │
└─────────────────────┘       │
          ▲                   │
          │ uses              │
          │                   │
┌─────────────────────┐       │
│    Application      │       │
│     Components      │       │
└─────────────────────┘       │
                              │
                     ┌────────┴────────┐
                     │ Service Registry │
                     ├─────────────────┤
                     │ + register_all()│
                     └─────────────────┘
```

1. **Service Provider (`src/utils/di.py`)**: A container that registers and resolves services
2. **Service Registry (`src/utils/service_registry.py`)**: Centralizes service registration
3. **Interfaces (`src/utils/interfaces.py`)**: Defines protocols (interfaces) for services

### Service Provider Implementation

The Service Provider is implemented in `src/utils/di.py`:

```python
"""Dependency injection container for managing services."""

from typing import Any, Callable, Dict, Type, TypeVar, Optional, cast

T = TypeVar('T')

class ServiceProvider:
    """
    A simple dependency injection container.

    Manages service registrations and resolves services when requested.
    """

    def __init__(self):
        """Initialize an empty service container."""
        self._services: Dict[Type, Any] = {}
        self._factory_funcs: Dict[Type, Callable[..., Any]] = {}
        self._singleton_classes: Dict[Type, Type] = {}
        self._transient_classes: Dict[Type, Type] = {}

    def register_singleton(self, interface_type: Type[T], instance: T) -> None:
        """
        Register a singleton instance for an interface.

        Args:
            interface_type: The interface or abstract type
            instance: The concrete instance to return for this type
        """
        self._services[interface_type] = instance

    def register_singleton_class(self, interface_type: Type[T],
                                 implementation_type: Type[T]) -> None:
        """
        Register a class that will be instantiated as a singleton.

        Args:
            interface_type: The interface or abstract type
            implementation_type: The concrete class to instantiate
        """
        self._singleton_classes[interface_type] = implementation_type

    def register_transient(self, interface_type: Type[T],
                          implementation_type: Type[T]) -> None:
        """
        Register a class that will be instantiated each time it's requested.

        Args:
            interface_type: The interface or abstract type
            implementation_type: The concrete class to instantiate
        """
        self._transient_classes[interface_type] = implementation_type

    def register_factory(self, interface_type: Type[T],
                         factory_func: Callable[..., T]) -> None:
        """
        Register a factory function to create instances.

        Args:
            interface_type: The interface or abstract type
            factory_func: A function that creates instances of the type
        """
        self._factory_funcs[interface_type] = factory_func

    def get_service(self, service_type: Type[T]) -> T:
        """
        Get a service of the specified type.

        Args:
            service_type: The type of service to retrieve

        Returns:
            An instance of the requested service type

        Raises:
            KeyError: If the service type is not registered
        """
        # Check for singleton instance
        if service_type in self._services:
            return cast(T, self._services[service_type])

        # Check for singleton class
        if service_type in self._singleton_classes:
            instance = self._singleton_classes[service_type]()
            self._services[service_type] = instance
            return cast(T, instance)

        # Check for transient class
        if service_type in self._transient_classes:
            return cast(T, self._transient_classes[service_type]())

        # Check for factory function
        if service_type in self._factory_funcs:
            return cast(T, self._factory_funcs[service_type]())

        raise KeyError(f"Service of type {service_type.__name__} is not registered")

# Global service provider instance
_provider = ServiceProvider()

def get_service(service_type: Type[T]) -> T:
    """
    Get a service from the global service provider.

    Args:
        service_type: The type of service to retrieve

    Returns:
        An instance of the requested service type
    """
    return _provider.get_service(service_type)

def register_singleton(interface_type: Type[T], instance: T) -> None:
    """Register a singleton with the global provider."""
    _provider.register_singleton(interface_type, instance)

def register_singleton_class(interface_type: Type[T], implementation_type: Type[T]) -> None:
    """Register a singleton class with the global provider."""
    _provider.register_singleton_class(interface_type, implementation_type)

def register_transient(interface_type: Type[T], implementation_type: Type[T]) -> None:
    """Register a transient class with the global provider."""
    _provider.register_transient(interface_type, implementation_type)

def register_factory(interface_type: Type[T], factory_func: Callable[..., T]) -> None:
    """Register a factory function with the global provider."""
    _provider.register_factory(interface_type, factory_func)
```

### Interface Definitions

Interfaces are defined using Python's Protocol and ABC classes:

```python
"""Interface definitions for the dependency injection system."""

from abc import ABC, abstractmethod
from typing import Dict, Any, Protocol, List, Optional


class FileHandlerProtocol(Protocol):
    """Protocol for file handling operations."""

    def read_file(self, path: str) -> str:
        """Read a file and return its contents."""
        ...

    def write_file(self, path: str, content: str) -> None:
        """Write content to a file."""
        ...


class DatabaseConnectionProtocol(Protocol):
    """Protocol for database connection operations."""

    def connect(self) -> Any:
        """Establish a database connection."""
        ...

    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """Execute a query and return the results."""
        ...


class ValidationServiceProtocol(Protocol):
    """Protocol for validation services."""

    def validate_file_exists(self, file_path: str) -> bool:
        """Validate that a file exists."""
        ...

    def validate_user_display_name(self, user_display_name: str) -> bool:
        """Validate a user display name."""
        ...


class ExtractorProtocol(ABC):
    """Abstract base class for data extractors."""

    @abstractmethod
    def extract(self, file_path: str, context: Any) -> Dict[str, Any]:
        """Extract data from a source."""
        pass


class TransformerProtocol(ABC):
    """Abstract base class for data transformers."""

    @abstractmethod
    def transform(self, data: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """Transform data into the target format."""
        pass


class LoaderProtocol(ABC):
    """Abstract base class for data loaders."""

    @abstractmethod
    def load(self, data: Dict[str, Any], context: Any) -> Dict[str, Any]:
        """Load data into the target destination."""
        pass
```

### Service Registry Implementation

The Service Registry is implemented in `src/utils/service_registry.py`:

```python
"""Registry for services in the dependency injection container."""

from typing import Dict, Any, Optional, Type

from .di import (
    register_singleton,
    register_singleton_class,
    register_transient,
    register_factory
)
from .interfaces import (
    FileHandlerProtocol,
    DatabaseConnectionProtocol,
    ValidationServiceProtocol,
    ExtractorProtocol,
    TransformerProtocol,
    LoaderProtocol
)
from ..file.file_handler import FileHandler
from ..db.database import DatabaseConnection
from ..parser.validation import ValidationService
from ..etl.extractor import Extractor
from ..etl.transformer import Transformer
from ..etl.loader import Loader


def register_services(config: Optional[Dict[str, Any]] = None) -> None:
    """
    Register all services with the dependency injection container.

    Args:
        config: Optional configuration dictionary
    """
    config = config or {}

    # Register singletons
    register_singleton_class(FileHandlerProtocol, FileHandler)
    register_singleton_class(ValidationServiceProtocol, ValidationService)

    # Register database with configuration
    db_config = config.get('database', {})
    db_connection = DatabaseConnection(db_config)
    register_singleton(DatabaseConnectionProtocol, db_connection)

    # Register ETL components
    register_transient(ExtractorProtocol, Extractor)
    register_transient(TransformerProtocol, Transformer)
    register_transient(LoaderProtocol, Loader)

    # Register any additional services
    # ...
```

### Registration Types and Lifecycles

The DI framework supports several types of registration:

1. **Singleton**: A single instance used throughout the application
   ```python
   # Register an existing instance
   register_singleton(DatabaseConnectionProtocol, db_connection)

   # Register a class to be instantiated once
   register_singleton_class(FileHandlerProtocol, FileHandler)
   ```

2. **Transient**: A new instance created each time the service is requested
   ```python
   register_transient(TransformerProtocol, Transformer)
   ```

3. **Factory**: A custom function that creates the service
   ```python
   def create_complex_service() -> ComplexService:
       # Custom logic to create and configure the service
       return ComplexService(dependency1, dependency2)

   register_factory(ComplexServiceProtocol, create_complex_service)
   ```

### Using the DI Framework

#### Resolving Dependencies

Services can be resolved from the DI container using the `get_service` function:

```python
from src.utils.di import get_service
from src.utils.interfaces import DatabaseConnectionProtocol

def process_data():
# Get the registered database connection
db_connection = get_service(DatabaseConnectionProtocol)

    # Use the connection
    results = db_connection.execute_query("SELECT * FROM skype_messages")
    # ...
```

#### Constructor Injection

Components should accept their dependencies through their constructor:

```python
from src.utils.interfaces import (
    DatabaseConnectionProtocol,
    FileHandlerProtocol
)

class MessageProcessor:
    """Process Skype messages."""

    def __init__(self, db_connection: DatabaseConnectionProtocol,
                 file_handler: FileHandlerProtocol):
        """
        Initialize the message processor.

        Args:
            db_connection: Database connection for storing messages
            file_handler: File handler for reading message files
        """
        self.db_connection = db_connection
        self.file_handler = file_handler

    def process_message_file(self, file_path: str) -> Dict[str, Any]:
        """Process a message file and store in database."""
        # Read the file
        content = self.file_handler.read_file(file_path)

        # Process content
        # ...

        # Store in database
        result = self.db_connection.execute_query(
            "INSERT INTO skype_messages (content) VALUES (%s) RETURNING id",
            {"content": processed_content}
        )

        return {"message_id": result[0]["id"]}
```

#### Bootstrapping the Application

The DI container should be set up at application startup:

```python
from src.utils.service_registry import register_services

def main():
    """Main entry point for the application."""
    # Register services
    config = load_config()
    register_services(config)

    # Rest of application logic
    # ...

if __name__ == "__main__":
    main()
```

### Testing with the DI Framework

The DI framework makes it easy to replace services with mocks for testing:

```python
import unittest
from unittest.mock import MagicMock
from src.utils.di import register_singleton
from src.utils.interfaces import DatabaseConnectionProtocol
from src.message_processor import MessageProcessor

class TestMessageProcessor(unittest.TestCase):
    def setUp(self):
# Create mock dependencies
        self.mock_db = MagicMock(spec=DatabaseConnectionProtocol)
        self.mock_file_handler = MagicMock()

        # Configure mock behavior
        self.mock_file_handler.read_file.return_value = '{"message": "test"}'
        self.mock_db.execute_query.return_value = [{"id": 123}]

        # Register mocks with DI container
        register_singleton(DatabaseConnectionProtocol, self.mock_db)

        # Create the system under test
        self.processor = MessageProcessor(self.mock_db, self.mock_file_handler)

    def test_process_message_file(self):
        # Act
        result = self.processor.process_message_file('test.json')

        # Assert
        self.mock_file_handler.read_file.assert_called_once_with('test.json')
        self.mock_db.execute_query.assert_called_once()
        self.assertEqual(result["message_id"], 123)
```

## Practical Examples

### Using External Dependencies

```python
from ..utils.dependencies import BEAUTIFULSOUP_AVAILABLE, BeautifulSoup, BS_PARSER

def extract_links_from_html(html_content: str) -> List[str]:
    """Extract links from HTML content."""
    links = []

    if BEAUTIFULSOUP_AVAILABLE:
        # Use BeautifulSoup for proper HTML parsing
        soup = BeautifulSoup(html_content, BS_PARSER)
        for a_tag in soup.find_all('a', href=True):
            links.append(a_tag['href'])
    else:
        # Fallback to simple regex - less reliable but works without BeautifulSoup
        import re
        href_pattern = re.compile(r'href=["\'](.*?)["\']')
        links = href_pattern.findall(html_content)

    return links
```

### Creating a Service with DI

```python
from src.utils.interfaces import (
    DatabaseConnectionProtocol,
    ExtractorProtocol,
    TransformerProtocol,
    LoaderProtocol
)
from src.utils.di import get_service

class ETLPipeline:
    """ETL pipeline for processing Skype exports."""

    def __init__(self, db_connection: Optional[DatabaseConnectionProtocol] = None,
                 extractor: Optional[ExtractorProtocol] = None,
                 transformer: Optional[TransformerProtocol] = None,
                 loader: Optional[LoaderProtocol] = None):
        """
        Initialize the ETL pipeline.

        If dependencies are not provided, they will be resolved from the DI container.
        """
        self.db_connection = db_connection or get_service(DatabaseConnectionProtocol)
        self.extractor = extractor or get_service(ExtractorProtocol)
        self.transformer = transformer or get_service(TransformerProtocol)
        self.loader = loader or get_service(LoaderProtocol)

    def run(self, file_path: str, context: Any) -> Dict[str, Any]:
        """Run the ETL pipeline."""
        # Extract
        raw_data = self.extractor.extract(file_path, context)

        # Transform
        transformed_data = self.transformer.transform(raw_data, context)

        # Load
        result = self.loader.load(transformed_data, context)

        return result
```

## Best Practices

### For External Dependencies

1. **Use the centralized dependency module**: Always import external dependencies through the centralized module.
2. **Check availability before use**: Always check if an optional dependency is available before using it.
3. **Provide fallbacks**: Implement fallback logic for optional dependencies.
4. **Document dependencies**: Clearly document which features require which dependencies.

### For Dependency Injection

1. **Program to interfaces**: Depend on protocols/interfaces, not concrete implementations.
2. **Use constructor injection**: Accept dependencies through the constructor.
3. **Keep services focused**: Each service should have a single, well-defined responsibility.
4. **Favor composition**: Use composition over inheritance for greater flexibility.
5. **Don't service locate unnecessarily**: Pass dependencies explicitly rather than fetching them from the container.

## Conclusion

The centralized dependency handling and dependency injection framework in SkypeParser work together to create a system that is:

- **Modular**: Components are loosely coupled and can be developed independently
- **Testable**: Dependencies can be easily mocked for testing
- **Maintainable**: Changes to one component don't ripple through the system
- **Extensible**: New implementations can be substituted without changing client code
- **Resilient**: The system can handle missing optional dependencies gracefully

By following the best practices outlined in this document, you can leverage these systems to create clean, maintainable code that is easy to test and extend.