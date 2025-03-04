# Centralized Dependency Handling in Skype Parser

This document outlines the centralized dependency handling approach implemented in the Skype Parser project.

## Overview

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

## Conclusion

The centralized dependency handling approach in the Skype Parser project ensures consistent, maintainable, and user-friendly handling of external dependencies. By following this approach, developers can create more robust and maintainable code, and users can better understand and resolve issues related to missing dependencies.