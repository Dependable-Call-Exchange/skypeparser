# Monkey Patching Guide for Skype Parser

This document explains the temporary monkey patching solution implemented to resolve the missing method issue in `ContentExtractor` and provides guidance for future improvements. It also covers the datetime serialization solution for JSON output.

## Problem Statement

### Issue 1: Missing Method in ContentExtractor

The core parser module directly instantiates `ContentExtractor` and tries to call a method `format_content_with_markup` on it, which doesn't exist in the class definition. The error occurs in `core_parser.py` in the `_process_message_content` function:

```python
extractor = ContentExtractor()
# ...
content = extractor.format_content_with_markup(content, structured_data)
```

This causes an `AttributeError` when processing messages, as the method doesn't exist on the class.

### Issue 2: Datetime Serialization in JSON

The parser code returns datetime objects in the parsed data structure, but the standard `json.dump()` function can't handle datetime objects natively, causing the error:

```
TypeError: Object of type datetime is not JSON serializable
```

## Implemented Solutions

### Solution 1: Monkey Patching for Missing Method

We've implemented a monkey patching solution that dynamically adds the missing method to the `ContentExtractor` class. This approach follows these SOLID principles:

1. **Single Responsibility Principle**: The monkey patch script has the single responsibility of patching the `ContentExtractor` class.
2. **Open/Closed Principle**: We've extended the functionality of the `ContentExtractor` class without modifying its source code.
3. **Interface Segregation**: We've maintained the expected interface that the core parser depends on.
4. **Dependency Inversion**: The patch script acts as a mediator between the interfaces.

#### Implementation Details

1. **Monkey Patch Script** (`scripts/monkey_patch.py`):
   - Imports the `ContentExtractor` class before any other module that might use it
   - Defines a replacement method that delegates to the standalone function
   - Adds the method to the class if it doesn't already exist

2. **Core Parser Update** (`src/parser/core_parser.py`):
   - Added more robust error handling to fall back to the standalone function if the method is still missing
   - Added better logging to help diagnose issues

3. **Entry Point Script** (`scripts/run_skype_parser.py`):
   - Ensures correct import order so the patch is applied before any imports that use the patched classes
   - Provides a clean entry point for users

### Solution 2: Datetime JSON Serialization

To address the datetime serialization issue, we've created a dedicated serialization utility module that follows the Single Responsibility Principle by keeping serialization concerns separate from the main business logic.

#### Implementation Details

1. **Serialization Utility** (`src/utils/serialization.py`):
   - Provides a custom JSON encoder class that handles datetime objects
   - Includes functions for serializing and deserializing complex data structures
   - Implements a fallback mechanism for manual conversion if the custom encoder fails

2. **Integration in ETL Script** (`scripts/custom_etl_script.py`):
   - Updated to use the serialization utility for saving JSON data
   - Removed direct calls to `json.dump()` in favor of the utility functions

This solution provides a consistent format for datetime values (ISO 8601) and is reusable across the application.

## Recommended Long-Term Solutions

While the implemented solutions work, they're not the ideal long-term approach. Here are recommended permanent fixes aligned with SOLID principles:

### For Missing Method Issue:

#### Option 1: Update ContentExtractor Class

Update the `ContentExtractor` class to include the `format_content_with_markup` method:

```python
class ContentExtractor:
    # ... existing methods ...

    def format_content_with_markup(self, content: str, structured_data: Optional[Dict[str, Any]] = None) -> str:
        """
        Format message content with markup for better readability.

        Args:
            content: Message content to format
            structured_data: Optional structured data extracted from the message

        Returns:
            Formatted message content with markup
        """
        # Call the standalone function in the same module
        return format_content_with_markup(content)
```

This solution follows the **Liskov Substitution Principle** by ensuring all instances of `ContentExtractor` have the necessary methods.

#### Option 2: Update CoreParser to Use Dependency Injection

Modify `core_parser.py` to use dependency injection instead of directly instantiating the `ContentExtractor`:

```python
def _process_message_content(message_data, message_type):
    """Process message content based on message type."""
    content = message_data.get('content', '')
    structured_data = {}

    # Skip processing for empty content
    if not content:
        return content, structured_data

    # Get the ContentExtractor from DI
    from src.utils.di import get_service
    from src.utils.interfaces import ContentExtractorProtocol
    extractor = get_service(ContentExtractorProtocol)

    # ... rest of the function ...
```

This solution follows the **Dependency Inversion Principle** by depending on abstractions rather than concrete implementations.

#### Option 3: Refactor CoreParser to Use the Standalone Function

Modify `core_parser.py` to directly use the standalone function:

```python
def _process_message_content(message_data, message_type):
    """Process message content based on message type."""
    content = message_data.get('content', '')
    structured_data = {}

    # Skip processing for empty content
    if not content:
        return content, structured_data

    # Use the ContentExtractor to extract structured data
    extractor = ContentExtractor()

    # For RichText messages, use the content extractor to get structured data
    if message_type == 'RichText':
        structured_data = extractor.extract_all(content)

        # Format the content with markup if needed
        if structured_data:
            from src.parser.content_extractor import format_content_with_markup
            content = format_content_with_markup(content)

    # ... rest of the function ...
```

This solution also respects the **Single Responsibility Principle** by using specialized functions for specific tasks.

### For Datetime Serialization:

#### Option 1: Add a Data Transfer Object Layer

Create a layer of Data Transfer Objects (DTOs) that convert complex domain objects to simple serializable objects:

```python
class MessageDTO:
    @staticmethod
    def from_domain(message: Dict[str, Any]) -> Dict[str, Any]:
        """Convert domain message to DTO"""
        dto = message.copy()
        # Convert datetime fields to ISO strings
        if 'timestamp' in dto and isinstance(dto['timestamp'], datetime):
            dto['timestamp'] = dto['timestamp'].isoformat()
        return dto
```

#### Option 2: Use a Serialization Framework

Consider using a comprehensive serialization framework like `marshmallow` that can handle complex types and perform validation:

```python
from marshmallow import Schema, fields

class MessageSchema(Schema):
    timestamp = fields.DateTime(format='iso')
    date = fields.String()
    time = fields.String()
    # ... other fields ...

# Usage
schema = MessageSchema(many=True)
serialized_data = schema.dump(messages)
```

## Migration Path

1. Continue using the implemented solutions in the short term
2. Plan for a refactoring sprint to implement the long-term solutions
3. Test thoroughly with various Skype export files to ensure compatibility
4. Deploy the permanent solutions and deprecate the temporary approaches
5. Update documentation to reflect the new architecture

## Usage Instructions

### Current Approach (with Monkey Patch and Serialization)

Use the wrapper script as your main entry point:

```bash
python scripts/run_skype_parser.py -f <export_file> -u <user_display_name> [-o <output_directory>] [-v]
```

For example:

```bash
python scripts/run_skype_parser.py -f 8_live_dave.leathers113_export.tar -u "David Leathers" -v
```

The `-v` flag enables verbose logging, which can help diagnose any issues.
