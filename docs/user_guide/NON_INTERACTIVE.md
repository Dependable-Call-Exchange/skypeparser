# Non-Interactive Library Functions

This document describes the changes made to remove interactive prompts from library functions in the SkypeParser project, making the library more reusable and testable.

## Overview

Previously, several library functions in the SkypeParser project contained interactive prompts that would ask the user for input during execution. This approach had several drawbacks:

1. It made the library difficult to use in automated or non-interactive environments
2. It made testing more complex, as tests would need to mock user input
3. It created a tight coupling between the library functions and the user interface
4. It made the library less reusable in different contexts (e.g., web applications, scripts)

To address these issues, we've removed all interactive prompts from library functions and replaced them with non-interactive alternatives.

## Changes Made

### 1. File Handler Module (`src/utils/file_handler.py`)

- **`read_tarfile` function**: Removed interactive prompt for selecting a JSON file from a tar archive
  - Now raises a `ValueError` with available options when multiple JSON files are found and neither `select_json` nor `auto_select` is provided
  - Added detailed error message listing available files to help users make a selection

### 2. File Output Module (`src/parser/file_output.py`)

- **`output_structured_data` function**: Removed interactive prompts for file overwrite confirmation
  - Now uses the `overwrite` and `skip_existing` flags to determine behavior
  - Returns `False` when a file exists and `overwrite` is `False`
  - Logs a message explaining why a file was not overwritten

- **`export_conversations_to_text` function**: Removed interactive prompts for file overwrite confirmation
  - Now uses the `overwrite` and `skip_existing` flags to determine behavior
  - Skips files that exist when `overwrite` is `False`
  - Logs a message explaining why a file was not overwritten

### 3. Core Parser Module (`src/parser/core_parser.py`)

- **`id_selector` function**: Removed interactive prompts for selecting conversation IDs
  - Added a new `selected_indices` parameter to specify which IDs to select
  - Returns all IDs if no selection is provided
  - Validates the provided indices and logs warnings for invalid selections

### 4. Skype to Postgres Module (`src/db/skype_to_postgres.py`)

- **`main` function**: Removed interactive prompts for user display name
  - Now uses a default value ("Me") if no display name is provided
  - Logs a message when using the default display name

### 5. Skype Parser Module (`src/parser/skype_parser.py`)

- **`main` function**: Removed interactive prompts for user display name
  - Now uses a default value ("Me") if no display name is provided
  - Logs a message when using the default display name

## Benefits

These changes provide several benefits:

1. **Improved Reusability**: The library can now be used in any context, including non-interactive environments
2. **Better Testability**: Functions can be tested without mocking user input
3. **Cleaner Separation of Concerns**: Library functions focus on their core functionality, not user interaction
4. **Enhanced Automation**: The library can be used in automated scripts and pipelines
5. **Improved Error Handling**: Clear error messages help users understand what went wrong and how to fix it

## Usage Examples

### Handling Multiple JSON Files in a Tar Archive

```python
from src.utils.file_handler import read_tarfile

try:
    # Auto-select the first JSON file
    data = read_tarfile("export.tar", auto_select=True)
except ValueError as e:
    # Handle the case where multiple JSON files are found
    print(f"Multiple JSON files found: {e}")
    # Ask the user for selection or use a default
    selection = 0  # Use the first file
    data = read_tarfile("export.tar", select_json=selection)
```

### Handling File Overwrite

```python
from src.parser.file_output import output_structured_data

# Skip existing files
result = output_structured_data(data, "json", "output_dir", "2023-01-01",
                               overwrite=False, skip_existing=True)

# Overwrite existing files
result = output_structured_data(data, "json", "output_dir", "2023-01-01",
                               overwrite=True)
```

### Selecting Conversation IDs

```python
from src.parser.core_parser import id_selector

# Get all conversation IDs
all_ids = id_selector(conversation_ids)

# Select specific conversations by index
selected_ids = id_selector(conversation_ids, selected_indices=[0, 2, 5])
```

## Conclusion

By removing interactive prompts from library functions, we've made the SkypeParser project more modular, reusable, and testable. This separation of concerns allows for better integration with different types of applications and environments, while still providing clear guidance through logging and error messages.

## Testing and Verification

To ensure that our changes work correctly and that all interactive prompts have been removed, we've:

1. **Created Comprehensive Tests**: Added tests for the file handler module to verify that the non-interactive behavior works correctly.
2. **Verified Existing Tests**: Ensured that existing tests for the validation module continue to pass.
3. **Searched for Interactive Prompts**: Performed a thorough search of the codebase to verify that all `input()` calls and other interactive prompts have been removed.
4. **Checked Documentation**: Updated docstrings and help text to reflect the non-interactive behavior.

These tests and verifications confirm that the SkypeParser project is now fully non-interactive and can be used in automated environments without requiring user input.