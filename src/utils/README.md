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