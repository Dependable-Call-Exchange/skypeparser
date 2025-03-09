# Parser Module

The Parser module provides functionality for parsing Skype export files and extracting meaningful data from them. It is designed with a modular architecture to ensure separation of concerns and maintainability.

## Module Structure

The Parser module consists of the following components:

### 1. Core Parser (`core_parser.py`)

Contains the core parsing functions for processing Skype export data:

- `timestamp_parser`: Parses timestamp strings into formatted strings and datetime objects with proper timezone handling
- `content_parser`: Cleans and formats message content using BeautifulSoup
- `tag_stripper`: Strips HTML/XML tags from text using regex
- `pretty_quotes`: Formats quoted messages for better readability and converts straight quotes to curly quotes
- `type_parser`: Maps message types to human-readable descriptions
- `banner_constructor`: Constructs a banner for the conversation with metadata
- `id_selector`: Interactive selector for conversation IDs
- `parse_skype_data`: Main function that parses raw Skype export data into a structured format

### 2. File Output (`file_output.py`)

Handles exporting parsed data to various file formats:

- `write_to_file`: Writes content to a file with proper error handling
- `output_structured_data`: Outputs structured data in JSON or CSV format
- `export_conversations_to_text`: Exports conversations to text files
- `export_conversations`: Main function that orchestrates the export process

### 3. Command-line Interface (`skype_parser.py`)

Provides a user-friendly command-line interface for:

- Parsing Skype export files (TAR or JSON)
- Exporting conversations in various formats
- Storing data in PostgreSQL using the ETL pipeline
- Selecting specific conversations to process

### 4. Deprecated Module (`parser_module.py`)

This module contains older implementations of parsing functions and is now deprecated. All functionality has been consolidated into `core_parser.py` with improved implementations. Please use the functions from `core_parser.py` instead.

## Usage

### Command-line Usage

```bash
# Parse a Skype export file and generate text files
python -m src.parser.skype_parser path/to/skype_export.tar -t -o output_dir -u "Your Name"

# Parse and export as JSON
python -m src.parser.skype_parser path/to/skype_export.tar -t -f json -o output_dir -u "Your Name"

# Parse and store in PostgreSQL using the ETL pipeline
python -m src.parser.skype_parser path/to/skype_export.tar -t --store-db --db-name skype_archive --db-user postgres
```

### Python API Usage

```python
from src.utils.file_handler import read_file
from src.parser.core_parser import parse_skype_data
from src.parser.file_output import export_conversations

# Read the Skype export file
data = read_file("path/to/skype_export.json")

# Parse the data
structured_data = parse_skype_data(data, "Your Name")

# Export the conversations
export_conversations(
    structured_data,
    output_format="json",
    output_dir="output_dir",
    overwrite=False,
    skip_existing=False,
    text_output=True
)
```

## Recent Improvements

### Consolidated Parsing Functions

The parsing functions have been consolidated from `parser_module.py` into `core_parser.py` with the following improvements:

1. **Better Timezone Handling**: The `timestamp_parser` function now properly handles timezone information in timestamps.
2. **Enhanced Quote Formatting**: The `pretty_quotes` function now handles both Skype-specific quote markers and converts straight quotes to curly quotes.
3. **Improved HTML Parsing**: The `tag_stripper` function uses a more precise regex for removing HTML tags.
4. **Consistent Error Handling**: All functions now have consistent error handling with appropriate logging.

## Data Structure

The `parse_skype_data` function returns a structured data dictionary with the following format:

```python
{
    "metadata": {
        "user_id": "user_id",
        "export_date": "export_date",
        "export_time": "export_time"
    },
    "conversations": {
        "conversation_id_1": {
            "display_name": "display_name",
            "id": "conversation_id_1",
            "export_date": "export_date",
            "export_time": "export_time",
            "messages": [
                {
                    "timestamp": "timestamp",
                    "date": "date",
                    "time": "time",
                    "from_id": "from_id",
                    "from_name": "from_name",
                    "type": "message_type",
                    "is_edited": False,
                    "content": "cleaned_content",
                    "content_raw": "raw_content"
                },
                # More messages...
            ]
        },
        # More conversations...
    }
}
```

## Dependencies

- BeautifulSoup4: For HTML parsing
- lxml: Recommended for better HTML parsing performance
- psycopg2: For PostgreSQL database operations (optional, only if using database storage)

## Integration with ETL Pipeline

The Parser module integrates with the ETL pipeline through the `skype_parser.py` command-line interface. When the `--store-db` flag is used, the parser will use the ETL pipeline to store the parsed data in a PostgreSQL database.

## Error Handling

The Parser module includes comprehensive error handling to ensure robustness:

- File reading errors are caught and reported
- JSON parsing errors are handled gracefully
- Missing fields in the Skype export data are detected and reported
- File writing errors are caught and reported

## Logging

The Parser module uses Python's built-in logging module to provide detailed information about the parsing process. The logging level can be controlled using the `--verbose` flag in the command-line interface.

## Future Improvements

- Add support for more output formats
- Enhance performance for large Skype export files
- Add more detailed error reporting
- Implement parallel processing for faster parsing
- Add unit tests for all parser components