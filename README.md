# Skype Parser and ETL Pipeline

A comprehensive toolkit for extracting, transforming, and loading Skype export data. This project provides tools for parsing Skype export files (TAR or JSON), cleaning the data, and storing it in both raw and transformed formats for analysis and archiving.

## Features

- **Extract** data from Skype export files (TAR or JSON)
- **Transform** raw data into a structured format
- **Load** both raw and transformed data into PostgreSQL
- **Export** conversations in multiple formats (text, JSON, CSV)
- **Web Integration** for processing uploaded files
- **Command-line Interface** for batch processing
- **Comprehensive Logging** for tracking the ETL process
- **Modular Design** with clear separation of concerns
- **Extensive Test Suite** for ensuring code quality and reliability

## Project Structure

```
SkypeParser/
├── src/
│   ├── utils/                 # Utility modules
│   │   ├── file_handler.py    # File reading and extraction utilities
│   │   ├── file_utils.py      # General file utilities
│   │   ├── validation.py      # Input validation utilities
│   │   ├── config.py          # Configuration utilities
│   │   ├── dependencies.py    # Dependency management
│   │   └── tar_extractor.py   # Command-line tool for TAR extraction
│   ├── parser/                # Parsing modules
│   │   ├── core_parser.py     # Core parsing functions
│   │   ├── file_output.py     # File output utilities
│   │   ├── parser_module.py   # Additional parsing utilities
│   │   └── skype_parser.py    # Command-line interface
│   └── db/                    # Database modules
│       ├── etl_pipeline.py    # ETL pipeline implementation
│       ├── raw_storage/       # Raw data storage utilities
│       ├── skype_to_postgres.py (deprecated)
│       └── store_skype_export.py (deprecated)
├── examples/                  # Example scripts
│   ├── web_etl_example.py     # Web application example
│   └── upload_handler_example.py # File upload example
├── tests/                     # Test modules
│   ├── test_validation.py     # Validation tests
│   ├── test_config.py         # Configuration tests
│   ├── test_file_handler.py   # File handler tests
│   ├── test_etl_pipeline.py   # ETL pipeline tests
│   ├── test_web_integration.py # Web integration tests
│   ├── test_core_parser.py    # Core parser tests
│   ├── test_skype_parser.py   # Skype parser tests
│   ├── test_file_output.py    # File output tests
│   ├── test_parser_module.py  # Parser module tests
│   ├── test_raw_storage.py    # Raw storage tests
│   ├── test_dependencies.py   # Dependencies tests
│   ├── test_tar_extractor.py  # TAR extractor tests
│   └── test_file_utils.py     # File utilities tests
├── run_tests.py               # Test runner script
└── requirements.txt           # Project dependencies
```

## Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/SkypeParser.git
   cd SkypeParser
   ```

2. Install the required dependencies:
   ```
   pip install -r requirements.txt
   ```

3. Set up a PostgreSQL database (if using database storage):
   ```
   createdb skype_archive
   ```

## Usage

### Command-line Usage

Parse a Skype export file and generate text files:

```bash
python -m src.parser.skype_parser path/to/skype_export.tar -t -o output_dir -u "Your Name"
```

Parse and store in PostgreSQL using the ETL pipeline:

```bash
python -m src.parser.skype_parser path/to/skype_export.tar -t --store-db --db-name skype_archive --db-user postgres
```

### Python API Usage

```python
from src.db.etl_pipeline import SkypeETLPipeline

# Initialize the ETL pipeline
etl = SkypeETLPipeline(
    db_name="skype_archive",
    db_user="postgres",
    db_password="your_password",
    db_host="localhost",
    db_port=5432
)

# Run the pipeline
result = etl.run_pipeline(
    input_file="path/to/skype_export.tar",
    is_tar=True,
    output_dir="output_dir",
    user_display_name="Your Name"
)

if result:
    print("ETL pipeline completed successfully")
else:
    print("ETL pipeline failed")
```

### Using the Parser Modules Directly

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

### Web Application Integration

See the `examples/web_etl_example.py` file for a complete example of integrating the ETL pipeline with a web application.

## Parser Module Components

### Core Parser (`core_parser.py`)

Contains the core parsing functions for processing Skype export data:

- `timestamp_parser`: Parses timestamp strings into formatted strings and datetime objects
- `content_parser`: Cleans and formats message content using BeautifulSoup
- `tag_stripper`: Strips HTML/XML tags from text using regex
- `pretty_quotes`: Formats quoted messages for better readability
- `type_parser`: Maps message types to human-readable descriptions
- `parse_skype_data`: Main function that parses raw Skype export data into a structured format

### File Output (`file_output.py`)

Handles exporting parsed data to various file formats:

- `write_to_file`: Writes content to a file with proper error handling
- `output_structured_data`: Outputs structured data in JSON or CSV format
- `export_conversations_to_text`: Exports conversations to text files
- `export_conversations`: Main function that orchestrates the export process

### Command-line Interface (`skype_parser.py`)

Provides a user-friendly command-line interface for:

- Parsing Skype export files (TAR or JSON)
- Exporting conversations in various formats
- Storing data in PostgreSQL using the ETL pipeline
- Selecting specific conversations to process

## ETL Pipeline Stages

### 1. Extraction

The extraction phase reads data from Skype export files (TAR or JSON) and validates the structure of the data. It ensures that all required fields are present and that the data is in the expected format.

### 2. Transformation

The transformation phase cleans and structures the raw data. It extracts key metadata, processes conversations and messages, and ensures that messages are sorted by timestamp. Special message types are handled appropriately, and edited messages are marked.

### 3. Loading

The loading phase inserts or updates both raw and transformed data in the PostgreSQL database. It handles potential conflicts and ensures that existing records are updated appropriately.

## Database Schema

The ETL pipeline uses the following database schema:

### Raw Data Table

```sql
CREATE TABLE IF NOT EXISTS skype_raw_exports (
    id SERIAL PRIMARY KEY,
    export_id VARCHAR(255) UNIQUE,
    export_date TIMESTAMP,
    user_id VARCHAR(255),
    raw_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Transformed Data Tables

```sql
CREATE TABLE IF NOT EXISTS skype_conversations (
    id SERIAL PRIMARY KEY,
    conversation_id VARCHAR(255) UNIQUE,
    display_name VARCHAR(255),
    export_id VARCHAR(255) REFERENCES skype_raw_exports(export_id),
    first_message_time TIMESTAMP,
    last_message_time TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS skype_messages (
    id SERIAL PRIMARY KEY,
    message_id VARCHAR(255) UNIQUE,
    conversation_id VARCHAR(255) REFERENCES skype_conversations(conversation_id),
    timestamp TIMESTAMP,
    from_id VARCHAR(255),
    from_name VARCHAR(255),
    content TEXT,
    content_raw TEXT,
    message_type VARCHAR(50),
    is_edited BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Testing

The project includes a comprehensive test suite to ensure code quality and reliability. The tests are organized by module and can be run individually or all at once.

### Running All Tests

To run all tests, use the `run_tests.py` script:

```bash
python run_tests.py
```

### Running Specific Tests

To run tests for a specific module, use the `--module` or `-m` flag:

```bash
python run_tests.py --module validation config
```

This will run only the tests for the validation and config modules.

For running multiple test modules, use multiple `-m` flags:

```bash
python run_tests.py -m tar_extractor -m file_handler
```

### Test Helpers Module

The project includes a test helpers module (`tests/test_helpers.py`) that provides utilities for writing tests:

```python
from tests.test_helpers import TestBase, patch_validation, create_test_file

class TestMyModule(TestBase):
    @patch_validation
    def test_my_function(self, mock_validate):
        # Test code here
        test_file = create_test_file(self.test_dir, "test.txt", "content")
        # Use test_file in your test
```

Key features of the test helpers module:
- `TestBase` class with common setup and teardown methods
- `patch_validation` decorator for handling path validation in tests
- Helper functions for creating test files, JSON files, and TAR files
- Utilities for mocking system functions like `sys.exit`

### Test Fixes and Best Practices

The test suite has been updated to handle path validation requirements and to match the actual implementation of the modules. See `docs/test_fixes.md` for details on the changes made and best practices for writing tests.

Key improvements include:
- Using relative paths within the test directory to avoid path validation issues
- Patching validation functions for testing purposes
- Ensuring tests match the actual implementation of the modules
- Standardized approach for test data management

### Verbose Output

For more detailed test output, use the `--verbose` or `-v` flag:

```bash
python run_tests.py --verbose
```

### Test Coverage

To generate a test coverage report, you can use the `pytest-cov` package:

```bash
pytest --cov=src tests/
```

This will show the test coverage for each module in the `src` directory.

## Dependencies

- Python 3.6+
- PostgreSQL 10+
- psycopg2
- Flask (for web integration)
- BeautifulSoup4 (for HTML parsing)
- lxml (recommended for better HTML parsing)

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Thanks to all contributors who have helped with the development of this project.
- Special thanks to the Skype team for providing the export functionality that makes this project possible.