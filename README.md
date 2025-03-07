# Skype Parser

A Python tool for parsing and analyzing Skype export data.

## Overview

This tool parses Skype export data (from the "Export Chat History" feature in Skype) and provides various ways to analyze and visualize the data. It can handle both JSON and TAR exports from Skype.

## Features

- Parse Skype export data from JSON or TAR files
- Extract conversations, messages, and metadata
- Store parsed data in a PostgreSQL database
- Analyze message patterns, frequencies, and content
- Generate visualizations of chat activity
- Support for various message types (text, media, calls, etc.)
- Enhanced message type handling for polls, scheduled calls, and more
- Attachment handling with download, organization, and metadata extraction
- Modular architecture with dependency injection for testability

## Installation

### Prerequisites

- Python 3.8+
- PostgreSQL (optional, for database storage)

### Setup

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/skype-parser.git
   cd skype-parser
   ```

2. Create a virtual environment:
   ```
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```
   pip install -r requirements.txt
   ```

4. Configure database (optional):
   - Create a PostgreSQL database
   - Update the database configuration in `config.json`

## Usage

### Basic Usage

```python
from src.parser import SkypeParser

# Parse a Skype export file
parser = SkypeParser()
data = parser.parse_file('path/to/skype_export.json')

# Or parse a TAR archive
data = parser.parse_file('path/to/skype_export.tar')

# Access parsed data
for conversation_id, conversation in data['conversations'].items():
    print(f"Conversation: {conversation['display_name']}")
    for message in conversation['messages']:
        print(f"  {message['sender_name']}: {message['content']}")
```

### Using the ETL Pipeline

The ETL (Extract, Transform, Load) pipeline provides a more structured way to process Skype export data:

```python
from src.db.etl.pipeline_manager import ETLPipeline

# Create an ETL pipeline
pipeline = ETLPipeline(
    db_config={
        'host': 'localhost',
        'port': 5432,
        'dbname': 'skype_data',
        'user': 'postgres',
        'password': 'password'
    },
    output_dir='output',
    # Enable attachment handling
    download_attachments=True,
    attachments_dir='output/attachments',
    generate_thumbnails=True,
    extract_metadata=True
)

# Run the pipeline
result = pipeline.run_pipeline(
    file_path='path/to/skype_export.json',
    user_display_name='Your Name'
)

print(f"Processed {result['conversation_count']} conversations with {result['message_count']} messages")
print(f"Export ID: {result['export_id']}")
```

### Dependency Injection

The project uses a lightweight dependency injection framework to improve testability and maintainability:

```python
from src.utils.di import get_service
from src.utils.interfaces import FileHandlerProtocol
from src.utils.service_registry import register_all_services

# Register all services
register_all_services(db_config=db_config)

# Get a service by its protocol
file_handler = get_service(FileHandlerProtocol)

# Use the service
data = file_handler.read_file("path/to/file.json")
```

## Architecture

The project is organized into several modules:

- `src/parser`: Core parsing functionality
- `src/db`: Database interaction and ETL pipeline
- `src/utils`: Utility functions and helpers
- `src/analysis`: Data analysis and visualization
- `tests`: Unit and integration tests

### Dependency Injection Framework

The project includes a lightweight dependency injection framework that provides:

- Protocol-based interfaces for all major components
- A service provider for registering and resolving dependencies
- Support for singleton, transient, and factory services
- Constructor injection for clean, testable code

Benefits of the DI approach:

- Improved testability through mock dependencies
- Loose coupling between components
- Explicit dependencies and contracts
- Easier maintenance and extension

## Testing

Run the tests with pytest:

```
pytest
```

Or run a specific test file:

```
pytest tests/unit/test_etl_pipeline.py
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.