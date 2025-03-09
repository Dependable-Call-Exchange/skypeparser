# Skype Parser

A Python tool for parsing and analyzing Skype export data.

<!-- Test change for pre-commit hooks -->

## Overview

This tool parses Skype export data (from the "Export Chat History" feature in Skype) and provides various ways to analyze and visualize the data. It can handle both JSON and TAR exports from Skype.

## Features

- Parse Skype export data from JSON or TAR files
- Extract conversations, messages, and metadata
- Store parsed data in a PostgreSQL database
- Memory-efficient streaming processing for large export files
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

### Streaming Processing for Large Files

For very large export files (>500MB or >100,000 messages), use the streaming approach:

```python
from src.parser.core_parser import parse_skype_data_streaming, stream_conversations

# Get statistics without loading all data into memory
stats = parse_skype_data_streaming(
    file_path='path/to/large_export.tar',
    user_display_name='Your Name'
)
print(f"Found {stats['conversation_count']} conversations and {stats['message_count']} messages")

# Process conversations one by one
for conversation in stream_conversations('path/to/large_export.tar'):
    # Process each conversation individually
    messages = conversation.get('MessageList', [])
    if len(messages) > 100:  # Filter by criteria
        # Process this conversation...
        pass
```

See [Processing Large Files](docs/user_guide/processing_large_files.md) for more details.

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

### Memory Efficiency

For large Skype exports (which can contain millions of messages), the project offers:

- Streaming JSON processing using the `ijson` library
- Incremental file parsing without loading entire files into memory
- Periodic garbage collection during long-running operations
- Memory usage monitoring and optimization
- Efficient temporary file management
- Configurable batch processing of conversations

## Testing

### Running Tests

The project includes comprehensive test suites using both unittest and pytest:

```bash
# Run all tests using unittest
python -m unittest discover tests

# Run pytest-based tests with enhanced logging
python run_pytest_tests.py --verbose --report
```

### Test Architecture

The test suite has been refactored to follow best practices:

1. **Pytest Integration**: Migrated from unittest to pytest for improved fixtures, parameterization, and reporting.

2. **Dependency Injection**: Tests use DI to inject mocks and test doubles instead of patching.

3. **Factory Pattern**: Test data is generated using factories, providing flexibility and reusability.

4. **Centralized Test Expectations**: Expected test output is managed centrally for improved maintenance.

5. **Test Isolation**: Each test is isolated and does not depend on global state or other tests.

For more details on the test improvements, see the [TEST_IMPROVEMENTS_REPORT.md](TEST_IMPROVEMENTS_REPORT.md) file.

## Development

### Code Quality and SOLID Principles

This project uses a combination of linters and static analyzers to enforce code quality and SOLID design principles:

- **mypy**: Strict type checking to ensure proper interface usage and dependency injection
- **pylint**: Enforces various SOLID principles through design rules
- **flake8**: Checks for code style and common bugs
- **pre-commit**: Automates checks before committing code

A custom checker is also included to detect direct instantiation of service classes in constructors, enforcing the Dependency Inversion Principle.

#### Setting up the development environment

1. Install development dependencies:
   ```bash
   pip install -r requirements-dev.txt
   ```

2. Install pre-commit hooks:
   ```bash
   pre-commit install
   ```

3. Run checks manually:
   ```bash
   # Run all pre-commit hooks
   pre-commit run --all-files

   # Run DI compliance check only
   python scripts/check_di_compliance.py src/

   # Run mypy type checking
   mypy src/ tests/
   ```

#### SOLID principles enforced

- **Single Responsibility**: Limits on class attributes and methods
- **Open/Closed**: Encourages proper abstraction and extension
- **Liskov Substitution**: Checks for proper inheritance patterns
- **Interface Segregation**: Limits constructor parameters and interface complexity
- **Dependency Inversion**: Prevents direct instantiation of services, encourages injection

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

# SkypeParser Scripts

This directory contains various scripts for parsing, analyzing, and managing Skype export data. Below is a comprehensive guide to each script and its functionality.

## ETL Pipeline Scripts

### `custom_etl_script.py`
A simple Skype export parser that extracts data from a Skype export file and saves it to JSON files, without database storage or dependency injection. It also generates an HTML report of the parsed data.

**Usage:**
```bash
python scripts/custom_etl_script.py
```

### `run_etl_pipeline.py`
Command-line interface for running the basic ETL pipeline on Skype export data.

**Usage:**
```bash
python scripts/run_etl_pipeline.py -f <skype_export_file> -u <your_display_name>
```

### `stream_skype_data.py`
Memory-efficient processing of large Skype export files using streaming techniques.

**Usage:**
```bash
python scripts/stream_skype_data.py -f <skype_export_file> -u <your_display_name> -v
```

### `run_etl_pipeline_enhanced.py`
Enhanced ETL pipeline runner with additional features such as parallel processing, memory management, checkpointing, and attachment downloading.

**Usage:**
```bash
python scripts/run_etl_pipeline_enhanced.py -f <skype_export_file> -u <your_display_name> [options]
```

**Key Options:**
- `-d <database_name>` - Specify database name
- `--parallel` - Enable parallel processing
- `--memory-limit <memory_limit_mb>` - Set memory limit
- `--download-attachments` - Download message attachments
- `--resume` - Resume from checkpoint

### `run_modular_etl.py`
Command-line interface for the modular ETL pipeline, with support for configuration from both files and command-line options.

**Usage:**
```bash
python scripts/run_modular_etl.py [-f <skype_export_file>] [-u <your_display_name>] [options]
```

### `run_streaming_etl.py`
Implements a streaming ETL process for handling large Skype exports with minimal memory footprint.

**Usage:**
```bash
python scripts/run_streaming_etl.py -f <skype_export_file> -u <your_display_name> [options]
```

## Migration Scripts

### `migrate_from_deprecated.py`
Migrates data from the deprecated schema to the current schema format.

**Usage:**
```bash
python scripts/migrate_from_deprecated.py -d <database_name> [options]
```

### `migrate_cli_commands.py`
Helper script for migrating from old command-line formats to the new unified CLI interface.

**Usage:**
```bash
python scripts/migrate_cli_commands.py convert "<old_command>"
```

### `migrate_to_modular_etl.py`
Migrates existing ETL pipeline configurations to the modular ETL format.

**Usage:**
```bash
python scripts/migrate_to_modular_etl.py -c <config_file> -o <output_config_file>
```

## Analysis and Server Scripts

### `run_analysis.py`
Performs analysis on the parsed Skype data, generating statistics and visualizations.

**Usage:**
```bash
python scripts/run_analysis.py -d <database_name> [options]
```

### `run_api_server.sh`
Bash script to run the Skype Parser API server and Celery worker for background task processing.

**Usage:**
```bash
./scripts/run_api_server.sh [options]
```

**Key Options:**
- `--api-host` - API server host (default: 0.0.0.0)
- `--api-port` - API server port (default: 5000)
- `--worker-concurrency` - Number of Celery workers (default: 2)
- `--redis-url` - Redis connection URL

## Utility Scripts

### `extract_message_types.py`
Extracts and categorizes different message types from Skype exports for analysis.

**Usage:**
```bash
python scripts/extract_message_types.py -f <skype_export_file>
```

### `update_imports.py`
Utility script to update import statements across the codebase when refactoring.

**Usage:**
```bash
python scripts/update_imports.py -o <old_module> -n <new_module>
```

### `check_di_compliance.py`
Pre-commit hook to check for direct service instantiation in constructors to enforce the Dependency Inversion Principle.

**Usage:**
```bash
python scripts/check_di_compliance.py <files...>
```

## Testing Scripts

### `run_tests.py`
Test runner for SkypeParser that can run specific test modules or all tests.

**Usage:**
```bash
python scripts/run_tests.py [--module <module>] [--suite <suite>] [--verbose]
```

### `run_integration_tests.py`
Runner for integration tests that require a complete environment including database.

**Usage:**
```bash
python scripts/run_integration_tests.py [--db <test_database>] [--skip-db-reset]
```

### `run_validation_tests.py`
Validates the output of the ETL pipeline against expected results.

**Usage:**
```bash
python scripts/run_validation_tests.py -f <skype_export_file> -e <expected_results_file>
```

## Getting Started

For most use cases, you'll want to start with the enhanced ETL pipeline:

```bash
python scripts/run_etl_pipeline_enhanced.py -f <your_skype_export_file> -u "<your_display_name>"
```

For development and testing:

```bash
# Run all tests
python scripts/run_tests.py --verbose

# Run integration tests
python scripts/run_integration_tests.py

# Check code quality
python scripts/check_di_compliance.py
```

## Contributing

When adding new scripts, please follow the existing patterns and ensure:
1. Each script has a clear docstring explaining its purpose
2. Command-line arguments are well documented
3. Error handling is implemented
4. Logging is set up appropriately
