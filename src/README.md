# SkypeParser Source Code

This directory contains the source code for the SkypeParser project, a Python tool for parsing, analyzing, and storing Skype export data.

## Directory Structure

The `src` directory is organized into several modules, each with a specific responsibility:

### Core Modules

- **`parser/`**: Core parsing functionality for Skype export files
- **`db/`**: Database interaction and ETL (Extract, Transform, Load) pipeline
- **`utils/`**: Utility functions, helpers, and the dependency injection framework
- **`analysis/`**: Data analysis and visualization tools

### Additional Modules

- **`api/`**: REST API for accessing and processing Skype data
- **`validation/`**: Data validation and schema verification
- **`logging/`**: Structured logging and error handling
- **`monitoring/`**: Performance monitoring and resource tracking
- **`messages/`**: Message type handling and processing
- **`files/`**: File handling and I/O operations
- **`conversations/`**: Conversation processing and analysis
- **`data_handlers/`**: Specialized data handlers for different content types

## Key Components

### Parser Module (`parser/`)

The parser module is responsible for extracting data from Skype export files.

- **`skype_parser.py`**: Main entry point for parsing Skype exports
- **`core_parser.py`**: Core parsing functionality with memory-efficient options
- **`content_extractor.py`**: Extracts and processes message content
- **`file_output.py`**: Handles exporting parsed data to various formats

### Database Module (`db/`)

The database module handles storing and retrieving Skype data from databases.

- **`etl/`**: Modular ETL pipeline for processing Skype data
  - **`modular_pipeline.py`**: Main ETL pipeline implementation
  - **`context.py`**: Context management for ETL operations
  - **`extractor.py`**: Data extraction component
  - **`transformer.py`**: Data transformation component
  - **`loader.py`**: Data loading component
  - **`pipeline_factory.py`**: Factory for creating pipeline components
- **`strategies/`**: Database insertion strategies
  - **`bulk_insertion.py`**: Optimized bulk insertion
  - **`individual_insertion.py`**: Individual record insertion
- **`handlers/`**: Specialized handlers for different data types
- **`clean_storage/`**: Clean data storage models and operations
- **`raw_storage/`**: Raw data storage models and operations

### Utilities Module (`utils/`)

The utilities module provides common functionality used throughout the project.

- **`di.py`**: Dependency injection framework
- **`service_registry.py`**: Service registration and resolution
- **`interfaces.py`**: Protocol definitions for dependency injection
- **`error_handling.py`**: Centralized error handling
- **`structured_logging.py`**: Enhanced logging with context
- **`config.py`**: Configuration management
- **`file_handler.py`**: File operations and I/O
- **`validation.py`**: Data validation utilities

### Analysis Module (`analysis/`)

The analysis module provides tools for analyzing and visualizing Skype data.

- **`queries.py`**: Database queries for common analytics tasks
- **`reporting.py`**: Report generation
- **`visualization.py`**: Data visualization tools

## Architecture

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

### Modular ETL Pipeline

The ETL pipeline is designed to be modular and extensible:

```python
from src.db.etl.modular_pipeline import ModularETLPipeline
from src.db.etl.pipeline_factory import PipelineFactory

# Create pipeline using factory
factory = PipelineFactory()
pipeline = factory.create_pipeline(
    db_config={"dbname": "skype_db", "user": "postgres"},
    parallel_processing=True
)

# Run pipeline
result = pipeline.process(
    file_path="export.tar",
    user_display_name="John Doe"
)
```

### Error Handling

The project uses a centralized error handling system:

```python
from src.utils.error_handling import handle_errors, ErrorContext

with handle_errors(ErrorContext(operation="file_reading", resource="export.tar")):
    data = read_file("export.tar")
```

## Development Guidelines

When working with the source code, please follow these guidelines:

1. **SOLID Principles**: Follow SOLID design principles
   - Single Responsibility: Each class should have one reason to change
   - Open/Closed: Open for extension, closed for modification
   - Liskov Substitution: Subtypes must be substitutable for their base types
   - Interface Segregation: Clients shouldn't depend on interfaces they don't use
   - Dependency Inversion: Depend on abstractions, not concretions

2. **Dependency Injection**: Use the DI framework for dependencies
   - Define protocols in `interfaces.py`
   - Register services in `service_registry.py`
   - Use constructor injection for dependencies
   - Avoid direct instantiation of service classes

3. **Error Handling**: Use the centralized error handling system
   - Use `handle_errors` context manager for operations that may fail
   - Provide meaningful error contexts
   - Log errors with appropriate severity levels

4. **Logging**: Use structured logging
   - Use the `log_call` decorator for function entry/exit logging
   - Use `log_execution_time` for performance monitoring
   - Include relevant context in log messages

5. **Testing**: Write tests for new functionality
   - Use dependency injection to make code testable
   - Create mock implementations for testing
   - Use factories for test data generation

## Usage Examples

### Basic Parsing

```python
from src.parser.skype_parser import SkypeParser

parser = SkypeParser()
data = parser.parse_file('path/to/skype_export.json')

# Access parsed data
for conversation_id, conversation in data['conversations'].items():
    print(f"Conversation: {conversation['display_name']}")
    for message in conversation['messages']:
        print(f"  {message['sender_name']}: {message['content']}")
```

### Memory-Efficient Parsing

```python
from src.parser.core_parser import parse_skype_data_streaming

# Get statistics without loading all data into memory
stats = parse_skype_data_streaming(
    file_path='path/to/large_export.tar',
    user_display_name='Your Name'
)
print(f"Found {stats['conversation_count']} conversations and {stats['message_count']} messages")
```

### Database Storage

```python
from src.db.etl.pipeline_manager import ETLPipeline

pipeline = ETLPipeline(
    db_config={
        'host': 'localhost',
        'port': 5432,
        'dbname': 'skype_data',
        'user': 'postgres',
        'password': 'password'
    }
)

result = pipeline.run_pipeline(
    file_path='path/to/skype_export.json',
    user_display_name='Your Name'
)
```

### Data Analysis

```python
from src.analysis.queries import SkypeQueryExamples
from src.utils.db_connection import DatabaseConnection

# Create database connection
db_conn = DatabaseConnection(
    host='localhost',
    port=5432,
    dbname='skype_data',
    user='postgres',
    password='password'
)

# Create query object
queries = SkypeQueryExamples(db_conn)

# Get message count by conversation
message_counts = queries.get_message_count_by_conversation()
for conv_name, count in message_counts:
    print(f"{conv_name}: {count} messages")

# Get active hours
active_hours = queries.get_active_hours()
for hour, count in active_hours:
    print(f"Hour {hour}: {count} messages")
```

## Contributing

When adding new functionality to the source code:

1. Follow the existing architecture and patterns
2. Add appropriate documentation and type hints
3. Write tests for your changes
4. Ensure all linters and type checkers pass
5. Update this README if you add new modules or significant functionality
