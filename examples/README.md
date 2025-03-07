# SkypeParser Examples

This directory contains example scripts and code demonstrating how to use various features of the SkypeParser library. These examples are designed to help you understand how to integrate and utilize SkypeParser in your own applications.

## Table of Contents

1. [ETL Pipeline Examples](#etl-pipeline-examples)
2. [Testing and Mock Examples](#testing-and-mock-examples)
3. [Context Management Examples](#context-management-examples)
4. [Database Connection Examples](#database-connection-examples)
5. [Web and Frontend Examples](#web-and-frontend-examples)
6. [Migration Examples](#migration-examples)

## ETL Pipeline Examples

### Basic ETL Pipeline

- **File**: [modular_etl_example.py](modular_etl_example.py)
- **Description**: Demonstrates how to use the modular ETL pipeline to process Skype export data and load it into a PostgreSQL database.
- **Usage**:
  ```bash
  python modular_etl_example.py -f /path/to/skype_export.json -u "Your Name" -c config/config.json
  ```
- **Features**:
  - Command-line argument parsing
  - Configuration loading
  - Basic ETL pipeline execution
  - Results reporting

### Enhanced ETL Pipeline

- **File**: [enhanced_modular_etl_example.py](enhanced_modular_etl_example.py)
- **Description**: Demonstrates the enhanced modular ETL pipeline with different configuration options and components, showing improvements from Stage 4 of the SkypeParser Improvement Plan.
- **Usage**:
  ```bash
  python enhanced_modular_etl_example.py /path/to/skype_export.json "Your Name"
  # Or for connection pooling example only:
  python enhanced_modular_etl_example.py --connection-pool
  ```
- **Features**:
  - Basic usage example
  - Factory pattern for component creation
  - Custom ETL components
  - Connection pooling for performance
  - Structured logging and error handling

### Advanced ETL Pipeline

- **File**: [advanced_etl_example.py](advanced_etl_example.py)
- **Description**: Demonstrates how to use the individual components of the modular ETL pipeline for more fine-grained control over the ETL process.
- **Usage**:
  ```bash
  python advanced_etl_example.py -f /path/to/skype_export.json -u "Your Name" -c config/config.json
  ```
- **Features**:
  - Progress tracking
  - Memory monitoring
  - Fine-grained control over extraction, transformation, and loading phases
  - Performance metrics

### Checkpoint Resumption

- **File**: [checkpoint_resumption_example.py](checkpoint_resumption_example.py)
- **Description**: Demonstrates how to implement checkpointing and resumption in the ETL pipeline, allowing recovery from failures.
- **Usage**:
  ```bash
  python checkpoint_resumption_example.py -f /path/to/skype_export.json -u "Your Name"
  ```
- **Features**:
  - Checkpoint creation after each phase
  - Simulated failure for demonstration
  - Pipeline resumption from the last checkpoint
  - Error handling and recovery

## Testing and Mock Examples

### Mock Functions

- **File**: [mock_functions_example.py](mock_functions_example.py)
- **Description**: Demonstrates how to use the `create_mock_functions` approach for testing without using patch decorators or context managers.
- **Usage**:
  ```bash
  python mock_functions_example.py
  ```
- **Features**:
  - Basic mock function usage
  - Custom test data creation
  - Integration with test functions
  - Use with pytest monkeypatch

## Context Management Examples

### ETL Context

- **File**: [etl_context_example.py](etl_context_example.py)
- **Description**: Demonstrates how to use the ETLContext to manage state across ETL components and how to access metrics and checkpoints.
- **Usage**:
  ```bash
  python etl_context_example.py -f /path/to/skype_export.json -u "Your Name"
  ```
- **Features**:
  - Running ETL with context
  - Manually running ETL components with shared context
  - Accessing metrics and state information
  - Checkpoint management

### ETL Context Phase Management

- **File**: [etl_context_phase_management_example.py](etl_context_phase_management_example.py)
- **Description**: Demonstrates the ETLContext phase management API, showing how to properly start phases, update progress, end phases, and handle errors.
- **Usage**:
  ```bash
  python etl_context_phase_management_example.py
  ```
- **Features**:
  - Basic phase management
  - Phase status management
  - Error handling in phases
  - Complete ETL flow with phases
  - Phase dependencies

## Database Connection Examples

### Supabase Connection

- **File**: [supabase_connection.py](supabase_connection.py)
- **Description**: Demonstrates how to connect to a Supabase database and perform operations.
- **Usage**:
  ```bash
  python supabase_connection.py
  ```

### Supabase Environment Connection

- **File**: [supabase_env_connection.py](supabase_env_connection.py)
- **Description**: Shows how to connect to Supabase using environment variables for credentials.
- **Usage**:
  ```bash
  python supabase_env_connection.py
  ```

### Test Supabase Connection

- **File**: [test_supabase_connection.py](test_supabase_connection.py)
- **Description**: Tests the Supabase connection and performs basic operations.
- **Usage**:
  ```bash
  python test_supabase_connection.py
  ```

## Web and Frontend Examples

### Upload Handler

- **File**: [upload_handler_example.py](upload_handler_example.py)
- **Description**: Demonstrates how to handle file uploads and process Skype exports through a web interface.
- **Usage**:
  ```bash
  python upload_handler_example.py
  ```

### Web ETL

- **File**: [web_etl_example.py](web_etl_example.py)
- **Description**: Shows how to integrate the ETL pipeline with a web application.
- **Usage**:
  ```bash
  python web_etl_example.py
  ```

### Frontend Example

- **Directory**: [frontend_example/](frontend_example/)
- **Description**: Contains a simple web frontend for uploading and processing Skype exports.
- **Files**:
  - `index.html`: HTML interface for file uploads and progress tracking
  - `serve.py`: Flask server that serves the frontend and proxies API requests
- **Usage**:
  ```bash
  cd frontend_example
  python serve.py
  ```
- **Features**:
  - File upload interface
  - Real-time progress tracking with WebSockets
  - API request proxying
  - Error handling

## Migration Examples

### Migration

- **File**: [migration_example.py](migration_example.py)
- **Description**: Demonstrates how to perform database migrations and schema updates.
- **Usage**:
  ```bash
  python migration_example.py
  ```

## Working with the Examples

### Prerequisites

Before running any of the examples, ensure you have:

1. Installed SkypeParser and its dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Set up a PostgreSQL database (for database-related examples)

3. Obtained a Skype export file (for ETL examples)

### Using the Examples as Templates

These examples are designed to be educational and to serve as templates for your own implementations. Feel free to copy and modify them to suit your specific needs.

### Common Patterns

Throughout these examples, you'll notice common patterns:

1. **Configuration Management**: Loading config from files or environment variables
2. **Error Handling**: Proper exception handling and error reporting
3. **Logging**: Structured logging for better debugging
4. **Progress Tracking**: Real-time progress updates
5. **Modularity**: Breaking down complex tasks into manageable components

## Contributing New Examples

If you've developed a useful example that demonstrates a feature or use case not covered here, please consider contributing it to the repository. Follow these steps:

1. Create your example script following the patterns established in existing examples
2. Add appropriate documentation and usage instructions
3. Submit a pull request

## Support

If you have questions about these examples or encounter issues while using them, please open an issue in the SkypeParser repository.
