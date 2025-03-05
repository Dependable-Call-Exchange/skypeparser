# Skype Parser

A Python library and API for parsing and analyzing Skype export data.

## Features

- Extract data from Skype export files (TAR or JSON)
- Transform raw data into a structured format
- Load data into a PostgreSQL database
- Query and analyze Skype conversations and messages
- REST API for web applications
- Asynchronous processing for large exports
- Dependency injection framework for flexible component management

## Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/skype-parser.git
cd skype-parser

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Modular ETL Pipeline

The recommended way to use the Skype Parser is through the modular ETL pipeline:

```python
from src.db import ETLPipeline

# Create the ETL pipeline
pipeline = ETLPipeline(
    db_config={
        'dbname': 'skype_logs',
        'user': 'postgres',
        'password': 'your_password',
        'host': 'localhost',
        'port': 5432
    },
    output_dir='output'
)

# Run the pipeline
results = pipeline.run_pipeline(
    file_path='skype_export.tar',
    user_display_name='Your Name'
)

# Print results
print(f"Processed {results['phases']['transform']['processed_messages']} messages")
print(f"Processed {results['phases']['transform']['processed_conversations']} conversations")
print(f"Export ID: {results['export_id']}")
```

### Using the Dependency Injection Framework

For more flexible and testable code, you can use the dependency injection framework:

```python
from src.utils.di import get_service
from src.utils.service_registry import register_all_services
from src.utils.interfaces import (
    FileHandlerProtocol,
    DatabaseConnectionProtocol,
    ExtractorProtocol,
    TransformerProtocol,
    LoaderProtocol
)

# Register all services
register_all_services(
    db_config={
        'host': 'localhost',
        'database': 'skype_logs',
        'user': 'postgres',
        'password': 'your_password',
        'port': 5432
    },
    output_dir='output'
)

# Get services from the container
file_handler = get_service(FileHandlerProtocol)
db_connection = get_service(DatabaseConnectionProtocol)
extractor = get_service(ExtractorProtocol)
transformer = get_service(TransformerProtocol)
loader = get_service(LoaderProtocol)

# Use the services
data = file_handler.read_file('skype_export.tar')
extracted_data = extractor.extract('skype_export.tar')
transformed_data = transformer.transform(extracted_data)
loader.load(transformed_data)
```

See the [Dependency Injection Framework documentation](src/utils/README.md) for more details.

### Advanced Usage with Individual Components

For more control, you can use the individual components directly:

```python
from src.db import ETLContext, Extractor, Transformer, Loader

# Create context
context = ETLContext(
    db_config={
        'dbname': 'skype_logs',
        'user': 'postgres',
        'password': 'your_password',
        'host': 'localhost',
        'port': 5432
    },
    output_dir='output',
    memory_limit_mb=2048,
    parallel_processing=True,
    chunk_size=2000
)

# Create components
extractor = Extractor(context=context)
transformer = Transformer(context=context)
loader = Loader(context=context)

# Extract data
raw_data = extractor.extract(file_path='skype_export.tar')

# Transform data
transformed_data = transformer.transform(raw_data, user_display_name='Your Name')

# Load data
loader.connect_db()
try:
    export_id = loader.load(raw_data, transformed_data, 'skype_export.tar')
    print(f"Data loaded with export ID: {export_id}")
finally:
    loader.close_db()
```

### Legacy API (Deprecated)

> **Note:** The legacy API is deprecated and will be removed in version 2.0.0.

```python
from src.db import SkypeETLPipeline

# Create the ETL pipeline
pipeline = SkypeETLPipeline(
    db_name='skype_logs',
    db_user='postgres',
    db_password='your_password',
    db_host='localhost',
    db_port=5432,
    output_dir='output'
)

# Run the pipeline
results = pipeline.run_pipeline(
    file_path='skype_export.tar',
    user_display_name='Your Name'
)

# Print results
print(f"Processed {results['message_count']} messages in {len(results['conversations'])} conversations")
print(f"Export ID: {results['export_id']}")
```

## API Usage

The Skype Parser includes a REST API for web applications:

```bash
# Start the API server
python -m src.api.server
```

Then you can use the API endpoints:

```bash
# Upload a Skype export file
curl -X POST -F "file=@skype_export.tar" -F "user_display_name=Your Name" http://localhost:5000/api/upload

# Check the status of an asynchronous task
curl http://localhost:5000/api/task/task_id
```

## Configuration

The Skype Parser can be configured using environment variables or a configuration file:

```bash
# Database configuration
export DB_NAME=skype_logs
export DB_USER=postgres
export DB_PASSWORD=your_password
export DB_HOST=localhost
export DB_PORT=5432

# API configuration
export API_KEY=your_api_key
export UPLOAD_FOLDER=/path/to/uploads
export OUTPUT_FOLDER=/path/to/output
export ASYNC_THRESHOLD=10000000  # 10MB
```

## Documentation

For more detailed documentation, see the following:

- [ETL Pipeline](docs/ETL_PIPELINE.md)
- [API Reference](docs/API_REFERENCE.md)
- [Database Schema](docs/DATABASE_SCHEMA.md)
- [Migration Guide](docs/MIGRATION.md)
- [ETL Migration Plan](docs/ETL_MIGRATION_PLAN.md)
- [ETL Migration Decisions](docs/ETL_MIGRATION_DECISIONS.md)

## Examples

Check out the examples directory for more usage examples:

- [Basic ETL Example](examples/basic_etl_example.py)
- [Advanced ETL Example](examples/advanced_etl_example.py)
- [API Client Example](examples/api_client_example.py)
- [Migration Example](examples/migration_example.py)

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.