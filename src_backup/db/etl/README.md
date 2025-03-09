# Modular ETL Pipeline for Skype Export Data

This module provides a modular Extract-Transform-Load (ETL) pipeline for processing Skype export data. It extracts data from Skype export files, transforms it into a structured format, and loads it into a PostgreSQL database.

## Architecture

The ETL pipeline is divided into three main components:

1. **Extractor**: Handles file reading and validation
2. **Transformer**: Processes raw data into structured format
3. **Loader**: Manages database operations

These components are orchestrated by the `ETLPipeline` class, which manages the flow of data between them.

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Extractor  │────▶│ Transformer │────▶│   Loader    │
└─────────────┘     └─────────────┘     └─────────────┘
       ▲                  ▲                   ▲
       │                  │                   │
       └──────────────────┼───────────────────┘
                          │
                   ┌─────────────┐
                   │ ETLPipeline │
                   └─────────────┘
```

## Components

### Extractor

The `Extractor` class is responsible for:

- Reading data from Skype export files (tar or JSON)
- Validating the file format and structure
- Extracting the raw data

### Transformer

The `Transformer` class is responsible for:

- Processing raw data into a structured format
- Handling conversations and messages
- Extracting metadata and content
- Supporting parallel processing for large datasets

### Loader

The `Loader` class is responsible for:

- Creating database tables if they don't exist
- Inserting raw and transformed data into the database
- Managing database connections and transactions

### Utilities

The module also includes utility classes:

- `ProgressTracker`: Tracks progress of ETL operations
- `MemoryMonitor`: Monitors memory usage and triggers garbage collection

## Usage

### Basic Usage

```python
from src.db.etl import ETLPipeline

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
result = pipeline.run_pipeline(
    file_path='skype_export.tar',
    user_display_name='Your Name'
)

print(f"Processed {result['phases']['transform']['processed_messages']} messages")
```

### Advanced Usage

You can customize the pipeline behavior:

```python
pipeline = ETLPipeline(
    db_config=db_config,
    output_dir='output',
    memory_limit_mb=2048,  # 2GB memory limit
    parallel_processing=True,  # Enable parallel processing
    chunk_size=2000  # Process messages in chunks of 2000
)
```

## Compatibility

For code that uses the old `SkypeETLPipeline` class, a compatibility layer is provided:

```python
from src.db import SkypeETLPipeline

# This uses the new modular pipeline internally
pipeline = SkypeETLPipeline(
    db_name='skype_logs',
    db_user='postgres',
    output_dir='output'
)

result = pipeline.run_pipeline(
    file_path='skype_export.tar',
    user_display_name='Your Name'
)
```

## Testing

A test script is provided in `tests/test_modular_etl.py`:

```bash
python tests/test_modular_etl.py -f skype_export.tar -u "Your Name" -d skype_logs -U postgres
```

## ETLContext

The `ETLContext` class provides a centralized way to manage state across ETL components. It handles:

- Configuration management
- Progress tracking
- Memory monitoring
- Error recording
- Checkpointing
- Telemetry and metrics

### Usage

```python
from src.db.etl import ETLContext, ETLPipeline

# Create a context
context = ETLContext(
    db_config={
        'dbname': 'skype_logs',
        'user': 'postgres',
        'password': 'password',
        'host': 'localhost',
        'port': 5432
    },
    output_dir='output',
    memory_limit_mb=1024,
    parallel_processing=True,
    chunk_size=1000,
    batch_size=500,
    task_id='my-etl-task'
)

# Create a pipeline with the context
pipeline = ETLPipeline(context=context)

# Run the pipeline
results = pipeline.run_pipeline(
    file_path='skype_export.json',
    user_display_name='User'
)

# Get metrics and summary
summary = context.get_summary()
print(f"Total duration: {summary['total_duration_seconds']} seconds")
print(f"Processed conversations: {summary['phases']['transform']['processed_conversations']}")
print(f"Processed messages: {summary['phases']['transform']['processed_messages']}")
```

### Manual Component Usage

You can also use the ETLContext with individual components:

```python
from src.db.etl import ETLContext, Extractor, Transformer, Loader

# Create a context
context = ETLContext(
    db_config={'dbname': 'skype_logs', 'user': 'postgres'},
    output_dir='output'
)

# Create components with the shared context
extractor = Extractor(context=context)
transformer = Transformer(context=context)
loader = Loader(context=context)

# Run the ETL process manually
context.start_phase("extract")
raw_data = extractor.extract(file_path='skype_export.json')
context.end_phase()

context.start_phase("transform")
transformed_data = transformer.transform(raw_data, 'User')
context.end_phase()

loader.connect_db()
context.start_phase("load")
export_id = loader.load(raw_data, transformed_data, 'skype_export.json')
context.end_phase()
loader.close_db()

# Get summary
summary = context.get_summary()
```

## ETLPipeline

The `ETLPipeline` class orchestrates the ETL process by coordinating the Extractor, Transformer, and Loader components.

### Usage

```python
from src.db.etl import ETLPipeline

# Create a pipeline
pipeline = ETLPipeline(
    db_config={
        'dbname': 'skype_logs',
        'user': 'postgres',
        'password': 'password',
        'host': 'localhost',
        'port': 5432
    },
    output_dir='output',
    memory_limit_mb=1024,
    parallel_processing=True
)

# Run the pipeline
results = pipeline.run_pipeline(
    file_path='skype_export.json',
    user_display_name='User'
)

print(f"Export ID: {results['export_id']}")
```

## Extractor

The `Extractor` class is responsible for extracting data from Skype export files.

### Usage

```python
from src.db.etl import Extractor

# Create an extractor
extractor = Extractor(output_dir='output')

# Extract data
raw_data = extractor.extract(file_path='skype_export.json')
```

## Transformer

The `Transformer` class is responsible for transforming raw data into a structured format.

### Usage

```python
from src.db.etl import Transformer

# Create a transformer
transformer = Transformer(parallel_processing=True)

# Transform data
transformed_data = transformer.transform(raw_data, 'User')
```

## Loader

The `Loader` class is responsible for loading transformed data into the database.

### Usage

```python
from src.db.etl import Loader

# Create a loader
loader = Loader(
    db_config={
        'dbname': 'skype_logs',
        'user': 'postgres',
        'password': 'password',
        'host': 'localhost',
        'port': 5432
    }
)

# Connect to the database
loader.connect_db()

# Load data
export_id = loader.load(raw_data, transformed_data, 'skype_export.json')

# Close the database connection
loader.close_db()
```

## Compatibility Layer

A compatibility layer is provided for code that uses the old `SkypeETLPipeline` class. This layer redirects calls to the new modular ETL pipeline.

### Usage

```python
from src.db import SkypeETLPipeline

# Create a pipeline using the compatibility layer
pipeline = SkypeETLPipeline(
    db_name='skype_logs',
    db_user='postgres',
    db_password='password',
    db_host='localhost',
    db_port=5432,
    output_dir='output'
)

# Run the pipeline
results = pipeline.run_pipeline(
    file_path='skype_export.json',
    user_display_name='User'
)

print(f"Export ID: {results['export_id']}")
```

## Examples

See the `examples` directory for example scripts that demonstrate how to use the ETL module.