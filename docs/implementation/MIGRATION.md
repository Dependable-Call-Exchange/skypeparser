# Migration Guide

This guide explains how to migrate from the deprecated modules to the new ETL pipeline.

## Overview

The following modules are deprecated and will be removed in a future version:

- `skype_to_postgres.py`
- `store_skype_export.py`

These modules have been replaced by the `SkypeETLPipeline` class, which provides a more robust and flexible solution for extracting, transforming, and loading Skype export data.

Additionally, a new modular ETL pipeline has been introduced, which provides a more maintainable and extensible architecture for ETL operations.

## Migration Steps

### From `skype_to_postgres.py`

If you're currently using `skype_to_postgres.py` to import Skype data into PostgreSQL, follow these steps to migrate to the new ETL pipeline:

#### Before:

```python
from src.db import import_to_postgres

# Command-line usage
import_to_postgres([
    '-f', 'skype_export.tar',
    '-u', 'Your Name',
    '-d', 'skype_logs',
    '-U', 'postgres',
    '--create-tables'
])

# Or direct function calls
from src.db import create_tables, import_skype_data
import psycopg2

conn = psycopg2.connect(dbname='skype_logs', user='postgres')
create_tables(conn)
import_skype_data(conn, data, 'Your Name')
```

#### After:

```python
from src.db import SkypeETLPipeline

# Create the ETL pipeline
pipeline = SkypeETLPipeline(
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
    is_tar=True,
    user_display_name='Your Name'
)

print(f"Processed {result['message_count']} messages in {len(result['conversations'])} conversations")
```

### From `store_skype_export.py`

If you're currently using `store_skype_export.py` to store Skype export data, follow these steps to migrate to the new ETL pipeline:

#### Before:

```python
from src.db import store_skype_export

# Command-line usage
store_skype_export([
    '-f', 'skype_export.tar',
    '-t',
    '-d', 'skype_logs',
    '-U', 'postgres'
])

# Or direct function calls
from src.db import clean_skype_data
from src.db.raw_storage import SkypeDataStorage

# Clean the data
cleaned_data = clean_skype_data(raw_data)

# Store the data
storage = SkypeDataStorage(
    dbname='skype_logs',
    user='postgres',
    password='your_password'
)
raw_id, cleaned_id = storage.store_skype_export(raw_data, cleaned_data)
```

#### After:

```python
from src.db import SkypeETLPipeline

# Create the ETL pipeline
pipeline = SkypeETLPipeline(
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
    is_tar=True,
    user_display_name='Your Name'
)

print(f"Processed {result['message_count']} messages in {len(result['conversations'])} conversations")
```

## Using the Configuration Module

The new ETL pipeline can be used with the configuration module to load settings from environment variables or a configuration file:

```python
from src.db import SkypeETLPipeline
from src.utils.config import load_config, get_db_config

# Load configuration
config = load_config('config/config.json')
db_config = get_db_config(config)

# Create the ETL pipeline
pipeline = SkypeETLPipeline(
    db_config=db_config,
    output_dir=config['output']['directory']
)

# Run the pipeline
result = pipeline.run_pipeline(
    file_path='skype_export.tar',
    is_tar=True,
    user_display_name='Your Name'
)
```

## Web Integration

If you're using the deprecated modules in a web application, you can migrate to the new ETL pipeline using the example in `examples/web_etl_example.py`:

```python
from flask import Flask, request, jsonify
from src.db import SkypeETLPipeline
from src.utils.config import load_config, get_db_config

app = Flask(__name__)

# Load configuration
config = load_config('config/config.json')
db_config = get_db_config(config)

@app.route('/upload', methods=['POST'])
def upload_file():
    # Check if a file was uploaded
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']

    # Check if the file is empty
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400

    # Get user display name
    user_display_name = request.form.get('user_display_name', '')

    # Process the file through the ETL pipeline
    pipeline = SkypeETLPipeline(
        db_config=db_config,
        output_dir='output'
    )

    # Run the pipeline with the uploaded file
    results = pipeline.run_pipeline(
        file_obj=file,
        user_display_name=user_display_name
    )

    # Return the results
    return jsonify(results)
```

## Modular ETL Pipeline

A new modular ETL pipeline has been introduced, which provides a more maintainable and extensible architecture for ETL operations. The modular pipeline separates the ETL process into distinct components:

- **Extractor**: Handles file reading and validation
- **Transformer**: Processes raw data into structured format
- **Loader**: Manages database operations

### Using the Modular ETL Pipeline

You can use the modular ETL pipeline directly:

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

The modular pipeline provides more flexibility and control over the ETL process:

```python
from src.db.etl import ETLPipeline

# Create the ETL pipeline with custom settings
pipeline = ETLPipeline(
    db_config=db_config,
    output_dir='output',
    memory_limit_mb=2048,  # 2GB memory limit
    parallel_processing=True,  # Enable parallel processing
    chunk_size=2000  # Process messages in chunks of 2000
)

# Run the pipeline
result = pipeline.run_pipeline(
    file_path='skype_export.tar',
    user_display_name='Your Name'
)

# Access detailed phase statistics
for phase, stats in result['phases'].items():
    print(f"{phase.capitalize()} phase:")
    print(f"  Duration: {stats.get('duration_seconds', 0):.2f} seconds")
    print(f"  Messages: {stats.get('processed_messages', 0)}")
    print(f"  Conversations: {stats.get('processed_conversations', 0)}")
```

### Using Individual Components

For advanced use cases, you can use the individual components directly:

```python
from src.db.etl import Extractor, Transformer, Loader

# Create components
extractor = Extractor(output_dir='output')
transformer = Transformer(parallel_processing=True)
loader = Loader(db_config=db_config)

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

## Compatibility Layer

For backward compatibility, the `SkypeETLPipeline` class now uses the modular ETL pipeline internally. This means you can continue to use the `SkypeETLPipeline` class as before, but with the benefits of the modular architecture.

## Timeline for Removal

The deprecated modules will be removed in version 2.0.0 of the package. Until then, they will continue to work but will issue deprecation warnings.

We recommend migrating to the new ETL pipeline as soon as possible to take advantage of the improved functionality and to avoid issues when the deprecated modules are removed.