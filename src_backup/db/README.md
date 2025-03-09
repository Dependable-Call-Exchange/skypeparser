# Database Module

This module provides functionality for storing and retrieving Skype export data in a PostgreSQL database. It implements a complete ETL (Extract, Transform, Load) pipeline for processing Skype export files.

## Overview

The database module is centered around the `SkypeETLPipeline` class, which orchestrates the entire ETL process. The pipeline extracts data from Skype export files (TAR or JSON), transforms it into a structured format, and loads both raw and transformed data into a PostgreSQL database.

## Key Components

### ETL Pipeline (`etl_pipeline.py`)

The main implementation of the ETL pipeline. It provides:

- **Extraction**: Reading and validating data from Skype export files
- **Transformation**: Cleaning and structuring the raw data
- **Loading**: Storing both raw and transformed data in PostgreSQL

### Raw Storage (`raw_storage/`)

Utilities for storing raw Skype export data in PostgreSQL. This is used by the legacy modules and is being phased out in favor of the ETL pipeline.

### Legacy Modules (Deprecated)

- **`skype_to_postgres.py`**: Imports Skype conversation data into PostgreSQL (deprecated)
- **`store_skype_export.py`**: Stores Skype export data in PostgreSQL (deprecated)

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

## Usage

### Basic Usage

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

### Advanced Usage

You can also use the individual methods of the `SkypeETLPipeline` class for more fine-grained control:

```python
# Initialize the ETL pipeline
etl = SkypeETLPipeline(
    db_name="skype_archive",
    db_user="postgres",
    db_password="your_password"
)

# Connect to the database
etl.connect()

# Extract data
extracted_data = etl.extract(
    input_file="path/to/skype_export.tar",
    is_tar=True,
    output_dir="output_dir"
)

# Transform data
transformed_data = etl.transform(
    extracted_data,
    user_display_name="Your Name"
)

# Load data
etl.load(
    raw_data=extracted_data,
    transformed_data=transformed_data
)

# Close the connection
etl.close()
```

## Error Handling

The ETL pipeline includes comprehensive error handling at each stage of the process. Errors are logged with appropriate context, and the pipeline will attempt to continue processing even if some parts of the data are invalid.

## Logging

The ETL pipeline uses the Python `logging` module to log information about the ETL process. You can configure the logging level and handlers as needed:

```python
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('etl.log')
    ]
)

# Initialize the ETL pipeline
etl = SkypeETLPipeline(...)
```

## Dependencies

- PostgreSQL 10+
- psycopg2
- Python 3.6+