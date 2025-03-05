# Clean Storage Module

This module provides functionality for storing and retrieving clean (transformed) Skype data in a PostgreSQL database. It complements the `raw_storage` module, which handles the storage of raw Skype export data.

## Overview

The clean storage module is designed to store the processed and structured data that results from the transformation phase of the ETL pipeline. It provides a clean interface for storing conversations and messages in a relational format, making it easier to query and analyze the data.

## Key Components

### Models (`models.py`)

Defines the database schema and SQL queries for the clean storage module:

- **Tables**: `clean_skype_conversations` and `clean_skype_messages`
- **SQL Queries**: For inserting, updating, and retrieving data

### Storage (`storage.py`)

Implements the `SkypeCleanDataStorage` class, which provides methods for:

- **Storing conversations**: `store_conversation()`
- **Storing messages**: `store_message()`
- **Retrieving conversations**: `get_conversations()`, `get_conversation()`
- **Retrieving messages**: `get_messages()`, `get_message()`
- **Storing transformed data**: `store_transformed_data()`

## Database Schema

### Conversations Table

```sql
CREATE TABLE IF NOT EXISTS clean_skype_conversations (
    id SERIAL PRIMARY KEY,
    conversation_id VARCHAR(255) UNIQUE NOT NULL,
    display_name VARCHAR(255),
    raw_export_id INTEGER NOT NULL,
    first_message_time TIMESTAMP,
    last_message_time TIMESTAMP,
    message_count INTEGER DEFAULT 0,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_raw_export
        FOREIGN KEY(raw_export_id)
        REFERENCES raw_skype_exports(id)
        ON DELETE CASCADE
);
```

### Messages Table

```sql
CREATE TABLE IF NOT EXISTS clean_skype_messages (
    id SERIAL PRIMARY KEY,
    message_id VARCHAR(255) UNIQUE,
    conversation_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    sender_id VARCHAR(255) NOT NULL,
    sender_name VARCHAR(255),
    message_type VARCHAR(50),
    content TEXT,
    raw_content TEXT,
    is_edited BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_conversation
        FOREIGN KEY(conversation_id)
        REFERENCES clean_skype_conversations(conversation_id)
        ON DELETE CASCADE
);
```

## Usage

### Basic Usage

```python
from src.db.clean_storage.storage import SkypeCleanDataStorage

# Initialize the storage
storage = SkypeCleanDataStorage({
    'dbname': 'skype_archive',
    'user': 'postgres',
    'password': 'your_password',
    'host': 'localhost',
    'port': 5432
})

# Store a conversation
conversation_id = storage.store_conversation(
    conversation_id='19:123456789@thread.skype',
    display_name='Group Chat',
    raw_export_id=1,
    first_message_time='2023-01-01T12:00:00Z',
    last_message_time='2023-01-02T12:00:00Z',
    message_count=10
)

# Store a message
message_id = storage.store_message(
    message_id='12345',
    conversation_id='19:123456789@thread.skype',
    timestamp='2023-01-01T12:00:00Z',
    sender_id='8:user1',
    sender_name='User 1',
    message_type='RichText',
    content='Hello, world!',
    raw_content='<div>Hello, world!</div>',
    is_edited=False
)

# Get conversations for a raw export
conversations = storage.get_conversations(raw_export_id=1)

# Get messages for a conversation
messages = storage.get_messages(conversation_id='19:123456789@thread.skype')

# Close the connection
storage.close()
```

### Integration with ETL Pipeline

The clean storage module is integrated with the ETL pipeline in `src/db/etl_pipeline.py`. The pipeline uses the `store_transformed_data()` method to store the transformed data in the database:

```python
# In the ETL pipeline's load method
raw_export_id = self.raw_storage.store_raw_data(
    data=raw_data,
    file_name=file_name,
    export_date=transformed_data['metadata'].get('exportDate')
)

self.clean_storage.store_transformed_data(
    transformed_data=transformed_data,
    raw_export_id=raw_export_id
)
```

## Error Handling

The clean storage module includes comprehensive error handling to ensure data integrity:

- **Connection errors**: Handled during initialization and connection pool management
- **SQL errors**: Caught and logged with appropriate context
- **Data validation**: Basic validation of input data before storage

## Dependencies

- PostgreSQL 10+
- psycopg2
- Python 3.6+