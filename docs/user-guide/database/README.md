# Database Integration

This directory contains documentation about integrating SkypeParser with different database systems.

## Table of Contents

- [PostgreSQL Setup](postgres.md) - PostgreSQL database setup instructions
- [Supabase Integration](supabase.md) - Supabase integration guide

## Overview

SkypeParser can store parsed Skype data in a PostgreSQL database, either self-hosted or using a managed service like Supabase. This allows for more advanced querying and analysis of the data.

## PostgreSQL Setup

PostgreSQL is the primary database backend for SkypeParser. It provides a robust, reliable, and performant storage solution for Skype data.

See [PostgreSQL Setup](postgres.md) for detailed instructions on setting up a PostgreSQL database for SkypeParser.

## Supabase Integration

Supabase is a Firebase alternative built on top of PostgreSQL. It provides a managed PostgreSQL database with additional features like authentication, storage, and real-time subscriptions.

See [Supabase Integration](supabase.md) for detailed instructions on integrating SkypeParser with Supabase.

## Database Schema

SkypeParser uses the following database schema:

- **skype_raw_exports**: Stores the raw export data
- **skype_conversations**: Stores transformed conversation metadata
- **skype_messages**: Stores transformed messages
- **skype_attachments**: Stores attachment metadata
- **skype_users**: Stores user information

## Common Database Operations

### Storing Data

To store parsed data in a PostgreSQL database:

```bash
python -m src.parser.skype_parser path/to/skype_export.tar -t --store-db --db-name skype_archive --db-user postgres -u "Your Name"
```

### Querying Data

To query the stored data, you can use the provided query utilities:

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
```

See [Data Analysis](../data-analysis.md) for more details on querying and analyzing the stored data.