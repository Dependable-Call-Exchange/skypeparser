# Supabase PostgreSQL Integration

This guide explains how to connect the Skype Parser ETL pipeline to a Supabase PostgreSQL database.

## Overview

[Supabase](https://supabase.com/) is an open-source Firebase alternative that provides a PostgreSQL database with additional features like authentication, storage, and real-time subscriptions. The Skype Parser ETL pipeline can connect to Supabase's PostgreSQL database to store and query Skype conversation data.

## Prerequisites

1. A Supabase account and project
2. The Skype Parser ETL pipeline installed and configured
3. Python 3.7+ with `psycopg2-binary` installed
4. `python-dotenv` package for environment variable management

## Connection Options

Supabase offers three main connection methods for PostgreSQL:

### 1. Direct Connection

Connects directly to your Postgres instance using IPv6.

```
postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-PROJECT-REF].supabase.co:5432/postgres
```

**Connection Parameters:**
- Host: `db.[YOUR-PROJECT-REF].supabase.co`
- Port: `5432`
- Database: `postgres`
- User: `postgres`
- Password: `[YOUR-PASSWORD]`

**Notes:**
- Suitable for long-lived, persistent connections
- Each client has a dedicated connection to Postgres
- Not IPv4 compatible (requires IPv4 add-on or using a pooler)

### 2. Supavisor Session Mode (Recommended)

Uses a proxy for persistent connections with IPv4 support.

```
postgres://postgres.[YOUR-PROJECT-REF]:[YOUR-PASSWORD]@aws-0-[REGION].pooler.supabase.com:5432/postgres
```

**Connection Parameters:**
- Host: `aws-0-[REGION].pooler.supabase.com`
- Port: `5432`
- Database: `postgres`
- User: `postgres.[YOUR-PROJECT-REF]`
- Password: `[YOUR-PASSWORD]`
- Pool Mode: `session`

**Notes:**
- IPv4 compatible
- Session pooler connections are IPv4 proxied for free
- Recommended for ETL workloads and persistent connections

### 3. Supavisor Transaction Mode

Uses a connection pooler for serverless/edge functions.

```
postgres://postgres.[YOUR-PROJECT-REF]:[YOUR-PASSWORD]@aws-0-[REGION].pooler.supabase.com:6543/postgres
```

**Connection Parameters:**
- Host: `aws-0-[REGION].pooler.supabase.com`
- Port: `6543`
- Database: `postgres`
- User: `postgres.[YOUR-PROJECT-REF]`
- Password: `[YOUR-PASSWORD]`
- Pool Mode: `transaction`

**Notes:**
- Suitable for a large number of connected clients
- Pre-warmed connection pool to Postgres
- IPv4 compatible
- Does not support PREPARE statements
- Not recommended for ETL workloads due to connection limitations

For the ETL pipeline, we recommend using **Supavisor Session Mode** since it's designed for persistent connections and supports both IPv4 and IPv6.

## Configuration

### Getting Your Supabase Connection Details

1. Log in to your Supabase dashboard
2. Select your project
3. Go to `Settings` > `Database`
4. Under `Connection string`, select `Session mode` and copy the connection string
5. Extract the connection parameters from the string:
   - Host: `aws-0-[REGION].pooler.supabase.com`
   - Port: `5432`
   - Database: `postgres`
   - User: `postgres.[YOUR-PROJECT-REF]`
   - Password: `[YOUR-PASSWORD]`

### Configuration Methods

You can configure the ETL pipeline to connect to Supabase using either a JSON configuration file or environment variables.

#### Option 1: Using a JSON Configuration File

Create a `supabase.json` file in the `config` directory with the following structure:

```json
{
  "database": {
    "host": "aws-0-[REGION].pooler.supabase.com",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres.[YOUR-PROJECT-REF]",
    "password": "[YOUR-PASSWORD]",
    "sslmode": "require"
  },
  "batch_size": 100,
  "checkpoint_interval": 1000
}
```

Replace the placeholders with your actual Supabase connection details.

#### Option 2: Using Environment Variables (Recommended)

Create a `.env` file in the root directory of your project:

```
# Supabase PostgreSQL Connection Details
user=postgres.[YOUR-PROJECT-REF]
password=[YOUR-PASSWORD]
host=aws-0-[REGION].pooler.supabase.com
port=5432
dbname=postgres
sslmode=require

# ETL Configuration
BATCH_SIZE=100
CHECKPOINT_INTERVAL=1000
```

This approach is more secure as it keeps sensitive information out of your code and configuration files.

## Running the ETL Pipeline with Supabase

### Using the Example Script with JSON Configuration

We provide an example script that demonstrates how to connect to Supabase using a JSON configuration file:

```bash
python examples/supabase_connection.py -f export.tar -u "Your Name" --config config/supabase.json
```

### Using the Example Script with Environment Variables

We also provide an example script that demonstrates how to connect to Supabase using environment variables:

```bash
python examples/supabase_env_connection.py -f export.tar -u "Your Name"
```

### Using the ETL Pipeline Directly with JSON Configuration

You can also use the ETL pipeline directly in your code with a JSON configuration:

```python
from src.db.etl.pipeline import ETLPipeline
from src.db.etl.context import ETLContext
import json

# Load configuration from file
with open('config/supabase.json', 'r') as f:
    config = json.load(f)

# Create ETL context with Supabase configuration
context = ETLContext(
    db_config=config['database'],
    batch_size=config.get('batch_size', 100),
    checkpoint_interval=config.get('checkpoint_interval', 1000)
)

# Create and run ETL pipeline
pipeline = ETLPipeline(context)
result = pipeline.run(
    input_file='path/to/export.tar',
    user_display_name='Your Name'
)
```

### Using the ETL Pipeline Directly with Environment Variables

You can use the ETL pipeline directly in your code with environment variables:

```python
from src.db.etl.pipeline import ETLPipeline
from src.db.etl.context import ETLContext
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Create database configuration dictionary
db_config = {
    'user': os.getenv('user'),
    'password': os.getenv('password'),
    'host': os.getenv('host'),
    'port': os.getenv('port'),
    'dbname': os.getenv('dbname'),
    'sslmode': os.getenv('sslmode', 'require')
}

# Create ETL context with Supabase configuration
context = ETLContext(
    db_config=db_config,
    batch_size=int(os.getenv('BATCH_SIZE', 100)),
    checkpoint_interval=int(os.getenv('CHECKPOINT_INTERVAL', 1000))
)

# Create and run ETL pipeline
pipeline = ETLPipeline(context)
result = pipeline.run(
    input_file='path/to/export.tar',
    user_display_name='Your Name'
)
```

## SSL Requirements

Supabase requires SSL connections. The `sslmode` parameter in the configuration should be set to `require` or higher. The ETL pipeline will automatically use SSL when connecting to Supabase.

## Row-Level Security (RLS)

Supabase uses PostgreSQL's Row-Level Security (RLS) to control access to data. If you're using RLS, you'll need to:

1. Add a `user_id` column to the tables created by the ETL pipeline
2. Create RLS policies for each table
3. Connect with the appropriate user credentials

Example RLS policy for the `skype_conversations` table:

```sql
CREATE POLICY "Users can view their own conversations"
  ON skype_conversations
  FOR SELECT
  USING (user_id = auth.uid());
```

## Troubleshooting

### Connection Issues

If you're having trouble connecting to Supabase:

1. Verify your connection details in the configuration file or environment variables
2. Ensure SSL is enabled (`sslmode` is set to `require`)
3. Check if your IP is allowed in Supabase's network settings
4. Verify that your database user has the necessary permissions
5. If using Direct Connection, ensure you're on an IPv6 network or use a pooler instead

### Common Error Messages

#### "no pg_hba.conf entry for host"
- This usually means your IP is not allowed to connect to the database
- Solution: Use the Session Pooler connection method instead of Direct Connection

#### "SSL connection is required"
- Supabase requires SSL connections
- Solution: Set `sslmode` to `require` in your configuration

#### "too many connections"
- You've reached the connection limit for your Supabase plan
- Solution: Close unused connections or upgrade your Supabase plan

### Performance Considerations

- Use batch operations for better performance when loading large datasets
- Consider using checkpoints for resumable operations
- Monitor your database connection pool size and adjust as needed
- For large datasets, consider increasing the `batch_size` parameter

## Further Reading

- [Supabase Documentation](https://supabase.com/docs)
- [PostgreSQL Connection Parameters](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS)
- [psycopg2 Documentation](https://www.psycopg.org/docs/)
- [python-dotenv Documentation](https://github.com/theskumar/python-dotenv)