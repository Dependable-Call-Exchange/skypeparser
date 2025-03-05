# Supabase PostgreSQL Integration

This guide explains how to connect the Skype Parser ETL pipeline to a Supabase PostgreSQL database.

## Overview

[Supabase](https://supabase.com/) is an open-source Firebase alternative that provides a PostgreSQL database with additional features like authentication, storage, and real-time subscriptions. The Skype Parser ETL pipeline can connect to Supabase's PostgreSQL database to store and query Skype conversation data.

## Prerequisites

1. A Supabase account and project
2. The Skype Parser ETL pipeline installed and configured
3. Python 3.7+ with `psycopg2-binary` installed

## Connection Options

Supabase offers three main connection methods for PostgreSQL:

### 1. Direct Connection

Connects directly to your Postgres instance using IPv6.

```
postgresql://postgres:[YOUR-PASSWORD]@db.[YOUR-PROJECT-REF].supabase.co:5432/postgres
```

### 2. Supavisor Session Mode (Recommended)

Uses a proxy for persistent connections with IPv4 support.

```
postgres://postgres.[YOUR-PROJECT-REF]:[YOUR-PASSWORD]@aws-0-[REGION].pooler.supabase.com:5432/postgres
```

### 3. Supavisor Transaction Mode

Uses a connection pooler for serverless/edge functions.

```
postgres://postgres.[YOUR-PROJECT-REF]:[YOUR-PASSWORD]@aws-0-[REGION].pooler.supabase.com:6543/postgres
```

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

### Creating a Configuration File

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

## Running the ETL Pipeline with Supabase

### Using the Example Script

We provide an example script that demonstrates how to connect to Supabase:

```bash
python examples/supabase_connection.py -f export.tar -u "Your Name" --config config/supabase.json
```

### Using the ETL Pipeline Directly

You can also use the ETL pipeline directly in your code:

```python
from src.db.etl.pipeline import ETLPipeline
from src.db.etl.context import ETLContext

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

1. Verify your connection details in the configuration file
2. Ensure SSL is enabled (`sslmode` is set to `require`)
3. Check if your IP is allowed in Supabase's network settings
4. Verify that your database user has the necessary permissions

### Performance Considerations

- Use batch operations for better performance when loading large datasets
- Consider using checkpoints for resumable operations
- Monitor your database connection pool size and adjust as needed

## Further Reading

- [Supabase Documentation](https://supabase.com/docs)
- [PostgreSQL Connection Parameters](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS)
- [psycopg2 Documentation](https://www.psycopg.org/docs/)