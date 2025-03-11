# SkypeParser User Guide

Welcome to the SkypeParser User Guide. This guide provides comprehensive documentation for using the SkypeParser tool to process, analyze, and store Skype export data.

## Overview

SkypeParser is a Python tool that allows you to:

- Parse Skype export data from JSON or TAR files
- Extract conversations, messages, and metadata
- Store parsed data in a PostgreSQL database
- Analyze message patterns, frequencies, and content
- Generate visualizations of chat activity
- Handle various message types and attachments
- Build web applications to browse and search your data

## Table of Contents

### Basic Usage

- [CLI Reference](cli-reference.md) - Command-line interface reference
- [Data Analysis](data-analysis.md) - Analyzing Skype data
- [Visualization](visualization.md) - Visualizing Skype data
- [Large Datasets](large-datasets.md) - Processing large datasets
- [Message Types](message-types.md) - Information about supported message types
- [Content Extraction](content-extraction.md) - Details on content extraction features
- [Checkpoint Resumption](checkpoint-resumption.md) - Resuming processing from checkpoints
- [Non-Interactive Mode](non-interactive.md) - Running in non-interactive mode

### Database Integration

- [PostgreSQL Setup](database/postgres.md) - PostgreSQL database setup instructions
- [Supabase Integration](database/supabase.md) - Supabase integration guide

### Web Development

- [Web App Development](web-app.md) - Building web applications with SkypeParser

## Common Workflows

### Basic Parsing

The simplest workflow is to parse a Skype export file and output the conversations to text files:

```bash
python -m src.parser.skype_parser path/to/skype_export.tar -t -o output_dir -u "Your Name"
```

See the [CLI Reference](cli-reference.md) for more details.

### Database Storage

To store the parsed data in a PostgreSQL database:

```bash
python -m src.parser.skype_parser path/to/skype_export.tar -t --store-db --db-name skype_archive --db-user postgres -u "Your Name"
```

See [PostgreSQL Setup](database/postgres.md) for more details.

### ETL Pipeline

For more advanced usage, you can use the ETL (Extract, Transform, Load) pipeline:

```bash
python scripts/run_etl_pipeline.py --file path/to/skype_export.tar --user "Your Name"
```

See [Checkpoint Resumption](checkpoint-resumption.md) for information on resuming processing from checkpoints.

### Data Analysis and Visualization

After processing your Skype data, you can analyze and visualize it using the provided tools:

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

See [Data Analysis](data-analysis.md) and [Visualization](visualization.md) for more details.

### Building a Web App

You can build a web application to browse and search your Skype data:

```bash
# Start the backend API
cd backend
python app.py

# Start the frontend
cd frontend
npm start
```

See [Web App Development](web-app.md) for more details.

## Advanced Features

### Message Type Handling

SkypeParser supports a wide range of message types, including:

- Text messages
- Media messages (images, videos, audio)
- Poll messages
- Call events
- Scheduled call invitations
- Location messages
- Contact messages

See [Message Types](message-types.md) for more information.

### Attachment Handling

SkypeParser can download attachments from URLs, generate thumbnails for image attachments, and extract metadata from attachments:

```bash
python scripts/run_etl_pipeline.py --file path/to/skype_export.tar --user "Your Name" --download-attachments --attachments-dir ./attachments
```

See [Content Extraction](content-extraction.md) for more details.

### Large Dataset Processing

For very large export files, SkypeParser provides memory-efficient processing options:

```python
from src.parser.core_parser import parse_skype_data_streaming

# Get statistics without loading all data into memory
stats = parse_skype_data_streaming(
    file_path='path/to/large_export.tar',
    user_display_name='Your Name'
)
```

See [Large Datasets](large-datasets.md) for more information.

## Troubleshooting

If you encounter issues while using SkypeParser, check the following:

1. **Database Connection Issues**:
   - Verify your database credentials in `config/config.json`
   - Ensure the database server is running and accessible
   - Check network connectivity and firewall settings

2. **Memory Issues**:
   - Increase the memory limit using the `--memory-limit` option
   - Process smaller batches using the `--batch-size` option
   - Ensure your system has sufficient available memory

3. **File Format Errors**:
   - Verify that your Skype export file is in a supported format (JSON or TAR)
   - Check that the file is not corrupted
   - Ensure you have read permissions for the file

For more troubleshooting information, see the [Getting Started Guide](../getting-started/README.md).

## Getting Help

If you encounter issues or have questions, please:

1. Check the documentation in the `docs` directory
2. Review the examples in the `examples` directory
3. Examine the logs for error messages
4. Open an issue on the GitHub repository