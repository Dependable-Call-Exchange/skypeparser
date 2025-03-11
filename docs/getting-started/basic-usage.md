# Basic Usage Guide

This guide provides a quick introduction to using the SkypeParser tool for processing Skype export data.

## Obtaining Skype Export Data

Before using SkypeParser, you need to export your Skype chat history:

1. Open Skype and sign in
2. Go to Settings > Export Skype Data
3. Select the conversations you want to export
4. Click "Export" and save the file (usually a `.tar` file)

For detailed instructions, see [Skype's official guide](https://support.skype.com/en/faq/FA34894/how-do-i-export-my-skype-files-and-chat-history).

## Command Line Usage

### Basic Parsing

The simplest way to use SkypeParser is to parse a Skype export file and output the conversations to text files:

```bash
python -m src.parser.skype_parser path/to/skype_export.tar -t -o output_dir -u "Your Name"
```

This command:
- Parses the Skype export file (`-t` indicates it's a TAR file)
- Outputs the conversations to the specified directory (`-o output_dir`)
- Uses your display name to identify your messages (`-u "Your Name"`)

### Output Formats

You can specify different output formats:

```bash
# Output as JSON
python -m src.parser.skype_parser path/to/skype_export.tar -t -f json -o output_dir -u "Your Name"

# Output as text
python -m src.parser.skype_parser path/to/skype_export.tar -t -f text -o output_dir -u "Your Name"
```

### Storing in Database

To store the parsed data in a PostgreSQL database:

```bash
python -m src.parser.skype_parser path/to/skype_export.tar -t --store-db --db-name skype_archive --db-user postgres -u "Your Name"
```

This command:
- Parses the Skype export file
- Stores the data in the specified database
- Uses the specified database user

## Using the ETL Pipeline

For more advanced usage, you can use the ETL (Extract, Transform, Load) pipeline:

```bash
python scripts/run_etl_pipeline.py --file path/to/skype_export.tar --user "Your Name"
```

This command:
- Extracts data from the Skype export file
- Transforms the data into a structured format
- Loads the data into the configured PostgreSQL database

### ETL Pipeline Options

The ETL pipeline supports various options:

```bash
# Download attachments
python scripts/run_etl_pipeline.py --file path/to/skype_export.tar --user "Your Name" --download-attachments --attachments-dir ./attachments

# Specify batch size for database operations
python scripts/run_etl_pipeline.py --file path/to/skype_export.tar --user "Your Name" --batch-size 1000

# Set memory limit
python scripts/run_etl_pipeline.py --file path/to/skype_export.tar --user "Your Name" --memory-limit 2048
```

## Python API Usage

You can also use SkypeParser as a Python library:

```python
from src.parser import SkypeParser

# Parse a Skype export file
parser = SkypeParser()
data = parser.parse_file('path/to/skype_export.json')

# Access parsed data
for conversation_id, conversation in data['conversations'].items():
    print(f"Conversation: {conversation['display_name']}")
    for message in conversation['messages']:
        print(f"  {message['sender_name']}: {message['content']}")
```

### Using the ETL Pipeline in Python

```python
from src.db.etl.pipeline_manager import ETLPipeline

# Create an ETL pipeline
pipeline = ETLPipeline(
    db_config={
        'host': 'localhost',
        'port': 5432,
        'dbname': 'skype_data',
        'user': 'postgres',
        'password': 'password'
    },
    output_dir='output'
)

# Run the pipeline
result = pipeline.run_pipeline(
    file_path='path/to/skype_export.json',
    user_display_name='Your Name'
)

print(f"Processed {result['conversation_count']} conversations with {result['message_count']} messages")
```

## Memory-Efficient Processing

For very large export files, use the streaming approach:

```python
from src.parser.core_parser import parse_skype_data_streaming

# Get statistics without loading all data into memory
stats = parse_skype_data_streaming(
    file_path='path/to/large_export.tar',
    user_display_name='Your Name'
)
print(f"Found {stats['conversation_count']} conversations and {stats['message_count']} messages")
```

## Next Steps

Now that you understand the basic usage of SkypeParser, you can:

- Explore the [Configuration Guide](configuration.md) to customize the tool
- Learn about [Message Types](../user-guide/message-types.md) and how they're handled
- Check out the [Data Analysis Guide](../user-guide/data-analysis.md) to analyze your Skype data
- Read about [Attachment Handling](../user-guide/content-extraction.md) to work with media files