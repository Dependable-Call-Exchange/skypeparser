# Skype Parser ETL Pipeline User Guide

## Overview

The Skype Parser ETL (Extract, Transform, Load) pipeline is a tool for processing Skype export data and storing it in a PostgreSQL database. It extracts data from Skype export files, transforms it into a structured format, and loads it into a database for analysis and reporting.

## Features

- **Extract** data from Skype export files (JSON and TAR formats)
- **Transform** raw data into a structured format
- **Load** transformed data into a PostgreSQL database
- **Resume** processing from checkpoints if errors occur
- **Validate** data integrity throughout the pipeline
- **Configure** the pipeline for different environments and use cases

## Prerequisites

Before using the Skype Parser ETL pipeline, ensure you have:

1. **Python 3.8+** installed on your system
2. **PostgreSQL database** (local or remote, such as Supabase)
3. **Skype export data** in JSON or TAR format
4. Required Python packages installed (see `requirements.txt`)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/skype-parser.git
   cd skype-parser
   ```

2. Create and activate a virtual environment:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

4. Configure the database connection (see [Configuration](#configuration) section)

## Basic Usage

### Command Line Interface

The simplest way to use the Skype Parser ETL pipeline is through the command line interface:

```bash
python scripts/run_etl_pipeline.py --file path/to/skype_export.tar --user "Your Name"
```

This will:
1. Extract data from the Skype export file
2. Transform the data into a structured format
3. Load the data into the configured PostgreSQL database

### Example with Attachment Handling

```bash
python scripts/run_etl_pipeline.py --file path/to/skype_export.tar --user "Your Name" --download-attachments --attachments-dir ./attachments
```

This will:
1. Extract data from the Skype export file
2. Transform the data into a structured format
3. Download attachments to the specified directory
4. Generate thumbnails for image attachments
5. Extract metadata from attachments
6. Load the data into the configured PostgreSQL database

### Options

- `--file`: Path to the Skype export file (required)
- `--user`: Your display name in Skype (required)
- `--output-dir`: Directory to save intermediate files (optional)
- `--config`: Path to custom configuration file (optional)
- `--resume`: Resume from a checkpoint (optional)
- `--checkpoint`: Path to specific checkpoint file (optional)
- `--batch-size`: Number of records to insert in a single batch (optional)
- `--memory-limit`: Memory limit in MB before forcing garbage collection (optional)
- `--download-attachments`: Download attachments from URLs (optional)
- `--attachments-dir`: Directory to store downloaded attachments (optional)
- `--no-thumbnails`: Disable thumbnail generation for image attachments (optional)
- `--no-metadata`: Disable metadata extraction from attachments (optional)

## Configuration

The Skype Parser ETL pipeline uses configuration files to manage database connections and other settings. See the [Configuration Guide](CONFIGURATION.md) for detailed information.

### Quick Setup

1. Copy the example configuration file:
   ```bash
   cp config/config.json.example config/config.json
   ```

2. Edit `config/config.json` with your database credentials:
   ```json
   {
     "database": {
       "host": "your-database-host",
       "port": 5432,
       "dbname": "your-database-name",
       "user": "your-username",
       "password": "your-password"
     }
   }
   ```

## Advanced Features

The Skype Parser tool includes several advanced features:

### Extended Message Type Support

The tool supports a wide range of message types, including:

- Text messages
- Media messages (images, videos, audio)
- Poll messages
- Call events
- Scheduled call invitations
- Location messages
- Contact messages

For more information, see [Message Types](message_types.md).

### Attachment Handling

The tool can download attachments from URLs, generate thumbnails for image attachments, and extract metadata from attachments. For more information, see [Usage Examples](USAGE_EXAMPLES.md).

### Analysis and Reporting

The tool includes functionality for analyzing and reporting on Skype data. This includes:

- Basic reporting functionality
- Data visualization options
- Example queries for common analytics

For more information, see [Analysis and Reporting](ANALYSIS_REPORTING.md).

### Enhanced CLI

The tool includes an enhanced command-line interface with additional options, progress reporting, and improved logging. For more information, see [Enhanced CLI](ENHANCED_CLI.md).

## Advanced Usage

For advanced usage scenarios, refer to the following guides:

- [Checkpoint Resumption](CHECKPOINT_RESUMPTION.md): Resume processing from checkpoints
- [Non-Interactive Mode](NON_INTERACTIVE.md): Run the pipeline in automated environments
- [Supabase Integration](SUPABASE_INTEGRATION.md): Use Supabase as a database backend
- [Content Extraction](content_extraction.md): Understand how content is extracted and processed
- [Message Types](message_types.md): Learn about different message types and how they're handled
- [Data Analysis](skype_data_analysis.md): Analyze the processed data
- [Large Datasets](LARGE_DATASETS.md): Process very large datasets (millions of messages)

## Examples

The `examples` directory contains sample code demonstrating various aspects of the Skype Parser ETL pipeline:

- `advanced_etl_example.py`: Advanced ETL pipeline usage
- `checkpoint_resumption_example.py`: Resuming from checkpoints
- `etl_context_example.py`: Working with the ETL context
- `modular_etl_example.py`: Using the modular ETL components
- `supabase_connection.py`: Connecting to Supabase
- `web_etl_example.py`: Web-based ETL pipeline

## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Verify your database credentials in `config/config.json`
   - Ensure the database server is running and accessible
   - Check network connectivity and firewall settings

2. **Memory Issues**
   - Increase the memory limit using the `--memory-limit` option
   - Process smaller batches using the `--batch-size` option
   - Ensure your system has sufficient available memory

3. **File Format Errors**
   - Verify that your Skype export file is in a supported format (JSON or TAR)
   - Check that the file is not corrupted
   - Ensure you have read permissions for the file

### Logging

The Skype Parser ETL pipeline generates logs that can help diagnose issues:

- `etl_pipeline.log`: General ETL pipeline logs
- `skype_export.log`: Skype export processing logs
- `validation_tests.log`: Data validation logs

## Support

If you encounter issues or have questions, please:

1. Check the documentation in the `docs` directory
2. Review the examples in the `examples` directory
3. Examine the logs for error messages
4. Open an issue on the GitHub repository

## License

This project is licensed under the MIT License - see the LICENSE file for details.