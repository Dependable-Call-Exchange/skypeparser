# Configuration System

This document describes the configuration system used in the SkypeParser project.

## Overview

The SkypeParser project uses a flexible configuration system that allows you to customize various aspects of the application through configuration files and environment variables. This makes it easy to adapt the application to different environments and use cases without modifying the code.

## Configuration Files

The project uses two main configuration files:

1. **Main Configuration File** (`config/config.json`): Contains general settings for the application, including database connection details, output settings, and logging configuration.

2. **Message Types Configuration File** (`config/message_types.json`): Contains mappings for Skype message types to their human-readable descriptions.

Example configuration files are provided in the `config` directory:
- `config.json.example`
- `message_types.json`

To use these files, copy them and remove the `.example` suffix:

```bash
cp config/config.json.example config/config.json
```

## Configuration Structure

### Main Configuration

The main configuration file has the following structure:

```json
{
    "database": {
        "host": "localhost",
        "port": 5432,
        "dbname": "Skype_archive",
        "user": "Postgres",
        "password": "your_password_here"
    },
    "output": {
        "directory": "output",
        "overwrite": false
    },
    "logging": {
        "level": "INFO",
        "file": "Skype_parser.log"
    }
}
```

### Message Types Configuration

The message types configuration file has the following structure:

```json
{
    "message_types": {
        "Event/Call": "***A call started/ended***",
        "Poll": "***Created a poll***",
        "RichText/Media_Album": "***Sent an album of images***",
        "RichText/Media_AudioMsg": "***Sent a voice message***",
        "RichText/Media_CallRecording": "***Sent a call recording***",
        "RichText/Media_Card": "***Sent a media card***",
        "RichText/Media_FlikMsg": "***Sent a moji***",
        "RichText/Media_GenericFile": "***Sent a file***",
        "RichText/Media_Video": "***Sent a video message***",
        "RichText/UriObject": "***Sent a photo***",
        "RichText/ScheduledCallInvite": "***Scheduled a call***",
        "RichText/Location": "***Sent a location***",
        "RichText/Contacts": "***Sent a contact***"
    },
    "default_message_format": "***Sent a {message_type}***"
}
```

## Environment Variables

You can also configure the application using environment variables, which take precedence over the configuration files. The following environment variables are supported:

### Database Settings
- `POSTGRES_HOST`: Database host
- `POSTGRES_PORT`: Database port
- `POSTGRES_DB`: Database name
- `POSTGRES_USER`: Database user
- `POSTGRES_PASSWORD`: Database password

### Output Settings
- `OUTPUT_DIR`: Directory to output files to
- `OUTPUT_OVERWRITE`: Whether to overwrite existing files (true/false)

### Logging Settings
- `LOG_LEVEL`: Logging level (INFO, DEBUG, WARNING, ERROR, CRITICAL)
- `LOG_FILE`: Log file path

## Using the Configuration System

The configuration system is used throughout the application to provide a consistent way to access configuration values. Here's how to use it in your code:

```python
from src.utils.config import load_config, get_db_config, get_message_type_description

# Load the configuration
config = load_config(
    config_file='path/to/config.json',
    message_types_file='path/to/message_types.json'
)

# Get database configuration
db_config = get_db_config(config)

# Get a message type description
description = get_message_type_description(config, 'Event/Call')
```

## Default Configuration

If no configuration files are provided, the application will use default values defined in the `DEFAULT_CONFIG` dictionary in `src/utils/config.py`. These defaults provide a reasonable starting point, but you should customize them for your specific needs.

## Adding New Configuration Options

To add new configuration options:

1. Add the default values to the `DEFAULT_CONFIG` dictionary in `src/utils/config.py`
2. Update the `load_config` function to load the new options from configuration files and environment variables
3. Add helper functions to access the new options if needed
4. Update the documentation to reflect the new options

## Best Practices

- Always use the configuration system instead of hardcoding values in your code
- Use environment variables for sensitive information like passwords
- Keep configuration files out of version control (add them to `.gitignore`)
- Document all configuration options and their default values
- Provide example configuration files with sensible defaults