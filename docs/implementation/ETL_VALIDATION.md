# ETL Validation Framework

This document describes the comprehensive validation framework implemented in the Skype Parser ETL pipeline.

## Overview

The ETL validation framework provides robust validation checks throughout the ETL pipeline to catch configuration and data issues early. It includes validation for:

- ETL context configuration
- Database connections and schema
- Input data structure and content
- Transformation data structure and content
- Checkpoint data for resumption

## Validation Components

### 1. ETL Context Validation

The `ETLContext` class now includes comprehensive validation of configuration parameters during initialization:

```python
def _validate_configuration(self, db_config, output_dir, memory_limit_mb, chunk_size, batch_size, max_workers):
    # Validate db_config
    if not isinstance(db_config, dict):
        raise ValueError("db_config must be a dictionary")

    # Validate output_dir
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Created output directory: {output_dir}")

    # Validate numeric parameters
    if not isinstance(memory_limit_mb, int) or memory_limit_mb <= 0:
        raise ValueError(f"memory_limit_mb must be a positive integer, got {memory_limit_mb}")

    if not isinstance(chunk_size, int) or chunk_size <= 0:
        raise ValueError(f"chunk_size must be a positive integer, got {chunk_size}")

    if not isinstance(batch_size, int) or batch_size <= 0:
        raise ValueError(f"batch_size must be a positive integer, got {batch_size}")

    if max_workers is not None and (not isinstance(max_workers, int) or max_workers <= 0):
        raise ValueError(f"max_workers must be a positive integer or None, got {max_workers}")

    logger.debug("ETL context configuration validated successfully")
```

### 2. Extractor Validation

The `Extractor` class now includes validation of input parameters and extracted data:

```python
def _validate_input_parameters(self, file_path, file_obj):
    if file_path is None and file_obj is None:
        raise ValueError("Either file_path or file_obj must be provided")

    if file_path is not None:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        file_ext = os.path.splitext(file_path)[1].lower()
        if file_ext not in ['.tar', '.json']:
            raise ValueError(f"Unsupported file extension: {file_ext}. Expected .tar or .json")

def _validate_extracted_data(self, raw_data):
    if not isinstance(raw_data, dict):
        raise ValueError("Extracted data must be a dictionary")

    if 'conversations' not in raw_data:
        raise ValueError("Extracted data must contain 'conversations' key")

    if not isinstance(raw_data['conversations'], list):
        raise ValueError("'conversations' must be a list")

    if len(raw_data['conversations']) == 0:
        logger.warning("No conversations found in the extracted data")

    # Validate each conversation
    for i, conv in enumerate(raw_data['conversations']):
        if not isinstance(conv, dict):
            raise ValueError(f"Conversation {i} must be a dictionary")

        # Check for required fields
        required_fields = ['id', 'MessageList']
        missing_fields = [f for f in required_fields if f not in conv]
        if missing_fields:
            logger.warning(f"Conversation {i} is missing required fields: {missing_fields}")

        # Validate message timestamps if present
        if 'MessageList' in conv and isinstance(conv['MessageList'], list):
            for j, msg in enumerate(conv['MessageList']):
                if 'originalarrivaltime' in msg:
                    timestamp = msg['originalarrivaltime']
                    if not self._is_valid_timestamp(timestamp):
                        logger.warning(f"Invalid timestamp in conversation {i}, message {j}: {timestamp}")
```

### 3. Transformer Validation

The `Transformer` class now includes validation of raw data, user display name, and transformed data:

```python
def _validate_raw_data(self, raw_data):
    if not isinstance(raw_data, dict):
        raise ValueError("Raw data must be a dictionary")

    if 'conversations' not in raw_data:
        raise ValueError("Raw data must contain 'conversations' key")

    if not isinstance(raw_data['conversations'], list):
        raise ValueError("'conversations' must be a list")

    # Validate each conversation
    for i, conv in enumerate(raw_data['conversations']):
        if not isinstance(conv, dict):
            raise ValueError(f"Conversation {i} must be a dictionary")

        # Check for required fields
        required_fields = ['id', 'MessageList']
        missing_fields = [f for f in required_fields if f not in conv]
        if missing_fields:
            logger.warning(f"Conversation {i} is missing required fields: {missing_fields}")

def _validate_user_display_name(self, user_display_name, raw_data):
    # If no display name is provided, try to get it from the raw data
    if not user_display_name:
        if 'userId' in raw_data:
            user_display_name = raw_data['userId']
            logger.info(f"Using user ID as display name: {user_display_name}")
        else:
            logger.warning("No user display name provided and no user ID found in raw data")
            user_display_name = "Unknown User"

    # Sanitize the display name
    if not isinstance(user_display_name, str):
        user_display_name = str(user_display_name)
        logger.warning(f"User display name converted to string: {user_display_name}")

    return user_display_name

def _validate_transformed_data(self, transformed_data):
    if not isinstance(transformed_data, dict):
        raise ValueError("Transformed data must be a dictionary")

    # Check for required metadata fields
    if 'metadata' not in transformed_data:
        raise ValueError("Transformed data must contain 'metadata' key")

    metadata = transformed_data['metadata']
    if not isinstance(metadata, dict):
        raise ValueError("'metadata' must be a dictionary")

    required_metadata_fields = ['user_id', 'export_date']
    missing_fields = [f for f in required_metadata_fields if f not in metadata]
    if missing_fields:
        logger.warning(f"Metadata is missing fields: {missing_fields}")

    # Check for conversations
    if 'conversations' not in transformed_data:
        raise ValueError("Transformed data must contain 'conversations' key")

    conversations = transformed_data['conversations']
    if not isinstance(conversations, dict):
        raise ValueError("'conversations' must be a dictionary")

    # Validate each conversation
    for conv_id, conv in conversations.items():
        if not isinstance(conv, dict):
            raise ValueError(f"Conversation '{conv_id}' must be a dictionary")

        # Check for required fields
        required_conv_fields = ['display_name', 'messages']
        missing_fields = [f for f in required_conv_fields if f not in conv]
        if missing_fields:
            logger.warning(f"Conversation '{conv_id}' is missing fields: {missing_fields}")

        # Validate messages
        if 'messages' in conv:
            if not isinstance(conv['messages'], list):
                raise ValueError(f"'messages' in conversation '{conv_id}' must be a list")
```

### 4. Loader Validation

The `Loader` class now includes validation of input data and database connections:

```python
def _validate_input_data(self, raw_data, transformed_data):
    # Validate raw_data
    if not isinstance(raw_data, dict):
        raise ValueError("Raw data must be a dictionary")

    # Check for required fields in raw_data
    required_raw_fields = ['userId', 'exportDate']
    missing_fields = [f for f in required_raw_fields if f not in raw_data]
    if missing_fields:
        logger.warning(f"Raw data is missing fields: {missing_fields}")

    # Validate transformed_data
    if not isinstance(transformed_data, dict):
        raise ValueError("Transformed data must be a dictionary")

    # Check for required fields in transformed_data
    if 'conversations' not in transformed_data:
        raise ValueError("Transformed data must contain 'conversations' key")

    if not isinstance(transformed_data['conversations'], dict):
        raise ValueError("'conversations' must be a dictionary")

def _validate_database_connection(self):
    if not self.conn:
        raise ValueError("Database connection is not established")

    try:
        # Check if the connection is alive
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            if result[0] != 1:
                raise ValueError("Database connection test failed")

        logger.debug("Database connection validated successfully")
    except Exception as e:
        raise ValueError(f"Database connection validation failed: {e}")
```

### 5. ETL Pipeline Validation

The `ETLPipeline` class now includes validation of pipeline input parameters:

```python
def _validate_pipeline_input(self, file_path, file_obj, user_display_name):
    # Validate that at least one of file_path or file_obj is provided
    if file_path is None and file_obj is None:
        raise ValueError("Either file_path or file_obj must be provided")

    # Validate file_path if provided
    if file_path is not None:
        if not isinstance(file_path, str):
            raise ValueError(f"file_path must be a string, got {type(file_path)}")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        file_ext = os.path.splitext(file_path)[1].lower()
        if file_ext not in ['.tar', '.json']:
            raise ValueError(f"Unsupported file extension: {file_ext}. Expected .tar or .json")

    # Validate user_display_name if provided
    if user_display_name is not None and not isinstance(user_display_name, str):
        logger.warning(f"user_display_name is not a string, converting: {user_display_name}")
        user_display_name = str(user_display_name)

    logger.debug("Pipeline input parameters validated successfully")
```

### 6. Centralized Validation Utilities

A new module `src/utils/etl_validation.py` provides centralized validation functions:

```python
def validate_supabase_config(config):
    """Validate Supabase configuration."""
    # Check required fields
    required_fields = ['host', 'port', 'dbname', 'user', 'password', 'sslmode']
    missing_fields = [f for f in required_fields if f not in config]
    if missing_fields:
        raise ETLValidationError(f"Missing required Supabase configuration fields: {missing_fields}")

    # Validate host format
    host = config.get('host', '')
    if not host:
        raise ETLValidationError("Supabase host cannot be empty")

    # Check for Supabase host patterns
    supabase_patterns = [
        r'db\.[a-z0-9-]+\.supabase\.co',  # Direct connection
        r'aws-0-[a-z0-9-]+\.pooler\.supabase\.com'  # Pooler connection
    ]

    if not any(re.match(pattern, host) for pattern in supabase_patterns):
        logger.warning(f"Host '{host}' does not match typical Supabase host patterns")

    # Validate port
    port = config.get('port')
    if not isinstance(port, int):
        try:
            port = int(port)
        except (ValueError, TypeError):
            raise ETLValidationError("Port must be an integer")

    # Check for Supabase port patterns
    if port not in [5432, 6543]:
        logger.warning(f"Port {port} is not a typical Supabase port (5432 for session mode, 6543 for transaction mode)")

    # Validate SSL mode
    sslmode = config.get('sslmode', '')
    if sslmode != 'require':
        logger.warning(f"SSL mode '{sslmode}' is not recommended for Supabase. 'require' is the recommended setting.")

    return True

def validate_database_schema(conn):
    """Validate that the database has the required schema."""
    required_tables = ['skype_raw_exports', 'skype_conversations', 'skype_messages']
    missing_tables = []

    try:
        with conn.cursor() as cursor:
            # Check if tables exist
            for table in required_tables:
                cursor.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = %s
                    )
                """, (table,))
                exists = cursor.fetchone()[0]
                if not exists:
                    missing_tables.append(table)

        if missing_tables:
            logger.warning(f"Missing required tables: {missing_tables}")
            return False, missing_tables

        return True, []

    except Exception as e:
        raise ETLValidationError(f"Error validating database schema: {e}")

def validate_transformed_data_structure(transformed_data):
    """Validate and sanitize transformed data structure."""
    # Check required top-level fields
    required_fields = ['metadata', 'conversations']
    missing_fields = [f for f in required_fields if f not in transformed_data]
    if missing_fields:
        raise ETLValidationError(f"Missing required transformed data fields: {missing_fields}")

    # Create a sanitized copy
    sanitized_data = {
        'metadata': transformed_data['metadata'].copy(),
        'conversations': {}
    }

    # Sanitize conversation data
    for conv_id, conv in transformed_data['conversations'].items():
        if not isinstance(conv, dict):
            logger.warning(f"Skipping invalid conversation '{conv_id}': not a dictionary")
            continue

        # Sanitize conversation ID
        sanitized_conv_id = re.sub(r'[<>:"/\\|?*]', '_', conv_id)
        if sanitized_conv_id != conv_id:
            logger.warning(f"Sanitized conversation ID from '{conv_id}' to '{sanitized_conv_id}'")

        # Create sanitized conversation
        sanitized_data['conversations'][sanitized_conv_id] = {
            'display_name': conv.get('display_name', ''),
            'messages': []
        }

        # Copy other fields
        for key, value in conv.items():
            if key != 'messages':
                sanitized_data['conversations'][sanitized_conv_id][key] = value

        # Sanitize messages
        if 'messages' in conv and isinstance(conv['messages'], list):
            for msg in conv['messages']:
                if not isinstance(msg, dict):
                    logger.warning(f"Skipping invalid message in conversation '{conv_id}': not a dictionary")
                    continue

                # Create sanitized message
                sanitized_msg = msg.copy()

                # Sanitize content fields
                for content_field in ['raw_content', 'cleaned_content']:
                    if content_field in sanitized_msg and sanitized_msg[content_field] is not None:
                        # Ensure content is a string
                        if not isinstance(sanitized_msg[content_field], str):
                            sanitized_msg[content_field] = str(sanitized_msg[content_field])

                sanitized_data['conversations'][sanitized_conv_id]['messages'].append(sanitized_msg)

    return sanitized_data
```

## Testing

The validation framework includes comprehensive unit and integration tests:

- **Unit Tests**: Located in `tests/unit/test_etl_validation.py`
- **Integration Tests**: Located in `tests/integration/test_etl_validation_integration.py`

To run the tests, use the `run_validation_tests.py` script:

```bash
# Run all tests
python run_validation_tests.py

# Run only unit tests
python run_validation_tests.py --type unit

# Run only integration tests
python run_validation_tests.py --type integration

# Run with verbose output
python run_validation_tests.py --verbose
```

## Benefits

The enhanced validation framework provides several benefits:

1. **Early Error Detection**: Catches configuration and data issues early in the pipeline
2. **Improved Data Quality**: Ensures data consistency and integrity throughout the ETL process
3. **Better Error Messages**: Provides clear, actionable error messages for troubleshooting
4. **Data Sanitization**: Automatically sanitizes problematic data where possible
5. **Robust Resumption**: Validates checkpoint data for reliable pipeline resumption

## Future Improvements

Potential future improvements to the validation framework:

1. **Schema Validation**: Add JSON schema validation for raw and transformed data
2. **Data Quality Metrics**: Track and report data quality metrics during validation
3. **Custom Validation Rules**: Allow users to define custom validation rules
4. **Validation Plugins**: Create a plugin system for extensible validation
5. **Validation Reporting**: Generate detailed validation reports for analysis