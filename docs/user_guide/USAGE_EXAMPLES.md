# Skype Parser ETL Pipeline Usage Examples

This document provides examples of common usage scenarios for the Skype Parser ETL pipeline.

## Basic Usage Examples

### 1. Process a Skype Export File

The most basic usage is to process a Skype export file and load it into the database:

```bash
python scripts/run_etl_pipeline.py --file path/to/skype_export.tar --user "Your Name"
```

This will:
1. Extract data from the Skype export file
2. Transform the data into a structured format
3. Load the data into the configured PostgreSQL database

### 2. Specify an Output Directory

You can specify an output directory for intermediate files:

```bash
python scripts/run_etl_pipeline.py --file path/to/skype_export.tar --user "Your Name" --output-dir ./output
```

This is useful for:
- Debugging the ETL process
- Analyzing intermediate data
- Creating checkpoints for resumable operations

### 3. Use a Custom Configuration File

You can use a custom configuration file:

```bash
python scripts/run_etl_pipeline.py --file path/to/skype_export.tar --user "Your Name" --config path/to/custom_config.json
```

This allows you to:
- Use different database connections for different exports
- Customize logging settings
- Configure performance parameters

## Advanced Usage Examples

### 1. Resume from a Checkpoint

If the ETL process fails, you can resume from a checkpoint:

```bash
python scripts/run_etl_pipeline.py --resume --checkpoint path/to/checkpoint_file.json
```

This will:
1. Load the checkpoint state
2. Resume processing from where it left off
3. Complete the remaining steps in the pipeline

### 2. Process Large Datasets

For large datasets, you can adjust memory and batch settings:

```bash
python scripts/run_etl_pipeline.py --file path/to/skype_export.tar --user "Your Name" --memory-limit 2048 --batch-size 500
```

This helps:
- Prevent memory issues with large exports
- Optimize database operations
- Improve overall performance

### 3. Non-Interactive Mode

For automated processing, you can use non-interactive mode:

```bash
python scripts/run_etl_pipeline.py --file path/to/skype_export.tar --user "Your Name" --non-interactive
```

This is useful for:
- Scheduled jobs
- CI/CD pipelines
- Batch processing

## Programmatic Usage Examples

### 1. Basic ETL Pipeline

```python
from src.db.etl.pipeline_manager import ETLPipeline
from src.utils.config import load_config, get_db_config

# Load configuration
config = load_config()
db_config = get_db_config(config)

# Create ETL pipeline
pipeline = ETLPipeline(db_config=db_config)

# Run the pipeline
result = pipeline.run_pipeline(
    file_path="path/to/skype_export.tar",
    user_display_name="Your Name"
)

print(f"Processed {result['conversation_count']} conversations with {result['message_count']} messages")
print(f"Export ID: {result['export_id']}")
```

### 2. Modular ETL Components

```python
from src.db.etl.extractor import Extractor
from src.db.etl.transformer import Transformer
from src.db.etl.loader import Loader
from src.db.etl.context import ETLContext
from src.utils.config import load_config, get_db_config

# Load configuration
config = load_config()
db_config = get_db_config(config)

# Create ETL context
context = ETLContext(db_config=db_config)

# Create ETL components
extractor = Extractor(context=context)
transformer = Transformer(context=context)
loader = Loader(context=context)

# Extract data
raw_data = extractor.extract(file_path="path/to/skype_export.tar")

# Transform data
transformed_data = transformer.transform(raw_data, user_display_name="Your Name")

# Load data
export_id = loader.load(raw_data, transformed_data, file_source="path/to/skype_export.tar")

print(f"Data loaded with export ID: {export_id}")
```

### 3. Advanced ETL Pipeline with Error Handling

```python
from src.db.etl.pipeline_manager import ETLPipeline
from src.utils.config import load_config, get_db_config
import logging
import json

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load configuration
config = load_config()
db_config = get_db_config(config)

# Create ETL pipeline
pipeline = ETLPipeline(
    db_config=db_config,
    output_dir="./output",
    memory_limit_mb=2048,
    batch_size=500
)

try:
    # Run the pipeline
    result = pipeline.run_pipeline(
        file_path="path/to/skype_export.tar",
        user_display_name="Your Name"
    )

    # Process successful result
    logger.info(f"ETL pipeline completed successfully")
    logger.info(f"Processed {result['conversation_count']} conversations with {result['message_count']} messages")
    logger.info(f"Export ID: {result['export_id']}")

    # Save result to file
    with open("etl_result.json", "w") as f:
        json.dump(result, f, indent=2)

except Exception as e:
    # Handle error
    logger.error(f"ETL pipeline failed: {str(e)}")

    # Try to resume from checkpoint
    try:
        checkpoints = pipeline.get_available_checkpoints()
        if checkpoints:
            latest_checkpoint = checkpoints[-1]
            logger.info(f"Resuming from checkpoint: {latest_checkpoint}")

            # Create new pipeline from checkpoint
            resume_pipeline = ETLPipeline.load_from_checkpoint(
                checkpoint_file=latest_checkpoint,
                db_config=db_config
            )

            # Resume pipeline
            result = resume_pipeline.run_pipeline(resume_from_checkpoint=True)
            logger.info(f"Pipeline resumed and completed successfully")
            logger.info(f"Export ID: {result['export_id']}")
    except Exception as resume_error:
        logger.error(f"Failed to resume pipeline: {str(resume_error)}")
```

## Integration Examples

### 1. Web Application Integration

```python
from flask import Flask, request, jsonify
from src.db.etl.pipeline_manager import ETLPipeline
from src.utils.config import load_config, get_db_config
import os
import tempfile

app = Flask(__name__)

@app.route('/api/process', methods=['POST'])
def process_skype_export():
    # Check if file was uploaded
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    user_name = request.form.get('user_name', 'Unknown User')

    # Save uploaded file to temporary location
    temp_dir = tempfile.mkdtemp()
    file_path = os.path.join(temp_dir, file.filename)
    file.save(file_path)

    try:
        # Load configuration
        config = load_config()
        db_config = get_db_config(config)

        # Create and run ETL pipeline
        pipeline = ETLPipeline(db_config=db_config)
        result = pipeline.run_pipeline(
            file_path=file_path,
            user_display_name=user_name
        )

        # Clean up temporary file
        os.remove(file_path)
        os.rmdir(temp_dir)

        # Return result
        return jsonify({
            'status': 'success',
            'export_id': result['export_id'],
            'conversation_count': result['conversation_count'],
            'message_count': result['message_count']
        })

    except Exception as e:
        # Clean up temporary file
        if os.path.exists(file_path):
            os.remove(file_path)
        if os.path.exists(temp_dir):
            os.rmdir(temp_dir)

        # Return error
        return jsonify({
            'status': 'error',
            'error': str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True)
```

### 2. Database Query Examples

After processing Skype exports, you can query the database to analyze the data:

```python
import psycopg2
from psycopg2.extras import RealDictCursor
from src.utils.config import load_config, get_db_config

# Load configuration
config = load_config()
db_config = get_db_config(config)

# Connect to database
conn = psycopg2.connect(
    host=db_config['host'],
    port=db_config['port'],
    dbname=db_config['dbname'],
    user=db_config['user'],
    password=db_config['password']
)

# Create cursor
cursor = conn.cursor(cursor_factory=RealDictCursor)

# Example 1: Get conversation statistics
cursor.execute("""
    SELECT
        c.conversation_id,
        c.display_name,
        c.message_count,
        c.first_message_time,
        c.last_message_time,
        EXTRACT(DAY FROM (c.last_message_time - c.first_message_time)) AS duration_days
    FROM
        skype_conversations c
    ORDER BY
        c.message_count DESC
    LIMIT 10
""")
conversations = cursor.fetchall()
print("Top 10 conversations by message count:")
for conv in conversations:
    print(f"{conv['display_name']}: {conv['message_count']} messages over {conv['duration_days']} days")

# Example 2: Get message type distribution
cursor.execute("""
    SELECT
        message_type,
        COUNT(*) as count,
        ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM skype_messages), 2) as percentage
    FROM
        skype_messages
    GROUP BY
        message_type
    ORDER BY
        count DESC
""")
message_types = cursor.fetchall()
print("\nMessage type distribution:")
for msg_type in message_types:
    print(f"{msg_type['message_type']}: {msg_type['count']} messages ({msg_type['percentage']}%)")

# Example 3: Get user activity by hour
cursor.execute("""
    SELECT
        EXTRACT(HOUR FROM timestamp) as hour,
        COUNT(*) as message_count
    FROM
        skype_messages
    GROUP BY
        hour
    ORDER BY
        hour
""")
hourly_activity = cursor.fetchall()
print("\nUser activity by hour:")
for activity in hourly_activity:
    print(f"{int(activity['hour']):02d}:00 - {int(activity['hour']):02d}:59: {activity['message_count']} messages")

# Close connection
cursor.close()
conn.close()
```

## Conclusion

These examples demonstrate the flexibility and power of the Skype Parser ETL pipeline. You can use it for simple one-off exports, integrate it into web applications, or build complex data analysis workflows. The modular design allows you to customize the pipeline to suit your specific needs.