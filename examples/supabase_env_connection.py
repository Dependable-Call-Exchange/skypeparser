#!/usr/bin/env python3
"""
Example script for connecting to Supabase PostgreSQL using environment variables.

This script demonstrates how to load Supabase connection details from a .env file
and use them with the ETL pipeline to process a Skype export file.

Usage:
    python supabase_env_connection.py -f path/to/skype_export.tar -u "Your Name"

Arguments:
    -f, --file: Path to the Skype export file (TAR or JSON)
    -u, --user-display-name: Your display name in the Skype logs
"""

import os
import sys
import argparse
import logging
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.db.etl.pipeline_manager import ETLPipeline
from src.db.etl.context import ETLContext
from src.utils.etl_validation import validate_supabase_config, validate_database_schema, ETLValidationError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("supabase_import.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def get_db_config_from_env():
    """
    Load database configuration from environment variables.

    Returns:
        dict: Database configuration dictionary

    Raises:
        ETLValidationError: If the configuration is invalid
    """
    # Load environment variables from .env file
    load_dotenv()

    # Required database parameters
    required_params = ['user', 'host', 'port', 'dbname']

    # Check if all required parameters are present
    missing_params = [param for param in required_params if not os.getenv(param)]
    if missing_params:
        raise ETLValidationError(f"Missing required environment variables: {', '.join(missing_params)}")

    # Create database configuration dictionary
    db_config = {
        'user': os.getenv('user'),
        'password': os.getenv('password'),
        'host': os.getenv('host'),
        'port': int(os.getenv('port')),  # Convert port to integer
        'dbname': os.getenv('dbname'),
        'sslmode': os.getenv('sslmode', 'require')  # Default to 'require' for Supabase
    }

    # Validate the Supabase configuration
    validate_supabase_config(db_config)

    return db_config

def validate_database_connection(db_config):
    """
    Validate database connection and schema.

    Args:
        db_config: Database configuration dictionary

    Returns:
        bool: True if the connection and schema are valid

    Raises:
        ETLValidationError: If the connection or schema is invalid
    """
    try:
        # Attempt to connect to the database
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            dbname=db_config['dbname'],
            user=db_config['user'],
            password=db_config['password'],
            sslmode=db_config['sslmode']
        )

        # Validate the database schema
        is_valid, missing_tables = validate_database_schema(conn)

        if not is_valid:
            logger.warning(f"Database schema validation failed. Missing tables: {missing_tables}")
            logger.info("The ETL pipeline will attempt to create missing tables.")

        # Close the connection
        conn.close()
        return True

    except Exception as e:
        raise ETLValidationError(f"Database connection validation failed: {e}")

def validate_input_file(file_path):
    """
    Validate the input file.

    Args:
        file_path: Path to the input file

    Returns:
        bool: True if the file is valid

    Raises:
        ETLValidationError: If the file is invalid
    """
    # Check if the file exists
    if not os.path.exists(file_path):
        raise ETLValidationError(f"Input file does not exist: {file_path}")

    # Check if the file is a TAR or JSON file
    file_ext = os.path.splitext(file_path)[1].lower()
    if file_ext not in ['.tar', '.json']:
        raise ETLValidationError(f"Input file must be a TAR or JSON file, got: {file_ext}")

    # Check if the file is readable
    if not os.access(file_path, os.R_OK):
        raise ETLValidationError(f"Input file is not readable: {file_path}")

    # Check if the file is not empty
    if os.path.getsize(file_path) == 0:
        raise ETLValidationError(f"Input file is empty: {file_path}")

    return True

def get_commandline_args():
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(description='Import Skype data to Supabase PostgreSQL')
    parser.add_argument('-f', '--file', required=True, help='Path to the Skype export file (TAR or JSON)')
    parser.add_argument('-u', '--user-display-name', required=True, help='Your display name in the Skype logs')
    parser.add_argument('--skip-validation', action='store_true', help='Skip database validation')
    parser.add_argument('--debug', action='store_true', help='Enable debug logging')

    return parser.parse_args()

def main():
    """
    Main function to run the ETL pipeline with Supabase connection.
    """
    try:
        # Parse command-line arguments
        args = get_commandline_args()

        # Set debug logging if requested
        if args.debug:
            logging.getLogger().setLevel(logging.DEBUG)
            logger.debug("Debug logging enabled")

        # Validate input file
        logger.info(f"Validating input file: {args.file}")
        validate_input_file(args.file)

        # Get database configuration from environment variables
        logger.info("Loading database configuration from environment variables")
        db_config = get_db_config_from_env()
        logger.info(f"Using database: {db_config['dbname']} on host: {db_config['host']}")

        # Validate database connection and schema
        if not args.skip_validation:
            logger.info("Validating database connection and schema")
            validate_database_connection(db_config)
        else:
            logger.warning("Database validation skipped")

        # Get batch size from environment variables or use default
        batch_size = int(os.getenv('BATCH_SIZE', 100))
        chunk_size = int(os.getenv('CHUNK_SIZE', 1000))
        memory_limit_mb = int(os.getenv('MEMORY_LIMIT_MB', 1024))

        # Create output directory if it doesn't exist
        output_dir = os.getenv('OUTPUT_DIR', 'output')
        os.makedirs(output_dir, exist_ok=True)

        # Validate output directory is writable
        if not os.access(output_dir, os.W_OK):
            raise ETLValidationError(f"Output directory is not writable: {output_dir}")

        # Create ETL context with Supabase configuration
        logger.info("Creating ETL context")
        context = ETLContext(
            db_config=db_config,
            output_dir=output_dir,
            memory_limit_mb=memory_limit_mb,
            parallel_processing=True,
            chunk_size=chunk_size,
            batch_size=batch_size,
            task_id=f"supabase-import-{Path(args.file).stem}"
        )

        # Create and run ETL pipeline
        logger.info(f"Starting ETL pipeline for file: {args.file}")
        pipeline = ETLPipeline(db_config=db_config, context=context)
        result = pipeline.run_pipeline(
            file_path=args.file,
            user_display_name=args.user_display_name
        )

        # Check result
        if result.get('success', False):
            logger.info(f"ETL pipeline completed successfully")
            logger.info(f"Export ID: {result.get('export_id')}")
            logger.info(f"Total duration: {result.get('total_duration_seconds', 0):.2f} seconds")

            # Log metrics if available
            metrics = result.get('metrics', {})
            if metrics:
                logger.info("ETL Pipeline Metrics:")
                for phase, phase_metrics in metrics.items():
                    logger.info(f"  {phase.upper()}:")
                    for metric, value in phase_metrics.items():
                        logger.info(f"    {metric}: {value}")
        else:
            logger.error(f"ETL pipeline failed: {result.get('error', 'Unknown error')}")
            sys.exit(1)

    except ETLValidationError as e:
        logger.error(f"Validation error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Error in ETL process: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()