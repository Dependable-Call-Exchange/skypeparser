#!/usr/bin/env python3
"""
Streaming ETL Pipeline Script

This script provides a command-line interface for running the Skype Parser ETL pipeline
with streaming processing, optimized for very large datasets (millions of messages).
"""

import os
import sys
import argparse
import logging
import json
import time
from typing import Dict, Any, Optional
from datetime import datetime

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db.etl import ETLContext
from src.db.etl.streaming_processor import StreamingProcessor
from src.utils.config import load_config, get_db_config
from src.utils.di import get_service
from src.utils.interfaces import DatabaseConnectionProtocol

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('streaming_etl.log')
    ]
)
logger = logging.getLogger(__name__)

def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Run the Skype Parser ETL pipeline with streaming processing for very large datasets',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Input file options
    parser.add_argument('-f', '--file', required=True,
                        help='Path to the Skype export file (.tar or .json)')
    parser.add_argument('-u', '--user', default='',
                        help='User display name for the export')

    # Configuration options
    parser.add_argument('-c', '--config', default='config/config.json',
                        help='Path to the configuration file')
    parser.add_argument('-o', '--output', default='output',
                        help='Output directory for intermediate files')

    # Database options
    parser.add_argument('--db-name',
                        help='Database name (overrides config file)')
    parser.add_argument('--db-user',
                        help='Database user (overrides config file)')
    parser.add_argument('--db-password',
                        help='Database password (overrides config file)')
    parser.add_argument('--db-host',
                        help='Database host (overrides config file)')
    parser.add_argument('--db-port', type=int,
                        help='Database port (overrides config file)')

    # Performance options
    parser.add_argument('-m', '--memory', type=int, default=1024,
                        help='Memory limit in MB')
    parser.add_argument('-b', '--batch-size', type=int, default=1000,
                        help='Batch size for processing and database operations')
    parser.add_argument('--checkpoint-interval', type=int, default=10000,
                        help='Number of messages to process before creating a checkpoint')

    # Debug options
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Enable verbose logging')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Enable debug logging')

    return parser.parse_args()

def setup_logging(args: argparse.Namespace) -> None:
    """Set up logging based on command line arguments.

    Args:
        args: Command line arguments
    """
    log_level = logging.INFO
    if args.debug:
        log_level = logging.DEBUG
    elif args.verbose:
        log_level = logging.INFO

    logging.getLogger().setLevel(log_level)
    logger.info(f"Log level set to: {logging.getLevelName(log_level)}")

def get_config(args: argparse.Namespace) -> Dict[str, Any]:
    """Load configuration from file and override with command line arguments.

    Args:
        args: Command line arguments

    Returns:
        Dict containing configuration
    """
    # Load configuration from file
    try:
        config = load_config(args.config)
        logger.info(f"Loaded configuration from {args.config}")
    except Exception as e:
        logger.warning(f"Failed to load configuration from {args.config}: {e}")
        config = {}

    # Get database configuration
    db_config = get_db_config(config)

    # Override with command line arguments
    if args.db_name:
        db_config['dbname'] = args.db_name
    if args.db_user:
        db_config['user'] = args.db_user
    if args.db_password:
        db_config['password'] = args.db_password
    if args.db_host:
        db_config['host'] = args.db_host
    if args.db_port:
        db_config['port'] = args.db_port

    # Ensure we have a valid database configuration
    required_fields = ['dbname', 'user']
    missing_fields = [f for f in required_fields if f not in db_config]
    if missing_fields:
        logger.error(f"Missing required database configuration fields: {missing_fields}")
        logger.error("Please provide these fields in the config file or as command line arguments")
        sys.exit(1)

    return {
        'db_config': db_config,
        'output_dir': args.output,
        'memory_limit_mb': args.memory,
        'batch_size': args.batch_size,
        'checkpoint_interval': args.checkpoint_interval
    }

def validate_input_file(file_path: str) -> None:
    """Validate that the input file exists and is a valid Skype export file.

    Args:
        file_path: Path to the input file

    Raises:
        FileNotFoundError: If the file does not exist
        ValueError: If the file is not a valid Skype export file
    """
    # Check that the file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")

    # Check file extension
    file_ext = os.path.splitext(file_path)[1].lower()
    if file_ext not in ['.tar', '.json']:
        logger.warning(f"Unexpected file extension: {file_ext}. Expected .tar or .json")

    # Check file size
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    logger.info(f"Input file size: {file_size_mb:.2f} MB")

    logger.info(f"Input file validated: {file_path}")

def run_streaming_etl(args: argparse.Namespace, config: Dict[str, Any]) -> Dict[str, Any]:
    """Run the streaming ETL pipeline.

    Args:
        args: Command line arguments
        config: Configuration dictionary

    Returns:
        Dict containing the results of the ETL pipeline
    """
    logger.info("Starting streaming ETL pipeline")
    start_time = time.time()

    # Create ETL context
    context = ETLContext(
        db_config=config['db_config'],
        output_dir=config['output_dir'],
        memory_limit_mb=config['memory_limit_mb'],
        user_display_name=args.user
    )

    # Get database connection
    db_connection = get_service(DatabaseConnectionProtocol)

    # Create streaming processor
    processor = StreamingProcessor(context=context, db_connection=db_connection)

    # Stream extract
    conversation_iterator = processor.stream_extract(args.file)

    # Stream transform and load
    result = processor.stream_transform_load(
        conversation_iterator=conversation_iterator,
        batch_size=config['batch_size']
    )

    # Add execution time to result
    end_time = time.time()
    execution_time = end_time - start_time
    result['execution_time_seconds'] = execution_time
    result['execution_time_formatted'] = str(datetime.timedelta(seconds=int(execution_time)))

    return result

def print_results(result: Dict[str, Any]) -> None:
    """Print the results of the ETL pipeline.

    Args:
        result: Results from the ETL pipeline
    """
    logger.info("Streaming ETL pipeline completed successfully")
    logger.info(f"Execution time: {result.get('execution_time_formatted')}")
    logger.info(f"Processed {result.get('conversations_processed', 0)} conversations")
    logger.info(f"Processed {result.get('messages_processed', 0)} messages")
    logger.info(f"Processing rate: {result.get('messages_processed', 0) / result.get('duration_seconds', 1):.2f} messages per second")

def save_results(result: Dict[str, Any], output_dir: str) -> None:
    """Save the results of the ETL pipeline to a file.

    Args:
        result: Results from the ETL pipeline
        output_dir: Output directory
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Generate output file path
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = os.path.join(
        output_dir,
        f"streaming_etl_result_{timestamp}.json"
    )

    # Save results to file
    try:
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2, default=str)
        logger.info(f"Results saved to: {output_file}")
    except Exception as e:
        logger.error(f"Failed to save results to {output_file}: {e}")

def main():
    """Main function."""
    # Parse command line arguments
    args = parse_args()

    # Set up logging
    setup_logging(args)

    try:
        # Validate input file
        validate_input_file(args.file)

        # Get configuration
        config = get_config(args)

        # Run the streaming ETL pipeline
        result = run_streaming_etl(args, config)

        # Print the results
        print_results(result)

        # Save the results
        save_results(result, config['output_dir'])

        # Exit with success
        sys.exit(0)
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"Invalid input: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Error running streaming ETL pipeline: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()