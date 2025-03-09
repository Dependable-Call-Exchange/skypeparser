#!/usr/bin/env python3
"""
Skype Parser ETL Pipeline CLI

This script provides a command-line interface for running the Skype Parser ETL pipeline.
It allows users to extract, transform, and load Skype export data into a PostgreSQL database.

Usage:
    python scripts/run_etl_pipeline.py -f path/to/skype_export.tar -u "Your Name"
    python scripts/run_etl_pipeline.py -f path/to/skype_export.tar -u "Your Name" --resume
    python scripts/run_etl_pipeline.py -f path/to/skype_export.tar -u "Your Name" --parallel
"""

import os
import sys
import argparse
import logging
import json
from typing import Dict, Any, Optional
from pathlib import Path

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db.etl import ETLPipeline
from src.utils.config import load_config, get_db_config
from src.utils.validation import validate_skype_data

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('etl_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Run the Skype Parser ETL pipeline to process Skype export data',
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
    parser.add_argument('-p', '--parallel', action='store_true',
                        help='Enable parallel processing')
    parser.add_argument('-s', '--chunk-size', type=int, default=1000,
                        help='Chunk size for batch processing')
    parser.add_argument('-b', '--batch-size', type=int, default=100,
                        help='Batch size for database operations')
    parser.add_argument('-w', '--workers', type=int,
                        help='Maximum number of worker threads/processes')

    # Checkpoint options
    parser.add_argument('-r', '--resume', action='store_true',
                        help='Resume from the latest checkpoint')
    parser.add_argument('--checkpoint',
                        help='Resume from a specific checkpoint file')
    parser.add_argument('--list-checkpoints', action='store_true',
                        help='List available checkpoints and exit')

    # Attachment options
    attachment_group = parser.add_argument_group('Attachment Options')
    attachment_group.add_argument('--download-attachments', action='store_true',
                        help='Download attachments from URLs')
    attachment_group.add_argument('--attachments-dir',
                        help='Directory to store downloaded attachments (defaults to output_dir/attachments)')
    attachment_group.add_argument('--no-thumbnails', action='store_true',
                        help='Disable thumbnail generation for image attachments')
    attachment_group.add_argument('--no-metadata', action='store_true',
                        help='Disable metadata extraction from attachments')

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
        'parallel_processing': args.parallel,
        'chunk_size': args.chunk_size,
        'batch_size': args.batch_size,
        'max_workers': args.workers,
        'download_attachments': args.download_attachments,
        'attachments_dir': args.attachments_dir,
        'generate_thumbnails': not args.no_thumbnails,
        'extract_metadata': not args.no_metadata
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

    logger.info(f"Input file validated: {file_path}")


def list_available_checkpoints(pipeline: ETLPipeline) -> None:
    """List available checkpoints and exit.

    Args:
        pipeline: ETL pipeline instance
    """
    checkpoints = pipeline.get_available_checkpoints()

    if not checkpoints:
        logger.info("No checkpoints available")
        return

    logger.info(f"Available checkpoints ({len(checkpoints)}):")
    for i, checkpoint in enumerate(checkpoints, 1):
        checkpoint_path = Path(checkpoint)
        logger.info(f"{i}. {checkpoint_path.name} - {checkpoint_path.parent}")


def run_etl_pipeline(args: argparse.Namespace, config: Dict[str, Any]) -> Dict[str, Any]:
    """Run the ETL pipeline with the given arguments and configuration.

    Args:
        args: Command line arguments
        config: Configuration dictionary

    Returns:
        Dict containing the results of the ETL pipeline
    """
    # Create the ETL pipeline
    pipeline = ETLPipeline(**config)

    # List checkpoints if requested
    if args.list_checkpoints:
        list_available_checkpoints(pipeline)
        sys.exit(0)

    # Resume from checkpoint if requested
    if args.checkpoint:
        logger.info(f"Resuming from checkpoint: {args.checkpoint}")
        pipeline = ETLPipeline.load_from_checkpoint(
            checkpoint_file=args.checkpoint,
            db_config=config['db_config']
        )
    elif args.resume:
        # Get the latest checkpoint
        checkpoints = pipeline.get_available_checkpoints()
        if checkpoints:
            latest_checkpoint = checkpoints[-1]
            logger.info(f"Resuming from latest checkpoint: {latest_checkpoint}")
            pipeline = ETLPipeline.load_from_checkpoint(
                checkpoint_file=latest_checkpoint,
                db_config=config['db_config']
            )
        else:
            logger.warning("No checkpoints available to resume from")

    # Run the pipeline
    logger.info(f"Running ETL pipeline with file: {args.file}")
    result = pipeline.run_pipeline(
        file_path=args.file,
        user_display_name=args.user,
        resume_from_checkpoint=args.resume or args.checkpoint is not None
    )

    return result


def print_results(result: Dict[str, Any]) -> None:
    """Print the results of the ETL pipeline.

    Args:
        result: Results from the ETL pipeline
    """
    # Check if the pipeline completed successfully
    if result.get('status') == 'completed':
        logger.info("ETL pipeline completed successfully")
        logger.info(f"Export ID: {result.get('export_id')}")
        logger.info(f"Task ID: {result.get('task_id')}")

        # Print conversation and message counts
        logger.info(f"Processed {result.get('conversation_count', 0)} conversations")
        logger.info(f"Processed {result.get('message_count', 0)} messages")

        # Print phase statistics
        for phase_name, phase_data in result.get('phases', {}).items():
            logger.info(f"{phase_name.capitalize()} phase: {phase_data.get('status', 'unknown')}")

            # Print phase-specific statistics
            if phase_name == 'extract':
                logger.info(f"  Extracted {phase_data.get('conversation_count', 0)} conversations")
            elif phase_name == 'transform':
                logger.info(f"  Transformed {phase_data.get('processed_conversations', 0)} conversations")
                logger.info(f"  Transformed {phase_data.get('processed_messages', 0)} messages")
            elif phase_name == 'load':
                logger.info(f"  Export ID: {phase_data.get('export_id', 'unknown')}")
    else:
        logger.error(f"ETL pipeline failed: {result.get('error', 'Unknown error')}")

        # Print phase statuses
        for phase_name, phase_data in result.get('phases', {}).items():
            logger.info(f"{phase_name.capitalize()} phase: {phase_data.get('status', 'unknown')}")


def save_results(result: Dict[str, Any], output_dir: str) -> None:
    """Save the results of the ETL pipeline to a file.

    Args:
        result: Results from the ETL pipeline
        output_dir: Output directory
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Generate output file path
    output_file = os.path.join(
        output_dir,
        f"etl_result_{result.get('task_id', 'unknown')}.json"
    )

    # Save results to file
    try:
        with open(output_file, 'w') as f:
            json.dump(result, f, indent=2)
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

        # Run the ETL pipeline
        result = run_etl_pipeline(args, config)

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
        logger.exception(f"Error running ETL pipeline: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()