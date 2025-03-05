#!/usr/bin/env python3
"""
Example usage of the modular ETL pipeline.

This script demonstrates how to use the modular ETL pipeline to process
Skype export data and load it into a PostgreSQL database.
"""

import os
import sys
import argparse
import logging
from typing import Dict, Any

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db.etl import ETLPipeline
from src.utils.config import load_config, get_db_config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Process Skype export data using the modular ETL pipeline')
    parser.add_argument('-f', '--file', required=True, help='Path to the Skype export file')
    parser.add_argument('-u', '--user', default='', help='User display name')
    parser.add_argument('-c', '--config', default='config/config.json', help='Path to the configuration file')
    parser.add_argument('-o', '--output', default='output', help='Output directory')
    parser.add_argument('-m', '--memory', type=int, default=1024, help='Memory limit in MB')
    parser.add_argument('-p', '--parallel', action='store_true', help='Enable parallel processing')
    parser.add_argument('-s', '--chunk-size', type=int, default=1000, help='Chunk size for batch processing')
    return parser.parse_args()


def get_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from a file.

    Args:
        config_path: Path to the configuration file

    Returns:
        Dict containing configuration
    """
    try:
        config = load_config(config_path)
        return config
    except Exception as e:
        logger.warning(f"Failed to load configuration from {config_path}: {e}")
        return {}


def run_etl_pipeline(args: argparse.Namespace) -> Dict[str, Any]:
    """Run the ETL pipeline with the given arguments.

    Args:
        args: Command line arguments

    Returns:
        Dict containing the results of the ETL pipeline
    """
    # Load configuration
    config = get_config(args.config)

    # Get database configuration
    db_config = get_db_config(config)

    # Create the ETL pipeline
    pipeline = ETLPipeline(
        db_config=db_config,
        output_dir=args.output,
        memory_limit_mb=args.memory,
        parallel_processing=args.parallel,
        chunk_size=args.chunk_size
    )

    # Run the pipeline
    logger.info(f"Running ETL pipeline with file: {args.file}")
    result = pipeline.run_pipeline(
        file_path=args.file,
        user_display_name=args.user
    )

    return result


def print_results(result: Dict[str, Any]) -> None:
    """Print the results of the ETL pipeline.

    Args:
        result: Results from the ETL pipeline
    """
    if result['success']:
        logger.info(f"ETL pipeline completed successfully")
        logger.info(f"Export ID: {result['export_id']}")

        # Print phase statistics
        for phase, stats in result['phases'].items():
            if stats:
                logger.info(f"{phase.capitalize()} phase statistics:")
                logger.info(f"  Duration: {stats.get('duration_seconds', 0):.2f} seconds")
                logger.info(f"  Messages: {stats.get('processed_messages', 0)}")
                logger.info(f"  Conversations: {stats.get('processed_conversations', 0)}")
                logger.info(f"  Messages per second: {stats.get('messages_per_second', 0):.2f}")
    else:
        logger.error(f"ETL pipeline failed: {result.get('error', 'Unknown error')}")


def main():
    """Main function."""
    args = parse_args()

    try:
        # Run the ETL pipeline
        result = run_etl_pipeline(args)

        # Print the results
        print_results(result)

    except Exception as e:
        logger.exception(f"Error running ETL pipeline: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()