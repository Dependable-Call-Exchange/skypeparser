#!/usr/bin/env python3
"""
Test script for the modular ETL pipeline.

This script tests the basic functionality of the modular ETL pipeline
by processing a sample Skype export file.
"""

import os
import sys
import logging
import argparse
from typing import Dict, Any

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db.etl import ETLPipeline

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Test the modular ETL pipeline')
    parser.add_argument('-f', '--file', required=True, help='Path to the Skype export file')
    parser.add_argument('-u', '--user', default='Test User', help='User display name')
    parser.add_argument('-d', '--dbname', default='skype_logs', help='Database name')
    parser.add_argument('-U', '--dbuser', default='postgres', help='Database user')
    parser.add_argument('-P', '--dbpass', default='', help='Database password')
    parser.add_argument('-H', '--dbhost', default='localhost', help='Database host')
    parser.add_argument('-p', '--dbport', default=5432, type=int, help='Database port')
    parser.add_argument('-o', '--output', default='output', help='Output directory')
    return parser.parse_args()

def test_etl_pipeline(args) -> Dict[str, Any]:
    """Test the ETL pipeline with the given arguments.

    Args:
        args: Command line arguments

    Returns:
        Dict containing the results of the ETL pipeline
    """
    # Create database configuration
    db_config = {
        'dbname': args.dbname,
        'user': args.dbuser,
        'password': args.dbpass,
        'host': args.dbhost,
        'port': args.dbport
    }

    # Create the ETL pipeline
    pipeline = ETLPipeline(
        db_config=db_config,
        output_dir=args.output
    )

    # Run the pipeline
    logger.info(f"Running ETL pipeline with file: {args.file}")
    results = pipeline.run_pipeline(
        file_path=args.file,
        user_display_name=args.user
    )

    return results

def main():
    """Main function."""
    args = parse_args()

    try:
        # Test the ETL pipeline
        results = test_etl_pipeline(args)

        # Print the results
        if results['success']:
            logger.info(f"ETL pipeline completed successfully")
            logger.info(f"Export ID: {results['export_id']}")

            # Print phase statistics
            for phase, stats in results['phases'].items():
                if stats:
                    logger.info(f"{phase.capitalize()} phase statistics:")
                    logger.info(f"  Duration: {stats.get('duration_seconds', 0):.2f} seconds")
                    logger.info(f"  Messages: {stats.get('processed_messages', 0)}")
                    logger.info(f"  Conversations: {stats.get('processed_conversations', 0)}")
                    logger.info(f"  Messages per second: {stats.get('messages_per_second', 0):.2f}")
        else:
            logger.error(f"ETL pipeline failed: {results.get('error', 'Unknown error')}")

    except Exception as e:
        logger.exception(f"Error testing ETL pipeline: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()