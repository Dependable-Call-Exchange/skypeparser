#!/usr/bin/env python3
"""
Supabase PostgreSQL Connection Example

This script demonstrates how to connect to a Supabase PostgreSQL database
using the ETL pipeline. It loads a Skype export file and stores the data
in Supabase.

Usage:
    python supabase_connection.py -f <skype_export_file> -u <your_display_name>
                                 [--config <config_file>]

Example:
    python supabase_connection.py -f export.tar -u "John Doe" --config ../config/supabase.json
"""

import os
import sys
import json
import argparse
import logging
from pathlib import Path

# Add the parent directory to the path so we can import the modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.db.etl.pipeline import ETLPipeline
from src.db.etl.context import ETLContext

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('supabase_import.log')
    ]
)
logger = logging.getLogger('supabase-connection')

def load_config(config_file: str) -> dict:
    """
    Load configuration from a JSON file.

    Args:
        config_file: Path to the JSON config file

    Returns:
        dict: Configuration parameters
    """
    if not os.path.exists(config_file):
        logger.error(f"Config file not found: {config_file}")
        sys.exit(1)

    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing config file: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error loading config file: {e}")
        sys.exit(1)

def get_commandline_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Connect to Supabase PostgreSQL and import Skype data',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # File input options
    parser.add_argument('-f', '--filename', required=True,
                       help='Path to the Skype export file (TAR or JSON)')
    parser.add_argument('-u', '--user-display-name', required=True,
                       help='Your display name in the Skype logs')

    # Configuration options
    parser.add_argument('--config', default='../config/supabase.json',
                       help='Path to Supabase configuration file')

    return parser.parse_args()

def main():
    """Main function to connect to Supabase and import Skype data."""
    try:
        args = get_commandline_args()

        # Load configuration
        config = load_config(args.config)

        # Extract database configuration
        db_config = config.get('database', {})

        # Validate that we have the required configuration
        required_fields = ['host', 'port', 'dbname', 'user', 'password']
        missing_fields = [f for f in required_fields if f not in db_config]
        if missing_fields:
            logger.error(f"Missing required database configuration fields: {missing_fields}")
            sys.exit(1)

        # Ensure SSL mode is set for Supabase
        if 'sslmode' not in db_config:
            logger.warning("SSL mode not specified in config, setting to 'require' for Supabase")
            db_config['sslmode'] = 'require'

        # Validate that the input file exists
        input_file = Path(args.filename)
        if not input_file.exists():
            logger.error(f"File not found: {input_file}")
            sys.exit(1)

        # Create ETL context
        context = ETLContext(
            db_config=db_config,
            batch_size=config.get('batch_size', 100),
            checkpoint_interval=config.get('checkpoint_interval', 1000)
        )

        # Create and run ETL pipeline
        pipeline = ETLPipeline(context)

        logger.info(f"Starting ETL pipeline with file: {input_file}")
        logger.info(f"Connecting to Supabase PostgreSQL at: {db_config['host']}")

        # Run the pipeline
        result = pipeline.run(
            input_file=str(input_file),
            user_display_name=args.user_display_name
        )

        if result:
            logger.info(f"Successfully imported data to Supabase PostgreSQL")
            logger.info(f"Processed {result.get('conversations', 0)} conversations with {result.get('messages', 0)} messages")
        else:
            logger.error("Failed to import data")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()