#!/usr/bin/env python3
"""
Supabase ETL Pipeline Runner

This script runs the ETL pipeline to process Skype export data and store it in a Supabase PostgreSQL database.
It uses the configuration from config/config.json which has been updated to use Supabase connection details.

Usage:
    python scripts/run_supabase_etl.py -f <skype_export_file> -u <your_display_name> [options]

Example:
    python scripts/run_supabase_etl.py -f 8_live_dave.leathers113_export.tar -u "David Leathers" --verbose
"""

import os
import sys
import json
import logging
import argparse
import traceback
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import project modules
from src.utils.config import load_config
from src.utils.di import get_service, get_service_provider
from src.utils.interfaces import FileHandlerProtocol
from src.utils.service_registry import register_etl_services, register_core_services
from src.db.etl.context import ETLContext
from src.db.etl.pipeline_manager import ETLPipeline

# Configure logging
def setup_logging(verbose: bool = False, debug: bool = False) -> None:
    """Set up logging configuration.

    Args:
        verbose: Enable verbose logging
        debug: Enable debug logging
    """
    log_level = logging.DEBUG if debug else (logging.INFO if verbose else logging.WARNING)
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'

    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)

    # Generate log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = f'logs/supabase_etl_{timestamp}.log'

    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized at level: {logging.getLevelName(log_level)}")
    logger.info(f"Log file: {log_file}")

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description='Process Skype export data and store it in Supabase PostgreSQL',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Required arguments
    parser.add_argument('-f', '--file', required=True,
                        help='Path to the Skype export file (TAR or JSON)')
    parser.add_argument('-u', '--user', required=True,
                        help='Your display name as it appears in Skype')

    # Optional arguments
    parser.add_argument('-c', '--config', default='config/config.json',
                        help='Path to the configuration file')
    parser.add_argument('-o', '--output', default='output',
                        help='Output directory for intermediate files')
    parser.add_argument('-b', '--batch-size', type=int,
                        help='Batch size for database operations')
    parser.add_argument('-s', '--chunk-size', type=int,
                        help='Chunk size for processing large files')
    parser.add_argument('-m', '--memory-limit', type=int,
                        help='Memory limit in MB')
    parser.add_argument('-p', '--parallel', action='store_true',
                        help='Enable parallel processing')

    # Logging options
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Enable verbose logging')
    parser.add_argument('-d', '--debug', action='store_true',
                        help='Enable debug logging')

    # Other options
    parser.add_argument('--skip-validation', action='store_true',
                        help='Skip database validation')
    parser.add_argument('--create-tables', action='store_true',
                        help='Create database tables if they do not exist')

    return parser.parse_args()

def validate_input_file(file_path: str) -> None:
    """Validate that the input file exists and has the correct extension.

    Args:
        file_path: Path to the input file

    Raises:
        FileNotFoundError: If the file does not exist
        ValueError: If the file has an invalid extension
    """
    logger = logging.getLogger(__name__)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")

    file_ext = os.path.splitext(file_path)[1].lower()
    if file_ext not in ['.tar', '.json']:
        logger.warning(f"Unexpected file extension: {file_ext}. Expected .tar or .json")

    logger.info(f"Input file validated: {file_path}")

def load_configuration(config_file: str, args: argparse.Namespace) -> Dict[str, Any]:
    """Load configuration from file and override with command line arguments.

    Args:
        config_file: Path to the configuration file
        args: Command line arguments

    Returns:
        Configuration dictionary
    """
    logger = logging.getLogger(__name__)

    try:
        # Load configuration from file
        config = load_config(config_file)
        logger.info(f"Loaded configuration from {config_file}")

        # Extract database configuration
        db_config = config.get('database', {})

        # Override with command line arguments
        etl_config = {
            'db_config': db_config,
            'output_dir': args.output,
            'batch_size': args.batch_size or config.get('batch_size', 100),
            'chunk_size': args.chunk_size or config.get('chunk_size', 1000),
            'memory_limit_mb': args.memory_limit or config.get('memory_limit_mb', 1024),
            'parallel_processing': args.parallel or config.get('parallel_processing', False),
            'create_tables': args.create_tables
        }

        return etl_config

    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise

def main() -> None:
    """Main function to run the ETL pipeline."""
    try:
        # Parse command line arguments
        args = parse_arguments()

        # Set up logging
        setup_logging(verbose=args.verbose, debug=args.debug)
        logger = logging.getLogger(__name__)

        # Validate input file
        validate_input_file(args.file)

        # Load configuration
        config = load_configuration(args.config, args)

        # Create output directory if it doesn't exist
        os.makedirs(config['output_dir'], exist_ok=True)

        # Create ETL context
        logger.info("Creating ETL context")
        context = ETLContext(
            db_config=config['db_config'],
            output_dir=config['output_dir'],
            batch_size=config['batch_size'],
            chunk_size=config['chunk_size'],
            memory_limit_mb=config['memory_limit_mb'],
            parallel_processing=config['parallel_processing']
        )

        # Register services
        logger.info("Registering ETL services")
        provider = get_service_provider()
        register_core_services(provider=provider)
        register_etl_services(
            db_config=config['db_config'],
            output_dir=config['output_dir'],
            provider=provider
        )

        # Create and run ETL pipeline
        logger.info("Creating ETL pipeline")
        pipeline = ETLPipeline(
            db_config=config['db_config'],
            context=context
        )

        logger.info(f"Running ETL pipeline on file: {args.file}")
        result = pipeline.run_pipeline(
            file_path=args.file,
            user_display_name=args.user
        )

        # Log results
        logger.info("ETL pipeline completed successfully")
        logger.info(f"Processed {result.get('conversation_count', 0)} conversations")
        logger.info(f"Processed {result.get('message_count', 0)} messages")
        logger.info(f"Export ID: {result.get('export_id', 'unknown')}")

        # Print summary to console
        print("\nETL Pipeline Summary:")
        print(f"Processed {result.get('conversation_count', 0):,} conversations")
        print(f"Processed {result.get('message_count', 0):,} messages")
        print(f"Export ID: {result.get('export_id', 'unknown')}")
        print(f"Data stored in Supabase PostgreSQL database: {config['db_config']['host']}")

    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        print(f"\nError: {e}")
        sys.exit(1)

    except ValueError as e:
        logger.error(f"Invalid input: {e}")
        print(f"\nError: {e}")
        sys.exit(1)

    except KeyboardInterrupt:
        logger.warning("Process interrupted by user")
        print("\nProcess interrupted by user")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        logger.debug(traceback.format_exc())
        print(f"\nError: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()