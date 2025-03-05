#!/usr/bin/env python3
"""
Dependency Injection Example

This script demonstrates how to use the dependency injection framework
to manage dependencies in the Skype Parser project.
"""

import os
import logging
import argparse
from typing import Dict, Any

from src.utils.di import get_service_provider, get_service
from src.utils.service_registry import register_all_services
from src.utils.interfaces import (
    FileHandlerProtocol,
    DatabaseConnectionProtocol,
    ExtractorProtocol,
    TransformerProtocol,
    LoaderProtocol
)
from src.db.etl.context import ETLContext

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Dependency Injection Example')

    parser.add_argument('--input-file', type=str, required=True,
                        help='Path to the input Skype export file (JSON or TAR)')

    parser.add_argument('--output-dir', type=str, default='output',
                        help='Directory to store output files')

    parser.add_argument('--db-host', type=str, default='localhost',
                        help='Database host')

    parser.add_argument('--db-name', type=str, default='skype_parser',
                        help='Database name')

    parser.add_argument('--db-user', type=str, default='postgres',
                        help='Database user')

    parser.add_argument('--db-password', type=str, default='postgres',
                        help='Database password')

    parser.add_argument('--db-port', type=int, default=5432,
                        help='Database port')

    return parser.parse_args()


def create_db_config(args) -> Dict[str, Any]:
    """Create database configuration from arguments."""
    return {
        'host': args.db_host,
        'database': args.db_name,
        'user': args.db_user,
        'password': args.db_password,
        'port': args.db_port
    }


def main():
    """Main function."""
    # Parse arguments
    args = parse_arguments()

    # Create database configuration
    db_config = create_db_config(args)

    # Create output directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    # Register all services with the dependency injection container
    register_all_services(db_config=db_config, output_dir=args.output_dir)

    # Get services from the container
    file_handler = get_service(FileHandlerProtocol)
    db_connection = get_service(DatabaseConnectionProtocol)
    extractor = get_service(ExtractorProtocol)
    transformer = get_service(TransformerProtocol)
    loader = get_service(LoaderProtocol)
    context = get_service(ETLContext)

    # Log the services we got
    logger.info(f"File handler: {file_handler}")
    logger.info(f"Database connection: {db_connection}")
    logger.info(f"Extractor: {extractor}")
    logger.info(f"Transformer: {transformer}")
    logger.info(f"Loader: {loader}")
    logger.info(f"Context: {context}")

    # Use the file handler to read the input file
    logger.info(f"Reading input file: {args.input_file}")
    try:
        data = file_handler.read_file(args.input_file)
        logger.info(f"Successfully read input file with {len(data)} top-level keys")
    except Exception as e:
        logger.error(f"Error reading input file: {e}")
        return

    # Connect to the database
    logger.info("Connecting to database")
    try:
        db_connection.connect()
        logger.info("Successfully connected to database")
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        return

    # Run a simple ETL process
    try:
        # Extract
        logger.info("Extracting data")
        extracted_data = extractor.extract(args.input_file)
        logger.info(f"Extracted {len(extracted_data)} records")

        # Transform
        logger.info("Transforming data")
        transformed_data = transformer.transform(extracted_data)
        logger.info(f"Transformed data into {len(transformed_data)} records")

        # Load
        logger.info("Loading data")
        loader.load(transformed_data)
        logger.info("Data loaded successfully")

    except Exception as e:
        logger.error(f"Error in ETL process: {e}")
    finally:
        # Disconnect from the database
        db_connection.disconnect()
        logger.info("Disconnected from database")


if __name__ == '__main__':
    main()