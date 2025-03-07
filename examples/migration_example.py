#!/usr/bin/env python3
"""
Migration Example

This example demonstrates how to migrate from the old SkypeETLPipeline
to the new modular ETL pipeline.
"""

import os
import logging
import argparse
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def run_old_pipeline(file_path: str, user_display_name: str, db_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run the old ETL pipeline.

    Args:
        file_path: Path to the Skype export file
        user_display_name: Display name of the user
        db_config: Database configuration

    Returns:
        Dict containing the results of the ETL pipeline
    """
    # Import the old ETL pipeline
    from src.db.etl import ETLPipeline

    # Initialize the pipeline
    pipeline = ETLPipeline(
        db_name=db_config.get('dbname'),
        db_user=db_config.get('user'),
        db_password=db_config.get('password'),
        db_host=db_config.get('host', 'localhost'),
        db_port=db_config.get('port', 5432),
        use_di=False
    )

    # Run the pipeline
    results = pipeline.run_pipeline(
        file_path=file_path,
        user_display_name=user_display_name
    )

    # Log the results
    logger.info(f"Old Pipeline - Processed {results.get('message_count', 0)} messages in {len(results.get('conversations', []))} conversations")

    return results

def run_new_pipeline(file_path: str, user_display_name: str, db_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run the new modular ETL pipeline.

    Args:
        file_path: Path to the Skype export file
        user_display_name: Display name of the user
        db_config: Database configuration

    Returns:
        Dict containing the results of the ETL pipeline
    """
    # Import the new ETL pipeline
    from src.db import ETLPipeline

    # Initialize the pipeline
    pipeline = ETLPipeline(
        db_config=db_config,
        output_dir='output'
    )

    # Run the pipeline
    results = pipeline.run_pipeline(
        file_path=file_path,
        user_display_name=user_display_name
    )

    # Log the results
    message_count = results.get('phases', {}).get('transform', {}).get('processed_messages', 0)
    conversation_count = results.get('phases', {}).get('transform', {}).get('processed_conversations', 0)
    logger.info(f"New Pipeline - Processed {message_count} messages in {conversation_count} conversations")

    return results

def run_new_pipeline_advanced(file_path: str, user_display_name: str, db_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Run the new modular ETL pipeline with advanced configuration.

    Args:
        file_path: Path to the Skype export file
        user_display_name: Display name of the user
        db_config: Database configuration

    Returns:
        Dict containing the results of the ETL pipeline
    """
    # Import the new ETL pipeline components
    from src.db import ETLContext, Extractor, Transformer, Loader

    # Create the context
    context = ETLContext(
        db_config=db_config,
        output_dir='output',
        memory_limit_mb=2048,
        parallel_processing=True,
        chunk_size=2000,
        batch_size=500,
        max_workers=None
    )

    # Create the components
    extractor = Extractor(context=context)
    transformer = Transformer(context=context)
    loader = Loader(context=context)

    # Extract data
    logger.info("Extracting data...")
    raw_data = extractor.extract(file_path=file_path)

    # Transform data
    logger.info("Transforming data...")
    transformed_data = transformer.transform(raw_data, user_display_name=user_display_name)

    # Load data
    logger.info("Loading data...")
    loader.connect_db()
    try:
        export_id = loader.load(raw_data, transformed_data, file_path)
        logger.info(f"Data loaded with export ID: {export_id}")
    finally:
        loader.close_db()

    # Get the context summary
    results = context.get_summary()

    # Log the results
    message_count = results.get('phases', {}).get('transform', {}).get('processed_messages', 0)
    conversation_count = results.get('phases', {}).get('transform', {}).get('processed_conversations', 0)
    logger.info(f"Advanced Pipeline - Processed {message_count} messages in {conversation_count} conversations")

    return results

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Migration example')
    parser.add_argument('file_path', help='Path to the Skype export file')
    parser.add_argument('--user', help='Display name of the user', default='User')
    parser.add_argument('--db-name', help='Database name', default='skype_logs')
    parser.add_argument('--db-user', help='Database user', default='postgres')
    parser.add_argument('--db-password', help='Database password')
    parser.add_argument('--db-host', help='Database host', default='localhost')
    parser.add_argument('--db-port', help='Database port', type=int, default=5432)
    parser.add_argument('--mode', help='Pipeline mode (old, new, advanced)', default='new')
    args = parser.parse_args()

    # Create database configuration
    db_config = {
        'dbname': args.db_name,
        'user': args.db_user,
        'password': args.db_password,
        'host': args.db_host,
        'port': args.db_port
    }

    # Run the appropriate pipeline
    if args.mode == 'old':
        logger.info("Running the old ETL pipeline...")
        run_old_pipeline(args.file_path, args.user, db_config)
    elif args.mode == 'new':
        logger.info("Running the new modular ETL pipeline...")
        run_new_pipeline(args.file_path, args.user, db_config)
    elif args.mode == 'advanced':
        logger.info("Running the new modular ETL pipeline with advanced configuration...")
        run_new_pipeline_advanced(args.file_path, args.user, db_config)
    else:
        logger.error(f"Invalid mode: {args.mode}")
        return

    logger.info("Done!")

if __name__ == '__main__':
    main()