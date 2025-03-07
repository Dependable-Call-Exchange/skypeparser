#!/usr/bin/env python3
"""
Enhanced ETL Pipeline Runner for Skype Export Data

This script provides a command-line interface for running the ETL pipeline
on Skype export data. It replaces the deprecated skype_to_postgres.py and
store_skype_export.py scripts with a unified interface.

Usage:
    python scripts/run_etl_pipeline_enhanced.py -f <skype_export_file> -u <your_display_name>
                                              [-d <database_name>] [-H <host>] [-P <port>]
                                              [-U <username>] [-W <password>] [--create-tables]
                                              [--output-dir <output_dir>] [--parallel]
                                              [--memory-limit <memory_limit_mb>]
                                              [--batch-size <batch_size>] [--chunk-size <chunk_size>]
                                              [--resume] [--checkpoint <checkpoint_id>]
                                              [--download-attachments] [--attachments-dir <dir>]
                                              [--no-thumbnails] [--config <config_file>]
                                              [--non-interactive] [--verbose]

Examples:
    # Basic usage
    python scripts/run_etl_pipeline_enhanced.py -f 8_live_dave.leathers113_export.tar -u "David Leathers"

    # With database options
    python scripts/run_etl_pipeline_enhanced.py -f 8_live_dave.leathers113_export.tar -u "David Leathers" \\
                                              -d skype_logs -U postgres -W mypassword --create-tables

    # With performance options
    python scripts/run_etl_pipeline_enhanced.py -f 8_live_dave.leathers113_export.tar -u "David Leathers" \\
                                              --parallel --memory-limit 2048 --batch-size 500

    # Resume from checkpoint
    python scripts/run_etl_pipeline_enhanced.py --resume --checkpoint checkpoint_id

    # Download attachments
    python scripts/run_etl_pipeline_enhanced.py -f 8_live_dave.leathers113_export.tar -u "David Leathers" \\
                                              --download-attachments --attachments-dir ./attachments
"""

import os
import sys
import json
import argparse
import logging
import datetime
from typing import Dict, Any, Optional, List
from pathlib import Path

# Add the parent directory to the path so we can import from src
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the modular ETL pipeline
from src.db.etl import ETLPipeline, ETLContext
from src.utils.config import load_config, get_db_config
from src.utils.attachment_handler import AttachmentHandler

# Configure logging
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

def parse_args():
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(description='Run the ETL pipeline on Skype export data')

    # Input file options
    file_group = parser.add_argument_group('Input File Options')
    file_group.add_argument('-f', '--file', dest='file_path', help='Path to the Skype export file (TAR or JSON)')

    # User options
    user_group = parser.add_argument_group('User Options')
    user_group.add_argument('-u', '--user', dest='user_display_name', help='Your display name in the Skype conversations')

    # Database options
    db_group = parser.add_argument_group('Database Options')
    db_group.add_argument('-d', '--database', dest='db_name', help='Database name')
    db_group.add_argument('-H', '--host', dest='db_host', default='localhost', help='Database host (default: localhost)')
    db_group.add_argument('-P', '--port', dest='db_port', type=int, default=5432, help='Database port (default: 5432)')
    db_group.add_argument('-U', '--username', dest='db_user', help='Database username')
    db_group.add_argument('-W', '--password', dest='db_password', help='Database password')
    db_group.add_argument('--create-tables', dest='create_tables', action='store_true', help='Create database tables if they don\'t exist')

    # Output options
    output_group = parser.add_argument_group('Output Options')
    output_group.add_argument('--output-dir', dest='output_dir', help='Directory to store output files')

    # Performance options
    perf_group = parser.add_argument_group('Performance Options')
    perf_group.add_argument('--parallel', dest='parallel', action='store_true', help='Enable parallel processing')
    perf_group.add_argument('--memory-limit', dest='memory_limit', type=int, default=1024, help='Memory limit in MB (default: 1024)')
    perf_group.add_argument('--batch-size', dest='batch_size', type=int, default=500, help='Database batch size (default: 500)')
    perf_group.add_argument('--chunk-size', dest='chunk_size', type=int, default=1000, help='Processing chunk size (default: 1000)')
    perf_group.add_argument('--max-workers', dest='max_workers', type=int, help='Maximum number of worker threads (default: CPU count)')

    # Checkpoint options
    checkpoint_group = parser.add_argument_group('Checkpoint Options')
    checkpoint_group.add_argument('--resume', dest='resume', action='store_true', help='Resume from the latest checkpoint')
    checkpoint_group.add_argument('--checkpoint', dest='checkpoint_id', help='Checkpoint ID to resume from')

    # Attachment options
    attachment_group = parser.add_argument_group('Attachment Options')
    attachment_group.add_argument('--download-attachments', dest='download_attachments', action='store_true', help='Download attachments from messages')
    attachment_group.add_argument('--attachments-dir', dest='attachments_dir', help='Directory to store attachments (default: ./attachments)')
    attachment_group.add_argument('--no-thumbnails', dest='no_thumbnails', action='store_true', help='Skip downloading thumbnails')

    # Configuration options
    config_group = parser.add_argument_group('Configuration Options')
    config_group.add_argument('--config', dest='config_file', help='Path to configuration file')
    config_group.add_argument('--non-interactive', dest='non_interactive', action='store_true', help='Run in non-interactive mode')
    config_group.add_argument('--verbose', dest='verbose', action='store_true', help='Enable verbose logging')

    # For backward compatibility with skype_to_postgres.py and store_skype_export.py
    parser.add_argument('--select-json', dest='select_json', help='Path to JSON file with conversation IDs to select')

    return parser.parse_args()

def get_db_config_from_args(args):
    """
    Get database configuration from command-line arguments.

    Args:
        args (argparse.Namespace): Parsed command-line arguments

    Returns:
        dict: Database configuration dictionary
    """
    db_config = {
        'host': args.db_host,
        'port': args.db_port,
        'dbname': args.db_name or 'skype_archive',
        'user': args.db_user or 'postgres',
        'password': args.db_password or ''
    }
    return db_config

def get_etl_context(args, config):
    """
    Create an ETL context from command-line arguments and configuration.

    Args:
        args (argparse.Namespace): Parsed command-line arguments
        config (dict): Configuration dictionary

    Returns:
        ETLContext: ETL context object
    """
    # Get database configuration
    db_config = get_db_config(config)

    # Override with command-line arguments if provided
    if args.db_name or args.db_host or args.db_port or args.db_user or args.db_password:
        db_config = get_db_config_from_args(args)

    # Create ETL context
    context = ETLContext(
        db_config=db_config,
        output_dir=args.output_dir or config.get('output', {}).get('directory', 'output'),
        memory_limit_mb=args.memory_limit,
        parallel_processing=args.parallel,
        chunk_size=args.chunk_size,
        batch_size=args.batch_size,
        max_workers=args.max_workers
    )

    return context

def download_attachments(pipeline_result, args):
    """
    Download attachments from messages.

    Args:
        pipeline_result (dict): Result of the ETL pipeline run
        args (argparse.Namespace): Parsed command-line arguments
    """
    if not args.download_attachments:
        return

    logger.info("Downloading attachments...")

    # Get transformed data
    transformed_data = pipeline_result.get('transformed_data', {})

    # Create attachment handler
    attachments_dir = args.attachments_dir or './attachments'
    handler = AttachmentHandler(
        output_dir=attachments_dir,
        download_thumbnails=not args.no_thumbnails
    )

    # Download attachments
    handler.download_all_attachments(transformed_data)

    logger.info(f"Attachments downloaded to {attachments_dir}")

def create_tables_if_needed(args, context):
    """
    Create database tables if requested.

    Args:
        args (argparse.Namespace): Parsed command-line arguments
        context (ETLContext): ETL context object
    """
    if not args.create_tables:
        return

    logger.info("Creating database tables...")

    # Connect to the database
    from src.db.connection import DatabaseConnection
    db_conn = DatabaseConnection(context.db_config)
    conn = db_conn.connect()

    try:
        # Create tables
        with conn.cursor() as cur:
            # Create raw data tables
            cur.execute("""
                CREATE TABLE IF NOT EXISTS raw_skype_data (
                    id SERIAL PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    export_date TIMESTAMP NOT NULL,
                    data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create clean data tables
            cur.execute("""
                CREATE TABLE IF NOT EXISTS clean_skype_data (
                    id SERIAL PRIMARY KEY,
                    raw_data_id INTEGER REFERENCES raw_skype_data(id),
                    user_display_name TEXT NOT NULL,
                    export_date TIMESTAMP NOT NULL,
                    data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create conversations table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS conversations (
                    id SERIAL PRIMARY KEY,
                    clean_data_id INTEGER REFERENCES clean_skype_data(id),
                    conversation_id TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    message_count INTEGER NOT NULL,
                    first_message_time TIMESTAMP,
                    last_message_time TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create messages table
            cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    conversation_id INTEGER REFERENCES conversations(id),
                    message_id TEXT NOT NULL,
                    sender_id TEXT NOT NULL,
                    sender_name TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    message_type TEXT NOT NULL,
                    content TEXT,
                    content_type TEXT,
                    has_attachments BOOLEAN DEFAULT FALSE,
                    attachments JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

        conn.commit()
        logger.info("Database tables created successfully")

    except Exception as e:
        conn.rollback()
        logger.error(f"Error creating database tables: {e}")
        raise

    finally:
        db_conn.close()

def print_summary(pipeline_result):
    """
    Print a summary of the ETL pipeline run.

    Args:
        pipeline_result (dict): Result of the ETL pipeline run
    """
    logger.info("ETL Pipeline Summary:")

    # Extract phase
    extract_result = pipeline_result.get('extract', {})
    extract_success = extract_result.get('success', False)
    extract_time = extract_result.get('time_taken', 0)
    logger.info(f"  Extract Phase: {'Success' if extract_success else 'Failed'} ({extract_time:.2f}s)")

    # Transform phase
    transform_result = pipeline_result.get('transform', {})
    transform_success = transform_result.get('success', False)
    transform_time = transform_result.get('time_taken', 0)
    logger.info(f"  Transform Phase: {'Success' if transform_success else 'Failed'} ({transform_time:.2f}s)")

    # Load phase
    load_result = pipeline_result.get('loading', {})
    load_success = load_result.get('success', False)
    load_time = load_result.get('time_taken', 0)
    logger.info(f"  Load Phase: {'Success' if load_success else 'Failed'} ({load_time:.2f}s)")

    # Overall
    total_time = pipeline_result.get('total_time_taken', 0)
    logger.info(f"  Total Time: {total_time:.2f}s")

    # Data stats
    transformed_data = pipeline_result.get('transformed_data', {})
    conversation_count = len(transformed_data.get('conversations', {}))
    message_count = sum(len(conv.get('messages', [])) for conv in transformed_data.get('conversations', {}).values())
    logger.info(f"  Conversations: {conversation_count}")
    logger.info(f"  Messages: {message_count}")

def main():
    """
    Main function to run the ETL pipeline.
    """
    # Parse command-line arguments
    args = parse_args()

    # Configure logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Load configuration
    config = load_config(args.config_file)

    try:
        # Create ETL context
        context = get_etl_context(args, config)

        # Create database tables if requested
        create_tables_if_needed(args, context)

        # Create ETL pipeline
        pipeline = ETLPipeline(context=context)

        # Run the pipeline
        if args.resume or args.checkpoint_id:
            # Resume from checkpoint
            checkpoint_id = args.checkpoint_id
            if not checkpoint_id:
                # Get the latest checkpoint
                checkpoints = pipeline.get_available_checkpoints()
                if not checkpoints:
                    logger.error("No checkpoints available to resume from")
                    sys.exit(1)
                checkpoint_id = checkpoints[0]

            logger.info(f"Resuming from checkpoint: {checkpoint_id}")
            result = pipeline.run_pipeline(
                resume_from_checkpoint=True,
                checkpoint_id=checkpoint_id
            )
        else:
            # Run from scratch
            if not args.file_path:
                logger.error("File path is required when not resuming from a checkpoint")
                sys.exit(1)

            logger.info(f"Processing file: {args.file_path}")
            result = pipeline.run_pipeline(
                file_path=args.file_path,
                user_display_name=args.user_display_name
            )

        # Download attachments if requested
        download_attachments(result, args)

        # Print summary
        print_summary(result)

        # Check for errors
        if not result.get('success', False):
            logger.error("ETL pipeline failed")
            if 'error_details' in result:
                logger.error(f"Error details: {result['error_details']}")
            sys.exit(1)

        logger.info("ETL pipeline completed successfully")
        return 0

    except KeyboardInterrupt:
        logger.info("ETL pipeline interrupted by user")
        return 130

    except Exception as e:
        logger.error(f"Error running ETL pipeline: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())