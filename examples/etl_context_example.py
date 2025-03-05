#!/usr/bin/env python3
"""
Example script demonstrating the use of ETLContext with the modular ETL pipeline.

This script shows how to use the ETLContext to manage state across ETL components
and how to access metrics and checkpoints.
"""

import os
import sys
import logging
import argparse
from typing import Dict, Any

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db.etl import ETLContext, ETLPipeline, Extractor, Transformer, Loader

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Example of using ETLContext with the modular ETL pipeline')
    parser.add_argument('-f', '--file', required=True, help='Path to the Skype export file')
    parser.add_argument('-u', '--user', default='Test User', help='User display name')
    parser.add_argument('-d', '--dbname', default='skype_logs', help='Database name')
    parser.add_argument('-U', '--dbuser', default='postgres', help='Database user')
    parser.add_argument('-P', '--dbpass', default='', help='Database password')
    parser.add_argument('-H', '--dbhost', default='localhost', help='Database host')
    parser.add_argument('-p', '--dbport', default=5432, type=int, help='Database port')
    parser.add_argument('-o', '--output', default='output', help='Output directory')
    parser.add_argument('--memory-limit', default=1024, type=int, help='Memory limit in MB')
    parser.add_argument('--parallel', action='store_true', help='Enable parallel processing')
    return parser.parse_args()

def run_etl_with_context(args) -> Dict[str, Any]:
    """Run the ETL pipeline with ETLContext.

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

    # Create the ETL context
    context = ETLContext(
        db_config=db_config,
        output_dir=args.output,
        memory_limit_mb=args.memory_limit,
        parallel_processing=args.parallel,
        task_id="example-task"
    )

    logger.info(f"Created ETL context with task ID: {context.task_id}")

    # Create the ETL pipeline with the context
    pipeline = ETLPipeline(
        db_config=db_config,  # This will be ignored as context is provided
        context=context
    )

    # Run the pipeline
    logger.info(f"Running ETL pipeline with file: {args.file}")
    results = pipeline.run_pipeline(
        file_path=args.file,
        user_display_name=args.user
    )

    return results

def run_etl_components_manually(args) -> Dict[str, Any]:
    """Run the ETL components manually with a shared context.

    This demonstrates how to use the ETLContext to share state between
    components when running them individually.

    Args:
        args: Command line arguments

    Returns:
        Dict containing the results of the ETL process
    """
    # Create database configuration
    db_config = {
        'dbname': args.dbname,
        'user': args.dbuser,
        'password': args.dbpass,
        'host': args.dbhost,
        'port': args.dbport
    }

    # Create the ETL context
    context = ETLContext(
        db_config=db_config,
        output_dir=args.output,
        memory_limit_mb=args.memory_limit,
        parallel_processing=args.parallel,
        task_id="manual-task"
    )

    logger.info(f"Created ETL context with task ID: {context.task_id}")

    # Create the individual components with the shared context
    extractor = Extractor(context=context)
    transformer = Transformer(context=context)
    loader = Loader(context=context)

    try:
        # Start extract phase
        context.start_phase("extract")

        # Extract data
        raw_data = extractor.extract(file_path=args.file)

        # End extract phase
        context.end_phase()

        # Start transform phase
        context.start_phase(
            "transform",
            total_conversations=len(raw_data.get('conversations', [])),
            total_messages=sum(len(conv.get('MessageList', [])) for conv in raw_data.get('conversations', []))
        )

        # Transform data
        transformed_data = transformer.transform(raw_data, args.user)

        # End transform phase
        context.end_phase()

        # Connect to database
        loader.connect_db()

        # Start load phase
        context.start_phase(
            "load",
            total_conversations=len(transformed_data.get('conversations', {})),
            total_messages=sum(len(conv.get('messages', [])) for conv in transformed_data.get('conversations', {}).values())
        )

        # Load data
        export_id = loader.load(raw_data, transformed_data, args.file)

        # End load phase
        context.end_phase()

        # Close database connection
        loader.close_db()

        # Get summary
        summary = context.get_summary()
        summary['success'] = True
        summary['export_id'] = export_id

        return summary

    except Exception as e:
        logger.exception(f"Error in ETL process: {e}")

        # Record error
        context.record_error(
            phase=context.current_phase or "unknown",
            error=e,
            fatal=True
        )

        # Close database connection if open
        loader.close_db()

        # Get error summary
        summary = context.get_summary()
        summary['success'] = False
        summary['error'] = str(e)

        return summary

def print_results(results: Dict[str, Any]) -> None:
    """Print the results of the ETL process.

    Args:
        results: Results dictionary from the ETL process
    """
    logger.info("ETL process results:")
    logger.info(f"Success: {results['success']}")

    if results['success']:
        logger.info(f"Export ID: {results['export_id']}")
        logger.info(f"Task ID: {results['task_id']}")
        logger.info(f"Total duration: {results['total_duration_seconds']:.2f} seconds")

        # Print phase statistics
        for phase, stats in results.get('phases', {}).items():
            if stats:
                logger.info(f"{phase.capitalize()} phase statistics:")
                logger.info(f"  Duration: {stats.get('duration_seconds', 0):.2f} seconds")
                logger.info(f"  Conversations: {stats.get('processed_conversations', 0)}")
                logger.info(f"  Messages: {stats.get('processed_messages', 0)}")
                if 'messages_per_second' in stats:
                    logger.info(f"  Messages per second: {stats.get('messages_per_second', 0):.2f}")
    else:
        logger.error(f"ETL process failed: {results.get('error', 'Unknown error')}")
        logger.error(f"Error count: {results.get('error_count', 0)}")

def main():
    """Main function."""
    args = parse_args()

    try:
        # Run the ETL pipeline with context
        logger.info("Running ETL pipeline with context")
        results = run_etl_with_context(args)
        print_results(results)

        # Uncomment to run components manually
        # logger.info("\nRunning ETL components manually with shared context")
        # manual_results = run_etl_components_manually(args)
        # print_results(manual_results)

    except Exception as e:
        logger.exception(f"Error running example: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()