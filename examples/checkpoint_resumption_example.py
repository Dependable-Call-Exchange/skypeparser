#!/usr/bin/env python3
"""
Example script demonstrating checkpoint resumption in the ETL pipeline.

This script shows how to:
1. Create and run an ETL pipeline
2. Save checkpoints after each phase
3. Simulate a failure during processing
4. Resume the pipeline from the last checkpoint
"""

import os
import sys
import logging
import argparse
import time
import random
from typing import Dict, Any, Optional

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
    parser = argparse.ArgumentParser(description='Example of checkpoint resumption in the ETL pipeline')
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
    parser.add_argument('--fail-phase', choices=['extract', 'transform', 'load'],
                        help='Simulate failure in a specific phase')
    parser.add_argument('--resume', action='store_true', help='Resume from checkpoint')
    parser.add_argument('--checkpoint-file', help='Path to checkpoint file to resume from')
    return parser.parse_args()

def run_pipeline_with_failure(args) -> Dict[str, Any]:
    """Run the ETL pipeline with a simulated failure.

    Args:
        args: Command line arguments

    Returns:
        Dict containing the results of the ETL pipeline or the path to the checkpoint file
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
        output_dir=args.output,
        memory_limit_mb=args.memory_limit,
        parallel_processing=args.parallel,
        task_id="checkpoint-example"
    )

    # Get the context for direct access
    context = pipeline.get_context()
    logger.info(f"Created ETL pipeline with task ID: {context.task_id}")

    # Define a custom exception for simulated failures
    class SimulatedFailure(Exception):
        """Exception raised to simulate a failure in the ETL pipeline."""
        pass

    # Monkey patch the phase methods to save checkpoints and simulate failures
    original_extract = pipeline._run_extract_phase
    original_transform = pipeline._run_transform_phase
    original_load = pipeline._run_load_phase

    def extract_with_checkpoint(*args, **kwargs):
        """Run extract phase and save checkpoint."""
        result = original_extract(*args, **kwargs)
        checkpoint_file = pipeline.save_checkpoint()
        logger.info(f"Saved checkpoint after extract phase: {checkpoint_file}")

        if args.fail_phase == 'extract':
            logger.warning("Simulating failure in extract phase")
            raise SimulatedFailure("Simulated failure in extract phase")

        return result

    def transform_with_checkpoint(*args, **kwargs):
        """Run transform phase and save checkpoint."""
        result = original_transform(*args, **kwargs)
        checkpoint_file = pipeline.save_checkpoint()
        logger.info(f"Saved checkpoint after transform phase: {checkpoint_file}")

        if args.fail_phase == 'transform':
            logger.warning("Simulating failure in transform phase")
            raise SimulatedFailure("Simulated failure in transform phase")

        return result

    def load_with_checkpoint(*args, **kwargs):
        """Run load phase and save checkpoint."""
        result = original_load(*args, **kwargs)
        checkpoint_file = pipeline.save_checkpoint()
        logger.info(f"Saved checkpoint after load phase: {checkpoint_file}")

        if args.fail_phase == 'load':
            logger.warning("Simulating failure in load phase")
            raise SimulatedFailure("Simulated failure in load phase")

        return result

    # Apply the monkey patches
    pipeline._run_extract_phase = lambda *a, **kw: extract_with_checkpoint(*a, **kw)
    pipeline._run_transform_phase = lambda *a, **kw: transform_with_checkpoint(*a, **kw)
    pipeline._run_load_phase = lambda *a, **kw: load_with_checkpoint(*a, **kw)

    try:
        # Run the pipeline
        logger.info(f"Running ETL pipeline with file: {args.file}")
        results = pipeline.run_pipeline(
            file_path=args.file,
            user_display_name=args.user
        )
        return results

    except SimulatedFailure as e:
        # This is our simulated failure, return the checkpoint file path
        logger.error(f"Pipeline failed as expected: {e}")
        return {
            'success': False,
            'error': str(e),
            'checkpoint_file': os.path.join(args.output, f"etl_checkpoint_{context.task_id}.json"),
            'failed_phase': args.fail_phase
        }

    except Exception as e:
        # This is an unexpected failure
        logger.exception(f"Unexpected error in ETL pipeline: {e}")
        return {
            'success': False,
            'error': str(e)
        }

def resume_from_checkpoint(args, checkpoint_file: str) -> Dict[str, Any]:
    """Resume the ETL pipeline from a checkpoint.

    Args:
        args: Command line arguments
        checkpoint_file: Path to the checkpoint file

    Returns:
        Dict containing the results of the resumed ETL pipeline
    """
    # Create database configuration
    db_config = {
        'dbname': args.dbname,
        'user': args.dbuser,
        'password': args.dbpass,
        'host': args.dbhost,
        'port': args.dbport
    }

    # Load the pipeline from the checkpoint
    logger.info(f"Loading pipeline from checkpoint: {checkpoint_file}")
    pipeline = ETLPipeline.load_from_checkpoint(checkpoint_file, db_config)

    # Get the context for information
    context = pipeline.get_context()
    logger.info(f"Loaded pipeline with task ID: {context.task_id}")

    # Get available checkpoints
    available_checkpoints = pipeline.get_available_checkpoints()
    logger.info(f"Available checkpoints: {available_checkpoints}")

    # Resume the pipeline
    logger.info("Resuming pipeline from checkpoint")
    results = pipeline.run_pipeline(
        file_path=args.file,
        user_display_name=args.user,
        resume_from_checkpoint=True
    )

    return results

def print_results(results: Dict[str, Any]) -> None:
    """Print the results of the ETL process.

    Args:
        results: Results dictionary from the ETL process
    """
    if not results.get('success', False):
        if 'checkpoint_file' in results:
            logger.info("Pipeline failed as expected, checkpoint saved")
            logger.info(f"Checkpoint file: {results['checkpoint_file']}")
            logger.info(f"Failed in phase: {results.get('failed_phase', 'unknown')}")
        else:
            logger.error(f"Pipeline failed unexpectedly: {results.get('error', 'Unknown error')}")
        return

    logger.info("ETL process results:")
    logger.info(f"Success: {results['success']}")
    logger.info(f"Export ID: {results['export_id']}")
    logger.info(f"Task ID: {results['task_id']}")
    logger.info(f"Total duration: {results['total_duration_seconds']:.2f} seconds")

    if results.get('resumed', False):
        logger.info(f"Pipeline was resumed from phase: {results.get('resumed_from_phase', 'unknown')}")

    # Print phase statistics
    for phase, stats in results.get('phases', {}).items():
        if stats:
            logger.info(f"{phase.capitalize()} phase statistics:")
            logger.info(f"  Duration: {stats.get('duration_seconds', 0):.2f} seconds")
            logger.info(f"  Conversations: {stats.get('processed_conversations', 0)}")
            logger.info(f"  Messages: {stats.get('processed_messages', 0)}")
            if 'messages_per_second' in stats:
                logger.info(f"  Messages per second: {stats.get('messages_per_second', 0):.2f}")

def main():
    """Main function."""
    args = parse_args()

    try:
        if args.resume:
            # Resume from checkpoint
            checkpoint_file = args.checkpoint_file
            if not checkpoint_file:
                logger.error("No checkpoint file specified for resumption")
                sys.exit(1)

            results = resume_from_checkpoint(args, checkpoint_file)
            print_results(results)
        else:
            # Run with simulated failure
            results = run_pipeline_with_failure(args)
            print_results(results)

            # If we simulated a failure and have a checkpoint, offer to resume
            if not results.get('success', False) and 'checkpoint_file' in results:
                checkpoint_file = results['checkpoint_file']
                logger.info("\nYou can resume the pipeline with:")
                logger.info(f"python {sys.argv[0]} -f {args.file} -u '{args.user}' --resume --checkpoint-file {checkpoint_file}")

    except Exception as e:
        logger.exception(f"Error running example: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()