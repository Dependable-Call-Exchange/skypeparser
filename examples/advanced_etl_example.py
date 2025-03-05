#!/usr/bin/env python3
"""
Advanced example usage of the modular ETL pipeline.

This script demonstrates how to use the individual components of the modular
ETL pipeline for more fine-grained control over the ETL process.
"""

import os
import sys
import argparse
import logging
import time
from typing import Dict, Any, Tuple

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db.etl import Extractor, Transformer, Loader
from src.db.etl.utils import ProgressTracker, MemoryMonitor
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
    parser = argparse.ArgumentParser(description='Process Skype export data using individual ETL components')
    parser.add_argument('-f', '--file', required=True, help='Path to the Skype export file')
    parser.add_argument('-u', '--user', default='', help='User display name')
    parser.add_argument('-c', '--config', default='config/config.json', help='Path to the configuration file')
    parser.add_argument('-o', '--output', default='output', help='Output directory')
    parser.add_argument('-m', '--memory', type=int, default=1024, help='Memory limit in MB')
    parser.add_argument('-p', '--parallel', action='store_true', help='Enable parallel processing')
    parser.add_argument('-s', '--chunk-size', type=int, default=1000, help='Chunk size for batch processing')
    parser.add_argument('--skip-extract', action='store_true', help='Skip extraction phase')
    parser.add_argument('--skip-transform', action='store_true', help='Skip transformation phase')
    parser.add_argument('--skip-load', action='store_true', help='Skip loading phase')
    parser.add_argument('--extract-only', action='store_true', help='Only run extraction phase')
    parser.add_argument('--transform-only', action='store_true', help='Only run transformation phase')
    parser.add_argument('--load-only', action='store_true', help='Only run loading phase')
    parser.add_argument('--raw-data', help='Path to raw data file (for transform-only or load-only)')
    parser.add_argument('--transformed-data', help='Path to transformed data file (for load-only)')
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


def run_extraction(args: argparse.Namespace, progress_tracker: ProgressTracker,
                 memory_monitor: MemoryMonitor) -> Dict[str, Any]:
    """Run the extraction phase.

    Args:
        args: Command line arguments
        progress_tracker: Progress tracker
        memory_monitor: Memory monitor

    Returns:
        Dict containing the raw data
    """
    # Skip if requested
    if args.skip_extract:
        logger.info("Skipping extraction phase")

        # Load raw data from file if provided
        if args.raw_data:
            import json
            with open(args.raw_data, 'r') as f:
                raw_data = json.load(f)
            logger.info(f"Loaded raw data from {args.raw_data}")
            return raw_data
        else:
            raise ValueError("Raw data file must be provided when skipping extraction phase")

    # Start progress tracking
    progress_tracker.start_phase('extract')

    # Check memory
    memory_monitor.check_memory()

    # Create extractor
    extractor = Extractor(output_dir=args.output)

    # Run extraction
    logger.info(f"Running extraction phase with file: {args.file}")
    start_time = time.time()
    raw_data = extractor.extract(file_path=args.file)
    end_time = time.time()

    # Check memory
    memory_monitor.check_memory()

    # Log results
    logger.info(f"Extraction completed in {end_time - start_time:.2f} seconds")
    logger.info(f"Extracted {len(raw_data.get('conversations', []))} conversations")

    # Finish progress tracking
    progress_tracker.finish_phase()

    # Save raw data to file if extract-only
    if args.extract_only:
        import json
        raw_data_file = os.path.join(args.output, 'raw_data.json')
        with open(raw_data_file, 'w') as f:
            json.dump(raw_data, f)
        logger.info(f"Saved raw data to {raw_data_file}")

    return raw_data


def run_transformation(args: argparse.Namespace, raw_data: Dict[str, Any],
                     progress_tracker: ProgressTracker,
                     memory_monitor: MemoryMonitor) -> Dict[str, Any]:
    """Run the transformation phase.

    Args:
        args: Command line arguments
        raw_data: Raw data from extraction phase
        progress_tracker: Progress tracker
        memory_monitor: Memory monitor

    Returns:
        Dict containing the transformed data
    """
    # Skip if requested
    if args.skip_transform:
        logger.info("Skipping transformation phase")

        # Load transformed data from file if provided
        if args.transformed_data:
            import json
            with open(args.transformed_data, 'r') as f:
                transformed_data = json.load(f)
            logger.info(f"Loaded transformed data from {args.transformed_data}")
            return transformed_data
        else:
            raise ValueError("Transformed data file must be provided when skipping transformation phase")

    # Count conversations and messages
    conversation_count = len(raw_data.get('conversations', []))
    message_count = sum(len(c.get('MessageList', [])) for c in raw_data.get('conversations', []))

    # Start progress tracking
    progress_tracker.start_phase('transform', conversation_count, message_count)

    # Check memory
    memory_monitor.check_memory()

    # Create transformer
    transformer = Transformer(
        parallel_processing=args.parallel,
        chunk_size=args.chunk_size
    )

    # Run transformation
    logger.info(f"Running transformation phase with {conversation_count} conversations and {message_count} messages")
    start_time = time.time()
    transformed_data = transformer.transform(raw_data, args.user)
    end_time = time.time()

    # Check memory
    memory_monitor.check_memory()

    # Log results
    logger.info(f"Transformation completed in {end_time - start_time:.2f} seconds")
    logger.info(f"Transformed {len(transformed_data.get('conversations', {}))} conversations")

    # Finish progress tracking
    progress_tracker.finish_phase()

    # Save transformed data to file if transform-only
    if args.transform_only:
        import json
        transformed_data_file = os.path.join(args.output, 'transformed_data.json')
        with open(transformed_data_file, 'w') as f:
            json.dump(transformed_data, f)
        logger.info(f"Saved transformed data to {transformed_data_file}")

    return transformed_data


def run_loading(args: argparse.Namespace, raw_data: Dict[str, Any],
              transformed_data: Dict[str, Any], db_config: Dict[str, Any],
              progress_tracker: ProgressTracker,
              memory_monitor: MemoryMonitor) -> int:
    """Run the loading phase.

    Args:
        args: Command line arguments
        raw_data: Raw data from extraction phase
        transformed_data: Transformed data from transformation phase
        db_config: Database configuration
        progress_tracker: Progress tracker
        memory_monitor: Memory monitor

    Returns:
        int: Export ID
    """
    # Skip if requested
    if args.skip_load:
        logger.info("Skipping loading phase")
        return 0

    # Count conversations and messages
    conversation_count = len(transformed_data.get('conversations', {}))
    message_count = sum(len(conv.get('messages', []))
                      for conv in transformed_data.get('conversations', {}).values())

    # Start progress tracking
    progress_tracker.start_phase('load', conversation_count, message_count)

    # Check memory
    memory_monitor.check_memory()

    # Create loader
    loader = Loader(db_config=db_config)

    # Connect to database
    loader.connect_db()

    try:
        # Run loading
        logger.info(f"Running loading phase with {conversation_count} conversations and {message_count} messages")
        start_time = time.time()
        export_id = loader.load(raw_data, transformed_data, args.file)
        end_time = time.time()

        # Check memory
        memory_monitor.check_memory()

        # Log results
        logger.info(f"Loading completed in {end_time - start_time:.2f} seconds")
        logger.info(f"Loaded data with export ID: {export_id}")

        # Finish progress tracking
        progress_tracker.finish_phase()

        return export_id
    finally:
        # Close database connection
        loader.close_db()


def run_etl_process(args: argparse.Namespace) -> Tuple[Dict[str, Any], Dict[str, Any], int]:
    """Run the ETL process with the given arguments.

    Args:
        args: Command line arguments

    Returns:
        Tuple containing raw data, transformed data, and export ID
    """
    # Load configuration
    config = get_config(args.config)

    # Get database configuration
    db_config = get_db_config(config)

    # Create utilities
    progress_tracker = ProgressTracker()
    memory_monitor = MemoryMonitor(memory_limit_mb=args.memory)

    # Create output directory if it doesn't exist
    os.makedirs(args.output, exist_ok=True)

    # Determine which phases to run
    if args.extract_only:
        args.skip_transform = True
        args.skip_load = True
    elif args.transform_only:
        args.skip_extract = True
        args.skip_load = True
    elif args.load_only:
        args.skip_extract = True
        args.skip_transform = True

    # Run extraction phase
    raw_data = run_extraction(args, progress_tracker, memory_monitor)

    # Run transformation phase
    transformed_data = run_transformation(args, raw_data, progress_tracker, memory_monitor)

    # Run loading phase
    export_id = run_loading(args, raw_data, transformed_data, db_config, progress_tracker, memory_monitor)

    return raw_data, transformed_data, export_id


def main():
    """Main function."""
    args = parse_args()

    try:
        # Run the ETL process
        raw_data, transformed_data, export_id = run_etl_process(args)

        # Print summary
        logger.info("ETL process completed successfully")
        if not args.skip_extract:
            logger.info(f"Extracted {len(raw_data.get('conversations', []))} conversations")
        if not args.skip_transform:
            logger.info(f"Transformed {len(transformed_data.get('conversations', {}))} conversations")
        if not args.skip_load:
            logger.info(f"Loaded data with export ID: {export_id}")

    except Exception as e:
        logger.exception(f"Error running ETL process: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()