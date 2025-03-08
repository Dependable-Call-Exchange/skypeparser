#!/usr/bin/env python3
"""
Optimized ETL Pipeline Example

This script demonstrates the optimized ETL pipeline with the improved loader component,
including connection pooling, dynamic batch sizing, and performance monitoring.
"""

import os
import sys
import json
import time
import logging
import argparse
from pathlib import Path
from typing import Dict, Any, Optional

# Add the parent directory to the path so we can import the modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.db.etl.context import ETLContext
from src.db.etl.extractor import Extractor
from src.db.etl.transformer import Transformer
from src.db.etl.loader import Loader
from src.db.connection_factory import create_connection_pool, create_db_connection
from src.utils.interfaces import DatabaseConnectionProtocol, ConnectionPoolProtocol

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('optimized_etl_pipeline.log')
    ]
)
logger = logging.getLogger('optimized-etl-example')


def load_config(config_file: str) -> Dict[str, Any]:
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


def run_optimized_etl_pipeline(
    file_path: str,
    user_display_name: str,
    db_config: Dict[str, Any],
    output_dir: str = 'output',
    batch_size: int = 100,
    memory_limit_mb: int = 1024,
    max_connections: int = 5,
    schema_file: Optional[str] = None
) -> Dict[str, Any]:
    """
    Run the optimized ETL pipeline.

    Args:
        file_path: Path to the Skype export file
        user_display_name: Display name of the user
        db_config: Database configuration
        output_dir: Directory to store output files
        batch_size: Base batch size for database operations
        memory_limit_mb: Memory limit in MB
        max_connections: Maximum number of database connections
        schema_file: Path to SQL schema file (optional)

    Returns:
        dict: Results of the ETL pipeline run
    """
    start_time = time.time()
    logger.info(f"Starting optimized ETL pipeline with file: {file_path}")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Create a connection pool
    connection_pool = create_connection_pool(
        db_config=db_config,
        min_connections=1,
        max_connections=max_connections,
        connection_timeout=30.0,
        idle_timeout=600.0,
        max_age=3600.0
    )

    # Get a database connection from the pool
    db_connection = create_db_connection(
        db_config=db_config,
        conn_type="pooled"
    )

    # Create ETL context
    context = ETLContext(
        db_config=db_config,
        output_dir=output_dir,
        memory_limit_mb=memory_limit_mb,
        parallel_processing=True,
        batch_size=batch_size,
        task_id=f"optimized-etl-{Path(file_path).stem}",
        user_display_name=user_display_name
    )

    try:
        # Create ETL components
        extractor = Extractor(context=context)
        transformer = Transformer(context=context)
        loader = Loader(
            context=context,
            batch_size=batch_size,
            db_connection=db_connection,
            max_retries=3,
            retry_delay=1.0,
            schema_file=schema_file
        )

        # Extract phase
        logger.info("Starting extraction phase")
        raw_data = extractor.extract(file_path=file_path)
        logger.info(f"Extraction completed: {len(raw_data.get('conversations', {}))} conversations found")

        # Transform phase
        logger.info("Starting transformation phase")
        transformed_data = transformer.transform(raw_data, user_display_name=user_display_name)

        # Count messages
        message_count = sum(len(conv.get('messages', []))
                           for conv in transformed_data.get('conversations', {}).values())
        logger.info(f"Transformation completed: {message_count} messages processed")

        # Load phase
        logger.info("Starting loading phase")
        loader.connect_db()
        try:
            export_id = loader.load(raw_data, transformed_data, file_path)
            logger.info(f"Loading completed: Export ID {export_id}")
        finally:
            loader.close_db()

        # Get metrics
        duration = time.time() - start_time
        metrics = {
            "duration": duration,
            "conversations": len(transformed_data.get('conversations', {})),
            "messages": message_count,
            "export_id": export_id,
            "loader_metrics": context.metrics.get("load", {})
        }

        # Print performance summary
        logger.info(f"ETL pipeline completed in {duration:.2f} seconds")
        logger.info(f"Processed {metrics['conversations']} conversations with {metrics['messages']} messages")

        if 'batch_sizes' in metrics.get('loader_metrics', {}):
            avg_batch_size = sum(metrics['loader_metrics']['batch_sizes']) / len(metrics['loader_metrics']['batch_sizes']) if metrics['loader_metrics']['batch_sizes'] else 0
            logger.info(f"Average batch size: {avg_batch_size:.1f}")

        if 'retries' in metrics.get('loader_metrics', {}):
            logger.info(f"Database retries: {metrics['loader_metrics']['retries']}")

        # Get connection pool stats
        pool_stats = connection_pool.get_stats()
        logger.info(f"Connection pool stats: {pool_stats}")

        return metrics

    except Exception as e:
        logger.error(f"Error in ETL pipeline: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise
    finally:
        # Close all connections in the pool
        try:
            connection_pool.close_all()
        except Exception as e:
            logger.error(f"Error closing connection pool: {e}")


def main():
    """Main function to run the optimized ETL pipeline."""
    parser = argparse.ArgumentParser(
        description='Run the optimized ETL pipeline',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # File input options
    parser.add_argument('-f', '--filename', required=True,
                       help='Path to the Skype export file (TAR or JSON)')
    parser.add_argument('-u', '--user-display-name', required=True,
                       help='Your display name in the Skype logs')

    # Configuration options
    parser.add_argument('--config', default='../config/config.json',
                       help='Path to configuration file')
    parser.add_argument('--output-dir', default='output',
                       help='Directory to store output files')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Base batch size for database operations')
    parser.add_argument('--memory-limit', type=int, default=1024,
                       help='Memory limit in MB')
    parser.add_argument('--max-connections', type=int, default=5,
                       help='Maximum number of database connections')
    parser.add_argument('--schema-file',
                       help='Path to SQL schema file (optional)')

    args = parser.parse_args()

    # Load configuration
    config = load_config(args.config)

    # Extract database configuration
    db_config = config.get('database', {})

    # Run the ETL pipeline
    run_optimized_etl_pipeline(
        file_path=args.filename,
        user_display_name=args.user_display_name,
        db_config=db_config,
        output_dir=args.output_dir,
        batch_size=args.batch_size,
        memory_limit_mb=args.memory_limit,
        max_connections=args.max_connections,
        schema_file=args.schema_file
    )


if __name__ == "__main__":
    main()