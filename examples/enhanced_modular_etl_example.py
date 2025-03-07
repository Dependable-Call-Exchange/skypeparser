#!/usr/bin/env python3
"""
Enhanced Modular ETL Pipeline Example

This script demonstrates how to use the enhanced modular ETL pipeline
with different configuration options and components, showing improvements
from Stage 4 of the SkypeParser Improvement Plan.
"""

import logging
import os
import sys
import time
from typing import Any, Dict

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import required modules
from src.db.etl.modular_pipeline import ModularETLPipeline
from src.db.etl.pipeline_factory import PipelineFactory
from src.utils.error_handling import ErrorContext, handle_errors
from src.utils.structured_logging import get_logger, setup_logging

# Set up logging
setup_logging(level="INFO", structured=True)
logger = get_logger(__name__)


def basic_usage_example(file_path: str, user_display_name: str):
    """
    Basic usage example of the enhanced modular ETL pipeline.

    Args:
        file_path: Path to the Skype export file
        user_display_name: User display name
    """
    logger.info("=== Basic Usage Example ===")

    # Create a simple configuration
    config = {
        "database": {
            "host": "localhost",
            "port": 5432,
            "dbname": "skype_archive",
            "user": "postgres",
            "password": "",
        },
        "output": {"directory": "output"},
        "logging": {"level": "INFO", "file": "etl_pipeline.log"},
    }

    # Create pipeline from configuration
    pipeline = ModularETLPipeline.from_config(config)

    # Run the pipeline
    start_time = time.time()
    result = pipeline.run_pipeline(
        file_path=file_path, user_display_name=user_display_name
    )
    end_time = time.time()

    # Print results
    logger.info(f"Pipeline completed in {end_time - start_time:.2f} seconds")
    logger.info(f"Status: {result['status']}")

    if result["status"] == "completed":
        logger.info(f"Export ID: {result['export_id']}")

        # Print metrics
        if "metrics" in result and "phase_metrics" in result["metrics"]:
            phase_metrics = result["metrics"]["phase_metrics"]
            for phase, phase_result in phase_metrics.items():
                if isinstance(phase_result, dict):
                    logger.info(
                        f"Phase {phase}: {phase_result.get('status', 'unknown')}"
                    )

                    if "conversations_processed" in phase_result:
                        logger.info(
                            f"  Conversations processed: {phase_result['conversations_processed']}"
                        )

                    if "messages_processed" in phase_result:
                        logger.info(
                            f"  Messages processed: {phase_result['messages_processed']}"
                        )
    else:
        logger.error(f"Pipeline failed: {result.get('error', 'unknown error')}")


def factory_usage_example(file_path: str, user_display_name: str):
    """
    Example of using the pipeline factory for creating ETL components.

    Args:
        file_path: Path to the Skype export file
        user_display_name: User display name
    """
    logger.info("=== Pipeline Factory Example ===")

    # Create a detailed configuration
    config = {
        "database": {
            "host": "localhost",
            "port": 5432,
            "dbname": "skype_archive",
            "user": "postgres",
            "password": "",
        },
        "output": {"directory": "output", "overwrite": True, "format": "json"},
        "logging": {
            "level": "DEBUG",
            "file": "etl_pipeline_advanced.log",
            "json_format": True,
            "structured": True,
        },
        "memory_limit_mb": 2048,
        "use_parallel_processing": True,
        "chunk_size": 2000,
        "db_batch_size": 200,
        "max_workers": 4,
    }

    # Create pipeline factory
    factory = PipelineFactory(config)

    # Create individual components
    extractor = factory.create_extractor()
    transformer = factory.create_transformer()
    loader = factory.create_loader()

    # Create pipeline with components
    pipeline = ModularETLPipeline(
        context=factory.get_context(),
        extractor=extractor,
        transformer=transformer,
        loader=loader,
    )

    # Run the pipeline
    start_time = time.time()
    result = pipeline.run_pipeline(
        file_path=file_path, user_display_name=user_display_name
    )
    end_time = time.time()

    # Print results
    logger.info(f"Pipeline completed in {end_time - start_time:.2f} seconds")
    logger.info(f"Status: {result['status']}")

    if result["status"] == "completed":
        logger.info(f"Export ID: {result['export_id']}")

        # Print execution times
        if "metrics" in result and "execution_times" in result["metrics"]:
            execution_times = result["metrics"]["execution_times"]
            for phase, duration in execution_times.items():
                logger.info(f"Phase {phase} execution time: {duration:.2f} seconds")
    else:
        logger.error(f"Pipeline failed: {result.get('error', 'unknown error')}")


def custom_component_example(file_path: str, user_display_name: str):
    """
    Example of using custom ETL components with dependency injection.

    Args:
        file_path: Path to the Skype export file
        user_display_name: User display name
    """
    logger.info("=== Custom Component Example ===")

    # Import required classes
    from src.db.connection_factory import create_db_connection
    from src.db.etl.context import ETLContext
    from src.db.etl.extractor import Extractor
    from src.db.etl.loader import Loader
    from src.db.etl.transformer import Transformer
    from src.utils.di import get_service_provider
    from src.utils.interfaces import (
        ExtractorProtocol,
        LoaderProtocol,
        TransformerProtocol,
    )

    # Create a configuration
    config = {
        "database": {
            "host": "localhost",
            "port": 5432,
            "dbname": "skype_archive",
            "user": "postgres",
            "password": "",
        },
        "output": {"directory": "output"},
    }

    # Create a context
    context = ETLContext(
        db_config=config["database"],
        output_dir=config["output"]["directory"],
        memory_limit_mb=1024,
        parallel_processing=True,
        chunk_size=1000,
        batch_size=100,
    )

    # Create a pooled database connection for better performance
    db_connection = create_db_connection(
        db_config=config["database"], conn_type="pooled", min_conn=2, max_conn=5
    )

    # Create standard components
    extractor = Extractor(context=context)
    transformer = Transformer(context=context)
    loader = Loader(context=context, db_connection=db_connection)

    # Create a custom CachingTransformer that extends the standard Transformer
    class CachingTransformer(Transformer):
        """A transformer that caches results for better performance."""

        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.cache = {}
            logger.info("CachingTransformer initialized")

        def transform(self, raw_data, user_display_name=None):
            """Transform raw data with caching."""
            # Generate a cache key
            cache_key = f"{hash(str(raw_data))}-{user_display_name}"

            # Check if result is in cache
            if cache_key in self.cache:
                logger.info("Using cached transformation result")
                return self.cache[cache_key]

            # Transform data
            result = super().transform(raw_data, user_display_name)

            # Cache result
            self.cache[cache_key] = result
            logger.info("Cached transformation result")

            return result

    # Replace the standard transformer with our custom implementation
    custom_transformer = CachingTransformer(context=context)

    # Create pipeline with custom components
    pipeline = ModularETLPipeline(
        context=context,
        extractor=extractor,
        transformer=custom_transformer,
        loader=loader,
    )

    # Run the pipeline
    start_time = time.time()
    result = pipeline.run_pipeline(
        file_path=file_path, user_display_name=user_display_name
    )
    end_time = time.time()

    # Print results
    logger.info(f"Pipeline completed in {end_time - start_time:.2f} seconds")
    logger.info(f"Status: {result['status']}")

    if result["status"] == "completed":
        logger.info(f"Export ID: {result['export_id']}")
    else:
        logger.error(f"Pipeline failed: {result.get('error', 'unknown error')}")


def connection_pooling_example():
    """
    Example demonstrating database connection pooling for better performance.
    """
    logger.info("=== Connection Pooling Example ===")

    from src.db.connection_factory import create_db_connection
    from src.db.connection_pool import PooledDatabaseConnection
    from src.utils.structured_logging import log_execution_time

    # Create database configuration
    db_config = {
        "host": "localhost",
        "port": 5432,
        "dbname": "skype_archive",
        "user": "postgres",
        "password": "",
    }

    # Create a pooled database connection
    db = create_db_connection(
        db_config=db_config, conn_type="pooled", min_conn=2, max_conn=10
    )

    # Connect to the database
    db.connect()

    # Define a test function that executes queries
    @log_execution_time(logger)
    def execute_queries(query_count: int):
        """Execute multiple queries to demonstrate connection pooling."""
        logger.info(f"Executing {query_count} queries...")

        for i in range(query_count):
            # Execute a simple query
            db.execute("SELECT 1 AS test")

    # Run the test with different query counts
    for count in [10, 100]:
        execute_queries(count)

    # Close the database connection
    db.close()


def run_examples():
    """Run all examples if file paths are provided."""
    # Check if file path is provided
    if len(sys.argv) < 2:
        print(
            "Usage: python enhanced_modular_etl_example.py <file_path> [user_display_name]"
        )
        print("\nNote: For the connection pooling example, you don't need a file path:")
        print("      python enhanced_modular_etl_example.py --connection-pool")
        return

    # Check if only running connection pooling example
    if sys.argv[1] == "--connection-pool":
        connection_pooling_example()
        return

    # Get file path and user display name
    file_path = sys.argv[1]
    user_display_name = sys.argv[2] if len(sys.argv) > 2 else None

    try:
        # Run examples
        with ErrorContext(operation="examples", file_path=file_path):
            basic_usage_example(file_path, user_display_name)
            factory_usage_example(file_path, user_display_name)
            custom_component_example(file_path, user_display_name)

    except Exception as e:
        logger.error(f"Error running examples: {e}", exc_info=True)


if __name__ == "__main__":
    run_examples()
