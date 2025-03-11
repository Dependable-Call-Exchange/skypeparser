#!/usr/bin/env python3
"""
ETL Logging Example

This script demonstrates how to use the centralized structured logging system
with the ETL pipeline components. It shows proper logging practices for
extract, transform, and load operations with context tracking and performance monitoring.

Features:
- Structured logging with context tracking
- Performance monitoring of ETL operations
- Error handling and graceful failure recovery
- Database query logging
- Metrics collection and reporting
- Command-line configuration
- Environment variable support

Usage:
    python etl_logging_example.py [options]

Options:
    --log-level LEVEL     Set the logging level (default: from env or INFO)
    --log-file PATH       Path to log file (default: from env or ./logs/etl/etl_example.log)
    --batch-count NUM     Number of batches to process (default: from env or 3)
    --fail-probability N  Probability of simulated failures (0-1, default: from env or 0.1)
    --help                Show this help message and exit

Environment Variables:
    ETL_LOG_LEVEL         Set the logging level
    ETL_LOG_FILE          Path to log file
    ETL_BATCH_COUNT       Number of batches to process
    ETL_FAIL_PROBABILITY  Probability of simulated failures (0-1)
"""

import os
import sys
import time
import random
import logging
import argparse
from typing import Dict, List, Any, Optional, Union, Tuple
from contextlib import contextmanager

# Add the parent directory to the path so we can import the src module
parent_dir = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Import ETL components
try:
    from src.db.etl.loader import Loader
    from src.db.etl.context import ETLContext
    from tests.fixtures.mock_fixtures import MockDatabase

    # Import logging utilities
    from src.utils.new_structured_logging import (
        initialize_logging,
        LogContext,
        log_execution_time,
        log_database_query,
        log_metrics,
        handle_errors
    )
except ImportError as e:
    print(f"Error importing required modules: {e}")
    print("Make sure you're running this script from the root of the SkypeParser project")
    print("or that the SkypeParser module is installed in your Python environment.")
    sys.exit(1)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments with environment variable fallbacks.

    Returns:
        argparse.Namespace: The parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="ETL Logging Example",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "--log-level",
        default=os.environ.get("ETL_LOG_LEVEL", "INFO"),
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level"
    )

    parser.add_argument(
        "--log-file",
        default=os.environ.get("ETL_LOG_FILE", None),
        help="Path to log file (default: ./logs/etl/etl_example.log)"
    )

    parser.add_argument(
        "--batch-count",
        type=int,
        default=int(os.environ.get("ETL_BATCH_COUNT", "3")),
        help="Number of batches to process"
    )

    parser.add_argument(
        "--fail-probability",
        type=float,
        default=float(os.environ.get("ETL_FAIL_PROBABILITY", "0.1")),
        help="Probability of simulated failures (0-1)"
    )

    args = parser.parse_args()

    # Validate arguments
    if args.fail_probability < 0 or args.fail_probability > 1:
        parser.error("Failure probability must be between 0 and 1")

    if args.batch_count < 1:
        parser.error("Batch count must be at least 1")

    return args


def setup_logging(args: argparse.Namespace) -> Dict[str, logging.Logger]:
    """
    Set up logging with the specified configuration.

    Args:
        args: Command-line arguments containing logging configuration

    Returns:
        Dict[str, logging.Logger]: Dictionary of loggers for different components
    """
    # Create log directory if it doesn't exist
    log_dir = os.path.join(os.getcwd(), "logs", "etl")
    os.makedirs(log_dir, exist_ok=True)

    # Determine log file path
    log_file = args.log_file
    if not log_file:
        log_file = os.path.join(log_dir, "etl_example.log")

    # Initialize logging
    initialize_logging(
        log_level=args.log_level,
        log_file=log_file,
        enable_console=True,
        enable_json=True
    )

    # Create loggers for different components
    loggers = {
        "etl": logging.getLogger("etl"),
        "extract": logging.getLogger("etl.extract"),
        "transform": logging.getLogger("etl.transform"),
        "load": logging.getLogger("etl.load"),
        "db": logging.getLogger("etl.database"),
        "performance": logging.getLogger("etl.performance"),
        "error": logging.getLogger("etl.errors")
    }

    # Log initialization
    loggers["etl"].info(
        "Logging system initialized",
        extra={
            "log_level": args.log_level,
            "log_file": log_file
        }
    )

    return loggers


@contextmanager
def timed_operation(name: str, logger: logging.Logger = None):
    """
    Context manager to time an operation and log its duration.

    Args:
        name: Name of the operation being timed
        logger: Logger to use for recording the timing (default: performance_logger)
    """
    if logger is None:
        logger = logging.getLogger("etl.performance")

    start_time = time.time()
    try:
        yield
    finally:
        duration = (time.time() - start_time) * 1000  # Convert to milliseconds
        logger.info(
            f"{name} completed",
            extra={
                "operation": name,
                "duration_ms": duration
            }
        )


def validate_source_id(source_id: str) -> bool:
    """
    Validate that a source ID is properly formatted.

    Args:
        source_id: Source ID to validate

    Returns:
        bool: True if the source ID is valid

    Raises:
        ValueError: If the source ID is invalid
    """
    if not isinstance(source_id, str):
        raise ValueError(f"Source ID must be a string, got {type(source_id)}")

    if not source_id:
        raise ValueError("Source ID cannot be empty")

    if len(source_id) > 100:
        raise ValueError(f"Source ID is too long ({len(source_id)} chars, max 100)")

    return True


@handle_errors(logger=None, log_level="ERROR", default_message="Error during extraction")
def extract_data(source_id: str, fail_probability: float = 0.1, logger: logging.Logger = None) -> Dict[str, Any]:
    """
    Simulate extracting data from a source.

    Args:
        source_id: ID of the source to extract data from
        fail_probability: Probability of simulated failure (0-1)
        logger: Logger to use (default: extract_logger)

    Returns:
        Dict: Extracted data with messages and metadata

    Raises:
        ValueError: If source_id is invalid or connection fails
        RuntimeError: For other extraction errors
    """
    if logger is None:
        logger = logging.getLogger("etl.extract")

    # Validate source_id
    try:
        validate_source_id(source_id)
    except ValueError as e:
        logger.error(f"Invalid source ID: {str(e)}", extra={"source_id": source_id})
        raise

    logger.info(
        f"Starting data extraction",
        extra={"source_id": source_id, "fail_probability": fail_probability}
    )

    # Simulate work
    time.sleep(0.2)

    # Simulate occasional errors
    if random.random() < fail_probability:
        error_type = random.choice(["connection", "timeout", "data_format"])

        if error_type == "connection":
            logger.error(
                f"Connection error during extraction",
                extra={"source_id": source_id, "error_type": error_type}
            )
            raise ValueError(f"Failed to connect to source {source_id}: Connection refused")
        elif error_type == "timeout":
            logger.error(
                f"Timeout during extraction",
                extra={"source_id": source_id, "error_type": error_type}
            )
            raise ValueError(f"Timeout while extracting from source {source_id}")
        else:
            logger.error(
                f"Data format error during extraction",
                extra={"source_id": source_id, "error_type": error_type}
            )
            raise RuntimeError(f"Invalid data format in source {source_id}")

    # Generate sample data
    record_count = random.randint(50, 200)
    data = {
        "messages": [
            {
                "id": f"msg_{i}_{source_id}",
                "content": f"Sample message {i} from {source_id}",
                "timestamp": time.time() - random.randint(1, 1000)
            }
            for i in range(record_count)
        ],
        "metadata": {
            "source_id": source_id,
            "record_count": record_count,
            "extraction_time": time.time()
        }
    }

    logger.info(
        "Data extraction completed",
        extra={
            "source_id": source_id,
            "record_count": record_count
        }
    )

    return data


@handle_errors(logger=None, log_level="ERROR", default_message="Error during transformation")
def transform_data(raw_data: Dict[str, Any], logger: logging.Logger = None) -> Dict[str, Any]:
    """
    Simulate transforming raw data.

    Args:
        raw_data: Data to transform, must contain 'messages' and 'metadata'
        logger: Logger to use (default: transform_logger)

    Returns:
        Dict: Transformed data

    Raises:
        ValueError: If input data is invalid
        RuntimeError: For other transformation errors
    """
    if logger is None:
        logger = logging.getLogger("etl.transform")

    # Validate input data
    if not isinstance(raw_data, dict):
        logger.error(f"Invalid data type: {type(raw_data)}")
        raise ValueError(f"Expected dict for raw_data, got {type(raw_data)}")

    if "messages" not in raw_data:
        logger.error("Missing 'messages' in raw data")
        raise ValueError("Raw data is missing required 'messages' field")

    if "metadata" not in raw_data:
        logger.error("Missing 'metadata' in raw data")
        raise ValueError("Raw data is missing required 'metadata' field")

    if not isinstance(raw_data["messages"], list):
        logger.error(f"Invalid messages type: {type(raw_data['messages'])}")
        raise ValueError(f"Expected list for messages, got {type(raw_data['messages'])}")

    logger.info(
        "Starting data transformation",
        extra={
            "record_count": len(raw_data.get("messages", [])),
            "source_id": raw_data.get("metadata", {}).get("source_id", "unknown")
        }
    )

    # Simulate work
    time.sleep(0.3)

    # Simulate occasional missing fields and handle gracefully
    transformed_records = []
    skipped_records = 0

    for msg in raw_data.get("messages", []):
        try:
            if not all(key in msg for key in ["id", "content", "timestamp"]):
                logger.warning(
                    "Skipping message with missing fields",
                    extra={
                        "message_data": msg,
                        "source_id": raw_data.get("metadata", {}).get("source_id", "unknown")
                    }
                )
                skipped_records += 1
                continue

            transformed_records.append({
                "message_id": msg["id"],
                "message_text": msg["content"],
                "timestamp": msg["timestamp"],
                "processed_at": time.time(),
                "source_id": raw_data.get("metadata", {}).get("source_id", "unknown")
            })
        except Exception as e:
            logger.warning(
                f"Error transforming message: {str(e)}",
                extra={
                    "message_data": msg,
                    "error": str(e),
                    "source_id": raw_data.get("metadata", {}).get("source_id", "unknown")
                }
            )
            skipped_records += 1

    # Create transformed data structure
    transformed_data = {
        "records": transformed_records,
        "metadata": raw_data.get("metadata", {}),
        "transformation_stats": {
            "input_count": len(raw_data.get("messages", [])),
            "output_count": len(transformed_records),
            "skipped_count": skipped_records,
            "transformation_time": time.time()
        }
    }

    # Log transformation metrics
    logger.info(
        "Data transformation completed",
        extra={
            "input_record_count": len(raw_data.get("messages", [])),
            "output_record_count": len(transformed_records),
            "skipped_record_count": skipped_records,
            "source_id": raw_data.get("metadata", {}).get("source_id", "unknown")
        }
    )

    return transformed_data


@log_execution_time(logger=None)
def load_data(transformed_data: Dict[str, Any], context: ETLContext, logger: logging.Logger = None) -> Dict[str, Any]:
    """
    Load transformed data using the ETL Loader.

    Args:
        transformed_data: Data to load, must contain 'records' and 'metadata'
        context: ETL context with database connection
        logger: Logger to use (default: load_logger)

    Returns:
        Dict: Results of the loading operation

    Raises:
        ValueError: If input data is invalid
        RuntimeError: For database connection or query errors
    """
    if logger is None:
        logger = logging.getLogger("etl.load")

    # Validate input data
    if not isinstance(transformed_data, dict):
        logger.error(f"Invalid data type: {type(transformed_data)}")
        raise ValueError(f"Expected dict for transformed_data, got {type(transformed_data)}")

    if "records" not in transformed_data:
        logger.error("Missing 'records' in transformed data")
        raise ValueError("Transformed data is missing required 'records' field")

    if not isinstance(transformed_data["records"], list):
        logger.error(f"Invalid records type: {type(transformed_data['records'])}")
        raise ValueError(f"Expected list for records, got {type(transformed_data['records'])}")

    # Validate context
    if not hasattr(context, "db_connection") or context.db_connection is None:
        logger.error("Missing database connection in context")
        raise ValueError("ETL context is missing required database connection")

    logger.info(
        "Starting data loading",
        extra={
            "record_count": len(transformed_data.get("records", [])),
            "source_id": transformed_data.get("metadata", {}).get("source_id", "unknown"),
            "batch_id": getattr(context, "batch_id", "unknown")
        }
    )

    # Create loader with the database connection from the context
    try:
        loader = Loader(db_connection=context.db_connection)

        # Log connection info
        logger.info(
            "Using database connection",
            extra={
                "mock_connection": isinstance(context.db_connection, MockDatabase),
                "connection_id": id(context.db_connection),
                "connection_type": type(context.db_connection).__name__
            }
        )
    except Exception as e:
        logger.error(
            f"Failed to create loader: {str(e)}",
            exc_info=True,
            extra={
                "error": str(e),
                "error_type": type(e).__name__
            }
        )
        raise RuntimeError(f"Failed to create loader: {str(e)}")

    try:
        # Simulate loading data
        cursor = context.db_connection.cursor()

        # Simulate inserting records
        record_count = len(transformed_data.get("records", []))
        batch_size = min(100, record_count)
        batches = (record_count + batch_size - 1) // batch_size

        success_count = 0
        error_count = 0

        for i in range(batches):
            start_idx = i * batch_size
            end_idx = min((i + 1) * batch_size, record_count)
            batch_records = transformed_data["records"][start_idx:end_idx]

            # Log the batch insert
            logger.info(
                f"Inserting batch {i+1}/{batches}",
                extra={
                    "batch_size": len(batch_records),
                    "start_index": start_idx,
                    "end_index": end_idx,
                    "source_id": transformed_data.get("metadata", {}).get("source_id", "unknown"),
                    "batch_id": getattr(context, "batch_id", "unknown")
                }
            )

            try:
                # Simulate the query execution
                query = "INSERT INTO messages (message_id, message_text, timestamp, processed_at, source_id) VALUES (%s, %s, %s, %s, %s)"

                # In a real scenario, we would execute this for each record
                for record in batch_records:
                    # Simulate success for most records
                    if random.random() < 0.05:  # 5% chance of failure
                        logger.warning(
                            f"Failed to insert record {record['message_id']}",
                            extra={
                                "message_id": record["message_id"],
                                "source_id": record.get("source_id", "unknown")
                            }
                        )
                        error_count += 1
                    else:
                        cursor.execute(
                            query,
                            (
                                record["message_id"],
                                record["message_text"],
                                record["timestamp"],
                                record["processed_at"],
                                record.get("source_id", "unknown")
                            )
                        )
                        success_count += 1

            except Exception as e:
                logger.error(
                    f"Error inserting batch {i+1}/{batches}: {str(e)}",
                    exc_info=True,
                    extra={
                        "batch_number": i+1,
                        "total_batches": batches,
                        "error": str(e),
                        "source_id": transformed_data.get("metadata", {}).get("source_id", "unknown")
                    }
                )
                error_count += len(batch_records)

        # Log success
        result = {
            "records_processed": record_count,
            "records_succeeded": success_count,
            "records_failed": error_count,
            "batches_processed": batches,
            "success": error_count == 0
        }

        logger.info(
            "Data loading completed",
            extra={
                "record_count": record_count,
                "success_count": success_count,
                "error_count": error_count,
                "success": error_count == 0,
                "source_id": transformed_data.get("metadata", {}).get("source_id", "unknown")
            }
        )

        return result
    except Exception as e:
        # Log error
        logger.error(
            f"Data loading failed: {str(e)}",
            exc_info=True,
            extra={
                "record_count": len(transformed_data.get("records", [])),
                "success": False,
                "error": str(e),
                "error_type": type(e).__name__,
                "source_id": transformed_data.get("metadata", {}).get("source_id", "unknown")
            }
        )
        raise RuntimeError(f"Data loading failed: {str(e)}")
    finally:
        # No need to close the mock connection
        pass


def simulate_database_queries(db_connection, logger: logging.Logger = None):
    """
    Simulate database queries and log them.

    Args:
        db_connection: Database connection to use
        logger: Logger to use (default: db_logger)
    """
    if logger is None:
        logger = logging.getLogger("etl.database")

    queries = [
        {
            "description": "Select recent messages",
            "query": "SELECT * FROM messages WHERE timestamp > %s",
            "params": {"timestamp": time.time() - 86400},
            "duration": random.uniform(0.05, 0.2),
            "rows": random.randint(10, 100)
        },
        {
            "description": "Insert new message",
            "query": "INSERT INTO messages (id, content, timestamp) VALUES (%s, %s, %s)",
            "params": {"id": f"msg_{int(time.time())}", "content": "Hello world", "timestamp": time.time()},
            "duration": random.uniform(0.01, 0.1),
            "rows": 1
        },
        {
            "description": "Update message status",
            "query": "UPDATE messages SET status = %s WHERE id = %s",
            "params": {"status": "processed", "id": f"msg_{int(time.time())}"},
            "duration": random.uniform(0.01, 0.05),
            "rows": 1
        }
    ]

    for query_info in queries:
        # Simulate query execution
        start_time = time.time()
        time.sleep(query_info["duration"])
        duration_ms = (time.time() - start_time) * 1000

        # Log query
        log_database_query(
            logger,
            query=query_info["query"],
            params=query_info["params"],
            duration_ms=duration_ms,
            rows_affected=query_info["rows"]
        )

        # Also log the description separately
        logger.info(
            query_info["description"],
            extra={
                "query_type": query_info["description"].split()[0].lower(),
                "query": query_info["query"]
            }
        )


def retry_operation(operation, max_retries: int = 3, retry_delay: float = 1.0,
                   logger: logging.Logger = None, **kwargs) -> Tuple[bool, Any]:
    """
    Retry an operation with exponential backoff.

    Args:
        operation: Function to retry
        max_retries: Maximum number of retry attempts
        retry_delay: Initial delay between retries (will increase exponentially)
        logger: Logger to use
        **kwargs: Arguments to pass to the operation

    Returns:
        Tuple[bool, Any]: (success, result)
    """
    if logger is None:
        logger = logging.getLogger("etl")

    last_exception = None

    for attempt in range(max_retries + 1):
        try:
            if attempt > 0:
                logger.info(
                    f"Retry attempt {attempt}/{max_retries}",
                    extra={"attempt": attempt, "max_retries": max_retries}
                )

            result = operation(**kwargs)
            return True, result

        except Exception as e:
            last_exception = e
            if attempt < max_retries:
                # Calculate backoff delay
                delay = retry_delay * (2 ** attempt)

                logger.warning(
                    f"Operation failed, retrying in {delay:.2f}s: {str(e)}",
                    extra={
                        "attempt": attempt + 1,
                        "max_retries": max_retries,
                        "retry_delay": delay,
                        "error": str(e),
                        "error_type": type(e).__name__
                    }
                )

                time.sleep(delay)
            else:
                logger.error(
                    f"Operation failed after {max_retries} retries: {str(e)}",
                    exc_info=True,
                    extra={
                        "max_retries": max_retries,
                        "error": str(e),
                        "error_type": type(e).__name__
                    }
                )

    return False, last_exception


def run_etl_pipeline(source_id: str, batch_id: str, fail_probability: float = 0.1,
                    loggers: Dict[str, logging.Logger] = None) -> bool:
    """
    Run the complete ETL pipeline with proper logging and retry mechanisms.

    Args:
        source_id: ID of the source to process
        batch_id: ID of the batch to process
        fail_probability: Probability of simulated failures (0-1)
        loggers: Dictionary of loggers to use

    Returns:
        bool: True if the pipeline completed successfully
    """
    if loggers is None:
        loggers = {
            "etl": logging.getLogger("etl"),
            "extract": logging.getLogger("etl.extract"),
            "transform": logging.getLogger("etl.transform"),
            "load": logging.getLogger("etl.load"),
            "db": logging.getLogger("etl.database"),
            "performance": logging.getLogger("etl.performance"),
            "error": logging.getLogger("etl.errors")
        }

    # Define a mock database configuration
    db_config = {
        "host": os.environ.get("DB_HOST", "mock-db-server"),
        "port": int(os.environ.get("DB_PORT", "5432")),
        "database": os.environ.get("DB_NAME", "mock_skype_data"),
        "user": os.environ.get("DB_USER", "mock_user"),
        "password": os.environ.get("DB_PASSWORD", "mock_password"),
        "connect_timeout": int(os.environ.get("DB_CONNECT_TIMEOUT", "10"))
    }

    # Create output directory for ETL context
    output_dir = os.path.join(os.getcwd(), "output", "etl", batch_id)
    os.makedirs(output_dir, exist_ok=True)

    # Initialize the ETL context with the required parameters
    context = ETLContext(db_config=db_config, output_dir=output_dir)
    context.request_id = f"req_{int(time.time())}"
    context.batch_id = batch_id

    # Set up mock database
    try:
        mock_db = MockDatabase()
        context.db_connection = mock_db.conn
    except Exception as e:
        loggers["etl"].error(
            f"Failed to set up mock database: {str(e)}",
            exc_info=True,
            extra={
                "source_id": source_id,
                "batch_id": batch_id,
                "error": str(e),
                "error_type": type(e).__name__
            }
        )
        return False

    # Use LogContext to track the ETL operation across all logs
    with LogContext(request_id=context.request_id, batch_id=batch_id, source_id=source_id):
        loggers["etl"].info(
            f"Starting ETL pipeline",
            extra={
                "source_id": source_id,
                "batch_id": batch_id,
                "fail_probability": fail_probability,
                "db_config": {k: v if k != "password" else "******" for k, v in db_config.items()}
            }
        )

        try:
            # Extract phase with retry
            with timed_operation("extract_phase", logger=loggers["performance"]):
                success, extract_result = retry_operation(
                    extract_data,
                    max_retries=2,
                    retry_delay=0.5,
                    logger=loggers["etl"],
                    source_id=source_id,
                    fail_probability=fail_probability
                )

                if not success:
                    loggers["etl"].error(
                        f"Extract phase failed after retries",
                        extra={
                            "source_id": source_id,
                            "batch_id": batch_id,
                            "error": str(extract_result) if extract_result else "Unknown error"
                        }
                    )
                    return False

                raw_data = extract_result

            # Log metrics after extract
            log_metrics(
                loggers["performance"],
                {
                    "phase": "extract",
                    "record_count": len(raw_data.get("messages", [])),
                    "source_id": source_id,
                    "batch_id": batch_id
                },
                message="Extract phase completed"
            )

            # Transform phase
            with timed_operation("transform_phase", logger=loggers["performance"]):
                success, transform_result = retry_operation(
                    transform_data,
                    max_retries=1,
                    retry_delay=0.5,
                    logger=loggers["etl"],
                    raw_data=raw_data
                )

                if not success:
                    loggers["etl"].error(
                        f"Transform phase failed after retries",
                        extra={
                            "source_id": source_id,
                            "batch_id": batch_id,
                            "error": str(transform_result) if transform_result else "Unknown error"
                        }
                    )
                    return False

                transformed_data = transform_result

            # Log metrics after transform
            log_metrics(
                loggers["performance"],
                {
                    "phase": "transform",
                    "input_record_count": len(raw_data.get("messages", [])),
                    "output_record_count": len(transformed_data.get("records", [])),
                    "source_id": source_id,
                    "batch_id": batch_id
                },
                message="Transform phase completed"
            )

            # Load phase
            with timed_operation("load_phase", logger=loggers["performance"]):
                success, load_result = retry_operation(
                    load_data,
                    max_retries=2,
                    retry_delay=0.5,
                    logger=loggers["etl"],
                    transformed_data=transformed_data,
                    context=context
                )

                if not success:
                    loggers["etl"].error(
                        f"Load phase failed after retries",
                        extra={
                            "source_id": source_id,
                            "batch_id": batch_id,
                            "error": str(load_result) if load_result else "Unknown error"
                        }
                    )
                    return False

                result = load_result

            # Simulate and log database queries
            simulate_database_queries(context.db_connection, logger=loggers["db"])

            # Log final success
            loggers["etl"].info(
                "ETL pipeline completed successfully",
                extra={
                    "source_id": source_id,
                    "batch_id": batch_id,
                    "record_count": len(transformed_data.get("records", [])),
                    "result": result
                }
            )

            return True

        except Exception as e:
            # Log pipeline failure
            loggers["etl"].error(
                f"ETL pipeline failed: {str(e)}",
                exc_info=True,
                extra={
                    "source_id": source_id,
                    "batch_id": batch_id,
                    "error": str(e),
                    "error_type": type(e).__name__
                }
            )
            return False


def main():
    """
    Run the ETL logging example with command-line configuration.
    """
    # Parse command-line arguments
    args = parse_args()

    # Set up logging
    loggers = setup_logging(args)

    # Print configuration
    loggers["etl"].info(
        "Starting ETL logging example",
        extra={
            "batch_count": args.batch_count,
            "fail_probability": args.fail_probability,
            "log_level": args.log_level
        }
    )

    # Track overall statistics
    stats = {
        "total_batches": args.batch_count,
        "successful_batches": 0,
        "failed_batches": 0,
        "start_time": time.time()
    }

    # Run multiple batches
    for i in range(args.batch_count):
        batch_id = f"batch_{int(time.time())}_{i}"
        source_id = f"source_{i}"

        loggers["etl"].info(
            f"Processing batch {i+1}/{args.batch_count}",
            extra={"batch_id": batch_id, "source_id": source_id}
        )

        success = run_etl_pipeline(
            source_id=source_id,
            batch_id=batch_id,
            fail_probability=args.fail_probability,
            loggers=loggers
        )

        if success:
            stats["successful_batches"] += 1
            loggers["etl"].info(
                f"Batch {i+1}/{args.batch_count} processed successfully",
                extra={"batch_id": batch_id, "source_id": source_id}
            )
        else:
            stats["failed_batches"] += 1
            loggers["etl"].warning(
                f"Batch {i+1}/{args.batch_count} processing failed",
                extra={"batch_id": batch_id, "source_id": source_id}
            )

    # Calculate total duration
    stats["end_time"] = time.time()
    stats["duration_seconds"] = stats["end_time"] - stats["start_time"]

    # Log summary
    loggers["etl"].info(
        "ETL logging example completed",
        extra={
            "total_batches": stats["total_batches"],
            "successful_batches": stats["successful_batches"],
            "failed_batches": stats["failed_batches"],
            "success_rate": stats["successful_batches"] / stats["total_batches"] if stats["total_batches"] > 0 else 0,
            "duration_seconds": stats["duration_seconds"]
        }
    )

    # Print summary to console
    print("\n== ETL Process Summary ==")
    print(f"Total batches processed: {stats['total_batches']}")
    print(f"Successful batches: {stats['successful_batches']}")
    print(f"Failed batches: {stats['failed_batches']}")
    print(f"Success rate: {stats['successful_batches'] / stats['total_batches'] * 100:.1f}%")
    print(f"Total duration: {stats['duration_seconds']:.2f} seconds")
    print(f"Log file: {args.log_file or os.path.join(os.getcwd(), 'logs', 'etl', 'etl_example.log')}")


if __name__ == "__main__":
    main()