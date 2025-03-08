#!/usr/bin/env python3
"""
Test Centralized Logging Script

This script tests the centralized logging system by demonstrating its various features.
"""

import os
import sys
import time
import json
import logging
import argparse
from pathlib import Path
from datetime import datetime

# Add the parent directory to the path so we can import the modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.utils.new_structured_logging import (
    get_logger,
    log_execution_time,
    log_call,
    handle_errors,
    with_context,
    LogContext,
    log_metrics,
    get_system_metrics,
    log_database_query,
    measure_execution_time,
    create_request_context,
    with_request_context
)
from src.utils.logging_config import configure_logging


def test_basic_logging():
    """Test basic logging functionality."""
    logger = get_logger("test.basic")

    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")

    # Log with extra data
    logger.info(
        "This is a message with extra data",
        extra={
            "user_id": "user123",
            "action": "login",
            "ip_address": "192.168.1.1"
        }
    )

    # Log with metrics
    logger.info(
        "This is a message with metrics",
        extra={
            "metrics": {
                "duration_ms": 123.45,
                "items_processed": 100,
                "success_rate": 0.98
            }
        }
    )


def test_context_tracking():
    """Test context tracking functionality."""
    logger = get_logger("test.context")

    # Using context manager
    with LogContext(request_id="req123", user_id="user456"):
        logger.info("This message has context from LogContext")

        # Nested context
        with LogContext(action="search", query="python logging"):
            logger.info("This message has nested context")

    # Context should be cleared now
    logger.info("This message should not have the previous context")

    # Using decorator
    @with_context(operation="test_operation", component="test_component")
    def test_function():
        logger = get_logger("test.context.function")
        logger.info("This message has context from decorator")

    test_function()

    # Using request context
    @with_request_context
    def handle_request():
        logger = get_logger("test.context.request")
        logger.info("This message has request context")

    handle_request()


def test_performance_monitoring():
    """Test performance monitoring functionality."""
    logger = get_logger("test.performance")

    # Using execution time decorator
    @log_execution_time(logger)
    def slow_function():
        time.sleep(0.5)
        return "result"

    slow_function()

    # Using manual measurement
    def another_slow_function():
        time.sleep(0.3)
        return "another result"

    result, duration_ms = measure_execution_time(another_slow_function)
    logger.info(
        f"Function completed in {duration_ms:.2f} ms",
        extra={"metrics": {"duration_ms": duration_ms}}
    )

    # Log system metrics
    log_metrics(logger, get_system_metrics())

    # Log database query
    log_database_query(
        logger,
        "SELECT * FROM users WHERE id = %s",
        {"id": 123},
        duration_ms=15.67,
        rows_affected=1
    )


def test_error_handling():
    """Test error handling functionality."""
    logger = get_logger("test.errors")

    # Using error handler decorator
    @handle_errors(logger, log_level="ERROR", default_message="Function failed")
    def function_with_error():
        raise ValueError("This is a test error")

    try:
        function_with_error()
    except ValueError:
        pass  # Error was re-raised

    # Using error handler with no re-raise
    @handle_errors(logger, log_level="ERROR", default_message="Function failed", reraise=False)
    def function_with_error_no_reraise():
        raise ValueError("This is another test error")

    function_with_error_no_reraise()  # No exception raised

    # Manual error logging
    try:
        1 / 0
    except Exception as e:
        logger.error(
            f"Error occurred: {e}",
            exc_info=True,
            extra={
                "operation": "division",
                "inputs": {"numerator": 1, "denominator": 0}
            }
        )


def test_etl_workflow():
    """Test logging in an ETL workflow."""
    logger = get_logger("test.etl")

    # Extract phase
    with LogContext(phase="extract", file_path="data.csv"):
        logger.info("Starting extract phase")

        # Log progress
        total_records = 1000
        for i in range(0, total_records, 100):
            current = min(i + 100, total_records)
            percentage = int((current / total_records) * 100)
            logger.info(
                f"Extracted {current}/{total_records} records ({percentage}%)",
                extra={
                    "metrics": {
                        "records_processed": current,
                        "total_records": total_records,
                        "percentage": percentage
                    }
                }
            )
            time.sleep(0.1)  # Simulate work

        logger.info(
            "Extract phase completed",
            extra={
                "metrics": {
                    "total_records": total_records,
                    "duration_ms": 500
                }
            }
        )

    # Transform phase
    with LogContext(phase="transform"):
        logger.info("Starting transform phase")

        # Log progress
        for i in range(0, total_records, 200):
            current = min(i + 200, total_records)
            percentage = int((current / total_records) * 100)
            logger.info(
                f"Transformed {current}/{total_records} records ({percentage}%)",
                extra={
                    "metrics": {
                        "records_processed": current,
                        "total_records": total_records,
                        "percentage": percentage
                    }
                }
            )
            time.sleep(0.05)  # Simulate work

        logger.info(
            "Transform phase completed",
            extra={
                "metrics": {
                    "total_records": total_records,
                    "duration_ms": 300
                }
            }
        )

    # Load phase
    with LogContext(phase="load", target="database"):
        logger.info("Starting load phase")

        # Log database operations
        batch_size = 250
        for i in range(0, total_records, batch_size):
            current = min(i + batch_size, total_records)
            percentage = int((current / total_records) * 100)

            # Log database query
            query = "INSERT INTO records (id, value) VALUES (%s, %s)"
            log_database_query(
                logger,
                query,
                {"batch_size": batch_size},
                duration_ms=25.5,
                rows_affected=batch_size
            )

            logger.info(
                f"Loaded {current}/{total_records} records ({percentage}%)",
                extra={
                    "metrics": {
                        "records_processed": current,
                        "total_records": total_records,
                        "percentage": percentage,
                        "batch_size": batch_size
                    }
                }
            )
            time.sleep(0.08)  # Simulate work

        logger.info(
            "Load phase completed",
            extra={
                "metrics": {
                    "total_records": total_records,
                    "duration_ms": 400,
                    "batches": total_records // batch_size + (1 if total_records % batch_size > 0 else 0)
                }
            }
        )

    # Overall workflow completion
    logger.info(
        "ETL workflow completed successfully",
        extra={
            "metrics": {
                "total_duration_ms": 1200,
                "total_records": total_records
            }
        }
    )


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Test centralized logging system")
    parser.add_argument(
        "--log-file",
        default="centralized_logging_test.log",
        help="Log file path"
    )
    parser.add_argument(
        "--log-level",
        default="DEBUG",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level"
    )
    parser.add_argument(
        "--no-console",
        action="store_true",
        help="Disable console logging"
    )
    parser.add_argument(
        "--no-json",
        action="store_true",
        help="Disable JSON formatting"
    )

    args = parser.parse_args()

    # Configure logging
    configure_logging(
        app_name="centralized_logging_test",
        environment="development",
        log_level=args.log_level,
        log_file=args.log_file,
        enable_console=not args.no_console,
        enable_json=not args.no_json
    )

    # Get logger for this module
    logger = get_logger(__name__)
    logger.info("Starting centralized logging tests")

    # Run tests
    test_basic_logging()
    test_context_tracking()
    test_performance_monitoring()
    test_error_handling()
    test_etl_workflow()

    logger.info("Centralized logging tests completed")

    # Print log file location
    print(f"Log file: {os.path.abspath(args.log_file)}")

    return 0


if __name__ == "__main__":
    sys.exit(main())