#!/usr/bin/env python3
"""
Initialize Error Handling and Logging

This script initializes and tests the enhanced error handling and logging
infrastructure for the SkypeParser project.

Usage:
    python -m src.utils.initialize_error_handling

"""

import logging
import os
import sys
import time
from typing import Any, Dict

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from src.core_utils.exceptions import InvalidInputError, SkypeParserError
from src.logging.error_handling import ErrorContext, handle_errors, report_error
from src.validation.schema_validation import initialize_schemas
from src.logging.structured_logging import get_logger, log_execution_time, setup_logging

# Initialize logger
logger = get_logger(__name__)


@log_execution_time(logger)
def test_structured_logging():
    """Test structured logging functionality."""
    logger.info("Testing basic logging")

    # Test structured logging with extra parameters
    logger.info(
        "Testing structured logging with data",
        extra={
            "structured_data": {
                "user_id": "test123",
                "action": "initialize",
                "component": "error_handling",
            }
        },
    )

    # Test context-based logging
    with ErrorContext(request_id="12345", user="test_user"):
        logger.info(
            "Testing logging with context",
            extra={"structured_data": {"operation": "validate", "status": "success"}},
        )

    # Test nested context
    with ErrorContext(outer_context="level1"):
        logger.info("Outer context")

        with ErrorContext(inner_context="level2"):
            logger.info("Inner nested context")

            with ErrorContext(deepest_context="level3"):
                logger.info("Deepest nested context")

        logger.info("Back to outer context")


@handle_errors(error_types=[ValueError, TypeError], log_level="ERROR")
def function_with_error(x: int, y: int) -> int:
    """
    Test function that raises an error.

    Args:
        x: First number
        y: Second number

    Returns:
        Result of x / y
    """
    if y == 0:
        raise ValueError("Division by zero")
    if not isinstance(x, int) or not isinstance(y, int):
        raise TypeError("Arguments must be integers")
    return x // y


def test_error_handling():
    """Test error handling functionality."""
    logger.info("Testing error handling")

    # Test with valid arguments
    try:
        result = function_with_error(10, 2)
        logger.info(f"Success: 10 / 2 = {result}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")

    # Test with division by zero
    try:
        with ErrorContext(operation="division", values={"x": 10, "y": 0}):
            result = function_with_error(10, 0)
            logger.info(f"This should not execute: {result}")
    except ValueError as e:
        logger.info(f"Expected error caught: {e}")

    # Test custom error
    try:
        with ErrorContext(component="validation", data_type="user_input"):
            raise InvalidInputError("Invalid user ID format")
    except SkypeParserError as e:
        error_details = report_error(
            e,
            log_level="WARNING",
            additional_context={"suggestion": "Use valid UUID format"},
        )
        logger.info(f"Error reported with details: {error_details['error_type']}")


def test_schema_initialization():
    """Test schema initialization."""
    logger.info("Initializing schemas")

    # Initialize schemas
    initialize_schemas(overwrite=True)

    # Get base directory
    base_dir = os.path.dirname(os.path.dirname(__file__))
    schema_dir = os.path.join(base_dir, "schemas")

    # Check if schemas were created
    if os.path.exists(schema_dir):
        schemas = [f for f in os.listdir(schema_dir) if f.endswith(".json")]
        logger.info(f"Created {len(schemas)} schemas: {', '.join(schemas)}")
    else:
        logger.error(f"Schema directory was not created: {schema_dir}")


def main():
    """Main function to test error handling and logging."""
    # Set up structured logging
    log_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "logs"
    )
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)

    # Set up logging
    setup_logging(
        level="DEBUG",
        log_file=os.path.join(log_dir, "error_handling_test.log"),
        json_format=False,
        structured=True,
        rotation="size",
    )

    # Welcome message
    logger.info("=" * 50)
    logger.info("Testing Error Handling and Logging")
    logger.info("=" * 50)

    # Run tests
    test_structured_logging()
    logger.info("-" * 50)

    test_error_handling()
    logger.info("-" * 50)

    test_schema_initialization()
    logger.info("-" * 50)

    logger.info("All tests completed")


if __name__ == "__main__":
    main()
