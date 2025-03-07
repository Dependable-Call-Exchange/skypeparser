#!/usr/bin/env python3
"""
Example demonstrating the ETLContext phase management API.

This example focuses specifically on the phase management methods of the ETLContext class,
showing how to properly start phases, update progress, end phases, and handle errors.
"""

import logging
import os
import sys
import time
from typing import Any, Dict, Optional

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.db.etl.context import ETLContext

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def demonstrate_basic_phase_management():
    """
    Demonstrate basic phase management with ETLContext.

    This function shows the basic usage of start_phase, update_progress, and end_phase.
    """
    logger.info("=== Basic Phase Management Example ===")

    # Create a simple ETL context
    context = ETLContext(db_config={"host": "localhost", "dbname": "example"})
    logger.info(f"Created ETL context with task ID: {context.task_id}")

    # Start the extract phase
    logger.info("Starting extract phase...")
    context.start_phase("extract", total_conversations=10, total_messages=500)

    # Simulate some work and update progress
    for i in range(5):
        time.sleep(0.2)  # Simulate work
        context.update_progress(conversations=2, messages=100)
        logger.info(f"Processed batch {i+1}/5: 2 conversations, 100 messages")

    # End the extract phase with results
    extract_result = context.end_phase(
        {
            "status": "completed",
            "conversations_processed": 10,
            "messages_processed": 500,
            "additional_info": "Extraction completed successfully",
        }
    )

    logger.info(
        f"Extract phase completed in {extract_result['duration_seconds']:.2f} seconds"
    )
    logger.info(f"Extract phase result: {extract_result}")


def demonstrate_phase_status_management():
    """
    Demonstrate phase status management with ETLContext.

    This function shows how to use get_phase_status and set_phase_status.
    """
    logger.info("\n=== Phase Status Management Example ===")

    # Create a simple ETL context
    context = ETLContext(db_config={"host": "localhost", "dbname": "example"})

    # Check initial phase status
    extract_status = context.get_phase_status("extract")
    logger.info(f"Initial extract phase status: {extract_status}")

    # Start the extract phase
    context.start_phase("extract")
    extract_status = context.get_phase_status("extract")
    logger.info(f"After starting, extract phase status: {extract_status}")

    # End the extract phase
    context.end_phase({"status": "completed"})
    extract_status = context.get_phase_status("extract")
    logger.info(f"After ending, extract phase status: {extract_status}")

    # Manually set a phase status
    logger.info("Manually setting transform phase status to 'failed'")
    context.set_phase_status("transform", "failed")
    transform_status = context.get_phase_status("transform")
    logger.info(f"Transform phase status: {transform_status}")

    # Check if we can proceed with load phase
    if transform_status == "failed":
        logger.info("Cannot proceed with load phase because transform phase failed")
    else:
        logger.info("Proceeding with load phase")
        context.start_phase("load")


def demonstrate_error_handling():
    """
    Demonstrate error handling with ETLContext.

    This function shows how to handle errors during phases.
    """
    logger.info("\n=== Error Handling Example ===")

    # Create a simple ETL context
    context = ETLContext(db_config={"host": "localhost", "dbname": "example"})

    try:
        # Start the extract phase
        context.start_phase("extract")
        logger.info("Started extract phase")

        # Simulate an error during extraction
        logger.info("Simulating an error during extraction...")
        raise ValueError("Simulated extraction error")

    except Exception as e:
        # Handle the error
        logger.error(f"Error during extraction: {str(e)}")

        # Record the error in the context
        context.record_error("extract", e)

        # Set the phase status to failed
        context.set_phase_status("extract", "failed")

        # Check the phase status
        extract_status = context.get_phase_status("extract")
        logger.info(f"After error, extract phase status: {extract_status}")

        # Get the error count
        error_count = len(context.errors.get("extract", []))
        logger.info(f"Error count for extract phase: {error_count}")


def demonstrate_complete_etl_flow():
    """
    Demonstrate a complete ETL flow with ETLContext.

    This function shows how to use ETLContext for a complete ETL process.
    """
    logger.info("\n=== Complete ETL Flow Example ===")

    # Create a simple ETL context
    context = ETLContext(db_config={"host": "localhost", "dbname": "example"})
    logger.info(f"Created ETL context with task ID: {context.task_id}")

    try:
        # Extract phase
        logger.info("Starting extract phase...")
        context.start_phase("extract", total_conversations=10, total_messages=500)

        # Simulate extraction work
        time.sleep(0.5)
        context.update_progress(conversations=10, messages=500)
        logger.info("Processed all conversations and messages")

        # End extract phase
        extract_result = context.end_phase(
            {
                "status": "completed",
                "conversations_processed": 10,
                "messages_processed": 500,
            }
        )
        logger.info(
            f"Extract phase completed in {extract_result['duration_seconds']:.2f} seconds"
        )

        # Transform phase
        logger.info("Starting transform phase...")
        context.start_phase(
            "transform",
            total_conversations=extract_result["conversations_processed"],
            total_messages=extract_result["messages_processed"],
        )

        # Simulate transformation work
        time.sleep(0.7)
        context.update_progress(conversations=5, messages=250)
        logger.info("Processed 50% of data")

        time.sleep(0.7)
        context.update_progress(conversations=5, messages=250)
        logger.info("Processed 100% of data")

        # End transform phase
        transform_result = context.end_phase(
            {
                "status": "completed",
                "conversations_processed": 10,
                "messages_processed": 500,
            }
        )
        logger.info(
            f"Transform phase completed in {transform_result['duration_seconds']:.2f} seconds"
        )

        # Load phase
        logger.info("Starting load phase...")
        context.start_phase("load")

        # Simulate loading work
        time.sleep(0.3)
        context.update_progress(conversations=10, messages=500)
        logger.info("Loaded all data")

        # End load phase
        load_result = context.end_phase(
            {
                "status": "completed",
                "conversations_processed": 10,
                "messages_processed": 500,
                "export_id": "EXP-12345",
            }
        )
        logger.info(
            f"Load phase completed in {load_result['duration_seconds']:.2f} seconds"
        )

        # Get summary
        summary = context.get_summary()
        logger.info(f"ETL process summary: {summary}")

    except Exception as e:
        logger.error(f"Error during ETL process: {str(e)}")

        # Handle the error based on the current phase
        if context.current_phase:
            context.set_phase_status(context.current_phase, "failed")
            context.record_error(context.current_phase, e)
            logger.error(f"Error occurred during {context.current_phase} phase")

        # Get summary even after error
        summary = context.get_summary()
        logger.info(f"ETL process summary after error: {summary}")


def demonstrate_phase_dependencies():
    """
    Demonstrate phase dependencies with ETLContext.

    This function shows how to check phase status before proceeding.
    """
    logger.info("\n=== Phase Dependencies Example ===")

    # Create a simple ETL context
    context = ETLContext(db_config={"host": "localhost", "dbname": "example"})

    # Start and complete extract phase
    context.start_phase("extract")
    context.end_phase({"status": "completed"})
    logger.info("Extract phase completed")

    # Check if extract phase completed successfully before starting transform
    if context.get_phase_status("extract") == "completed":
        logger.info(
            "Extract phase completed successfully, proceeding with transform phase"
        )
        context.start_phase("transform")

        # Simulate an error in transform phase
        logger.info("Simulating an error in transform phase")
        context.set_phase_status("transform", "failed")
        context.record_error("transform", ValueError("Simulated transform error"))
    else:
        logger.info(
            "Extract phase did not complete successfully, cannot proceed with transform"
        )

    # Check if transform phase completed successfully before starting load
    if context.get_phase_status("transform") == "completed":
        logger.info(
            "Transform phase completed successfully, proceeding with load phase"
        )
        context.start_phase("load")
        context.end_phase({"status": "completed"})
    else:
        logger.info(
            "Transform phase did not complete successfully, cannot proceed with load"
        )


if __name__ == "__main__":
    # Run all examples
    demonstrate_basic_phase_management()
    demonstrate_phase_status_management()
    demonstrate_error_handling()
    demonstrate_complete_etl_flow()
    demonstrate_phase_dependencies()

    logger.info("\nAll examples completed successfully!")
