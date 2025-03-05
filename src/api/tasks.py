"""
Task Queue Module for Skype Parser API

This module provides asynchronous task processing for the Skype Parser API
using Celery. It handles long-running tasks such as processing large Skype
export files.
"""

import os
import logging
import tempfile
from typing import Dict, Any, Optional

from celery import Celery
from celery.signals import task_success, task_failure

# Import the new ETL pipeline
from src.db import ETLPipeline, ETLContext
from src.db.progress_tracker import get_tracker, create_tracker, remove_tracker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize Celery
celery_app = Celery('skype_parser')

# Configure Celery
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
)

@celery_app.task(bind=True, name='process_skype_export')
def process_skype_export(
    self,
    file_path: str,
    user_display_name: Optional[str] = None,
    db_config: Optional[Dict[str, Any]] = None,
    output_dir: Optional[str] = None,
    task_id: Optional[str] = None,
    cleanup: bool = True
) -> Dict[str, Any]:
    """
    Process a Skype export file asynchronously.

    Args:
        file_path: Path to the Skype export file
        user_display_name: Display name of the user
        db_config: Database configuration
        output_dir: Output directory for transformed data
        task_id: Task ID for progress tracking
        cleanup: Whether to clean up the file after processing

    Returns:
        dict: Results of the ETL pipeline
    """
    # Use the Celery task ID if no task ID is provided
    task_id = task_id or self.request.id

    try:
        # Create a progress tracker for this task
        tracker = get_tracker(task_id) or create_tracker(task_id)
        tracker.update(status="starting", message="Starting asynchronous processing...")

        # Create an ETL context with the task ID
        context = ETLContext(
            db_config=db_config,
            output_dir=output_dir,
            task_id=task_id
        )

        # Initialize the ETL pipeline with the context
        pipeline = ETLPipeline(
            context=context
        )

        # Run the pipeline
        results = pipeline.run_pipeline(
            file_path=file_path,
            user_display_name=user_display_name
        )

        # Add task ID to results
        results['task_id'] = task_id

        # Mark task as complete
        tracker.complete(message="Processing completed successfully")

        # Clean up the file if requested
        if cleanup and os.path.exists(file_path):
            try:
                os.remove(file_path)
                logger.info(f"Cleaned up temporary file: {file_path}")
            except Exception as e:
                logger.warning(f"Failed to clean up temporary file {file_path}: {e}")

        # Convert results to the format expected by the API
        # This maintains backward compatibility with code that expects the old format
        if results.get('success', False):
            # Extract conversation and message counts
            conversation_count = 0
            message_count = 0

            for phase, stats in results.get('phases', {}).items():
                if phase == 'transform' and stats:
                    conversation_count = stats.get('processed_conversations', 0)
                    message_count = stats.get('processed_messages', 0)

            # Add backward compatibility fields
            results['conversations'] = conversation_count
            results['message_count'] = message_count

        return results

    except Exception as e:
        logger.error(f"Error processing Skype export: {e}", exc_info=True)

        # Update the tracker with the error
        tracker = get_tracker(task_id)
        if tracker:
            tracker.error(str(e))

        # Return error results
        return {
            'success': False,
            'error': str(e),
            'task_id': task_id
        }

@task_success.connect(sender='process_skype_export')
def on_task_success(sender=None, result=None, **kwargs):
    """
    Handle successful task completion.
    """
    if result and 'task_id' in result:
        task_id = result['task_id']
        logger.info(f"Task {task_id} completed successfully")

        # Clean up the tracker
        remove_tracker(task_id)

@task_failure.connect(sender='process_skype_export')
def on_task_failure(sender=None, task_id=None, exception=None, **kwargs):
    """
    Handle task failure.
    """
    logger.error(f"Task {task_id} failed: {exception}")

    # Update the tracker with the error
    tracker = get_tracker(task_id)
    if tracker:
        tracker.error(str(exception))
        remove_tracker(task_id)

def submit_task(
    file_path: str,
    user_display_name: Optional[str] = None,
    db_config: Optional[Dict[str, Any]] = None,
    output_dir: Optional[str] = None,
    cleanup: bool = True
) -> str:
    """
    Submit a task to process a Skype export file asynchronously.

    Args:
        file_path: Path to the Skype export file
        user_display_name: Display name of the user
        db_config: Database configuration
        output_dir: Output directory for transformed data
        cleanup: Whether to clean up the file after processing

    Returns:
        str: Task ID
    """
    # Submit the task
    task = process_skype_export.delay(
        file_path=file_path,
        user_display_name=user_display_name,
        db_config=db_config,
        output_dir=output_dir,
        cleanup=cleanup
    )

    # Create a progress tracker for this task
    create_tracker(task.id)

    logger.info(f"Submitted task {task.id} to process Skype export file: {file_path}")

    return task.id