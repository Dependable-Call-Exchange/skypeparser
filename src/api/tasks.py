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

from src.db.etl_pipeline import SkypeETLPipeline
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
    broker_url=os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0'),
    result_backend=os.environ.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0'),
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
        output_dir: Directory for output files
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

        # Initialize the ETL pipeline
        pipeline = SkypeETLPipeline(
            db_config=db_config,
            output_dir=output_dir
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

        return results

    except Exception as e:
        logger.error(f"Error processing Skype export: {e}", exc_info=True)

        # Mark task as failed
        if tracker := get_tracker(task_id):
            tracker.fail(message=f"Processing failed: {str(e)}")

        # Re-raise the exception to mark the task as failed
        raise


@task_success.connect(sender='process_skype_export')
def on_task_success(sender=None, result=None, **kwargs):
    """Handle successful task completion."""
    if result and isinstance(result, dict) and 'task_id' in result:
        task_id = result['task_id']
        logger.info(f"Task {task_id} completed successfully")

        # Keep the tracker around for a while to allow clients to fetch the final status
        # In a production system, you might want to use a scheduled task to clean up old trackers


@task_failure.connect(sender='process_skype_export')
def on_task_failure(sender=None, task_id=None, exception=None, **kwargs):
    """Handle task failure."""
    logger.error(f"Task {task_id} failed: {exception}")

    # Keep the tracker around for a while to allow clients to fetch the final status
    # In a production system, you might want to use a scheduled task to clean up old trackers


def submit_task(
    file_path: str,
    user_display_name: Optional[str] = None,
    db_config: Optional[Dict[str, Any]] = None,
    output_dir: Optional[str] = None,
    cleanup: bool = True
) -> str:
    """
    Submit a task to process a Skype export file.

    Args:
        file_path: Path to the Skype export file
        user_display_name: Display name of the user
        db_config: Database configuration
        output_dir: Directory for output files
        cleanup: Whether to clean up the file after processing

    Returns:
        str: Task ID
    """
    # Submit the task to Celery
    task = process_skype_export.delay(
        file_path=file_path,
        user_display_name=user_display_name,
        db_config=db_config,
        output_dir=output_dir,
        cleanup=cleanup
    )

    # Return the task ID
    return task.id