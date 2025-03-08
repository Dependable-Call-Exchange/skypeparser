"""
Progress Tracker Module

This module provides functionality for tracking progress during ETL operations
and emitting progress events to listeners.
"""

import logging
import time
from typing import Any, Callable, Dict, List, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ProgressTracker:
    """
    Tracks progress during ETL operations and emits progress events to listeners.

    This class provides methods for updating progress and notifying listeners
    about progress changes.
    """

    def __init__(self, task_id: str, total_steps: int = 100):
        """
        Initialize the progress tracker.

        Args:
            task_id: Unique identifier for the task
            total_steps: Total number of steps in the task
        """
        self.task_id = task_id
        self.total_steps = total_steps
        self.current_step = 0
        self.status = "initializing"
        self.message = "Initializing task..."
        self.start_time = time.time()
        self.listeners: List[Callable[[Dict[str, Any]], None]] = []
        self.export_id = None
        self.error = None

        # Emit initial progress
        self._emit_progress()

    def add_listener(self, listener: Callable[[Dict[str, Any]], None]) -> None:
        """
        Add a listener for progress events.

        Args:
            listener: Function to call when progress is updated
        """
        self.listeners.append(listener)

        # Send current progress to the new listener
        listener(self._get_progress_data())

    def update(
        self,
        step: Optional[int] = None,
        increment: int = 0,
        status: Optional[str] = None,
        message: Optional[str] = None,
        export_id: Optional[int] = None,
    ) -> None:
        """
        Update the progress.

        Args:
            step: Current step (if provided, overrides the current step)
            increment: Amount to increment the current step by
            status: Current status (if provided, updates the status)
            message: Current message (if provided, updates the message)
            export_id: Export ID (if provided, updates the export ID)
        """
        # Update step
        if step is not None:
            self.current_step = min(step, self.total_steps)
        else:
            self.current_step = min(self.current_step + increment, self.total_steps)

        # Update status and message
        if status is not None:
            self.status = status

        if message is not None:
            self.message = message

        # Update export ID
        if export_id is not None:
            self.export_id = export_id

        # Emit progress
        self._emit_progress()

    def complete(
        self,
        message: str = "Task completed successfully",
        export_id: Optional[int] = None,
    ) -> None:
        """
        Mark the task as complete.

        Args:
            message: Completion message
            export_id: Export ID (if provided, updates the export ID)
        """
        self.current_step = self.total_steps
        self.status = "completed"
        self.message = message
        if export_id is not None:
            self.export_id = export_id
        self._emit_progress()

    def error(self, message: str = "Task failed", error: Optional[str] = None) -> None:
        """
        Mark the task as failed with an error.

        Args:
            message: Failure message
            error: Error details
        """
        self.status = "failed"
        self.message = message
        self.error = error
        self._emit_progress()

    def fail(self, message: str = "Task failed") -> None:
        """
        Mark the task as failed.

        Args:
            message: Failure message
        """
        self.status = "failed"
        self.message = message
        self._emit_progress()

    def get_status(self) -> Dict[str, Any]:
        """
        Get a simplified status object suitable for API responses.

        Returns:
            Dict containing status information
        """
        percent_complete = (
            (self.current_step / self.total_steps) * 100 if self.total_steps > 0 else 0
        )

        status_obj = {
            "status": self.status,
            "progress": round(percent_complete),
            "message": self.message,
        }

        # Add export_id if available
        if self.export_id is not None:
            status_obj["export_id"] = self.export_id

        # Add error if available
        if self.error is not None:
            status_obj["error"] = self.error

        return status_obj

    def _emit_progress(self) -> None:
        """Emit progress to all listeners."""
        progress_data = self._get_progress_data()

        for listener in self.listeners:
            try:
                listener(progress_data)
            except Exception as e:
                logger.error(f"Error notifying listener: {e}")

    def _get_progress_data(self) -> Dict[str, Any]:
        """
        Get the current progress data.

        Returns:
            Dict containing progress data
        """
        elapsed_time = time.time() - self.start_time
        percent_complete = (
            (self.current_step / self.total_steps) * 100 if self.total_steps > 0 else 0
        )

        data = {
            "task_id": self.task_id,
            "status": self.status,
            "message": self.message,
            "current_step": self.current_step,
            "total_steps": self.total_steps,
            "percent_complete": round(percent_complete, 2),
            "elapsed_time": round(elapsed_time, 2),
            "timestamp": time.time(),
        }

        # Add export_id if available
        if self.export_id is not None:
            data["export_id"] = self.export_id

        # Add error if available
        if self.error is not None:
            data["error"] = self.error

        return data


# Global registry of progress trackers
_trackers: Dict[str, ProgressTracker] = {}


def get_tracker(task_id: str) -> Optional[ProgressTracker]:
    """
    Get a progress tracker by task ID.

    Args:
        task_id: Task ID to look up

    Returns:
        ProgressTracker if found, None otherwise
    """
    return _trackers.get(task_id)


def create_tracker(task_id: str, total_steps: int = 100) -> ProgressTracker:
    """
    Create a new progress tracker.

    Args:
        task_id: Unique identifier for the task
        total_steps: Total number of steps in the task

    Returns:
        Newly created ProgressTracker
    """
    tracker = ProgressTracker(task_id, total_steps)
    _trackers[task_id] = tracker
    return tracker


def remove_tracker(task_id: str) -> None:
    """
    Remove a progress tracker.

    Args:
        task_id: Task ID to remove
    """
    if task_id in _trackers:
        del _trackers[task_id]
