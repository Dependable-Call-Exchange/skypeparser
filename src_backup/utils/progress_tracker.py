"""
Progress tracker for ETL pipeline.

This module provides the ProgressTracker class that handles
progress tracking for ETL processes.
"""

import logging
import time
from typing import Dict, Any, Optional

from src.utils.new_structured_logging import get_logger

logger = get_logger(__name__)


class ProgressTracker:
    """Handles progress tracking."""

    def __init__(self):
        """Initialize the progress tracker."""
        self.current = 0
        self.total = 0
        self.start_time = None
        self.last_update_time = None
        self.last_log_time = None
        self.log_interval = 5.0  # Log progress every 5 seconds
        self.metrics = {
            "items_per_second": 0.0,
            "estimated_time_remaining": 0.0,
            "percent_complete": 0.0,
        }

    def update(self, current: int, total: int) -> Dict[str, Any]:
        """
        Update progress.

        Args:
            current: Current progress
            total: Total items to process

        Returns:
            Dictionary with progress metrics
        """
        # Initialize start time if not set
        if self.start_time is None:
            self.start_time = time.time()
            self.last_update_time = self.start_time
            self.last_log_time = self.start_time

        # Update current and total
        self.current = current
        self.total = total

        # Calculate progress metrics
        now = time.time()
        elapsed = now - self.start_time
        elapsed_since_last_update = now - self.last_update_time

        # Calculate percentage
        percent_complete = (current / total) * 100 if total > 0 else 0

        # Calculate items per second
        if elapsed > 0:
            items_per_second = current / elapsed
        else:
            items_per_second = 0

        # Calculate estimated time remaining
        if items_per_second > 0:
            estimated_time_remaining = (total - current) / items_per_second
        else:
            estimated_time_remaining = 0

        # Update metrics
        self.metrics = {
            "current": current,
            "total": total,
            "percent_complete": percent_complete,
            "elapsed_seconds": elapsed,
            "items_per_second": items_per_second,
            "estimated_time_remaining": estimated_time_remaining,
        }

        # Log progress at regular intervals
        if self.last_log_time is None or (now - self.last_log_time) >= self.log_interval:
            self._log_progress()
            self.last_log_time = now

        # Update last update time
        self.last_update_time = now

        return self.metrics

    def _log_progress(self) -> None:
        """Log progress."""
        if self.total <= 0:
            return

        # Format metrics for logging
        percent = self.metrics["percent_complete"]
        items_per_second = self.metrics["items_per_second"]
        remaining = self.metrics["estimated_time_remaining"]

        # Format remaining time
        if remaining < 60:
            remaining_str = f"{remaining:.1f} seconds"
        elif remaining < 3600:
            remaining_str = f"{remaining / 60:.1f} minutes"
        else:
            remaining_str = f"{remaining / 3600:.1f} hours"

        # Log progress
        logger.info(
            f"Progress: {self.current}/{self.total} ({percent:.1f}%) - "
            f"{items_per_second:.1f} items/sec - "
            f"Estimated time remaining: {remaining_str}",
            extra={
                "progress": {
                    "current": self.current,
                    "total": self.total,
                    "percent": percent,
                    "items_per_second": items_per_second,
                    "estimated_time_remaining": remaining,
                }
            }
        )

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get progress metrics.

        Returns:
            Dictionary with progress metrics
        """
        return self.metrics

    def reset(self) -> None:
        """Reset progress tracker."""
        self.current = 0
        self.total = 0
        self.start_time = None
        self.last_update_time = None
        self.last_log_time = None
        self.metrics = {
            "items_per_second": 0.0,
            "estimated_time_remaining": 0.0,
            "percent_complete": 0.0,
        }