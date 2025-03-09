"""
Memory monitor for ETL pipeline.

This module provides the MemoryMonitor class that handles
memory monitoring for ETL processes.
"""

import gc
import logging
import os
import psutil
from typing import Dict, Any

from src.utils.new_structured_logging import get_logger, handle_errors, log_execution_time

logger = get_logger(__name__)


class MemoryMonitor:
    """Handles memory monitoring."""

    def __init__(self, memory_limit_mb: int = 1024):
        """
        Initialize the memory monitor.

        Args:
            memory_limit_mb: Memory limit in MB before forcing garbage collection
        """
        self.memory_limit_mb = memory_limit_mb
        self.last_memory_mb = 0
        self.peak_memory_mb = 0
        self.warning_threshold = 0.8  # 80% of memory limit
        self.critical_threshold = 0.95  # 95% of memory limit
        self.process = psutil.Process(os.getpid())

        # Log initialization
        logger.debug(
            "Initialized MemoryMonitor",
            extra={
                "memory_limit_mb": memory_limit_mb,
                "warning_threshold": self.warning_threshold,
                "critical_threshold": self.critical_threshold,
            }
        )

    @log_execution_time(level=logging.DEBUG)
    @handle_errors(log_level="ERROR", default_message="Error checking memory")
    def check_memory(self) -> Dict[str, Any]:
        """
        Check current memory usage.

        Returns:
            Dict with memory usage information
        """
        # Get memory usage
        memory_info = self.process.memory_info()
        used_mb = memory_info.rss / (1024 * 1024)  # Convert to MB

        # Update peak memory
        if used_mb > self.peak_memory_mb:
            self.peak_memory_mb = used_mb

        # Calculate percentage of memory limit
        percent = (used_mb / self.memory_limit_mb) * 100 if self.memory_limit_mb > 0 else 0

        # Create memory info dictionary
        memory_data = {
            "used_mb": used_mb,
            "peak_mb": self.peak_memory_mb,
            "limit_mb": self.memory_limit_mb,
            "percent": percent,
            "rss_bytes": memory_info.rss,
            "vms_bytes": memory_info.vms,
        }

        # Check if memory usage exceeds warning threshold
        if used_mb > self.memory_limit_mb * self.warning_threshold:
            logger.warning(
                f"Memory usage is high: {used_mb:.2f} MB ({percent:.1f}% of limit)",
                extra=memory_data
            )

            # Check if memory usage exceeds critical threshold
            if used_mb > self.memory_limit_mb * self.critical_threshold:
                logger.warning(
                    f"Memory usage is critical: {used_mb:.2f} MB ({percent:.1f}% of limit). Forcing garbage collection.",
                    extra=memory_data
                )
                # Force garbage collection
                gc.collect()

                # Get memory usage after garbage collection
                memory_info = self.process.memory_info()
                used_mb_after = memory_info.rss / (1024 * 1024)
                percent_after = (used_mb_after / self.memory_limit_mb) * 100 if self.memory_limit_mb > 0 else 0

                # Log memory usage after garbage collection
                logger.info(
                    f"Memory usage after garbage collection: {used_mb_after:.2f} MB ({percent_after:.1f}% of limit)",
                    extra={
                        "used_mb_before": used_mb,
                        "used_mb_after": used_mb_after,
                        "freed_mb": used_mb - used_mb_after,
                        "percent_before": percent,
                        "percent_after": percent_after,
                    }
                )

                # Update memory data
                memory_data["used_mb"] = used_mb_after
                memory_data["percent"] = percent_after
                memory_data["rss_bytes"] = memory_info.rss
                memory_data["vms_bytes"] = memory_info.vms
                memory_data["gc_triggered"] = True

        # Update last memory usage
        self.last_memory_mb = memory_data["used_mb"]

        return memory_data

    def get_peak_memory(self) -> float:
        """
        Get peak memory usage.

        Returns:
            Peak memory usage in MB
        """
        return self.peak_memory_mb

    def reset_peak_memory(self) -> None:
        """Reset peak memory usage."""
        self.peak_memory_mb = 0