"""
Utilities module for the ETL pipeline.

This module provides utility classes for tracking progress and monitoring
memory usage during ETL operations.
"""

import gc
import os
import time
import logging
import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)

class ProgressTracker:
    """Tracks progress of ETL operations and provides reporting."""

    def __init__(self):
        """Initialize the progress tracker."""
        self.total_conversations = 0
        self.processed_conversations = 0
        self.total_messages = 0
        self.processed_messages = 0
        self.start_time = None
        self.phase = None

    def start_phase(self, phase: str, total_conversations: int = 0, total_messages: int = 0) -> None:
        """Start tracking a new phase.

        Args:
            phase: Name of the phase (extraction, transformation, loading)
            total_conversations: Total number of conversations to process
            total_messages: Total number of messages to process
        """
        self.phase = phase
        self.total_conversations = total_conversations
        self.total_messages = total_messages
        self.processed_conversations = 0
        self.processed_messages = 0
        self.start_time = datetime.datetime.now()
        logger.info(f"Starting {phase} phase with {total_conversations} conversations and {total_messages} messages")

    def update_conversation_progress(self, increment: int = 1) -> None:
        """Update the conversation progress.

        Args:
            increment: Number of conversations processed
        """
        self.processed_conversations += increment
        self._log_progress()

    def update_message_progress(self, increment: int = 1) -> None:
        """Update the message progress.

        Args:
            increment: Number of messages processed
        """
        self.processed_messages += increment
        # Only log every 1000 messages to avoid log spam
        if self.processed_messages % 1000 == 0:
            self._log_progress()

    def _log_progress(self) -> None:
        """Log the current progress."""
        if self.total_conversations > 0:
            conv_progress = (self.processed_conversations / self.total_conversations) * 100
            logger.info(f"{self.phase} progress: {self.processed_conversations}/{self.total_conversations} conversations ({conv_progress:.1f}%)")

        if self.total_messages > 0:
            msg_progress = (self.processed_messages / self.total_messages) * 100
            logger.info(f"{self.phase} progress: {self.processed_messages}/{self.total_messages} messages ({msg_progress:.1f}%)")

    def finish_phase(self) -> Dict[str, Any]:
        """Finish the current phase and return statistics.

        Returns:
            Dict containing statistics about the completed phase
        """
        end_time = datetime.datetime.now()
        duration = (end_time - self.start_time).total_seconds()

        stats = {
            'phase': self.phase,
            'total_conversations': self.total_conversations,
            'processed_conversations': self.processed_conversations,
            'total_messages': self.total_messages,
            'processed_messages': self.processed_messages,
            'duration_seconds': duration,
            'messages_per_second': self.processed_messages / duration if duration > 0 else 0
        }

        logger.info(f"Completed {self.phase} phase in {duration:.2f} seconds")
        logger.info(f"Processed {self.processed_conversations}/{self.total_conversations} conversations and {self.processed_messages}/{self.total_messages} messages")
        logger.info(f"Processing rate: {stats['messages_per_second']:.2f} messages per second")

        return stats

class MemoryMonitor:
    """Monitors memory usage and triggers garbage collection when needed."""

    def __init__(self, memory_limit_mb: int = 1024):
        """Initialize the memory monitor.

        Args:
            memory_limit_mb: Memory limit in MB before forcing garbage collection
        """
        self.memory_limit_mb = memory_limit_mb
        self.last_gc_time = time.time()
        self.gc_interval = 60  # Minimum time between forced GC in seconds

    def check_memory(self) -> None:
        """Check current memory usage and trigger garbage collection if needed."""
        try:
            import psutil
            process = psutil.Process(os.getpid())
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)

            # Log memory usage every 5 minutes or when approaching limit
            current_time = time.time()
            if current_time - self.last_gc_time > 300 or memory_mb > self.memory_limit_mb * 0.8:
                logger.info(f"Current memory usage: {memory_mb:.2f} MB (limit: {self.memory_limit_mb} MB)")

            # Force garbage collection if memory usage exceeds limit and enough time has passed
            if memory_mb > self.memory_limit_mb and current_time - self.last_gc_time > self.gc_interval:
                self._force_garbage_collection()
                self.last_gc_time = current_time

        except ImportError:
            logger.warning("psutil not available, memory monitoring disabled")

    def _force_garbage_collection(self) -> None:
        """Force garbage collection to free memory."""
        try:
            import psutil
            process = psutil.Process(os.getpid())

            # Get memory usage before collection
            before_mb = process.memory_info().rss / (1024 * 1024)

            # Force collection
            gc.collect()

            # Get memory usage after collection
            after_mb = process.memory_info().rss / (1024 * 1024)
            freed_mb = before_mb - after_mb

            logger.info(f"Forced garbage collection freed {freed_mb:.2f} MB")

        except ImportError:
            gc.collect()
            logger.info("Forced garbage collection (memory freed unknown)")
