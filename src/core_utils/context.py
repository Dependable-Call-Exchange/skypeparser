"""
ETL context module.

This module provides the ETLContext class that centralizes state management
across ETL pipeline components.
"""

import datetime
import json
import logging
import os
import uuid
from typing import Any, ClassVar, Dict, List, Optional, BinaryIO

from src.monitoring.checkpoint_manager import CheckpointManager
from src.validation.configuration_validator import ConfigurationValidator
from src.logging.error_logger import ErrorLogger
from src.monitoring.memory_monitor import MemoryMonitor
from src.logging.new_structured_logging import (
    get_logger,
    handle_errors,
    log_call,
    log_execution_time,
    with_context,
)
from src.monitoring.phase_manager import PhaseManager
from src.monitoring.progress_tracker import ProgressTracker

logger = get_logger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime objects."""

    def default(self, obj):
        """Convert datetime objects to ISO format strings."""
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super().default(obj)


class ETLContext:
    """
    Shared context object for the ETL pipeline.

    This class centralizes state management across the ETL pipeline components,
    including configuration, progress tracking, memory monitoring, and telemetry.
    It also provides checkpointing capabilities for resuming failed operations.

    The context maintains the following state:
    - Configuration parameters (db_config, output_dir, etc.)
    - Progress tracking for conversations and messages
    - Memory usage monitoring
    - Phase results and status tracking
    - Error recording and reporting
    - Checkpointing for resumable operations
    """

    # Class variables for checkpoint serialization
    SERIALIZABLE_ATTRIBUTES: ClassVar[List[str]] = [
        "db_config",
        "output_dir",
        "memory_limit_mb",
        "parallel_processing",
        "chunk_size",
        "batch_size",
        "max_workers",
        "task_id",
        "start_time",
        "current_phase",
        "phase_results",
        "phase_statuses",
        "checkpoints",
        "errors",
        "export_id",
        "metrics",
        "user_id",
        "user_display_name",
        "export_date",
        "custom_metadata",
        "download_attachments",
        "attachments_dir",
        "generate_thumbnails",
        "extract_metadata",
    ]

    # Data attributes that need special handling for serialization
    DATA_ATTRIBUTES: ClassVar[List[str]] = ["raw_data", "transformed_data"]

    def __init__(
        self,
        db_config: Dict[str, Any],
        output_dir: str,
        batch_size: int = 1000,
        chunk_size: int = 1000,
        memory_limit_mb: int = 1024,
        parallel_processing: bool = True,
        file_path: str = None,
        max_workers: Optional[int] = None,
        task_id: Optional[str] = None,
        user_id: Optional[str] = None,
        user_display_name: Optional[str] = None,
        export_date: Optional[str] = None,
        download_attachments: bool = False,
        attachments_dir: Optional[str] = None,
        generate_thumbnails: bool = True,
        extract_metadata: bool = True,
        progress_tracker: Optional[ProgressTracker] = None,
        memory_monitor: Optional[MemoryMonitor] = None,
        phase_manager: Optional[PhaseManager] = None,
        error_logger: Optional[ErrorLogger] = None,
        checkpoint_manager: Optional[CheckpointManager] = None,
    ) -> None:
        """
        Initialize the ETL context.

        Args:
            db_config: Database configuration dictionary
            output_dir: Output directory
            batch_size: Batch size for database operations
            chunk_size: Chunk size for processing
            memory_limit_mb: Memory limit in MB
            parallel_processing: Whether to use parallel processing
            file_path: Path to the input file
            max_workers: Maximum number of worker threads/processes
            task_id: Unique identifier for this ETL task (generated if not provided)
            user_id: Unique identifier for the user whose data is being processed
            user_display_name: Display name of the user
            export_date: Date when the data was exported
            download_attachments: Whether to download attachments from URLs
            attachments_dir: Directory to store downloaded attachments (defaults to output_dir/attachments)
            generate_thumbnails: Whether to generate thumbnails for image attachments
            extract_metadata: Whether to extract metadata from attachments
            progress_tracker: Optional progress tracker instance
            memory_monitor: Optional memory monitor instance
            phase_manager: Optional phase manager instance
            error_logger: Optional error logger instance
            checkpoint_manager: Optional checkpoint manager instance
        """
        # Validate configuration parameters
        ConfigurationValidator.validate_configuration(
            db_config, output_dir, memory_limit_mb, chunk_size, batch_size, max_workers
        )

        # Core configuration
        self.db_config: Dict[str, Any] = db_config
        self.output_dir: str = output_dir
        self.batch_size: int = batch_size
        self.chunk_size: int = chunk_size
        self.memory_limit_mb: int = memory_limit_mb
        self.parallel_processing: bool = parallel_processing
        self.file_path: str = file_path

        # Task information
        self.task_id: str = task_id or str(uuid.uuid4())
        self.start_time: datetime.datetime = datetime.datetime.now()
        self.completed_time: Optional[datetime.datetime] = None
        self.current_phase: Optional[str] = None
        self.phase_results: Dict[str, Any] = {}
        self.phase_statuses: Dict[str, str] = {}
        self.checkpoints: Dict[str, Any] = {}
        self.errors: List[Dict[str, Any]] = []
        self.export_id: Optional[int] = None
        self.metrics: Dict[str, Any] = {
            "memory_usage": [],
            "errors": 0,
            "duration": {},
            "processed_items": {}
        }

        # File information
        self.file_source: Optional[str] = None
        self.file_obj: Optional[BinaryIO] = None

        # User information
        self.user_id: Optional[str] = user_id
        self.user_display_name: Optional[str] = user_display_name
        self.export_date: Optional[str] = export_date

        # Attachment settings
        self.download_attachments: bool = download_attachments
        self.attachments_dir: Optional[str] = attachments_dir or (
            os.path.join(output_dir, "attachments") if output_dir else None
        )
        self.generate_thumbnails: bool = generate_thumbnails
        self.extract_metadata: bool = extract_metadata

        # Custom metadata for extensibility
        self.custom_metadata: Dict[str, Any] = {}

        # Initialize utility components
        self.progress_tracker = progress_tracker or ProgressTracker()
        self.memory_monitor = memory_monitor or MemoryMonitor(memory_limit_mb)
        self.phase_manager = phase_manager or PhaseManager()
        self.error_logger = error_logger or ErrorLogger()
        self.checkpoint_manager = checkpoint_manager or CheckpointManager(
            os.path.join(output_dir, "checkpoints") if output_dir else "checkpoints"
        )

        # Data references
        self.raw_data: Optional[Dict[str, Any]] = None
        self.transformed_data: Optional[Dict[str, Any]] = None

        # Create output directory if it doesn't exist
        if self.output_dir and not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)
            logger.info(f"Created output directory: {self.output_dir}")

        # Create attachments directory if needed
        if (
            self.download_attachments
            and self.attachments_dir
            and not os.path.exists(self.attachments_dir)
        ):
            os.makedirs(self.attachments_dir, exist_ok=True)
            logger.info(f"Created attachments directory: {self.attachments_dir}")

        # Log initialization
        logger.info(f"Initialized ETL context with task ID: {self.task_id}")

    def set_file_source(self, file_path: Optional[str] = None, file_obj: Optional[BinaryIO] = None) -> None:
        """
        Set the file source for this ETL context.

        This method provides backward compatibility with newer pipeline implementations
        that expect a setter method rather than direct attribute access.

        Args:
            file_path: The path to the source file
            file_obj: A file-like object containing the source data
        """
        self.file_source = file_path
        self.file_obj = file_obj
        logger.debug(f"Set file source: {file_path}")

    @with_context(operation="start_phase")
    @log_call(level=logging.INFO)
    def start_phase(
        self,
        phase_name: str,
        total_conversations: Optional[int] = None,
        total_messages: Optional[int] = None,
    ) -> None:
        """
        Start a new phase of the ETL process.

        Args:
            phase_name: Name of the phase to start
            total_conversations: Optional total number of conversations to process
            total_messages: Optional total number of messages to process
        """
        self.phase_manager.start_phase(phase_name, total_conversations, total_messages)
        self._record_memory_usage(phase_name, "start")

    @with_context(operation="end_phase")
    @log_call(level=logging.INFO)
    def end_phase(self, phase_name: str, status: str = "completed") -> None:
        """
        End a phase of the ETL process.

        Args:
            phase_name: Name of the phase to end
            status: Status of the phase (completed, warning, failed)
        """
        self.phase_manager.end_phase(phase_name, status)
        self._record_memory_usage(phase_name, "end")

    @with_context(operation="record_error")
    @log_call(level=logging.ERROR)
    def record_error(
        self,
        phase: str,
        error_message: str,
        error_details: Optional[Dict[str, Any]] = None,
        fatal: bool = True,
    ) -> None:
        """
        Record an error that occurred during the ETL process.

        Args:
            phase: Phase where the error occurred
            error_message: Error message
            error_details: Additional error details
            fatal: Whether the error is fatal and should stop the pipeline
        """
        error_record = self.error_logger.record_error(phase, error_message, error_details, fatal)

        # Update metrics
        self.metrics["errors"] += 1

        # Update phase status if it exists
        if self.phase_manager.get_phase_status(phase):
            if fatal:
                self.phase_manager.end_phase(phase, "failed")
            else:
                self.phase_manager.end_phase(phase, "warning")

    @with_context(operation="create_checkpoint")
    @log_execution_time(level=logging.INFO)
    def create_checkpoint(self, checkpoint_id: Optional[str] = None) -> str:
        """
        Create a checkpoint of the current ETL state.

        Args:
            checkpoint_id: Optional ID for the checkpoint (generated if not provided)

        Returns:
            Checkpoint ID
        """
        # Create state dictionary
        state = {
            "task_id": self.task_id,
            "current_phase": self.phase_manager.current_phase,
            "phase_statuses": self.phase_manager.phase_statuses,
            "phase_results": self.phase_manager.phase_results,
            "metrics": self.metrics,
            "raw_data": self.raw_data,
            "transformed_data": self.transformed_data,
            "file_source": self.file_source,
            "export_id": self.export_id,
            "user_id": self.user_id,
            "user_display_name": self.user_display_name,
            "export_date": self.export_date,
            "custom_metadata": self.custom_metadata,
        }

        # Create checkpoint
        return self.checkpoint_manager.create_checkpoint(state, checkpoint_id)

    # Add alias for backward compatibility
    _create_checkpoint = create_checkpoint

    @with_context(operation="restore_checkpoint")
    @log_execution_time(level=logging.INFO)
    def restore_checkpoint(self, checkpoint_id: str) -> bool:
        """
        Restore a checkpoint.

        Args:
            checkpoint_id: Checkpoint ID

        Returns:
            True if the checkpoint was restored successfully, False otherwise
        """
        try:
            # Restore checkpoint
            checkpoint = self.checkpoint_manager.restore_checkpoint(checkpoint_id)

            # Restore state
            for key, value in checkpoint.items():
                if hasattr(self, key) and key not in ["task_id", "start_time"]:
                    setattr(self, key, value)

            # Restore phase manager state
            if "phase_statuses" in checkpoint:
                self.phase_manager.phase_statuses = checkpoint["phase_statuses"]
            if "phase_results" in checkpoint:
                self.phase_manager.phase_results = checkpoint["phase_results"]
            if "current_phase" in checkpoint:
                self.phase_manager.current_phase = checkpoint["current_phase"]

            # Log successful restoration
            logger.info(
                f"Restored checkpoint: {checkpoint_id}",
                extra={
                    "checkpoint_id": checkpoint_id,
                    "task_id": self.task_id,
                    "current_phase": self.phase_manager.current_phase,
                },
            )

            return True
        except Exception as e:
            logger.error(
                f"Error restoring checkpoint: {e}",
                exc_info=True,
                extra={
                    "checkpoint_id": checkpoint_id,
                    "error": str(e),
                },
            )
            return False

    @with_context(operation="record_memory_usage")
    def _record_memory_usage(self, phase: str, stage: str) -> None:
        """
        Record memory usage.

        Args:
            phase: Phase name
            stage: Stage of the phase (start, end)
        """
        memory_info = self.memory_monitor.check_memory()
        memory_info["phase"] = phase
        memory_info["stage"] = stage
        memory_info["timestamp"] = datetime.datetime.now().isoformat()

        # Add to metrics
        self.metrics["memory_usage"].append(memory_info)

        # Log memory usage
        logger.debug(
            f"Memory usage for phase {phase} ({stage}): {memory_info['used_mb']:.2f} MB",
            extra={
                "phase": phase,
                "stage": stage,
                "memory_info": memory_info,
            },
        )

    @with_context(operation="update_progress")
    @log_call(level=logging.DEBUG)
    def update_progress(
        self, phase: str, current: int, total: int, item_type: str = "items"
    ) -> None:
        """
        Update progress for a phase.

        Args:
            phase: Phase name
            current: Current progress
            total: Total items to process
            item_type: Type of items being processed
        """
        # Update progress tracker
        self.progress_tracker.update(current, total)

        # Update phase metrics
        self.phase_manager.update_phase_metric(
            phase, f"processed_{item_type}", current
        )
        self.phase_manager.update_phase_metric(
            phase, f"total_{item_type}", total
        )

        # Update metrics
        if "processed_items" not in self.metrics:
            self.metrics["processed_items"] = {}
        if phase not in self.metrics["processed_items"]:
            self.metrics["processed_items"][phase] = {}
        self.metrics["processed_items"][phase][item_type] = current

        # Log progress
        progress_percentage = (current / total) * 100 if total > 0 else 0
        logger.debug(
            f"Progress for phase {phase}: {current}/{total} {item_type} ({progress_percentage:.2f}%)",
            extra={
                "phase": phase,
                "current": current,
                "total": total,
                "item_type": item_type,
                "progress_percentage": progress_percentage,
            },
        )

    @with_context(operation="get_summary")
    @log_execution_time(level=logging.DEBUG)
    def get_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the ETL process.

        Returns:
            Dictionary with ETL summary
        """
        # Calculate duration
        end_time = datetime.datetime.now()
        duration = (end_time - self.start_time).total_seconds()

        # Create summary
        summary = {
            "task_id": self.task_id,
            "start_time": self.start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration": duration,
            "user_id": self.user_id,
            "user_display_name": self.user_display_name,
            "export_date": self.export_date,
            "phases": self.phase_manager.get_phase_summary(),
            "errors": {
                "count": len(self.error_logger.errors),
                "fatal_count": len(self.error_logger.get_errors(fatal_only=True)),
                "details": self.error_logger.errors,
            },
            "metrics": self.metrics,
            "custom_metadata": self.custom_metadata,
        }

        return summary

    @with_context(operation="save_summary")
    @log_execution_time(level=logging.INFO)
    def save_summary(self, output_path: Optional[str] = None) -> str:
        """
        Save a summary of the ETL process to a file.

        Args:
            output_path: Optional path to save the summary file

        Returns:
            Path to the summary file
        """
        # Get summary
        summary = self.get_summary()

        # Create output path if not provided
        if output_path is None:
            if self.output_dir is None:
                raise ValueError("Output directory is required to save summary")
            output_path = os.path.join(
                self.output_dir, f"etl_summary_{self.task_id}.json"
            )

        # Save summary to file
        with open(output_path, "w") as f:
            json.dump(summary, f, indent=2, cls=DateTimeEncoder)

        # Log summary
        logger.info(
            f"Saved ETL summary to {output_path}",
            extra={
                "task_id": self.task_id,
                "output_path": output_path,
            },
        )

        return output_path

    @with_context(operation="can_resume_from_phase")
    @log_call(level=logging.INFO)
    def can_resume_from_phase(self, phase_name: str) -> bool:
        """
        Check if the ETL process can be resumed from a specific phase.

        Args:
            phase_name: Name of the phase to resume from

        Returns:
            True if the process can be resumed from the specified phase, False otherwise
        """
        return self.phase_manager.can_resume_from_phase(phase_name)

    @property
    def has_checkpoint(self) -> bool:
        """
        Check if there are any checkpoints.

        Returns:
            True if there are checkpoints, False otherwise
        """
        return len(self.checkpoint_manager.list_checkpoints()) > 0

    def serialize_checkpoint(self, checkpoint_id: str) -> Dict[str, Any]:
        """
        Serialize a checkpoint for external storage.

        Args:
            checkpoint_id: Checkpoint ID

        Returns:
            Serialized checkpoint
        """
        # Get checkpoint
        checkpoint = self.checkpoint_manager.get_checkpoint(checkpoint_id)
        if not checkpoint:
            raise ValueError(f"Checkpoint {checkpoint_id} does not exist")

        # Create a copy to avoid modifying the original
        serialized = checkpoint.copy()

        # Add additional metadata
        serialized["serialized_at"] = datetime.datetime.now().isoformat()
        serialized["context_task_id"] = self.task_id

        return serialized

    @with_context(operation="check_memory")
    @log_call(level=logging.DEBUG)
    def check_memory(self) -> Dict[str, Any]:
        """
        Check current memory usage.

        Returns:
            Dict with memory usage information
        """
        return self.memory_monitor.check_memory()
