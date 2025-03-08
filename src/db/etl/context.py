"""
ETL Context module for the ETL pipeline.

This module provides a shared context object that manages state across
the different components of the ETL pipeline, including progress tracking,
memory monitoring, configuration, and telemetry.
"""

import base64
import datetime
import json
import logging
import os
import pickle
import time
import uuid
from typing import Any, BinaryIO, ClassVar, Dict, List, Optional, Type

from src.utils.new_structured_logging import (
    LogContext,
    get_logger,
    get_system_metrics,
    handle_errors,
    log_call,
    log_execution_time,
    log_metrics,
    with_context,
)

from .utils import MemoryMonitor, ProgressTracker

logger = get_logger(__name__)


# Custom JSON encoder for datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
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

    Phase statuses are tracked in the `phase_statuses` dictionary with the following values:
    - 'in_progress': The phase is currently running
    - 'completed': The phase completed successfully
    - 'warning': The phase completed with non-fatal warnings
    - 'failed': The phase failed with errors
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
        output_dir: Optional[str] = None,
        memory_limit_mb: int = 1024,
        parallel_processing: bool = True,
        chunk_size: int = 1000,
        batch_size: int = 100,
        max_workers: Optional[int] = None,
        task_id: Optional[str] = None,
        user_id: Optional[str] = None,
        user_display_name: Optional[str] = None,
        export_date: Optional[str] = None,
        download_attachments: bool = False,
        attachments_dir: Optional[str] = None,
        generate_thumbnails: bool = True,
        extract_metadata: bool = True,
    ) -> None:
        """
        Initialize the ETL context.

        Args:
            db_config: Database configuration dictionary
            output_dir: Optional directory to save intermediate files
            memory_limit_mb: Memory limit in MB before forcing garbage collection
            parallel_processing: Whether to use parallel processing for transformations
            chunk_size: Size of message chunks for batch processing
            batch_size: Size of database batch operations
            max_workers: Maximum number of worker threads/processes
            task_id: Unique identifier for this ETL task (generated if not provided)
            user_id: Unique identifier for the user whose data is being processed
            user_display_name: Display name of the user
            export_date: Date when the data was exported
            download_attachments: Whether to download attachments from URLs
            attachments_dir: Directory to store downloaded attachments (defaults to output_dir/attachments)
            generate_thumbnails: Whether to generate thumbnails for image attachments
            extract_metadata: Whether to extract metadata from attachments
        """
        # Validate configuration parameters
        self._validate_configuration(
            db_config, output_dir, memory_limit_mb, chunk_size, batch_size, max_workers
        )

        # Core configuration
        self.db_config: Dict[str, Any] = db_config
        self.output_dir: Optional[str] = output_dir

        # Performance configuration
        self.memory_limit_mb: int = memory_limit_mb
        self.parallel_processing: bool = parallel_processing
        self.chunk_size: int = chunk_size
        self.batch_size: int = batch_size
        self.max_workers: Optional[int] = max_workers

        # Task identification
        self.task_id: str = task_id or str(uuid.uuid4())
        self.start_time: datetime.datetime = datetime.datetime.now()

        # User information
        self.user_id: str = (
            user_id or f"user_{hash(user_display_name or 'unknown') % 10000}"
        )
        self.user_display_name: Optional[str] = user_display_name
        self.export_date: str = export_date or datetime.datetime.now().isoformat()

        # Attachment handling configuration
        self.download_attachments: bool = download_attachments
        self.attachments_dir: Optional[str] = attachments_dir or (
            output_dir and os.path.join(output_dir, "attachments")
        )
        self.generate_thumbnails: bool = generate_thumbnails
        self.extract_metadata: bool = extract_metadata

        # Shared utilities
        self.progress_tracker: ProgressTracker = ProgressTracker()
        self.memory_monitor: MemoryMonitor = MemoryMonitor(
            memory_limit_mb=memory_limit_mb
        )

        # State tracking
        self.current_phase: Optional[str] = None
        self.phase_results: Dict[str, Dict[str, Any]] = {}
        self.phase_statuses: Dict[str, Any] = {}
        self.checkpoints: Dict[str, Dict[str, Any]] = {}
        self.errors: List[Dict[str, Any]] = []

        # Data references
        self.raw_data: Optional[Dict[str, Any]] = None
        self.transformed_data: Optional[Dict[str, Any]] = None
        self.file_source: Optional[str] = None
        self.export_id: Optional[int] = None

        # Telemetry
        self.metrics: Dict[str, Any] = {
            "start_time": self.start_time,
            "memory_usage": [],
            "duration": {},
            "processed_items": {},
            "errors": 0,
        }

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
        logger.info(
            f"Initialized ETL context with task ID: {self.task_id}",
            extra={
                "task_id": self.task_id,
                "user_id": self.user_id,
                "output_dir": self.output_dir,
                "parallel_processing": self.parallel_processing,
                "chunk_size": self.chunk_size,
                "batch_size": self.batch_size,
                "max_workers": self.max_workers,
                "memory_limit_mb": self.memory_limit_mb,
                "download_attachments": self.download_attachments,
            },
        )

    @handle_errors(log_level="ERROR", default_message="Error validating configuration")
    def _validate_configuration(
        self,
        db_config: Dict[str, Any],
        output_dir: Optional[str],
        memory_limit_mb: int,
        chunk_size: int,
        batch_size: int,
        max_workers: Optional[int],
    ) -> None:
        """
        Validate all configuration parameters.

        Args:
            db_config: Database configuration dictionary
            output_dir: Optional directory to save intermediate files
            memory_limit_mb: Memory limit in MB
            chunk_size: Size of message chunks for batch processing
            batch_size: Size of database batch operations
            max_workers: Maximum number of worker threads/processes

        Raises:
            ValueError: If any configuration parameter is invalid
        """
        # Validate database configuration
        if not isinstance(db_config, dict):
            raise ValueError("Database configuration must be a dictionary")

        # Check if we're in a test environment
        in_test_env = os.environ.get("POSTGRES_TEST_DB") == "true"

        # Import validation function here to avoid circular imports
        from src.utils.validation import validate_db_config

        try:
            # Skip strict validation in test environment
            if not in_test_env:
                validate_db_config(db_config)
        except Exception as e:
            if not in_test_env:
                logger.error(
                    f"Invalid database configuration: {str(e)}",
                    exc_info=True,
                    extra={
                        "db_config": {
                            k: v for k, v in db_config.items() if k != "password"
                        }
                    },
                )
                raise ValueError(f"Invalid database configuration: {str(e)}")
            else:
                logger.warning(
                    f"Database configuration validation skipped in test environment: {str(e)}",
                    extra={
                        "db_config": {
                            k: v for k, v in db_config.items() if k != "password"
                        }
                    },
                )

        # Validate output directory
        if output_dir is not None and not isinstance(output_dir, str):
            logger.error(
                "Output directory must be a string", extra={"output_dir": output_dir}
            )
            raise ValueError("Output directory must be a string")

        # Validate memory limit
        if not isinstance(memory_limit_mb, int) or memory_limit_mb <= 0:
            logger.error(
                "Memory limit must be a positive integer",
                extra={"memory_limit_mb": memory_limit_mb},
            )
            raise ValueError("Memory limit must be a positive integer")

        # Validate chunk size
        if not isinstance(chunk_size, int) or chunk_size <= 0:
            logger.error(
                "Chunk size must be a positive integer",
                extra={"chunk_size": chunk_size},
            )
            raise ValueError("Chunk size must be a positive integer")

        # Validate batch size
        if not isinstance(batch_size, int) or batch_size <= 0:
            logger.error(
                "Batch size must be a positive integer",
                extra={"batch_size": batch_size},
            )
            raise ValueError("Batch size must be a positive integer")

        # Validate max workers
        if max_workers is not None and (
            not isinstance(max_workers, int) or max_workers <= 0
        ):
            logger.error(
                "Max workers must be a positive integer",
                extra={"max_workers": max_workers},
            )
            raise ValueError("Max workers must be a positive integer")

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
        # Record current phase
        self.current_phase = phase_name

        # Initialize phase status
        self.phase_statuses[phase_name] = "in_progress"

        # Initialize phase results
        self.phase_results[phase_name] = {
            "start_time": datetime.datetime.now().isoformat(),
            "end_time": None,
            "duration": None,
            "status": "in_progress",
            "metrics": {},
        }

        # Store conversation and message counts if provided
        if total_conversations is not None:
            self.phase_results[phase_name]["metrics"][
                "total_conversations"
            ] = total_conversations

        if total_messages is not None:
            self.phase_results[phase_name]["metrics"]["total_messages"] = total_messages

        # Log phase start
        extra_data = {
            "task_id": self.task_id,
            "phase": phase_name,
            "start_time": self.phase_results[phase_name]["start_time"],
        }

        if total_conversations is not None:
            extra_data["total_conversations"] = total_conversations

        if total_messages is not None:
            extra_data["total_messages"] = total_messages

        logger.info(f"Started ETL phase: {phase_name}", extra=extra_data)

        # Record memory usage
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
        # Check if phase exists
        if phase_name not in self.phase_results:
            logger.warning(
                f"Attempted to end non-existent phase: {phase_name}",
                extra={"task_id": self.task_id, "phase": phase_name},
            )
            return

        # Record end time
        end_time = datetime.datetime.now()
        self.phase_results[phase_name]["end_time"] = end_time.isoformat()

        # Calculate duration
        start_time = datetime.datetime.fromisoformat(
            self.phase_results[phase_name]["start_time"]
        )
        duration = (end_time - start_time).total_seconds()
        self.phase_results[phase_name]["duration"] = duration

        # Update status
        self.phase_results[phase_name]["status"] = status
        self.phase_statuses[phase_name] = status

        # Record in metrics
        self.metrics["duration"][phase_name] = duration

        # Record memory usage
        self._record_memory_usage(phase_name, "end")

        # Log phase end
        logger.info(
            f"Ended ETL phase: {phase_name} with status: {status}",
            extra={
                "task_id": self.task_id,
                "phase": phase_name,
                "status": status,
                "duration": duration,
                "metrics": self.phase_results[phase_name].get("metrics", {}),
            },
        )

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
        # Create error record
        error_record = {
            "phase": phase,
            "timestamp": datetime.datetime.now().isoformat(),
            "message": error_message,
            "details": error_details or {},
            "fatal": fatal,
        }

        # Add to errors list
        self.errors.append(error_record)

        # Update metrics
        self.metrics["errors"] += 1

        # Update phase status if it exists
        if phase in self.phase_statuses:
            if fatal:
                self.phase_statuses[phase] = "failed"
            else:
                self.phase_statuses[phase] = "warning"

        # Log error with appropriate level
        log_level = logging.ERROR if fatal else logging.WARNING
        log_message = f"{'Fatal' if fatal else 'Non-fatal'} error in ETL phase {phase}: {error_message}"

        logger.log(
            log_level,
            log_message,
            extra={
                "task_id": self.task_id,
                "phase": phase,
                "error_details": error_details,
                "fatal": fatal,
            },
        )

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
        # Generate checkpoint ID if not provided
        if checkpoint_id is None:
            checkpoint_id = str(uuid.uuid4())

        # Create checkpoint data
        checkpoint_data = {
            "id": checkpoint_id,
            "timestamp": datetime.datetime.now().isoformat(),
            "task_id": self.task_id,
            "current_phase": self.current_phase,
            "phase_statuses": self.phase_statuses.copy(),
            "serialized_attributes": {},
            "serialized_data": {},
        }

        # Serialize attributes
        for attr in self.SERIALIZABLE_ATTRIBUTES:
            if hasattr(self, attr):
                value = getattr(self, attr)
                if (
                    isinstance(value, (dict, list, str, int, float, bool))
                    or value is None
                ):
                    checkpoint_data["serialized_attributes"][attr] = value
                else:
                    try:
                        # Try to convert to JSON-serializable format
                        checkpoint_data["serialized_attributes"][attr] = json.loads(
                            json.dumps(value, cls=DateTimeEncoder)
                        )
                    except (TypeError, json.JSONDecodeError):
                        logger.warning(
                            f"Could not serialize attribute {attr} for checkpoint",
                            extra={
                                "task_id": self.task_id,
                                "checkpoint_id": checkpoint_id,
                            },
                        )

        # Serialize data attributes
        for attr in self.DATA_ATTRIBUTES:
            if hasattr(self, attr) and getattr(self, attr) is not None:
                try:
                    # Serialize data to base64-encoded pickle
                    data = getattr(self, attr)
                    serialized = base64.b64encode(pickle.dumps(data)).decode("utf-8")
                    checkpoint_data["serialized_data"][attr] = serialized
                except Exception as e:
                    logger.warning(
                        f"Could not serialize data attribute {attr} for checkpoint: {str(e)}",
                        extra={"task_id": self.task_id, "checkpoint_id": checkpoint_id},
                    )

        # Store checkpoint
        self.checkpoints[checkpoint_id] = checkpoint_data

        # Save checkpoint to file if output directory is specified
        if self.output_dir:
            self._save_checkpoint_to_file(checkpoint_id, checkpoint_data)

        # Log checkpoint creation
        logger.info(
            f"Created ETL checkpoint: {checkpoint_id}",
            extra={
                "task_id": self.task_id,
                "checkpoint_id": checkpoint_id,
                "current_phase": self.current_phase,
                "phase_statuses": self.phase_statuses,
            },
        )

        return checkpoint_id

    # Add alias for backward compatibility
    _create_checkpoint = create_checkpoint

    @handle_errors(log_level="ERROR", default_message="Error saving checkpoint to file")
    def _save_checkpoint_to_file(
        self, checkpoint_id: str, checkpoint_data: Dict[str, Any]
    ) -> None:
        """
        Save a checkpoint to a file.

        Args:
            checkpoint_id: Checkpoint ID
            checkpoint_data: Checkpoint data
        """
        # Create checkpoint file path
        checkpoint_file = os.path.join(
            self.output_dir, f"checkpoint_{checkpoint_id}.json"
        )

        # Save checkpoint to file
        with open(checkpoint_file, "w") as f:
            json.dump(checkpoint_data, f, cls=DateTimeEncoder, indent=2)

        # Log checkpoint save
        logger.info(
            f"Saved checkpoint to file: {checkpoint_file}",
            extra={
                "task_id": self.task_id,
                "checkpoint_id": checkpoint_id,
                "checkpoint_file": checkpoint_file,
            },
        )

    @with_context(operation="restore_checkpoint")
    @log_execution_time(level=logging.INFO)
    def restore_checkpoint(self, checkpoint_id: str) -> bool:
        """
        Restore the ETL state from a checkpoint.

        Args:
            checkpoint_id: ID of the checkpoint to restore

        Returns:
            True if the checkpoint was restored successfully, False otherwise
        """
        # Check if checkpoint exists in memory
        if checkpoint_id in self.checkpoints:
            checkpoint_data = self.checkpoints[checkpoint_id]
        else:
            # Try to load checkpoint from file
            checkpoint_file = os.path.join(
                self.output_dir, f"checkpoint_{checkpoint_id}.json"
            )
            if not os.path.exists(checkpoint_file):
                logger.error(
                    f"Checkpoint file not found: {checkpoint_file}",
                    extra={"task_id": self.task_id, "checkpoint_id": checkpoint_id},
                )
                return False

            try:
                with open(checkpoint_file, "r") as f:
                    checkpoint_data = json.load(f)
            except Exception as e:
                logger.error(
                    f"Error loading checkpoint file: {str(e)}",
                    exc_info=True,
                    extra={"task_id": self.task_id, "checkpoint_id": checkpoint_id},
                )
                return False

        # Restore serialized attributes
        for attr, value in checkpoint_data.get("serialized_attributes", {}).items():
            if attr in self.SERIALIZABLE_ATTRIBUTES:
                setattr(self, attr, value)

        # Restore serialized data
        for attr, serialized in checkpoint_data.get("serialized_data", {}).items():
            if attr in self.DATA_ATTRIBUTES:
                try:
                    # Deserialize data from base64-encoded pickle
                    data = pickle.loads(base64.b64decode(serialized))
                    setattr(self, attr, data)
                except Exception as e:
                    logger.error(
                        f"Error deserializing data attribute {attr} from checkpoint: {str(e)}",
                        exc_info=True,
                        extra={"task_id": self.task_id, "checkpoint_id": checkpoint_id},
                    )
                    return False

        # Restore current phase
        self.current_phase = checkpoint_data.get("current_phase")

        # Restore phase statuses
        self.phase_statuses = checkpoint_data.get("phase_statuses", {}).copy()

        # Log checkpoint restoration
        logger.info(
            f"Restored ETL state from checkpoint: {checkpoint_id}",
            extra={
                "task_id": self.task_id,
                "checkpoint_id": checkpoint_id,
                "current_phase": self.current_phase,
                "phase_statuses": self.phase_statuses,
            },
        )

        return True

    @with_context(operation="record_memory_usage")
    def _record_memory_usage(self, phase: str, stage: str) -> None:
        """
        Record current memory usage.

        Args:
            phase: Current phase
            stage: Stage within the phase (start, end)
        """
        # Get memory usage
        memory_usage = get_system_metrics()

        # Add to metrics
        self.metrics["memory_usage"].append(
            {
                "timestamp": datetime.datetime.now().isoformat(),
                "phase": phase,
                "stage": stage,
                "memory_rss_mb": memory_usage["memory_rss_mb"],
                "memory_vms_mb": memory_usage["memory_vms_mb"],
                "cpu_percent": memory_usage["cpu_percent"],
            }
        )

        # Log memory usage
        logger.debug(
            f"Memory usage at {stage} of phase {phase}: {memory_usage['memory_rss_mb']:.2f} MB (RSS)",
            extra={
                "task_id": self.task_id,
                "phase": phase,
                "stage": stage,
                "metrics": {
                    "memory_rss_mb": memory_usage["memory_rss_mb"],
                    "memory_vms_mb": memory_usage["memory_vms_mb"],
                    "cpu_percent": memory_usage["cpu_percent"],
                },
            },
        )

        # Check memory limit
        if memory_usage["memory_rss_mb"] > self.memory_limit_mb:
            logger.warning(
                f"Memory usage exceeds limit: {memory_usage['memory_rss_mb']:.2f} MB > {self.memory_limit_mb} MB",
                extra={
                    "task_id": self.task_id,
                    "phase": phase,
                    "memory_rss_mb": memory_usage["memory_rss_mb"],
                    "memory_limit_mb": self.memory_limit_mb,
                },
            )
            # Trigger garbage collection
            import gc

            gc.collect()

    @with_context(operation="update_progress")
    @log_call(level=logging.DEBUG)
    def update_progress(
        self, phase: str, current: int, total: int, item_type: str = "items"
    ) -> None:
        """
        Update progress for the current phase.

        Args:
            phase: Current phase
            current: Current progress
            total: Total items to process
            item_type: Type of items being processed
        """
        # Update progress tracker
        self.progress_tracker.update(phase, current, total)

        # Calculate percentage
        percentage = int((current / total) * 100) if total > 0 else 0

        # Log progress
        if current % max(1, int(total / 10)) == 0 or current == total:
            logger.info(
                f"Progress for phase {phase}: {current}/{total} {item_type} ({percentage}%)",
                extra={
                    "task_id": self.task_id,
                    "phase": phase,
                    "current": current,
                    "total": total,
                    "percentage": percentage,
                    "item_type": item_type,
                },
            )

        # Update processed items in metrics
        if phase not in self.metrics["processed_items"]:
            self.metrics["processed_items"][phase] = {}
        self.metrics["processed_items"][phase] = {
            "current": current,
            "total": total,
            "percentage": percentage,
            "item_type": item_type,
            "last_updated": datetime.datetime.now().isoformat(),
        }

    @with_context(operation="get_summary")
    @log_execution_time(level=logging.DEBUG)
    def get_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the ETL process.

        Returns:
            Dictionary with ETL process summary
        """
        # Calculate overall duration
        end_time = datetime.datetime.now()
        duration = (end_time - self.start_time).total_seconds()

        # Create summary
        summary = {
            "task_id": self.task_id,
            "start_time": self.start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration": duration,
            "phases": self.phase_statuses.copy(),
            "errors": len(self.errors),
            "user_id": self.user_id,
            "user_display_name": self.user_display_name,
            "export_date": self.export_date,
            "export_id": self.export_id,
            "metrics": {
                "duration": self.metrics["duration"],
                "processed_items": self.metrics["processed_items"],
                "errors": self.metrics["errors"],
                "memory_usage": {
                    "current_rss_mb": get_system_metrics()["memory_rss_mb"],
                    "peak_rss_mb": max(
                        [
                            m.get("memory_rss_mb", 0)
                            for m in self.metrics["memory_usage"]
                        ]
                        if self.metrics["memory_usage"]
                        else [0]
                    ),
                },
            },
        }

        # Log summary
        logger.info(
            f"ETL process summary for task {self.task_id}",
            extra={
                "task_id": self.task_id,
                "duration": duration,
                "phases": self.phase_statuses,
                "errors": len(self.errors),
                "export_id": self.export_id,
            },
        )

        return summary

    @with_context(operation="save_summary")
    @log_execution_time(level=logging.INFO)
    def save_summary(self, output_path: Optional[str] = None) -> str:
        """
        Save a summary of the ETL process to a file.

        Args:
            output_path: Path to save the summary (generated if not provided)

        Returns:
            Path to the saved summary file
        """
        # Get summary
        summary = self.get_summary()

        # Generate output path if not provided
        if output_path is None:
            if not self.output_dir:
                raise ValueError(
                    "Output directory is required when output_path is not provided"
                )

            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(self.output_dir, f"etl_summary_{timestamp}.json")

        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Save summary to file
        with open(output_path, "w") as f:
            json.dump(summary, f, cls=DateTimeEncoder, indent=2)

        # Log summary save
        logger.info(
            f"Saved ETL summary to file: {output_path}",
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
        Check if the pipeline can resume from a specific phase.

        Args:
            phase_name: Name of the phase to check

        Returns:
            True if the pipeline can resume from the specified phase, False otherwise
        """
        # Check if we have a checkpoint for the previous phase
        phases = ["extract", "transform", "load"]

        if phase_name not in phases:
            logger.error(f"Invalid phase name: {phase_name}")
            return False

        # If we're trying to resume from the first phase, we can always do that
        if phase_name == "extract":
            return True

        # For other phases, check if the previous phase was completed
        phase_index = phases.index(phase_name)
        previous_phase = phases[phase_index - 1]

        # Check if the previous phase was completed successfully
        previous_phase_status = self.phase_statuses.get(previous_phase)
        if previous_phase_status == "completed":
            # Also check if we have the necessary data
            if previous_phase == "extract" and not self.raw_data:
                logger.warning(
                    "Cannot resume from transform phase: raw data is missing"
                )
                return False
            elif previous_phase == "transform" and not self.transformed_data:
                logger.warning(
                    "Cannot resume from load phase: transformed data is missing"
                )
                return False
            return True

        logger.warning(
            f"Cannot resume from {phase_name} phase: previous phase {previous_phase} "
            f"status is {previous_phase_status}"
        )
        return False

    @with_context(operation="check_memory")
    @log_call(level=logging.DEBUG)
    def check_memory(self) -> Dict[str, Any]:
        """
        Check current memory usage.

        Returns:
            Dict with memory usage information
        """
        return self.memory_monitor.check_memory()
