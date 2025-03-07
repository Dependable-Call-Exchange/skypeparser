"""
ETL Context module for the ETL pipeline.

This module provides a shared context object that manages state across
the different components of the ETL pipeline, including progress tracking,
memory monitoring, configuration, and telemetry.
"""

import logging
import datetime
import uuid
import json
import pickle
import base64
import os
from typing import Dict, Any, Optional, List, BinaryIO, ClassVar, Type

from .utils import ProgressTracker, MemoryMonitor

logger = logging.getLogger(__name__)

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
    """

    # Class variables for checkpoint serialization
    SERIALIZABLE_ATTRIBUTES: ClassVar[List[str]] = [
        'db_config', 'output_dir', 'memory_limit_mb', 'parallel_processing',
        'chunk_size', 'batch_size', 'max_workers', 'task_id', 'start_time',
        'current_phase', 'phase_results', 'checkpoints', 'errors', 'export_id',
        'metrics', 'user_id', 'user_display_name', 'export_date', 'custom_metadata',
        'download_attachments', 'attachments_dir', 'generate_thumbnails', 'extract_metadata'
    ]

    # Data attributes that need special handling for serialization
    DATA_ATTRIBUTES: ClassVar[List[str]] = ['raw_data', 'transformed_data']

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
        extract_metadata: bool = True
    ):
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
        self._validate_configuration(db_config, output_dir, memory_limit_mb,
                                    chunk_size, batch_size, max_workers)

        # Core configuration
        self.db_config = db_config
        self.output_dir = output_dir

        # Performance configuration
        self.memory_limit_mb = memory_limit_mb
        self.parallel_processing = parallel_processing
        self.chunk_size = chunk_size
        self.batch_size = batch_size
        self.max_workers = max_workers

        # Task identification
        self.task_id = task_id or str(uuid.uuid4())
        self.start_time = datetime.datetime.now()

        # User information
        self.user_id = user_id or f"user_{hash(user_display_name or 'unknown') % 10000}"
        self.user_display_name = user_display_name
        self.export_date = export_date or datetime.datetime.now().isoformat()

        # Attachment handling configuration
        self.download_attachments = download_attachments
        self.attachments_dir = attachments_dir or (output_dir and os.path.join(output_dir, 'attachments'))
        self.generate_thumbnails = generate_thumbnails
        self.extract_metadata = extract_metadata

        # Shared utilities
        self.progress_tracker = ProgressTracker()
        self.memory_monitor = MemoryMonitor(memory_limit_mb=memory_limit_mb)

        # State tracking
        self.current_phase = None
        self.phase_results = {}
        self.checkpoints = {}
        self.errors = []

        # Data references
        self.raw_data = None
        self.transformed_data = None
        self.file_source = None
        self.export_id = None

        # Telemetry
        self.metrics = {
            'start_time': self.start_time,
            'memory_usage': [],
            'duration': {},
            'processed_items': {},
            'errors': 0
        }

        logger.info(f"Initialized ETL context with task ID: {self.task_id}")

    def _validate_configuration(self, db_config: Dict[str, Any],
                               output_dir: Optional[str],
                               memory_limit_mb: int,
                               chunk_size: int,
                               batch_size: int,
                               max_workers: Optional[int]) -> None:
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
        in_test_env = os.environ.get('POSTGRES_TEST_DB') == 'true'

        # Import validation function here to avoid circular imports
        from src.utils.validation import validate_db_config
        try:
            # Skip strict validation in test environment
            if not in_test_env:
                validate_db_config(db_config)
        except Exception as e:
            if not in_test_env:
                raise ValueError(f"Invalid database configuration: {str(e)}")
            else:
                logger.warning(f"Database configuration validation skipped in test environment: {str(e)}")

        # Validate output directory
        if output_dir is not None and not isinstance(output_dir, str):
            raise ValueError("Output directory must be a string")

        # Validate memory limit
        if not isinstance(memory_limit_mb, int) or memory_limit_mb <= 0:
            raise ValueError("Memory limit must be a positive integer")

        # Validate chunk size
        if not isinstance(chunk_size, int) or chunk_size <= 0:
            raise ValueError("Chunk size must be a positive integer")

        # Validate batch size
        if not isinstance(batch_size, int) or batch_size <= 0:
            raise ValueError("Batch size must be a positive integer")

        # Validate max workers
        if max_workers is not None and (not isinstance(max_workers, int) or max_workers <= 0):
            raise ValueError("Max workers must be a positive integer")

        logger.info("All configuration parameters validated successfully")

    def start_phase(self, phase_name: str, total_conversations: int = 0, total_messages: int = 0) -> None:
        """
        Start a new phase in the ETL pipeline.

        Args:
            phase_name: Name of the phase (extract, transform, load)
            total_conversations: Total number of conversations to process
            total_messages: Total number of messages to process
        """
        self.current_phase = phase_name
        self.progress_tracker.start_phase(phase_name, total_conversations, total_messages)

        # Record phase start in metrics
        self.metrics['duration'][phase_name] = {
            'start': datetime.datetime.now(),
            'end': None,
            'duration_seconds': None
        }

        logger.info(f"Starting {phase_name} phase")

    def end_phase(self, result: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        End the current phase and record results.

        Args:
            result: Optional result data for the phase

        Returns:
            Dict containing statistics about the completed phase
        """
        if not self.current_phase:
            logger.warning("Attempting to end a phase when no phase is active")
            return {}

        # Get statistics from progress tracker
        stats = self.progress_tracker.finish_phase()

        # Record phase end in metrics
        phase_name = self.current_phase
        end_time = datetime.datetime.now()
        self.metrics['duration'][phase_name]['end'] = end_time
        self.metrics['duration'][phase_name]['duration_seconds'] = stats['duration_seconds']

        # Store processed items metrics
        self.metrics['processed_items'][phase_name] = {
            'conversations': stats['processed_conversations'],
            'messages': stats['processed_messages']
        }

        # Combine stats with result
        if result:
            stats.update(result)

        # Store phase result
        self.phase_results[phase_name] = stats

        # Create checkpoint for this phase
        self._create_checkpoint(phase_name)

        logger.info(f"Completed {phase_name} phase in {stats['duration_seconds']:.2f} seconds")

        # Reset current phase
        self.current_phase = None

        return stats

    def update_progress(self, conversations: int = 0, messages: int = 0) -> None:
        """
        Update progress for the current phase.

        Args:
            conversations: Number of conversations processed
            messages: Number of messages processed
        """
        if conversations > 0:
            self.progress_tracker.update_conversation_progress(conversations)

        if messages > 0:
            self.progress_tracker.update_message_progress(messages)

    def check_memory(self) -> None:
        """
        Check memory usage and trigger garbage collection if needed.
        """
        self.memory_monitor.check_memory()

        # Record memory usage in metrics
        if hasattr(self.memory_monitor, 'last_memory_mb'):
            self.metrics['memory_usage'].append({
                'timestamp': datetime.datetime.now(),
                'memory_mb': self.memory_monitor.last_memory_mb
            })

    def record_error(self, phase: str, error: Exception, fatal: bool = False) -> None:
        """
        Record an error that occurred during processing.

        Args:
            phase: The phase where the error occurred
            error: The exception that was raised
            fatal: Whether this error is fatal to the pipeline
        """
        error_info = {
            'phase': phase,
            'timestamp': datetime.datetime.now(),
            'error_type': type(error).__name__,
            'error_message': str(error),
            'fatal': fatal
        }

        self.errors.append(error_info)
        self.metrics['errors'] += 1

        if fatal:
            logger.error(f"Fatal error in {phase} phase: {error}")
        else:
            logger.warning(f"Non-fatal error in {phase} phase: {error}")

    def _create_checkpoint(self, phase_name: str) -> None:
        """
        Create a checkpoint after completing a phase.

        Args:
            phase_name: Name of the completed phase
        """
        checkpoint = {
            'phase': phase_name,
            'timestamp': datetime.datetime.now(),
            'metrics': {
                'conversations': self.metrics['processed_items'].get(phase_name, {}).get('conversations', 0),
                'messages': self.metrics['processed_items'].get(phase_name, {}).get('messages', 0)
            }
        }

        # Store references to data based on phase
        if phase_name == 'extract':
            checkpoint['raw_data_available'] = self.raw_data is not None
        elif phase_name == 'transform':
            checkpoint['transformed_data_available'] = self.transformed_data is not None
        elif phase_name == 'load':
            checkpoint['export_id'] = self.export_id

        self.checkpoints[phase_name] = checkpoint
        logger.info(f"Created checkpoint for {phase_name} phase")

    def can_resume_from_phase(self, phase_name: str) -> bool:
        """
        Check if the pipeline can resume from a specific phase.

        Args:
            phase_name: Name of the phase to resume from

        Returns:
            bool: True if the pipeline can resume from this phase
        """
        if phase_name not in self.checkpoints:
            return False

        checkpoint = self.checkpoints[phase_name]

        if phase_name == 'extract':
            return checkpoint.get('raw_data_available', False)
        elif phase_name == 'transform':
            return checkpoint.get('transformed_data_available', False)

        return False

    def get_summary(self) -> Dict[str, Any]:
        """
        Get a summary of the ETL process.

        Returns:
            Dict containing summary information about the ETL process
        """
        end_time = datetime.datetime.now()
        total_duration = (end_time - self.start_time).total_seconds()

        return {
            'task_id': self.task_id,
            'start_time': self.start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'total_duration_seconds': total_duration,
            'phases': self.phase_results,
            'export_id': self.export_id,
            'error_count': len(self.errors),
            'fatal_error': any(e['fatal'] for e in self.errors)
        }

    def set_file_source(self, file_path: Optional[str] = None, file_obj: Optional[BinaryIO] = None) -> None:
        """
        Set the file source for the ETL process.

        Args:
            file_path: Path to the input file
            file_obj: File-like object containing input data
        """
        self.file_source = file_path

        if file_path:
            logger.info(f"Using file source: {file_path}")
        elif file_obj and hasattr(file_obj, 'name'):
            logger.info(f"Using file object: {file_obj.name}")
        else:
            logger.info("Using file object (no name available)")

    def serialize_checkpoint(self) -> Dict[str, Any]:
        """
        Serialize the context state to a dictionary for checkpointing.

        Returns:
            Dict containing serialized context state
        """
        # Create base serialized data
        serialized = {
            'checkpoint_version': '1.0',
            'serialized_at': datetime.datetime.now().isoformat(),
            'context': {}
        }

        # Ensure db_config is included and not empty
        if not hasattr(self, 'db_config') or not self.db_config:
            logger.warning("Database configuration is missing or empty in context")

        # Serialize basic attributes
        for attr in self.SERIALIZABLE_ATTRIBUTES:
            if hasattr(self, attr):
                value = getattr(self, attr)

                # Handle datetime objects
                if isinstance(value, datetime.datetime):
                    serialized['context'][attr] = value.isoformat()
                else:
                    serialized['context'][attr] = value

        # Handle data attributes separately
        data_files = {}
        for attr in self.DATA_ATTRIBUTES:
            if hasattr(self, attr) and getattr(self, attr) is not None:
                # If we have an output directory, save data to files
                if self.output_dir:
                    data_file = os.path.join(self.output_dir, f"{self.task_id}_{attr}.json")
                    with open(data_file, 'w') as f:
                        json.dump(getattr(self, attr), f)
                    data_files[attr] = data_file
                    serialized['context'][f"{attr}_file"] = data_file
                else:
                    # Otherwise, include data directly in the serialized output
                    # This could be large, so we might want to consider alternatives
                    serialized['context'][attr] = getattr(self, attr)

        # Include file source
        if self.file_source:
            serialized['context']['file_source'] = self.file_source

        # Include checkpoint metadata
        serialized['available_checkpoints'] = list(self.checkpoints.keys())
        serialized['data_files'] = data_files

        return serialized

    @classmethod
    def restore_from_checkpoint(cls, checkpoint_data: Dict[str, Any]) -> 'ETLContext':
        """
        Restore a context from serialized checkpoint data.

        Args:
            checkpoint_data: Serialized checkpoint data

        Returns:
            ETLContext: Restored context instance
        """
        # Extract context data - handle both old and new format
        context_data = checkpoint_data.get('context', checkpoint_data)

        # Handle required initialization parameters
        db_config = context_data.get('db_config', {})

        # Log warning if db_config is empty
        if not db_config:
            logger.warning("Database configuration is missing or empty in checkpoint")

        output_dir = context_data.get('output_dir')
        memory_limit_mb = context_data.get('memory_limit_mb', 1024)
        parallel_processing = context_data.get('parallel_processing', True)
        chunk_size = context_data.get('chunk_size', 1000)
        batch_size = context_data.get('batch_size', 100)
        max_workers = context_data.get('max_workers')
        task_id = context_data.get('task_id')
        user_id = context_data.get('user_id')
        user_display_name = context_data.get('user_display_name')
        export_date = context_data.get('export_date')

        # Create new context instance
        context = cls(
            db_config=db_config,
            output_dir=output_dir,
            memory_limit_mb=memory_limit_mb,
            parallel_processing=parallel_processing,
            chunk_size=chunk_size,
            batch_size=batch_size,
            max_workers=max_workers,
            task_id=task_id,
            user_id=user_id,
            user_display_name=user_display_name,
            export_date=export_date
        )

        # Restore other serializable attributes
        for attr in cls.SERIALIZABLE_ATTRIBUTES:
            if attr in context_data and attr not in ['db_config', 'output_dir', 'memory_limit_mb',
                                                    'parallel_processing', 'chunk_size', 'batch_size',
                                                    'max_workers', 'task_id', 'user_id', 'user_display_name',
                                                    'export_date']:
                value = context_data[attr]

                # Handle datetime objects
                if attr == 'start_time' or (isinstance(value, str) and 'T' in value and value.endswith('Z')):
                    try:
                        value = datetime.datetime.fromisoformat(value.replace('Z', '+00:00'))
                    except ValueError:
                        # If parsing fails, keep the string value
                        pass

                setattr(context, attr, value)

        # Restore current phase
        if 'current_phase' in context_data:
            context.current_phase = context_data['current_phase']

        # Restore phase results
        if 'phase_results' in context_data:
            context.phase_results = context_data['phase_results']

        # Restore errors
        if 'errors' in context_data:
            context.errors = context_data['errors']

        # Restore custom metadata
        if 'custom_metadata' in context_data:
            context.custom_metadata = context_data['custom_metadata']

        logger.info(f"Restored ETL context with task ID: {context.task_id}")
        return context

    def save_checkpoint_to_file(self, checkpoint_dir: Optional[str] = None) -> str:
        """
        Save the current context state to a checkpoint file.

        Args:
            checkpoint_dir: Directory to save the checkpoint file (defaults to self.output_dir)

        Returns:
            str: Path to the saved checkpoint file
        """
        # Use provided directory or context output directory
        save_dir = checkpoint_dir or self.output_dir
        if not save_dir:
            raise ValueError("No output directory specified for saving checkpoint")

        # Create directory if it doesn't exist
        os.makedirs(save_dir, exist_ok=True)

        # Generate checkpoint filename
        checkpoint_file = os.path.join(save_dir, f"etl_checkpoint_{self.task_id}.json")

        # Serialize context to JSON
        checkpoint_data = self.serialize_checkpoint()

        # Save to file using custom encoder for datetime objects
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2, cls=DateTimeEncoder)

        logger.info(f"Saved checkpoint to {checkpoint_file}")
        return checkpoint_file

    @classmethod
    def load_from_checkpoint_file(cls, checkpoint_file: str) -> 'ETLContext':
        """
        Load a context from a checkpoint file.

        Args:
            checkpoint_file: Path to the checkpoint file

        Returns:
            ETLContext: Restored context instance
        """
        # Load checkpoint data
        with open(checkpoint_file, 'r') as f:
            checkpoint_data = json.load(f)

        # Restore context from checkpoint
        context = cls.restore_from_checkpoint(checkpoint_data)

        logger.info(f"Loaded context from checkpoint file: {checkpoint_file}")
        return context

    def get_phase_status(self, phase: str) -> str:
        """Get the status of a specific phase.

        Args:
            phase: The phase to get the status for

        Returns:
            The status of the phase
        """
        return self.phase_results.get(phase, {}).get('status', 'pending')

    def has_checkpoint(self) -> bool:
        """Check if the context has checkpoint data.

        Returns:
            True if the context has checkpoint data, False otherwise
        """
        return bool(self.checkpoints) or hasattr(self, 'current_phase')

    def set_phase_status(self, phase: str, status: str) -> None:
        """
        Set the status of a phase.

        Args:
            phase: The phase name
            status: The status to set
        """
        if phase not in self.phase_results:
            self.phase_results[phase] = {}

        self.phase_results[phase]['status'] = status
        logger.debug(f"Set phase {phase} status to {status}")

    def set_export_id(self, export_id: int) -> None:
        """
        Set the export ID after data has been loaded into the database.

        Args:
            export_id: The export ID from the database
        """
        self.export_id = export_id
        logger.debug(f"Set export ID to {export_id}")