"""
Pipeline manager module for the ETL pipeline.

This module provides the main ETL pipeline class that orchestrates the
extraction, transformation, and loading of Skype export data.
"""

import logging
import json
import os
from typing import Dict, Any, Optional, BinaryIO, List
from datetime import datetime

from src.utils.interfaces import (
    ExtractorProtocol,
    TransformerProtocol,
    LoaderProtocol,
    DatabaseConnectionProtocol
)
from src.utils.di import get_service, get_service_provider
from .context import ETLContext

import psutil

# Configure logger
logger = logging.getLogger(__name__)

class MemoryMonitor:
    """
    Monitor memory usage during ETL operations.

    This class provides utilities to check memory usage and ensure
    that operations don't exceed specified memory limits.
    """

    def __init__(self, memory_limit_mb: int = 1024):
        """
        Initialize the memory monitor.

        Args:
            memory_limit_mb: Maximum memory usage allowed in MB
        """
        self.memory_limit_mb = memory_limit_mb
        self.memory_limit_bytes = memory_limit_mb * 1024 * 1024
        self.logger = logging.getLogger(__name__)

    def check_memory(self) -> Dict[str, Any]:
        """
        Check current memory usage.

        Returns:
            Dict with memory usage information
        """
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        current_usage_mb = memory_info.rss / (1024 * 1024)

        memory_data = {
            "current_usage_mb": current_usage_mb,
            "limit_mb": self.memory_limit_mb,
            "percentage": (current_usage_mb / self.memory_limit_mb) * 100,
            "exceeded": current_usage_mb > self.memory_limit_mb
        }

        if memory_data["exceeded"]:
            self.logger.warning(
                f"Memory usage exceeded: {memory_data['current_usage_mb']:.2f}MB "
                f"(limit: {self.memory_limit_mb}MB)"
            )

        return memory_data

    def is_memory_available(self, required_mb: float) -> bool:
        """
        Check if there's enough memory available for an operation.

        Args:
            required_mb: Required memory in MB for the operation

        Returns:
            True if enough memory is available, False otherwise
        """
        memory_data = self.check_memory()
        available_mb = self.memory_limit_mb - memory_data["current_usage_mb"]

        if available_mb < required_mb:
            self.logger.warning(
                f"Not enough memory available. Required: {required_mb:.2f}MB, "
                f"Available: {available_mb:.2f}MB"
            )
            return False

        return True

class ETLPipeline:
    """
    Main ETL pipeline class that orchestrates the extraction, transformation,
    and loading of Skype export data.
    """

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
        context: Optional[ETLContext] = None,
        use_di: bool = True
    ):
        """Initialize the ETL pipeline.

        Args:
            db_config: Database configuration
            output_dir: Directory to store output files
            memory_limit_mb: Memory limit in MB
            parallel_processing: Whether to use parallel processing
            chunk_size: Size of chunks for processing
            batch_size: Size of batches for database operations
            max_workers: Maximum number of workers for parallel processing
            task_id: Task ID for the pipeline
            context: Optional ETL context to use
            use_di: Whether to use dependency injection
        """
        self.db_config = db_config
        self.output_dir = output_dir
        self.memory_limit_mb = memory_limit_mb
        self.parallel_processing = parallel_processing
        self.chunk_size = chunk_size
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.task_id = task_id
        self.use_di = use_di
        self.logger = logger

        # Initialize context
        if context:
            self.context = context
        else:
            self.context = ETLContext(
                db_config=db_config,
                output_dir=output_dir,
                memory_limit_mb=memory_limit_mb,
                parallel_processing=parallel_processing,
                chunk_size=chunk_size,
                batch_size=batch_size,
                max_workers=max_workers,
                task_id=task_id
            )

        # Initialize services
        if use_di:
            self.extractor = get_service(ExtractorProtocol)
            self.transformer = get_service(TransformerProtocol)
            self.loader = get_service(LoaderProtocol)
            self.db_connection = get_service(DatabaseConnectionProtocol)
        else:
            # For testing, allow services to be injected
            self.extractor = None
            self.transformer = None
            self.loader = None
            self.db_connection = None

        self.logger.info("ETL pipeline initialized")

    def run_pipeline(
        self,
        file_path: Optional[str] = None,
        file_obj: Optional[BinaryIO] = None,
        user_display_name: Optional[str] = None,
        resume_from_checkpoint: bool = False
    ) -> Dict[str, Any]:
        """Run the ETL pipeline.

        Args:
            file_path: Path to the Skype export file
            file_obj: File-like object containing the Skype export
            user_display_name: Display name of the user
            resume_from_checkpoint: Whether to resume from a checkpoint

        Returns:
            Dictionary containing the results of the pipeline run

        Raises:
            ValueError: If input parameters are invalid
            Exception: If an error occurs during pipeline execution
        """
        # Resume from checkpoint if requested
        if resume_from_checkpoint:
            return self._resume_pipeline(file_path, file_obj, user_display_name)

        # Validate input parameters
        self._validate_pipeline_input(file_path, file_obj, user_display_name)

        # Set user display name in context
        if user_display_name:
            self.context.user_display_name = user_display_name
            # Only set user_id if it's not already set
            if not hasattr(self.context, 'user_id') or self.context.user_id is None:
                self.context.user_id = f"user_{hash(user_display_name) % 10000}"
        else:
            # Set a default user_id if user_display_name is not provided and user_id is not set
            if not hasattr(self.context, 'user_id') or self.context.user_id is None:
                self.context.user_id = "unknown_user"

        # Set export_date if not already set
        if not hasattr(self.context, 'export_date') or self.context.export_date is None:
            self.context.export_date = datetime.datetime.now().isoformat()

        # Initialize results dictionary
        results = {
            'task_id': self.context.task_id,
            'phases': {
                'extract': {'status': 'pending'},
                'transform': {'status': 'pending'},
                'load': {'status': 'pending'}
            }
        }

        try:
            # Extract phase
            logger.info("Starting extraction phase")
            raw_data = self._run_extract_phase(file_path, file_obj)
            results['phases']['extract'] = {
                'status': 'completed',
                'conversation_count': len(raw_data.get('conversations', {}))
            }

            # Transform phase
            logger.info("Starting transformation phase")
            transformed_data = self._run_transform_phase(raw_data, user_display_name)
            results['phases']['transform'] = {
                'status': 'completed',
                'processed_conversations': len(transformed_data.get('conversations', {})),
                'processed_messages': sum(len(conv.get('messages', []))
                                         for conv in transformed_data.get('conversations', {}).values())
            }

            # Load phase
            logger.info("Starting loading phase")
            export_id = self._run_load_phase(raw_data, transformed_data, file_path)
            results['phases']['load'] = {
                'status': 'completed',
                'export_id': export_id
            }

            # Set overall results
            results['status'] = 'completed'
            results['export_id'] = export_id
            results['conversation_count'] = len(transformed_data.get('conversations', {}))
            results['message_count'] = sum(len(conv.get('messages', []))
                                          for conv in transformed_data.get('conversations', {}).values())

            logger.info(f"ETL pipeline completed successfully with export ID: {export_id}")
            return results

        except Exception as e:
            # Log error
            logger.error(f"Error in ETL pipeline: {e}")

            # Update results with error
            results['status'] = 'failed'
            results['error'] = str(e)
            results['error_details'] = self._generate_error_report(e)

            # Record error in context
            if hasattr(self.context, 'current_phase') and self.context.current_phase:
                self.context.record_error(self.context.current_phase, str(e))

            # Create checkpoint for potential resumption
            if hasattr(self.context, 'current_phase') and self.context.current_phase:
                try:
                    checkpoint_path = self.context.save_checkpoint_to_file()
                    results['checkpoint_path'] = checkpoint_path
                    logger.info(f"Created checkpoint at {checkpoint_path} for potential resumption")
                except Exception as checkpoint_error:
                    logger.error(f"Failed to create checkpoint: {checkpoint_error}")

            # Re-raise exception
            raise

    def _validate_pipeline_input(
        self,
        file_path: Optional[str],
        file_obj: Optional[BinaryIO],
        user_display_name: Optional[str]
    ) -> None:
        """Validate pipeline input parameters.

        Args:
            file_path: Path to the Skype export file
            file_obj: File-like object containing the Skype export
            user_display_name: Display name of the user

        Raises:
            ValueError: If input parameters are invalid
        """
        # Check that at least one of file_path or file_obj is provided
        if file_path is None and file_obj is None:
            error_msg = "Either file_path or file_obj must be provided"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Validate file_path if provided
        if file_path is not None:
            if not isinstance(file_path, str):
                error_msg = f"file_path must be a string, got {type(file_path).__name__}"
                logger.error(error_msg)
                raise ValueError(error_msg)

            if not os.path.exists(file_path):
                error_msg = f"File does not exist: {file_path}"
                logger.error(error_msg)
                raise ValueError(error_msg)

            if not os.path.isfile(file_path):
                error_msg = f"Path is not a file: {file_path}"
                logger.error(error_msg)
                raise ValueError(error_msg)

        # Validate user_display_name if provided
        if user_display_name is not None and not isinstance(user_display_name, str):
            error_msg = f"user_display_name must be a string, got {type(user_display_name).__name__}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def _resume_pipeline(
        self,
        file_path: Optional[str] = None,
        file_obj: Optional[BinaryIO] = None,
        user_display_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Resume the ETL pipeline from a checkpoint.

        Args:
            file_path: Path to the Skype export file
            file_obj: File-like object containing the Skype export
            user_display_name: Display name of the user

        Returns:
            Dictionary containing the results of the pipeline run

        Raises:
            ValueError: If no checkpoint is available
            Exception: If an error occurs during pipeline execution
        """
        # Check if context has checkpoint data
        if not self.context.has_checkpoint():
            error_msg = "No checkpoint available to resume from"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Initialize results dictionary
        results = {
            'task_id': self.context.task_id,
            'phases': {
                'extract': {'status': 'pending'},
                'transform': {'status': 'pending'},
                'load': {'status': 'pending'}
            },
            'resumed_from_checkpoint': True
        }

        try:
            # Check which phase to resume from
            if self.context.get_phase_status('extract') == 'completed':
                # Extract phase already completed
                raw_data = self.context.get_raw_data()
                results['phases']['extract'] = {
                    'status': 'completed',
                    'conversation_count': len(raw_data.get('conversations', {})),
                    'from_checkpoint': True
                }
                logger.info("Resumed from extract phase checkpoint")
            else:
                # Run extract phase
                logger.info("Starting extraction phase")
                raw_data = self._run_extract_phase(file_path, file_obj)
                results['phases']['extract'] = {
                    'status': 'completed',
                    'conversation_count': len(raw_data.get('conversations', {}))
                }

            # Check if transform phase is already completed
            if self.context.get_phase_status('transform') == 'completed':
                # Transform phase already completed
                transformed_data = self.context.get_transformed_data()
                results['phases']['transform'] = {
                    'status': 'completed',
                    'processed_conversations': len(transformed_data.get('conversations', {})),
                    'processed_messages': sum(len(conv.get('messages', []))
                                             for conv in transformed_data.get('conversations', {}).values()),
                    'from_checkpoint': True
                }
                logger.info("Resumed from transform phase checkpoint")
            else:
                # Run transform phase
                logger.info("Starting transformation phase")
                transformed_data = self._run_transform_phase(raw_data, user_display_name)
                results['phases']['transform'] = {
                    'status': 'completed',
                    'processed_conversations': len(transformed_data.get('conversations', {})),
                    'processed_messages': sum(len(conv.get('messages', []))
                                             for conv in transformed_data.get('conversations', {}).values())
                }

            # Check if load phase is already completed
            if self.context.get_phase_status('load') == 'completed':
                # Load phase already completed
                export_id = self.context.get_export_id()
                results['phases']['load'] = {
                    'status': 'completed',
                    'export_id': export_id,
                    'from_checkpoint': True
                }
                logger.info("Resumed from load phase checkpoint")
            else:
                # Run load phase
                logger.info("Starting loading phase")
                export_id = self._run_load_phase(raw_data, transformed_data, file_path)
                results['phases']['load'] = {
                    'status': 'completed',
                    'export_id': export_id
                }

            # Set overall results
            results['status'] = 'completed'
            results['export_id'] = export_id
            results['conversation_count'] = len(transformed_data.get('conversations', {}))
            results['message_count'] = sum(len(conv.get('messages', []))
                                          for conv in transformed_data.get('conversations', {}).values())

            logger.info(f"ETL pipeline resumed and completed successfully with export ID: {export_id}")
            return results

        except Exception as e:
            # Log error
            logger.error(f"Error resuming ETL pipeline: {e}")

            # Update results with error
            results['status'] = 'failed'
            results['error'] = str(e)
            results['error_details'] = self._generate_error_report(e)

            # Record error in context
            if hasattr(self.context, 'current_phase') and self.context.current_phase:
                self.context.record_error(self.context.current_phase, str(e))

            # Create checkpoint for potential resumption
            if hasattr(self.context, 'current_phase') and self.context.current_phase:
                try:
                    checkpoint_path = self.context.save_checkpoint_to_file()
                    results['checkpoint_path'] = checkpoint_path
                    logger.info(f"Created checkpoint at {checkpoint_path} for potential resumption")
                except Exception as checkpoint_error:
                    logger.error(f"Failed to create checkpoint: {checkpoint_error}")

            # Re-raise exception
            raise

    def save_checkpoint(self, checkpoint_dir=None):
        """
        Save the current pipeline state to a checkpoint file.

        Args:
            checkpoint_dir (str, optional): Directory to save the checkpoint file.
                Defaults to output_dir.

        Returns:
            str: Path to the created checkpoint file.

        Raises:
            IOError: If the checkpoint file cannot be created.
        """
        try:
            # Use output_dir if checkpoint_dir is not provided
            checkpoint_dir = checkpoint_dir or self.context.output_dir

            # Create checkpoint directory if it doesn't exist
            os.makedirs(checkpoint_dir, exist_ok=True)

            # Construct checkpoint file path
            checkpoint_file = os.path.join(
                checkpoint_dir,
                f"etl_checkpoint_{self.context.task_id}.json"
            )

            # Create a serializable representation of the context
            checkpoint_data = {
                "task_id": self.context.task_id,
                "start_time": self._format_datetime(self.context.start_time),
                "current_phase": self.context.current_phase,
                "db_config": self.context.db_config,
                "output_dir": self.context.output_dir,
                "memory_limit_mb": self.context.memory_limit_mb,
                "parallel_processing": self.context.parallel_processing,
                "chunk_size": self.context.chunk_size,
                "batch_size": self.context.batch_size,
                "max_workers": self.context.max_workers,
                "phase_statuses": self.context.phase_statuses,
                "phase_results": self.context.phase_results,
                "checkpoints": self.context.checkpoints,
                "errors": self.context.errors,
                "export_id": self.context.export_id,
                "metrics": {
                    "start_time": self._format_datetime(self.context.metrics.get("start_time")),
                    "memory_usage": self.context.metrics.get("memory_usage", []),
                    "duration": {
                        phase: {
                            "start": self._format_datetime(times.get("start")),
                            "end": self._format_datetime(times.get("end")),
                            "duration": times.get("duration")
                        } if times else {}
                        for phase, times in self.context.metrics.get("duration", {}).items()
                    }
                }
            }

            # Add all serializable attributes from the context
            for attr in self.context.SERIALIZABLE_ATTRIBUTES:
                if hasattr(self.context, attr) and attr not in checkpoint_data:
                    value = getattr(self.context, attr)
                    checkpoint_data[attr] = self._format_datetime(value) if isinstance(value, datetime) else value

            # Save checkpoint data to file
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)

            self.logger.info(f"Saved checkpoint to {checkpoint_file}")
            return checkpoint_file
        except Exception as e:
            self.logger.error(f"Error saving checkpoint: {str(e)}")
            raise

    def _format_datetime(self, dt_value):
        """
        Format a datetime object as an ISO string for JSON serialization.

        Args:
            dt_value: The datetime value to format

        Returns:
            str or None: ISO formatted string or None if input is None
        """
        if dt_value is None:
            return None
        if isinstance(dt_value, datetime):
            return dt_value.isoformat()
        return dt_value

    @classmethod
    def load_from_checkpoint(cls, checkpoint_file: str, db_config: Optional[Dict[str, Any]] = None) -> 'ETLPipeline':
        """
        Load a pipeline from a checkpoint file.

        Args:
            checkpoint_file: Path to the checkpoint file
            db_config: Optional database configuration to use instead of the one in the checkpoint

        Returns:
            A new ETLPipeline instance initialized from the checkpoint

        Raises:
            ValueError: If the checkpoint file does not exist or is invalid
            Exception: If an error occurs during pipeline initialization
        """
        if not os.path.exists(checkpoint_file):
            raise ValueError(f"Checkpoint file {checkpoint_file} does not exist")

        try:
            # Load checkpoint data from file
            with open(checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)

            # Extract context data
            context_data = checkpoint_data

            # Log warning if db_config is empty
            if not context_data.get('db_config'):
                logger.warning("No database configuration found in checkpoint")

            # Use provided db_config if available, otherwise use the one from the checkpoint
            db_config = db_config or context_data.get('db_config', {})

            # Initialize parameters from checkpoint
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

            # Create a new context with the extracted parameters
            context = ETLContext(
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

            # Parse datetime strings back to datetime objects
            if 'start_time' in context_data and context_data['start_time']:
                context.start_time = cls._parse_datetime(context_data['start_time'])

            # Restore other serializable attributes from checkpoint
            for attr in context.SERIALIZABLE_ATTRIBUTES:
                if attr in context_data and attr not in ['db_config', 'output_dir', 'memory_limit_mb',
                                                        'parallel_processing', 'chunk_size', 'batch_size',
                                                        'max_workers', 'task_id', 'start_time', 'user_id',
                                                        'user_display_name', 'export_date']:
                    value = context_data[attr]
                    setattr(context, attr, value)

            # Restore metrics with datetime parsing
            if 'metrics' in context_data:
                metrics = context_data.get('metrics', {})
                if 'start_time' in metrics and metrics['start_time']:
                    metrics['start_time'] = cls._parse_datetime(metrics['start_time'])

                # Parse datetime values in duration data
                if 'duration' in metrics:
                    for phase, times in metrics['duration'].items():
                        if 'start' in times and times['start']:
                            times['start'] = cls._parse_datetime(times['start'])
                        if 'end' in times and times['end']:
                            times['end'] = cls._parse_datetime(times['end'])

                context.metrics = metrics

            # Restore current phase, phase results, errors, and custom metadata
            context.current_phase = context_data.get('current_phase')
            context.phase_statuses = context_data.get('phase_statuses', {})
            context.phase_results = context_data.get('phase_results', {})
            context.errors = context_data.get('errors', [])
            context.custom_metadata = context_data.get('custom_metadata', {})

            # Create a new pipeline with the restored context
            pipeline = cls(
                db_config=db_config,
                context=context
            )

            logger.info(f"Pipeline loaded from checkpoint: {checkpoint_file}")
            return pipeline
        except Exception as e:
            logger.error(f"Error loading checkpoint: {str(e)}")
            raise

    @classmethod
    def _parse_datetime(cls, dt_str):
        """
        Parse an ISO format datetime string back to a datetime object.

        Args:
            dt_str: ISO formatted datetime string

        Returns:
            datetime object or None if input is None
        """
        if dt_str is None:
            return None
        try:
            return datetime.datetime.fromisoformat(dt_str)
        except (ValueError, TypeError):
            return dt_str

    def get_available_checkpoints(self) -> List[str]:
        """Get a list of available checkpoint files.

        Returns:
            List of paths to available checkpoint files
        """
        checkpoint_dir = self.context.output_dir
        if not checkpoint_dir or not os.path.exists(checkpoint_dir):
            return []

        # Find all checkpoint files (both old "checkpoint_" and new "etl_checkpoint_" format)
        checkpoint_files = [os.path.join(checkpoint_dir, f) for f in os.listdir(checkpoint_dir)
                           if (f.startswith("etl_checkpoint_") or f.startswith("checkpoint_")) and f.endswith(".json")]

        return checkpoint_files

    def _run_extract_phase(self, file_path: Optional[str] = None, file_obj: Optional[BinaryIO] = None) -> Dict[str, Any]:
        """Run the extract phase of the ETL pipeline.

        Args:
            file_path: Path to the Skype export file
            file_obj: File-like object containing the Skype export

        Returns:
            Raw data extracted from the source

        Raises:
            Exception: If an error occurs during extraction
        """
        try:
            # Update context phase
            self.context.start_phase('extract')

            # Run extraction
            raw_data = self.extractor.extract(file_path=file_path, file_obj=file_obj)

            # Save checkpoint
            if self.context.output_dir:
                self.save_checkpoint()

            return raw_data
        except Exception as e:
            # Log error
            logger.error(f"Error in extract phase: {e}")

            # Update context with error
            self.context.record_error('extract', str(e))

            # Re-raise exception
            raise

    def _run_transform_phase(self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None) -> Dict[str, Any]:
        """Run the transform phase of the ETL pipeline.

        Args:
            raw_data: Raw data from the extract phase
            user_display_name: Display name of the user

        Returns:
            Transformed data

        Raises:
            Exception: If an error occurs during transformation
        """
        try:
            # Update context phase
            self.context.start_phase('transform')

            # Run transformation
            transformed_data = self.transformer.transform(raw_data, user_display_name)

            # Save checkpoint
            if self.context.output_dir:
                self.save_checkpoint()

            return transformed_data
        except Exception as e:
            # Log error
            logger.error(f"Error in transform phase: {e}")

            # Update context with error
            self.context.record_error('transform', str(e))

            # Re-raise exception
            raise

    def _run_load_phase(self, raw_data: Dict[str, Any], transformed_data: Dict[str, Any], file_source: Optional[str] = None) -> int:
        """Run the load phase of the ETL pipeline.

        Args:
            raw_data: Raw data from the extract phase
            transformed_data: Transformed data from the transform phase
            file_source: Original file source (path or name)

        Returns:
            Export ID of the loaded data

        Raises:
            Exception: If an error occurs during loading
        """
        try:
            # Update context phase
            self.context.start_phase('load')

            # Connect to database
            self.loader.connect_db()

            try:
                # Run loading
                export_id = self.loader.load(raw_data, transformed_data, file_source)

                # Save checkpoint
                if self.context.output_dir:
                    self.save_checkpoint()

                return export_id
            finally:
                # Close database connection
                self.loader.close_db()
        except Exception as e:
            # Log error
            logger.error(f"Error in load phase: {e}")

            # Update context with error
            self.context.record_error('load', str(e))

            # Re-raise exception
            raise

    def get_context(self) -> ETLContext:
        """Get the ETL context.

        Returns:
            The ETL context
        """
        return self.context

    def _generate_error_report(self, error: Exception) -> Dict[str, Any]:
        """Generate a detailed error report.

        Args:
            error: The exception that occurred

        Returns:
            Dictionary containing detailed error information
        """
        import traceback
        import sys

        error_report = {
            'error_type': type(error).__name__,
            'error_message': str(error),
            'traceback': traceback.format_exc(),
            'phase': getattr(self.context, 'current_phase', 'unknown'),
            'timestamp': datetime.now().isoformat(),
            'context': {}
        }

        # Add relevant context information
        if hasattr(self.context, 'user_id'):
            error_report['context']['user_id'] = self.context.user_id
        if hasattr(self.context, 'user_display_name'):
            error_report['context']['user_display_name'] = self.context.user_display_name
        if hasattr(self.context, 'task_id'):
            error_report['context']['task_id'] = self.context.task_id
        if hasattr(self.context, 'file_source'):
            error_report['context']['file_source'] = self.context.file_source

        # Add phase-specific information
        if hasattr(self.context, 'current_phase'):
            phase = self.context.current_phase
            if phase == 'extract':
                error_report['context']['extract_progress'] = getattr(self.context.progress_tracker, 'extract_progress', {})
            elif phase == 'transform':
                error_report['context']['transform_progress'] = getattr(self.context.progress_tracker, 'transform_progress', {})
            elif phase == 'load':
                error_report['context']['load_progress'] = getattr(self.context.progress_tracker, 'load_progress', {})

        return error_report

    def run(self) -> Dict[str, Any]:
        """Run the ETL pipeline using the context's file_path.

        This is a convenience method that calls run_pipeline with the file_path
        from the context.

        Returns:
            Dictionary containing the results of the pipeline run
        """
        if not hasattr(self.context, 'file_path') or self.context.file_path is None:
            raise ValueError("Context file_path is not set")

        user_display_name = getattr(self.context, 'user_display_name', None)
        return self.run_pipeline(
            file_path=self.context.file_path,
            user_display_name=user_display_name
        )
