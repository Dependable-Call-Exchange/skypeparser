"""
Pipeline manager module for the ETL pipeline.

This module provides the main ETL pipeline class that orchestrates the
extraction, transformation, and loading of Skype export data.
"""

import logging
import json
import os
from typing import Dict, Any, Optional, BinaryIO, List
import datetime

from src.utils.interfaces import (
    ExtractorProtocol,
    TransformerProtocol,
    LoaderProtocol,
    DatabaseConnectionProtocol
)
from src.utils.di import get_service, get_service_provider
from .context import ETLContext

logger = logging.getLogger(__name__)

class ETLPipeline:
    """Main ETL pipeline class that orchestrates the ETL process."""

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
            db_config: Database configuration dictionary
            output_dir: Optional directory to save intermediate files
            memory_limit_mb: Memory limit in MB before forcing garbage collection
            parallel_processing: Whether to use parallel processing for transformations
            chunk_size: Size of message chunks for batch processing
            batch_size: Size of database batch operations
            max_workers: Maximum number of worker threads/processes
            task_id: Unique identifier for this ETL task (generated if not provided)
            context: Optional existing ETLContext to use (for resuming from checkpoints)
            use_di: Whether to use dependency injection for resolving dependencies
        """
        # Use provided context or create a new one
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

        # Register services with the dependency injection container if using DI
        self.use_di = use_di
        if use_di:
            # Import here to avoid circular import
            from src.utils.service_registry import register_all_services
            register_all_services(db_config=db_config, output_dir=output_dir)

            # Get services from the container
            self.extractor = get_service(ExtractorProtocol)
            self.transformer = get_service(TransformerProtocol)
            self.loader = get_service(LoaderProtocol)
        else:
            # Create components directly
            from .extractor import Extractor
            from .transformer import Transformer
            from .loader import Loader

            self.extractor = Extractor(context=self.context)
            self.transformer = Transformer(context=self.context)
            self.loader = Loader(context=self.context)

        logger.info("ETL pipeline initialized")

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
        # Validate input parameters
        self._validate_pipeline_input(file_path, file_obj, user_display_name)

        # Set user display name in context
        if user_display_name:
            self.context.user_display_name = user_display_name
            # Always set user_id based on user_display_name
            self.context.user_id = f"user_{hash(user_display_name) % 10000}"
        else:
            # Set a default user_id if user_display_name is not provided
            self.context.user_id = "unknown_user"

        # Set export_date if not already set
        if not hasattr(self.context, 'export_date') or self.context.export_date is None:
            self.context.export_date = datetime.datetime.now().isoformat()

        # Resume from checkpoint if requested
        if resume_from_checkpoint:
            return self._resume_pipeline(file_path, file_obj, user_display_name)

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

            # Re-raise exception
            raise

    def save_checkpoint(self, checkpoint_dir: Optional[str] = None) -> str:
        """Save the current pipeline state to a checkpoint file.

        Args:
            checkpoint_dir: Directory to save the checkpoint file

        Returns:
            Path to the saved checkpoint file

        Raises:
            ValueError: If checkpoint directory is not writable
            Exception: If an error occurs during checkpoint saving
        """
        try:
            # Use provided checkpoint directory or default to output directory
            checkpoint_dir = checkpoint_dir or self.context.output_dir

            # If no output directory is available, skip checkpoint
            if not checkpoint_dir:
                logger.warning("No checkpoint directory available, skipping checkpoint")
                return ""

            # Ensure checkpoint directory exists
            if not os.path.exists(checkpoint_dir):
                os.makedirs(checkpoint_dir, exist_ok=True)

            # Create checkpoint file path
            checkpoint_file = os.path.join(checkpoint_dir, f"checkpoint_{self.context.task_id}.json")

            # Create a serializable representation of the context
            checkpoint_data = {
                'task_id': self.context.task_id,
                'start_time': self.context.start_time.isoformat() if hasattr(self.context, 'start_time') else None,
                'current_phase': self.context.current_phase if hasattr(self.context, 'current_phase') else None,
                'db_config': self.context.db_config,
                'output_dir': self.context.output_dir,
                'memory_limit_mb': self.context.memory_limit_mb,
                'parallel_processing': self.context.parallel_processing,
                'chunk_size': self.context.chunk_size,
                'batch_size': self.context.batch_size,
                'max_workers': self.context.max_workers
            }

            # Save checkpoint data to file
            with open(checkpoint_file, 'w') as f:
                json.dump(checkpoint_data, f, indent=2)

            logger.info(f"Checkpoint saved to {checkpoint_file}")
            return checkpoint_file
        except Exception as e:
            logger.error(f"Error saving checkpoint: {e}")
            # Don't raise the exception, just log it and continue
            return ""

    @classmethod
    def load_from_checkpoint(cls, checkpoint_file: str, db_config: Optional[Dict[str, Any]] = None) -> 'ETLPipeline':
        """Load a pipeline from a checkpoint file.

        Args:
            checkpoint_file: Path to the checkpoint file
            db_config: Optional database configuration to override the one in the checkpoint

        Returns:
            A new ETLPipeline instance initialized from the checkpoint

        Raises:
            ValueError: If checkpoint file is invalid or not found
            Exception: If an error occurs during checkpoint loading
        """
        try:
            # Validate checkpoint file
            if not os.path.exists(checkpoint_file):
                error_msg = f"Checkpoint file not found: {checkpoint_file}"
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Load checkpoint data from file
            with open(checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)

            # Extract configuration from checkpoint
            checkpoint_db_config = checkpoint_data.get('db_config', {})
            output_dir = checkpoint_data.get('output_dir')
            memory_limit_mb = checkpoint_data.get('memory_limit_mb', 1024)
            parallel_processing = checkpoint_data.get('parallel_processing', True)
            chunk_size = checkpoint_data.get('chunk_size', 1000)
            batch_size = checkpoint_data.get('batch_size', 100)
            max_workers = checkpoint_data.get('max_workers')
            task_id = checkpoint_data.get('task_id')

            # Use provided db_config if available, otherwise use the one from the checkpoint
            if db_config is None:
                db_config = checkpoint_db_config

            # Create a new context with the checkpoint configuration
            context = ETLContext(
                db_config=db_config,
                output_dir=output_dir,
                memory_limit_mb=memory_limit_mb,
                parallel_processing=parallel_processing,
                chunk_size=chunk_size,
                batch_size=batch_size,
                max_workers=max_workers,
                task_id=task_id
            )

            # Create a new pipeline with the context
            pipeline = cls(
                db_config=db_config,
                context=context,
                output_dir=output_dir,
                memory_limit_mb=memory_limit_mb,
                parallel_processing=parallel_processing,
                chunk_size=chunk_size,
                batch_size=batch_size,
                max_workers=max_workers
            )

            logger.info(f"Pipeline loaded from checkpoint: {checkpoint_file}")
            return pipeline
        except Exception as e:
            logger.error(f"Error loading checkpoint: {e}")
            raise

    def get_available_checkpoints(self) -> List[str]:
        """Get a list of available checkpoint files.

        Returns:
            List of paths to available checkpoint files
        """
        checkpoint_dir = self.context.output_dir
        if not os.path.exists(checkpoint_dir):
            return []

        # Find all checkpoint files
        checkpoint_files = [os.path.join(checkpoint_dir, f) for f in os.listdir(checkpoint_dir)
                           if f.startswith("checkpoint_") and f.endswith(".json")]

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
