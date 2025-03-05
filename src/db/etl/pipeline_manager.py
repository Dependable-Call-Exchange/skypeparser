"""
Pipeline manager module for the ETL pipeline.

This module provides the main ETL pipeline class that orchestrates the
extraction, transformation, and loading of Skype export data.
"""

import logging
import json
import os
from typing import Dict, Any, Optional, BinaryIO, List

from .extractor import Extractor
from .transformer import Transformer
from .loader import Loader
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
        context: Optional[ETLContext] = None
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
        """
        # Use provided context or create a new one
        if context:
            self.context = context
            logger.info(f"Using existing ETL context with task ID: {self.context.task_id}")
        else:
            # Create shared context
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
            logger.info(f"Created new ETL context with task ID: {self.context.task_id}")

        # Initialize components with shared context
        self.extractor = Extractor(context=self.context)
        self.transformer = Transformer(context=self.context)
        self.loader = Loader(context=self.context)

        logger.info("Initialized ETL pipeline with shared context")

    def run_pipeline(
        self,
        file_path: Optional[str] = None,
        file_obj: Optional[BinaryIO] = None,
        user_display_name: Optional[str] = None,
        resume_from_checkpoint: bool = False
    ) -> Dict[str, Any]:
        """Run the complete ETL pipeline.

        Args:
            file_path: Path to the Skype export file
            file_obj: File-like object containing Skype export data
            user_display_name: Display name of the user
            resume_from_checkpoint: Whether to attempt to resume from the latest checkpoint

        Returns:
            Dict containing pipeline results and statistics
        """
        logger.info("Starting ETL pipeline")

        # Set file source in context
        self.context.set_file_source(file_path, file_obj)

        try:
            # Check if we should resume from a checkpoint
            if resume_from_checkpoint and self.context.checkpoints:
                logger.info("Attempting to resume from checkpoint")
                return self._resume_pipeline(file_path, file_obj, user_display_name)

            # Run the pipeline normally
            # Extract phase
            raw_data = self._run_extract_phase(file_path, file_obj)

            # Transform phase
            transformed_data = self._run_transform_phase(raw_data, user_display_name)

            # Load phase
            export_id = self._run_load_phase(raw_data, transformed_data, file_path)

            # Get final summary
            summary = self.context.get_summary()
            summary['success'] = True
            summary['export_id'] = export_id

            logger.info(f"ETL pipeline completed successfully with export ID: {export_id}")
            return summary

        except Exception as e:
            logger.exception(f"Error in ETL pipeline: {e}")

            # Record fatal error
            self.context.record_error(
                phase=self.context.current_phase or "unknown",
                error=e,
                fatal=True
            )

            # Get error summary
            summary = self.context.get_summary()
            summary['success'] = False
            summary['error'] = str(e)

            return summary

        finally:
            # Close any open connections
            self.loader.close_db()

    def _resume_pipeline(
        self,
        file_path: Optional[str] = None,
        file_obj: Optional[BinaryIO] = None,
        user_display_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Resume the pipeline from the latest checkpoint.

        Args:
            file_path: Path to the Skype export file
            file_obj: File-like object containing Skype export data
            user_display_name: Display name of the user

        Returns:
            Dict containing pipeline results and statistics
        """
        # Determine the latest completed phase
        completed_phases = list(self.context.checkpoints.keys())
        if not completed_phases:
            logger.warning("No checkpoints found to resume from")
            return self.run_pipeline(file_path, file_obj, user_display_name, resume_from_checkpoint=False)

        # Sort phases in order of execution
        phase_order = {"extract": 1, "transform": 2, "load": 3}
        completed_phases.sort(key=lambda p: phase_order.get(p, 999))
        latest_phase = completed_phases[-1]

        logger.info(f"Resuming from checkpoint after '{latest_phase}' phase")

        try:
            # Resume based on the latest completed phase
            if latest_phase == "extract":
                # Extract phase is complete, resume from transform
                if not self.context.raw_data:
                    logger.warning("Raw data not available in context, cannot resume")
                    return self.run_pipeline(file_path, file_obj, user_display_name, resume_from_checkpoint=False)

                raw_data = self.context.raw_data

                # Run transform phase
                transformed_data = self._run_transform_phase(raw_data, user_display_name)

                # Run load phase
                export_id = self._run_load_phase(raw_data, transformed_data, file_path)

            elif latest_phase == "transform":
                # Transform phase is complete, resume from load
                if not self.context.raw_data or not self.context.transformed_data:
                    logger.warning("Required data not available in context, cannot resume")
                    return self.run_pipeline(file_path, file_obj, user_display_name, resume_from_checkpoint=False)

                raw_data = self.context.raw_data
                transformed_data = self.context.transformed_data

                # Run load phase
                export_id = self._run_load_phase(raw_data, transformed_data, file_path)

            elif latest_phase == "load":
                # Load phase is complete, nothing to resume
                logger.info("All phases already completed, nothing to resume")
                export_id = self.context.export_id
                if not export_id:
                    logger.warning("Export ID not available in context")
                    return self.run_pipeline(file_path, file_obj, user_display_name, resume_from_checkpoint=False)
            else:
                logger.warning(f"Unknown phase '{latest_phase}', cannot resume")
                return self.run_pipeline(file_path, file_obj, user_display_name, resume_from_checkpoint=False)

            # Get final summary
            summary = self.context.get_summary()
            summary['success'] = True
            summary['export_id'] = export_id
            summary['resumed'] = True
            summary['resumed_from_phase'] = latest_phase

            logger.info(f"ETL pipeline resumed and completed successfully with export ID: {export_id}")
            return summary

        except Exception as e:
            logger.exception(f"Error resuming ETL pipeline: {e}")

            # Record fatal error
            self.context.record_error(
                phase=self.context.current_phase or "resume",
                error=e,
                fatal=True
            )

            # Get error summary
            summary = self.context.get_summary()
            summary['success'] = False
            summary['error'] = str(e)
            summary['resumed'] = True
            summary['resumed_from_phase'] = latest_phase

            return summary

    def save_checkpoint(self, checkpoint_dir: Optional[str] = None) -> str:
        """Save the current context and checkpoints to a file.

        Args:
            checkpoint_dir: Directory to save the checkpoint file (defaults to context.output_dir)

        Returns:
            str: Path to the saved checkpoint file
        """
        # Use provided directory or context output directory
        save_dir = checkpoint_dir or self.context.output_dir
        if not save_dir:
            raise ValueError("No output directory specified for saving checkpoint")

        # Create directory if it doesn't exist
        os.makedirs(save_dir, exist_ok=True)

        # Generate checkpoint filename
        checkpoint_file = os.path.join(save_dir, f"etl_checkpoint_{self.context.task_id}.json")

        # Serialize context to JSON
        checkpoint_data = self.context.serialize_checkpoint()

        # Save to file
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint_data, f, indent=2)

        logger.info(f"Saved checkpoint to {checkpoint_file}")
        return checkpoint_file

    @classmethod
    def load_from_checkpoint(cls, checkpoint_file: str, db_config: Optional[Dict[str, Any]] = None) -> 'ETLPipeline':
        """Load a pipeline from a checkpoint file.

        Args:
            checkpoint_file: Path to the checkpoint file
            db_config: Optional database configuration to override the one in the checkpoint

        Returns:
            ETLPipeline: A new pipeline instance with the restored context
        """
        # Load checkpoint data
        with open(checkpoint_file, 'r') as f:
            checkpoint_data = json.load(f)

        # Create context from checkpoint
        context = ETLContext.restore_from_checkpoint(checkpoint_data)

        # Override database configuration if provided
        if db_config:
            context.db_config = db_config

        # Create pipeline with restored context
        pipeline = cls(
            db_config={},  # Empty dict as we're using the context
            context=context
        )

        logger.info(f"Loaded pipeline from checkpoint file: {checkpoint_file}")
        return pipeline

    def get_available_checkpoints(self) -> List[str]:
        """Get a list of available checkpoint phases.

        Returns:
            List[str]: List of phase names that have checkpoints
        """
        return list(self.context.checkpoints.keys())

    def _run_extract_phase(self, file_path: Optional[str] = None, file_obj: Optional[BinaryIO] = None) -> Dict[str, Any]:
        """Run the extraction phase of the pipeline.

        Args:
            file_path: Path to the Skype export file
            file_obj: File-like object containing Skype export data

        Returns:
            Dict containing the raw data
        """
        try:
            # Start extract phase in context
            self.context.start_phase("extract")

            # Run extraction
            raw_data = self.extractor.extract(file_path, file_obj)

            # Store raw data in context
            self.context.raw_data = raw_data

            # End extract phase
            self.context.end_phase({
                'success': True,
                'conversation_count': len(raw_data.get('conversations', [])),
                'user_id': raw_data.get('userId', '')
            })

            return raw_data

        except Exception as e:
            # Record error and re-raise
            self.context.record_error("extract", e, fatal=True)
            raise

    def _run_transform_phase(self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None) -> Dict[str, Any]:
        """Run the transformation phase of the pipeline.

        Args:
            raw_data: Raw data from the extraction phase
            user_display_name: Display name of the user

        Returns:
            Dict containing the transformed data
        """
        try:
            # Start transform phase in context
            self.context.start_phase(
                "transform",
                total_conversations=len(raw_data.get('conversations', [])),
                total_messages=sum(len(conv.get('MessageList', [])) for conv in raw_data.get('conversations', []))
            )

            # Run transformation
            transformed_data = self.transformer.transform(raw_data, user_display_name)

            # Store transformed data in context
            self.context.transformed_data = transformed_data

            # End transform phase
            self.context.end_phase({
                'success': True,
                'conversation_count': len(transformed_data.get('conversations', {}))
            })

            return transformed_data

        except Exception as e:
            # Record error and re-raise
            self.context.record_error("transform", e, fatal=True)
            raise

    def _run_load_phase(self, raw_data: Dict[str, Any], transformed_data: Dict[str, Any], file_source: Optional[str] = None) -> int:
        """Run the loading phase of the pipeline.

        Args:
            raw_data: Raw data from the extraction phase
            transformed_data: Transformed data from the transformation phase
            file_source: Source of the data (e.g., file path)

        Returns:
            int: The export ID in the database
        """
        try:
            # Start load phase in context
            self.context.start_phase(
                "load",
                total_conversations=len(transformed_data.get('conversations', {})),
                total_messages=sum(len(conv.get('messages', [])) for conv in transformed_data.get('conversations', {}).values())
            )

            # Run loading
            export_id = self.loader.load(raw_data, transformed_data, file_source)

            # Store export ID in context
            self.context.export_id = export_id

            # End load phase
            self.context.end_phase({
                'success': True,
                'export_id': export_id
            })

            return export_id

        except Exception as e:
            # Record error and re-raise
            self.context.record_error("load", e, fatal=True)
            raise

    def get_context(self) -> ETLContext:
        """Get the ETL context.

        Returns:
            ETLContext: The shared context object
        """
        return self.context
