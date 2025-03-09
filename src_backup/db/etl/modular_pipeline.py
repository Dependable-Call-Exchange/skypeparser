#!/usr/bin/env python3
"""
Modular ETL Pipeline module.

This module provides a highly modular ETL pipeline for processing Skype export data,
with improved error handling, performance optimizations, and better separation of concerns.
"""

import logging
import os
import time
from datetime import datetime
from typing import Any, BinaryIO, Dict, List, Optional, Tuple

from src.utils.error_handling import (
    ErrorContext,
    generate_error_response,
    handle_errors,
    is_fatal_error,
    report_error,
)
from src.utils.interfaces import ExtractorProtocol, LoaderProtocol, TransformerProtocol
from src.utils.structured_logging import log_call, log_execution_time

from .context import ETLContext

# Set up logger
logger = logging.getLogger(__name__)


class ModularETLPipeline:
    """
    A highly modular implementation of the ETL pipeline for Skype data processing.

    This class orchestrates the extraction, transformation, and loading process,
    with a focus on modularity, error handling, and performance.
    """

    def __init__(
        self,
        context: ETLContext,
        extractor: ExtractorProtocol,
        transformer: TransformerProtocol,
        loader: LoaderProtocol,
    ):
        """
        Initialize the modular ETL pipeline.

        Args:
            context: Shared ETL context
            extractor: Component for extracting data
            transformer: Component for transforming data
            loader: Component for loading data into the database
        """
        self.context = context
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader

        # Track metrics
        self.start_time = None
        self.execution_times = {}

        logger.info("ModularETLPipeline initialized")

    @log_call(logger)
    @log_execution_time(logger)
    @handle_errors(log_level="ERROR", default_message="Error in ETL pipeline")
    def run_pipeline(
        self,
        file_path: Optional[str] = None,
        file_obj: Optional[BinaryIO] = None,
        user_display_name: Optional[str] = None,
        resume_from_checkpoint: bool = False,
    ) -> Dict[str, Any]:
        """
        Run the complete ETL pipeline.

        Args:
            file_path: Path to the Skype export file
            file_obj: File-like object containing the Skype export
            user_display_name: Display name of the user
            resume_from_checkpoint: Whether to resume from a checkpoint

        Returns:
            Dictionary containing pipeline results

        Raises:
            Exception: If an error occurs during pipeline execution
        """
        # Initialize result structure
        results = {
            "status": "started",
            "start_time": datetime.now().isoformat(),
            "phases": {},
            "metrics": {},
        }

        # Record start time
        self.start_time = time.time()

        # Set file source in context
        self.context.set_file_source(file_path=file_path, file_obj=file_obj)

        try:
            # Validate input parameters
            with ErrorContext(
                phase="validation",
                input_params={
                    "file_path": file_path is not None,
                    "file_obj": file_obj is not None,
                    "user_display_name": user_display_name,
                },
            ):
                self._validate_pipeline_input(file_path, file_obj, user_display_name)

            # Resume from checkpoint if requested
            if resume_from_checkpoint and self.context.has_checkpoint():
                logger.info("Resuming from checkpoint")
                return self._resume_pipeline(file_path, file_obj, user_display_name)

            # Extract phase
            logger.info("Starting extraction phase")
            with ErrorContext(phase="extract"):
                raw_data = self._run_extract_phase(file_path, file_obj)
                results["phases"]["extract"] = self.context.phase_results.get(
                    "extract", {}
                )

            # Transform phase
            logger.info("Starting transformation phase")
            with ErrorContext(phase="transform"):
                transformed_data = self._run_transform_phase(
                    raw_data, user_display_name
                )
                results["phases"]["transform"] = self.context.phase_results.get(
                    "transform", {}
                )

            # Load phase
            logger.info("Starting load phase")
            with ErrorContext(phase="load"):
                export_id = self._run_load_phase(raw_data, transformed_data, file_path)
                results["phases"]["load"] = self.context.phase_results.get("load", {})
                results["export_id"] = export_id

            # Record end time and metrics
            end_time = time.time()
            duration = end_time - self.start_time

            # Update results with metrics
            results["status"] = "completed"
            results["end_time"] = datetime.now().isoformat()
            results["duration_seconds"] = duration
            results["metrics"] = self._get_metrics()

            logger.info(
                f"ETL pipeline completed successfully in {duration:.2f} seconds"
            )
            logger.debug(f"Pipeline execution times: {self.execution_times}")

            return results

        except Exception as e:
            # Record error and update results
            error_details = report_error(
                error=e,
                log_level="ERROR" if is_fatal_error(e) else "WARNING",
                additional_context={"current_phase": self.context.current_phase},
            )

            results["status"] = "failed"
            results["error"] = str(e)
            results["error_details"] = error_details

            # Create checkpoint if possible
            if hasattr(self.context, "current_phase") and self.context.current_phase:
                try:
                    checkpoint_path = self.context.save_checkpoint_to_file()
                    results["checkpoint_path"] = checkpoint_path
                    logger.info(
                        f"Created checkpoint at {checkpoint_path} for potential resumption"
                    )
                except Exception as checkpoint_error:
                    logger.error(f"Failed to create checkpoint: {checkpoint_error}")

            # Re-raise exception
            raise

    def _validate_pipeline_input(
        self,
        file_path: Optional[str],
        file_obj: Optional[BinaryIO],
        user_display_name: Optional[str],
    ) -> None:
        """
        Validate pipeline input parameters.

        Args:
            file_path: Path to the Skype export file
            file_obj: File-like object containing the Skype export
            user_display_name: Display name of the user

        Raises:
            ValueError: If the input parameters are invalid
        """
        # Check if either file_path or file_obj is provided
        if file_path is None and file_obj is None:
            raise ValueError("Either file_path or file_obj must be provided")

        # Validate file_path if provided
        if file_path is not None:
            if not os.path.exists(file_path):
                raise ValueError(f"File not found: {file_path}")

            if not os.path.isfile(file_path):
                raise ValueError(f"Not a file: {file_path}")

    @log_execution_time(logger)
    def _run_extract_phase(
        self, file_path: Optional[str] = None, file_obj: Optional[BinaryIO] = None
    ) -> Dict[str, Any]:
        """
        Run the extract phase of the pipeline.

        Args:
            file_path: Path to the Skype export file
            file_obj: File-like object containing the Skype export

        Returns:
            Dictionary containing extracted data

        Raises:
            Exception: If an error occurs during extraction
        """
        phase_start = time.time()

        # Run extraction
        raw_data = self.extractor.extract(file_path=file_path, file_obj=file_obj)

        # Record execution time
        phase_end = time.time()
        self.execution_times["extract"] = phase_end - phase_start

        return raw_data

    @log_execution_time(logger)
    def _run_transform_phase(
        self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Run the transform phase of the pipeline.

        Args:
            raw_data: Raw data from the extract phase
            user_display_name: Display name of the user

        Returns:
            Dictionary containing transformed data

        Raises:
            Exception: If an error occurs during transformation
        """
        phase_start = time.time()

        # Run transformation
        transformed_data = self.transformer.transform(
            raw_data=raw_data, user_display_name=user_display_name
        )

        # Record execution time
        phase_end = time.time()
        self.execution_times["transform"] = phase_end - phase_start

        return transformed_data

    @log_execution_time(logger)
    def _run_load_phase(
        self,
        raw_data: Dict[str, Any],
        transformed_data: Dict[str, Any],
        file_source: Optional[str] = None,
    ) -> int:
        """
        Run the load phase of the pipeline.

        Args:
            raw_data: Raw data from the extract phase
            transformed_data: Transformed data from the transform phase
            file_source: Original file source path

        Returns:
            Export ID

        Raises:
            Exception: If an error occurs during loading
        """
        phase_start = time.time()

        # Connect to database
        self.loader.connect_db()

        try:
            # Run load
            export_id = self.loader.load(
                raw_data=raw_data,
                transformed_data=transformed_data,
                file_source=file_source,
            )

            # Close database connection
            self.loader.close_db()

            # Record execution time
            phase_end = time.time()
            self.execution_times["load"] = phase_end - phase_start

            return export_id

        except Exception as e:
            # Ensure database connection is closed on error
            try:
                self.loader.close_db()
            except Exception:
                pass

            # Re-raise the original exception
            raise

    @log_execution_time(logger)
    @handle_errors(log_level="ERROR", default_message="Error resuming pipeline")
    def _resume_pipeline(
        self,
        file_path: Optional[str] = None,
        file_obj: Optional[BinaryIO] = None,
        user_display_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Resume the pipeline from a checkpoint.

        Args:
            file_path: Path to the Skype export file
            file_obj: File-like object containing the Skype export
            user_display_name: Display name of the user

        Returns:
            Dictionary containing pipeline results
        """
        logger.info("Resuming pipeline from checkpoint")

        # Initialize result structure
        results = {
            "status": "resumed",
            "resume_time": datetime.now().isoformat(),
            "phases": {},
            "metrics": {},
        }

        # Get the last completed phase
        completed_phases = [
            phase
            for phase, checkpoint in self.context.checkpoints.items()
            if checkpoint.get("status") == "completed"
        ]

        last_completed_phase = completed_phases[-1] if completed_phases else None
        logger.info(f"Last completed phase: {last_completed_phase}")

        try:
            # Set file source in context if new one provided
            if file_path or file_obj:
                self.context.set_file_source(file_path=file_path, file_obj=file_obj)

            # Resume based on the last completed phase
            if last_completed_phase == "extract":
                # Extract phase already completed, resume from transform
                logger.info("Resuming from transform phase")

                # Get raw data from context
                raw_data = self.context.raw_data

                # Run transform phase
                with ErrorContext(phase="transform"):
                    transformed_data = self._run_transform_phase(
                        raw_data, user_display_name
                    )
                    results["phases"]["transform"] = self.context.phase_results.get(
                        "transform", {}
                    )

                # Run load phase
                with ErrorContext(phase="load"):
                    export_id = self._run_load_phase(
                        raw_data, transformed_data, file_path
                    )
                    results["phases"]["load"] = self.context.phase_results.get(
                        "load", {}
                    )
                    results["export_id"] = export_id

            elif last_completed_phase == "transform":
                # Transform phase already completed, resume from load
                logger.info("Resuming from load phase")

                # Get raw and transformed data from context
                raw_data = self.context.raw_data
                transformed_data = self.context.transformed_data

                # Run load phase
                with ErrorContext(phase="load"):
                    export_id = self._run_load_phase(
                        raw_data, transformed_data, file_path
                    )
                    results["phases"]["load"] = self.context.phase_results.get(
                        "load", {}
                    )
                    results["export_id"] = export_id

            else:
                # No completed phases or unknown phase, restart pipeline
                logger.warning("No valid checkpoint found, restarting pipeline")
                return self.run_pipeline(file_path, file_obj, user_display_name, False)

            # Update results with metrics
            results["status"] = "completed"
            results["end_time"] = datetime.now().isoformat()
            results["metrics"] = self._get_metrics()

            logger.info("Pipeline successfully resumed and completed")

            return results

        except Exception as e:
            # Record error and update results
            error_details = report_error(
                error=e,
                log_level="ERROR",
                additional_context={
                    "resuming": True,
                    "current_phase": self.context.current_phase,
                },
            )

            results["status"] = "failed"
            results["error"] = str(e)
            results["error_details"] = error_details

            # Re-raise exception
            raise

    def _get_metrics(self) -> Dict[str, Any]:
        """
        Get performance metrics for the pipeline execution.

        Returns:
            Dictionary containing metrics
        """
        metrics = {
            "execution_times": self.execution_times,
            "total_duration": sum(self.execution_times.values()),
            "phase_metrics": {
                phase: result
                for phase, result in self.context.phase_results.items()
                if phase in ["extract", "transform", "load"]
            },
        }

        # Add context metrics
        if hasattr(self.context, "metrics"):
            metrics.update(self.context.metrics)

        return metrics

    def get_context(self) -> ETLContext:
        """
        Get the ETL context.

        Returns:
            The ETL context
        """
        return self.context

    @classmethod
    def from_config(cls, config: Dict[str, Any]):
        """
        Create a pipeline from configuration.

        Args:
            config: Configuration dictionary

        Returns:
            ModularETLPipeline instance
        """
        from .pipeline_factory import PipelineFactory

        # Create factory and pipeline
        factory = PipelineFactory(config)
        pipeline = factory.create_pipeline(cls)

        return pipeline
