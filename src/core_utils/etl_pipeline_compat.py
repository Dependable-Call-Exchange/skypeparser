#!/usr/bin/env python3
"""
Compatibility layer for the old ETL pipeline.

This module provides a compatibility layer for code that uses the old
SkypeETLPipeline class, redirecting calls to the new modular ETL pipeline.
"""

import logging
from typing import Dict, List, Any, Optional, BinaryIO

from src.etl import ETLPipeline, ETLContext

logger = logging.getLogger(__name__)

class SkypeETLPipeline:
    """
    Compatibility class that mimics the old SkypeETLPipeline interface
    but uses the new modular ETL pipeline internally.
    """

    def __init__(
        self,
        db_config: Optional[Dict[str, Any]] = None,
        db_name: Optional[str] = None,
        db_user: Optional[str] = None,
        db_password: Optional[str] = None,
        db_host: str = "localhost",
        db_port: int = 5432,
        output_dir: Optional[str] = None,
        memory_limit_mb: int = 1024,
        parallel_processing: bool = True,
        chunk_size: int = 1000,
        batch_size: int = 500,
        max_workers: Optional[int] = None
    ):
        """
        Initialize the compatibility layer.

        Args:
            db_config: Database configuration dictionary
            db_name: Database name (alternative to db_config)
            db_user: Database user (alternative to db_config)
            db_password: Database password (alternative to db_config)
            db_host: Database host (alternative to db_config)
            db_port: Database port (alternative to db_config)
            output_dir: Optional directory to save intermediate files
            memory_limit_mb: Memory limit in MB before forcing garbage collection
            parallel_processing: Whether to use parallel processing for transformations
            chunk_size: Size of message chunks for batch processing
            batch_size: Size of database batch inserts
            max_workers: Maximum number of worker processes for parallel processing
        """
        # Create db_config if not provided
        if db_config is None:
            db_config = {
                'dbname': db_name,
                'user': db_user,
                'password': db_password,
                'host': db_host,
                'port': db_port
            }

        # Create the ETL context
        self.context = ETLContext(
            db_config=db_config,
            output_dir=output_dir,
            memory_limit_mb=memory_limit_mb,
            parallel_processing=parallel_processing,
            chunk_size=chunk_size,
            batch_size=batch_size,
            max_workers=max_workers,
            task_id=f"compat-{id(self)}"
        )

        # Create the new ETL pipeline with the context
        self.pipeline = ETLPipeline(
            db_config=db_config,  # This will be ignored as context is provided
            context=self.context
        )

        # Store configuration
        self.db_config = db_config
        self.output_dir = output_dir

        logger.info("Initialized SkypeETLPipeline compatibility layer with ETLContext")

    def run_pipeline(
        self,
        file_path: Optional[str] = None,
        file_obj: Optional[BinaryIO] = None,
        is_tar: bool = False,
        user_display_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Run the ETL pipeline with the given parameters.

        Args:
            file_path: Path to the Skype export file
            file_obj: File-like object containing Skype export data
            is_tar: Whether the file is a tar file (ignored, detected automatically)
            user_display_name: Display name of the user

        Returns:
            Dict containing pipeline results and statistics
        """
        logger.info("Running ETL pipeline through compatibility layer")

        # Run the new pipeline
        results = self.pipeline.run_pipeline(
            file_path=file_path,
            file_obj=file_obj,
            user_display_name=user_display_name
        )

        # Get the context summary
        context_summary = self.context.get_summary()

        # Convert results to the old format if needed
        if results['success']:
            # Extract conversation and message counts
            conversation_count = 0
            message_count = 0

            for phase, stats in context_summary.get('phases', {}).items():
                if phase == 'transform' and stats:
                    conversation_count = stats.get('processed_conversations', 0)
                    message_count = stats.get('processed_messages', 0)

            # Return in the old format
            return {
                'success': True,
                'export_id': results['export_id'],
                'conversations': conversation_count,
                'message_count': message_count,
                'duration_seconds': context_summary.get('total_duration_seconds', 0)
            }
        else:
            return {
                'success': False,
                'error': results.get('error', 'Unknown error'),
                'error_count': context_summary.get('error_count', 0)
            }

    def connect_db(self):
        """Connect to the database (compatibility method)."""
        self.pipeline.loader.connect_db()

    def close_db(self):
        """Close the database connection (compatibility method)."""
        self.pipeline.loader.close_db()

    def get_context(self) -> ETLContext:
        """
        Get the ETLContext instance used by this pipeline.

        This is a new method that allows access to the context for
        advanced monitoring and state management.

        Returns:
            The ETLContext instance
        """
        return self.context

    # Add any other methods from the old ETL pipeline that might be used externally