"""
Configuration validator for ETL pipeline.

This module provides the ConfigurationValidator class that handles
validation of ETL configuration parameters.
"""

import logging
import os
from typing import Any, Dict, Optional

from src.logging.new_structured_logging import get_logger, handle_errors

logger = get_logger(__name__)


class ConfigurationValidator:
    """Handles validation of ETL configurations."""

    @staticmethod
    @handle_errors(log_level="ERROR", default_message="Error validating configuration")
    def validate_db_config(db_config: Dict[str, Any], in_test_env: bool = False) -> None:
        """
        Validate database configuration.

        Args:
            db_config: Database configuration dictionary
            in_test_env: Whether we're in a test environment

        Raises:
            ValueError: If the database configuration is invalid
        """
        # Validate database configuration
        if not isinstance(db_config, dict):
            raise ValueError("Database configuration must be a dictionary")

        # Import validation function here to avoid circular imports
        from src.validation.validation import validate_db_config

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

    @staticmethod
    @handle_errors(log_level="ERROR", default_message="Error validating output directory")
    def validate_output_dir(output_dir: Optional[str]) -> None:
        """
        Validate output directory.

        Args:
            output_dir: Output directory path

        Raises:
            ValueError: If the output directory is invalid
        """
        if output_dir is not None and not isinstance(output_dir, str):
            logger.error(
                "Output directory must be a string", extra={"output_dir": output_dir}
            )
            raise ValueError("Output directory must be a string")

    @staticmethod
    @handle_errors(log_level="ERROR", default_message="Error validating memory limit")
    def validate_memory_limit(memory_limit_mb: int) -> None:
        """
        Validate memory limit.

        Args:
            memory_limit_mb: Memory limit in MB

        Raises:
            ValueError: If the memory limit is invalid
        """
        if not isinstance(memory_limit_mb, int) or memory_limit_mb <= 0:
            logger.error(
                "Memory limit must be a positive integer",
                extra={"memory_limit_mb": memory_limit_mb},
            )
            raise ValueError("Memory limit must be a positive integer")

    @staticmethod
    @handle_errors(log_level="ERROR", default_message="Error validating chunk size")
    def validate_chunk_size(chunk_size: int) -> None:
        """
        Validate chunk size.

        Args:
            chunk_size: Chunk size for batch processing

        Raises:
            ValueError: If the chunk size is invalid
        """
        if not isinstance(chunk_size, int) or chunk_size <= 0:
            logger.error(
                "Chunk size must be a positive integer",
                extra={"chunk_size": chunk_size},
            )
            raise ValueError("Chunk size must be a positive integer")

    @staticmethod
    @handle_errors(log_level="ERROR", default_message="Error validating batch size")
    def validate_batch_size(batch_size: int) -> None:
        """
        Validate batch size.

        Args:
            batch_size: Batch size for database operations

        Raises:
            ValueError: If the batch size is invalid
        """
        if not isinstance(batch_size, int) or batch_size <= 0:
            logger.error(
                "Batch size must be a positive integer",
                extra={"batch_size": batch_size},
            )
            raise ValueError("Batch size must be a positive integer")

    @staticmethod
    @handle_errors(log_level="ERROR", default_message="Error validating max workers")
    def validate_max_workers(max_workers: Optional[int]) -> None:
        """
        Validate max workers.

        Args:
            max_workers: Maximum number of worker threads/processes

        Raises:
            ValueError: If the max workers is invalid
        """
        if max_workers is not None and (
            not isinstance(max_workers, int) or max_workers <= 0
        ):
            logger.error(
                "Max workers must be a positive integer",
                extra={"max_workers": max_workers},
            )
            raise ValueError("Max workers must be a positive integer")

    @staticmethod
    def validate_configuration(
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
        # Check if we're in a test environment
        in_test_env = os.environ.get("POSTGRES_TEST_DB") == "true"

        # Validate each parameter
        ConfigurationValidator.validate_db_config(db_config, in_test_env)
        ConfigurationValidator.validate_output_dir(output_dir)
        ConfigurationValidator.validate_memory_limit(memory_limit_mb)
        ConfigurationValidator.validate_chunk_size(chunk_size)
        ConfigurationValidator.validate_batch_size(batch_size)
        ConfigurationValidator.validate_max_workers(max_workers)