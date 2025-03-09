"""
Error logger for ETL pipeline.

This module provides the ErrorLogger class that handles
error recording and logging for ETL processes.
"""

import datetime
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from src.utils.new_structured_logging import get_logger, log_call, with_context

logger = get_logger(__name__)


class ErrorStrategy(ABC):
    """Abstract base class for error handling strategies."""

    @abstractmethod
    def handle_error(
        self,
        phase: str,
        error_message: str,
        error_details: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Handle an error.

        Args:
            phase: Phase where the error occurred
            error_message: Error message
            error_details: Additional error details

        Returns:
            Error record
        """
        pass


class FatalErrorStrategy(ErrorStrategy):
    """Strategy for handling fatal errors."""

    def handle_error(
        self,
        phase: str,
        error_message: str,
        error_details: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Handle a fatal error.

        Args:
            phase: Phase where the error occurred
            error_message: Error message
            error_details: Additional error details

        Returns:
            Error record
        """
        # Create error record
        error_record = {
            "phase": phase,
            "timestamp": datetime.datetime.now().isoformat(),
            "message": error_message,
            "details": error_details or {},
            "fatal": True,
        }

        # Log error
        logger.error(
            f"Fatal error in ETL phase {phase}: {error_message}",
            extra={
                "phase": phase,
                "error_details": error_details,
                "fatal": True,
            }
        )

        return error_record


class WarningErrorStrategy(ErrorStrategy):
    """Strategy for handling non-fatal errors (warnings)."""

    def handle_error(
        self,
        phase: str,
        error_message: str,
        error_details: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Handle a non-fatal error (warning).

        Args:
            phase: Phase where the error occurred
            error_message: Error message
            error_details: Additional error details

        Returns:
            Error record
        """
        # Create error record
        error_record = {
            "phase": phase,
            "timestamp": datetime.datetime.now().isoformat(),
            "message": error_message,
            "details": error_details or {},
            "fatal": False,
        }

        # Log warning
        logger.warning(
            f"Non-fatal error in ETL phase {phase}: {error_message}",
            extra={
                "phase": phase,
                "error_details": error_details,
                "fatal": False,
            }
        )

        return error_record


class ErrorLogger:
    """Handles error recording and logging."""

    def __init__(self):
        """Initialize the error logger."""
        self.errors: List[Dict[str, Any]] = []
        self.fatal_strategy = FatalErrorStrategy()
        self.warning_strategy = WarningErrorStrategy()

    @with_context(operation="record_error")
    @log_call(level=logging.ERROR)
    def record_error(
        self,
        phase: str,
        error_message: str,
        error_details: Optional[Dict[str, Any]] = None,
        fatal: bool = True,
    ) -> Dict[str, Any]:
        """
        Record an error that occurred during the ETL process.

        Args:
            phase: Phase where the error occurred
            error_message: Error message
            error_details: Additional error details
            fatal: Whether the error is fatal and should stop the pipeline

        Returns:
            Error record
        """
        # Use appropriate strategy based on error severity
        strategy = self.fatal_strategy if fatal else self.warning_strategy
        error_record = strategy.handle_error(phase, error_message, error_details)

        # Add to errors list
        self.errors.append(error_record)

        return error_record

    def get_errors(self, phase: Optional[str] = None, fatal_only: bool = False) -> List[Dict[str, Any]]:
        """
        Get errors.

        Args:
            phase: Optional phase to filter errors
            fatal_only: Whether to return only fatal errors

        Returns:
            List of error records
        """
        filtered_errors = self.errors

        # Filter by phase
        if phase is not None:
            filtered_errors = [error for error in filtered_errors if error["phase"] == phase]

        # Filter by fatal flag
        if fatal_only:
            filtered_errors = [error for error in filtered_errors if error["fatal"]]

        return filtered_errors

    def has_errors(self, phase: Optional[str] = None, fatal_only: bool = False) -> bool:
        """
        Check if there are any errors.

        Args:
            phase: Optional phase to filter errors
            fatal_only: Whether to check only for fatal errors

        Returns:
            True if there are errors, False otherwise
        """
        return len(self.get_errors(phase, fatal_only)) > 0

    def clear_errors(self, phase: Optional[str] = None) -> None:
        """
        Clear errors.

        Args:
            phase: Optional phase to filter errors to clear
        """
        if phase is None:
            self.errors = []
        else:
            self.errors = [error for error in self.errors if error["phase"] != phase]

    def get_error_count(self, phase: Optional[str] = None, fatal_only: bool = False) -> int:
        """
        Get error count.

        Args:
            phase: Optional phase to filter errors
            fatal_only: Whether to count only fatal errors

        Returns:
            Number of errors
        """
        return len(self.get_errors(phase, fatal_only))