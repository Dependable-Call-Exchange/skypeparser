"""
Phase manager for ETL pipeline.

This module provides the PhaseManager class that handles
phase management for ETL processes.
"""

import datetime
import logging
from typing import Any, Dict, List, Optional

from src.utils.new_structured_logging import get_logger, log_call, log_metrics, with_context

logger = get_logger(__name__)


class PhaseManager:
    """Handles phase management for ETL processes."""

    def __init__(self):
        """Initialize the phase manager."""
        self.current_phase: Optional[str] = None
        self.phase_results: Dict[str, Dict[str, Any]] = {}
        self.phase_statuses: Dict[str, str] = {}
        self.metrics: Dict[str, Dict[str, Any]] = {}

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
            "phase": phase_name,
            "start_time": self.phase_results[phase_name]["start_time"],
        }

        if total_conversations is not None:
            extra_data["total_conversations"] = total_conversations

        if total_messages is not None:
            extra_data["total_messages"] = total_messages

        logger.info(f"Started ETL phase: {phase_name}", extra=extra_data)

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
                extra={"phase": phase_name},
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
        if "duration" not in self.metrics:
            self.metrics["duration"] = {}
        self.metrics["duration"][phase_name] = duration

        # Log phase end
        logger.info(
            f"Ended ETL phase: {phase_name} with status: {status}",
            extra={
                "phase": phase_name,
                "status": status,
                "duration": duration,
                "metrics": self.phase_results[phase_name].get("metrics", {}),
            },
        )

    def update_phase_metric(
        self, phase_name: str, metric_name: str, metric_value: Any
    ) -> None:
        """
        Update a metric for a phase.

        Args:
            phase_name: Name of the phase
            metric_name: Name of the metric
            metric_value: Value of the metric
        """
        # Check if phase exists
        if phase_name not in self.phase_results:
            logger.warning(
                f"Attempted to update metric for non-existent phase: {phase_name}",
                extra={"phase": phase_name, "metric_name": metric_name},
            )
            return

        # Update metric
        self.phase_results[phase_name]["metrics"][metric_name] = metric_value

        # Log metric update
        logger.debug(
            f"Updated metric {metric_name} for phase {phase_name}: {metric_value}",
            extra={
                "phase": phase_name,
                "metric_name": metric_name,
                "metric_value": metric_value,
            },
        )

    def get_phase_status(self, phase_name: str) -> Optional[str]:
        """
        Get the status of a phase.

        Args:
            phase_name: Name of the phase

        Returns:
            Status of the phase or None if the phase does not exist
        """
        return self.phase_statuses.get(phase_name)

    def get_phase_metrics(self, phase_name: str) -> Dict[str, Any]:
        """
        Get the metrics for a phase.

        Args:
            phase_name: Name of the phase

        Returns:
            Metrics for the phase or an empty dictionary if the phase does not exist
        """
        if phase_name not in self.phase_results:
            return {}
        return self.phase_results[phase_name].get("metrics", {})

    def get_phase_duration(self, phase_name: str) -> Optional[float]:
        """
        Get the duration of a phase.

        Args:
            phase_name: Name of the phase

        Returns:
            Duration of the phase in seconds or None if the phase does not exist or is not completed
        """
        if phase_name not in self.phase_results:
            return None
        return self.phase_results[phase_name].get("duration")

    def get_all_phases(self) -> List[str]:
        """
        Get all phases.

        Returns:
            List of phase names
        """
        return list(self.phase_results.keys())

    def get_completed_phases(self) -> List[str]:
        """
        Get completed phases.

        Returns:
            List of completed phase names
        """
        return [
            phase
            for phase, status in self.phase_statuses.items()
            if status == "completed"
        ]

    def get_failed_phases(self) -> List[str]:
        """
        Get failed phases.

        Returns:
            List of failed phase names
        """
        return [
            phase
            for phase, status in self.phase_statuses.items()
            if status == "failed"
        ]

    def get_in_progress_phases(self) -> List[str]:
        """
        Get in-progress phases.

        Returns:
            List of in-progress phase names
        """
        return [
            phase
            for phase, status in self.phase_statuses.items()
            if status == "in_progress"
        ]

    def get_warning_phases(self) -> List[str]:
        """
        Get warning phases.

        Returns:
            List of warning phase names
        """
        return [
            phase
            for phase, status in self.phase_statuses.items()
            if status == "warning"
        ]

    def get_phase_summary(self) -> Dict[str, Any]:
        """
        Get a summary of all phases.

        Returns:
            Dictionary with phase summary
        """
        return {
            "phases": self.phase_results,
            "current_phase": self.current_phase,
            "completed_phases": self.get_completed_phases(),
            "failed_phases": self.get_failed_phases(),
            "in_progress_phases": self.get_in_progress_phases(),
            "warning_phases": self.get_warning_phases(),
        }

    def can_resume_from_phase(self, phase_name: str) -> bool:
        """
        Check if the ETL process can be resumed from a specific phase.

        Args:
            phase_name: Name of the phase to resume from

        Returns:
            True if the process can be resumed from the specified phase, False otherwise
        """
        # Check if phase exists
        if phase_name not in self.phase_statuses:
            logger.warning(
                f"Cannot resume from non-existent phase: {phase_name}",
                extra={"phase": phase_name},
            )
            return False

        # Check if previous phases are completed
        all_phases = self.get_all_phases()
        phase_index = all_phases.index(phase_name)
        previous_phases = all_phases[:phase_index]

        for prev_phase in previous_phases:
            if self.phase_statuses.get(prev_phase) != "completed":
                logger.warning(
                    f"Cannot resume from phase {phase_name} because previous phase {prev_phase} is not completed",
                    extra={
                        "phase": phase_name,
                        "previous_phase": prev_phase,
                        "previous_phase_status": self.phase_statuses.get(prev_phase),
                    },
                )
                return False

        return True