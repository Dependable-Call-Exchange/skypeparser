"""
Mock implementation of progress tracker for testing.

This module provides a mock implementation of the progress tracker
for use in unit tests.
"""

from typing import Any, Dict, Optional


class MockProgressTracker:
    """
    Mock progress tracker for testing.

    This class provides a mock implementation of the progress tracker
    with configurable behavior for testing different scenarios.
    """

    def __init__(self):
        """Initialize the mock progress tracker."""
        self.update_progress_calls = []
        self.start_phase_calls = []
        self.end_phase_calls = []
        self.fail_phase_calls = []
        self.get_progress_calls = []
        self.current_phase = None
        self.phases = {}

    def update_progress(self, phase: str, progress: float, message: Optional[str] = None) -> None:
        """
        Update the progress of a phase.

        Args:
            phase: Name of the phase
            progress: Progress value (0.0 to 1.0)
            message: Optional progress message
        """
        self.update_progress_calls.append((phase, progress, message))
        self.phases[phase] = {
            "progress": progress,
            "message": message,
            "status": "in_progress"
        }

    def start_phase(self, phase: str, message: Optional[str] = None) -> None:
        """
        Start a new phase.

        Args:
            phase: Name of the phase
            message: Optional start message
        """
        self.start_phase_calls.append((phase, message))
        self.current_phase = phase
        self.phases[phase] = {
            "progress": 0.0,
            "message": message,
            "status": "started"
        }

    def end_phase(self, phase: str, message: Optional[str] = None) -> None:
        """
        End a phase successfully.

        Args:
            phase: Name of the phase
            message: Optional end message
        """
        self.end_phase_calls.append((phase, message))
        if phase in self.phases:
            self.phases[phase]["progress"] = 1.0
            self.phases[phase]["message"] = message
            self.phases[phase]["status"] = "completed"

    def fail_phase(self, phase: str, message: Optional[str] = None) -> None:
        """
        Mark a phase as failed.

        Args:
            phase: Name of the phase
            message: Optional failure message
        """
        self.fail_phase_calls.append((phase, message))
        if phase in self.phases:
            self.phases[phase]["message"] = message
            self.phases[phase]["status"] = "failed"

    def get_progress(self) -> Dict[str, Any]:
        """
        Get the current progress of all phases.

        Returns:
            Dictionary with progress information for all phases
        """
        self.get_progress_calls.append(True)
        return self.phases