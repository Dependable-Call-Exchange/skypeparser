"""
Insertion strategy interface.

This module defines the InsertionStrategy abstract base class that all
insertion strategies must implement.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any


class InsertionStrategy(ABC):
    """Abstract base class for insertion strategies."""

    @abstractmethod
    def insert(self, db_manager, data: Dict[str, Any]) -> Dict[str, int]:
        """Insert data into the database.

        Args:
            db_manager: Database manager instance
            data: Data to insert

        Returns:
            Dictionary with counts of inserted records
        """
        pass