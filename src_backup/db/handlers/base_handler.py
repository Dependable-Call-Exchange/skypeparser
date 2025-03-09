"""
Base handler interface for database insertion operations.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Tuple, Optional


class BaseHandler(ABC):
    """
    Abstract base class for data type handlers.

    Each handler is responsible for a specific data type and implements
    methods for both bulk and individual insertion.
    """

    @classmethod
    @abstractmethod
    def get_type(cls) -> str:
        """
        Returns the data type that this handler processes.

        Returns:
            str: The name of the data type (e.g., 'messages', 'conversations')
        """
        pass

    @staticmethod
    @abstractmethod
    def insert_bulk(db_manager, data: Dict[str, Any], batch_size: int,
                   archive_id: Optional[str] = None) -> int:
        """
        Insert data in bulk.

        Args:
            db_manager: Database manager instance
            data: Data to insert
            batch_size: Size of each batch
            archive_id: Optional archive ID to associate with the data

        Returns:
            int: Number of records inserted
        """
        pass

    @staticmethod
    @abstractmethod
    def insert_individual(db_manager, data: Dict[str, Any],
                         archive_id: Optional[str] = None) -> int:
        """
        Insert data individually (one by one).

        Args:
            db_manager: Database manager instance
            data: Data to insert
            archive_id: Optional archive ID to associate with the data

        Returns:
            int: Number of records inserted
        """
        pass