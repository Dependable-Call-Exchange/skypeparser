"""
Transaction manager for database operations.

This module provides a transaction manager that encapsulates transaction logic
and provides methods for handling database transactions.
"""
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, TypeVar, Union

from src.utils.new_structured_logging import get_logger, log_execution_time, handle_errors

logger = get_logger(__name__)

# Define a generic type for return values
T = TypeVar('T')


class TransactionManager:
    """
    Transaction manager for database operations.

    This class encapsulates transaction logic and provides methods for handling
    database transactions, including retries and error handling.
    """

    def __init__(self, db_manager):
        """
        Initialize the transaction manager.

        Args:
            db_manager: Database manager instance
        """
        self.db_manager = db_manager

    @log_execution_time(level=logging.DEBUG)
    @handle_errors()
    def execute_in_transaction(self, func: Callable[..., T], *args: Any, **kwargs: Any) -> T:
        """
        Execute a function within a transaction.

        Args:
            func: Function to execute
            *args: Positional arguments to pass to the function
            **kwargs: Keyword arguments to pass to the function

        Returns:
            The return value of the function

        Raises:
            Exception: If the function raises an exception
        """
        # Begin transaction
        self.db_manager.begin_transaction()

        try:
            # Execute the function
            result = func(*args, **kwargs)

            # Commit transaction
            self.db_manager.commit()

            return result
        except Exception as e:
            # Roll back transaction
            self.db_manager.rollback()

            logger.error(f"Error executing function in transaction: {str(e)}")

            # Re-raise the exception
            raise

    @log_execution_time(level=logging.DEBUG)
    @handle_errors()
    def execute_with_retry(self, func: Callable[..., T], max_retries: int = 3,
                          *args: Any, **kwargs: Any) -> T:
        """
        Execute a function with retry logic.

        Args:
            func: Function to execute
            max_retries: Maximum number of retries
            *args: Positional arguments to pass to the function
            **kwargs: Keyword arguments to pass to the function

        Returns:
            The return value of the function

        Raises:
            Exception: If the function fails after all retries
        """
        attempts = 0
        last_error = None

        while attempts <= max_retries:
            try:
                # Execute the function within a transaction
                return self.execute_in_transaction(func, *args, **kwargs)
            except Exception as e:
                attempts += 1
                last_error = e

                if attempts <= max_retries:
                    logger.warning(f"Retry {attempts}/{max_retries} after error: {str(e)}")
                else:
                    logger.error(f"Failed after {max_retries} retries: {str(e)}")
                    raise last_error