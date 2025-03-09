"""
Data inserter for handling data insertion strategies.

This module provides the DataInserter class and various insertion strategies
for inserting data into the database.
"""

import logging
from typing import Any, Dict, Optional

from src.logging.new_structured_logging import get_logger, log_execution_time, handle_errors
from src.core_utils.test_utils import get_fast_test_mode
from src.core_utils.insertion_strategy import InsertionStrategy
from src.core_utils.strategy_factory import StrategyFactory, StrategyType

logger = get_logger(__name__)

# Fast test mode optimization
FAST_TEST_MODE = get_fast_test_mode()


class DataInserter:
    """Manages data insertion using various strategies."""

    def __init__(self, db_manager, strategy: Optional[InsertionStrategy] = None):
        """Initialize the data inserter.

        Args:
            db_manager: Database manager instance
            strategy: Insertion strategy to use (optional)
        """
        self.db_manager = db_manager

        # If no strategy is provided, use bulk insertion by default
        if strategy is None:
            self.strategy = StrategyFactory.create_strategy(StrategyType.BULK)
        else:
            self.strategy = strategy

        logger.info(f"Initialized DataInserter with {self.strategy.__class__.__name__}")

    def set_strategy(self, strategy: InsertionStrategy) -> None:
        """Set the insertion strategy.

        Args:
            strategy: Insertion strategy to use
        """
        self.strategy = strategy
        logger.info(f"Set insertion strategy to {strategy.__class__.__name__}")

    @log_execution_time(level=logging.INFO)
    @handle_errors()
    def insert(self, data: Dict[str, Any]) -> Dict[str, int]:
        """Insert data into the database using the current strategy.

        Args:
            data: Data to insert

        Returns:
            Dictionary with counts of inserted records
        """
        logger.info(f"Inserting data using {self.strategy.__class__.__name__}")
        return self.strategy.insert(self.db_manager, data)

    def create_and_set_strategy(self, strategy_type: str, config: Optional[Dict[str, Any]] = None) -> None:
        """
        Create a strategy of the specified type and set it as the current strategy.

        Args:
            strategy_type: Type of strategy to create ('bulk' or 'individual')
            config: Configuration parameters for the strategy
        """
        strategy = StrategyFactory.create_strategy(strategy_type, config)
        self.set_strategy(strategy)