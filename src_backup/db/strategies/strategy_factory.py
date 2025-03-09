"""
Strategy factory for insertion strategies.

This module provides a factory for creating insertion strategies based on configuration.
"""
from typing import Dict, Any, Optional

from src.utils.new_structured_logging import get_logger
from src.db.strategies.insertion_strategy import InsertionStrategy
from src.db.strategies.bulk_insertion import BulkInsertionStrategy
from src.db.strategies.individual_insertion import IndividualInsertionStrategy

logger = get_logger(__name__)


class StrategyType:
    """Enumeration of strategy types."""
    BULK = "bulk"
    INDIVIDUAL = "individual"


class StrategyFactory:
    """Factory for creating insertion strategies."""

    @staticmethod
    def create_strategy(strategy_type: str, config: Optional[Dict[str, Any]] = None) -> InsertionStrategy:
        """
        Create an insertion strategy of the specified type with the given configuration.

        Args:
            strategy_type: Type of strategy to create ('bulk' or 'individual')
            config: Configuration parameters for the strategy

        Returns:
            An insertion strategy instance

        Raises:
            ValueError: If the strategy type is not recognized
        """
        # Use empty dict as default if config is None
        config = config or {}

        if strategy_type.lower() == StrategyType.BULK:
            # Extract configuration parameters
            batch_size = config.get("batch_size", 1000)
            adaptive_sizing = config.get("adaptive_sizing", True)
            min_batch_size = config.get("min_batch_size", 100)
            max_batch_size = config.get("max_batch_size", 5000)
            size_increase_factor = config.get("size_increase_factor", 1.5)
            size_decrease_factor = config.get("size_decrease_factor", 0.5)

            logger.info(f"Creating bulk insertion strategy with batch size {batch_size}")

            return BulkInsertionStrategy(
                batch_size=batch_size,
                adaptive_sizing=adaptive_sizing,
                min_batch_size=min_batch_size,
                max_batch_size=max_batch_size,
                size_increase_factor=size_increase_factor,
                size_decrease_factor=size_decrease_factor
            )

        elif strategy_type.lower() == StrategyType.INDIVIDUAL:
            logger.info("Creating individual insertion strategy")
            return IndividualInsertionStrategy()

        else:
            error_msg = f"Unknown insertion strategy type: {strategy_type}"
            logger.error(error_msg)
            raise ValueError(error_msg)