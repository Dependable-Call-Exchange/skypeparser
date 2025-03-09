"""
Handler registry for database insertion operations.

This module provides a registry for data type handlers.
"""
import logging
from typing import Dict, Type, List, Optional

from src.utils.new_structured_logging import get_logger
from src.db.handlers.base_handler import BaseHandler
from src.db.handlers.archive_handler import ArchiveHandler
from src.db.handlers.conversation_handler import ConversationHandler
from src.db.handlers.message_handler import MessageHandler
from src.db.handlers.user_handler import UserHandler

logger = get_logger(__name__)


class HandlerRegistry:
    """
    Registry for data type handlers.

    This class maintains a registry of handlers for different data types and
    provides methods to access them.
    """

    def __init__(self):
        """Initialize the handler registry with default handlers."""
        self._handlers: Dict[str, Type[BaseHandler]] = {}

        # Register default handlers
        self.register_handler(ArchiveHandler)
        self.register_handler(ConversationHandler)
        self.register_handler(MessageHandler)
        self.register_handler(UserHandler)

        logger.info(f"Initialized handler registry with {len(self._handlers)} handlers")

    def register_handler(self, handler_class: Type[BaseHandler]) -> None:
        """
        Register a handler for a specific data type.

        Args:
            handler_class: Handler class to register
        """
        data_type = handler_class.get_type()
        self._handlers[data_type] = handler_class
        logger.debug(f"Registered handler {handler_class.__name__} for data type '{data_type}'")

    def get_handler(self, data_type: str) -> Optional[Type[BaseHandler]]:
        """
        Get a handler for a specific data type.

        Args:
            data_type: Data type to get a handler for

        Returns:
            Handler class for the data type, or None if not found
        """
        if data_type not in self._handlers:
            logger.warning(f"No handler registered for data type '{data_type}'")
            return None

        return self._handlers[data_type]

    def get_all_handlers(self) -> List[Type[BaseHandler]]:
        """
        Get all registered handlers.

        Returns:
            List of all registered handler classes
        """
        return list(self._handlers.values())

    def get_all_data_types(self) -> List[str]:
        """
        Get all registered data types.

        Returns:
            List of all registered data types
        """
        return list(self._handlers.keys())