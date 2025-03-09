"""
Database handlers module.

This package contains handlers for different data types that can be inserted into the database.
Each handler is responsible for a specific data type (messages, conversations, users, archives)
and implements methods for both bulk and individual insertion.
"""

from src.data_handlers.base_handler import BaseHandler
from src.data_handlers.archive_handler import ArchiveHandler
from src.data_handlers.conversation_handler import ConversationHandler
from src.data_handlers.message_handler import MessageHandler
from src.data_handlers.user_handler import UserHandler
from src.data_handlers.handler_registry import HandlerRegistry

__all__ = [
    'BaseHandler',
    'ArchiveHandler',
    'ConversationHandler',
    'MessageHandler',
    'UserHandler',
    'HandlerRegistry',
]