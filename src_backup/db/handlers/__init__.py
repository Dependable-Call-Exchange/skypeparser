"""
Database handlers module.

This package contains handlers for different data types that can be inserted into the database.
Each handler is responsible for a specific data type (messages, conversations, users, archives)
and implements methods for both bulk and individual insertion.
"""

from src.db.handlers.base_handler import BaseHandler
from src.db.handlers.archive_handler import ArchiveHandler
from src.db.handlers.conversation_handler import ConversationHandler
from src.db.handlers.message_handler import MessageHandler
from src.db.handlers.user_handler import UserHandler
from src.db.handlers.handler_registry import HandlerRegistry

__all__ = [
    'BaseHandler',
    'ArchiveHandler',
    'ConversationHandler',
    'MessageHandler',
    'UserHandler',
    'HandlerRegistry',
]