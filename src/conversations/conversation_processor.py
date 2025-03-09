"""
Conversation processor for transforming conversations.

This module provides the ConversationProcessor class that handles
transformation of conversations.
"""

import logging
from typing import Dict, Any, Callable, List, Optional

from src.logging.new_structured_logging import get_logger

logger = get_logger(__name__)


class ConversationProcessor:
    """Handles transformation of conversations."""

    def __init__(self):
        """Initialize the conversation processor."""
        self.metrics = {
            "conversation_count": 0,
            "skipped_conversations": 0,
        }

    def transform_conversations(
        self,
        conversations: Dict[str, Any],
        transform_func: Callable[[Dict[str, Any]], Dict[str, Any]],
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Transform conversations using the provided function.

        Args:
            conversations: Conversations to transform
            transform_func: Function to transform each conversation
            context: Optional context to pass to the transform function

        Returns:
            Transformed conversations
        """
        if not conversations:
            logger.debug("No conversations to transform")
            return {}

        logger.debug(f"Transforming {len(conversations)} conversations")

        transformed_conversations = {}

        for conv_id, conv in conversations.items():
            if not isinstance(conv, dict):
                logger.debug(f"Skipping non-dict conversation: {conv_id}")
                self.metrics["skipped_conversations"] += 1
                continue

            try:
                if context:
                    transformed_conv = transform_func(conv, **context)
                else:
                    transformed_conv = transform_func(conv)

                if transformed_conv:
                    transformed_conversations[conv_id] = transformed_conv
                    self.metrics["conversation_count"] += 1
                else:
                    logger.debug(f"Skipping conversation with empty transformation: {conv_id}")
                    self.metrics["skipped_conversations"] += 1
            except Exception as e:
                logger.error(f"Error transforming conversation {conv_id}: {e}")
                self.metrics["skipped_conversations"] += 1
                # Continue processing other conversations
                continue

        logger.debug(f"Transformed {len(transformed_conversations)} conversations")
        return transformed_conversations

    def transform_conversation_list(
        self,
        conversations: List[Dict[str, Any]],
        transform_func: Callable[[Dict[str, Any]], Dict[str, Any]],
        context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Dict[str, Any]]:
        """Transform a list of conversations using the provided function.

        Args:
            conversations: List of conversations to transform
            transform_func: Function to transform each conversation
            context: Optional context to pass to the transform function

        Returns:
            Transformed conversations
        """
        if not conversations:
            logger.debug("No conversations to transform")
            return {}

        logger.debug(f"Transforming {len(conversations)} conversations from list")

        transformed_conversations = {}

        for conv in conversations:
            if not isinstance(conv, dict):
                logger.debug("Skipping non-dict conversation in list")
                self.metrics["skipped_conversations"] += 1
                continue

            if "id" not in conv:
                logger.debug("Skipping conversation without ID")
                self.metrics["skipped_conversations"] += 1
                continue

            conv_id = conv["id"]

            try:
                if context:
                    transformed_conv = transform_func(conv, **context)
                else:
                    transformed_conv = transform_func(conv)

                if transformed_conv:
                    transformed_conversations[conv_id] = transformed_conv
                    self.metrics["conversation_count"] += 1
                else:
                    logger.debug(f"Skipping conversation with empty transformation: {conv_id}")
                    self.metrics["skipped_conversations"] += 1
            except Exception as e:
                logger.error(f"Error transforming conversation {conv_id}: {e}")
                self.metrics["skipped_conversations"] += 1
                # Continue processing other conversations
                continue

        logger.debug(f"Transformed {len(transformed_conversations)} conversations from list")
        return transformed_conversations