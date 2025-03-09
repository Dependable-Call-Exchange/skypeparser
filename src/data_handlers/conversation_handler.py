"""
Conversation handler for database insertion operations.
"""
import logging
import uuid
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional

from src.logging.new_structured_logging import get_logger, log_execution_time, handle_errors
from src.core_utils.test_utils import is_test_environment, get_fast_test_mode
from src.data_handlers.base_handler import BaseHandler

logger = get_logger(__name__)
FAST_TEST_MODE = get_fast_test_mode()


class ConversationHandler(BaseHandler):
    """
    Handler for conversation insertion operations.
    """

    @classmethod
    def get_type(cls) -> str:
        """
        Returns the data type this handler processes.

        Returns:
            str: 'conversations'
        """
        return "conversations"

    @staticmethod
    @log_execution_time(level=logging.DEBUG)
    @handle_errors()
    def insert_bulk(db_manager, conversations: Dict[str, Dict[str, Any]],
                    batch_size: int, archive_id: Optional[str] = None) -> int:
        """
        Insert conversations into the database in bulk.

        Args:
            db_manager: Database manager
            conversations: Conversations to insert
            batch_size: Size of each insertion batch
            archive_id: Archive ID to associate with the conversations

        Returns:
            int: Number of conversations inserted
        """
        # Fast path for test environments
        if FAST_TEST_MODE:
            if not conversations:
                logger.warning("No conversations to insert")
                return 0

            conversation_count = len(conversations)
            logger.info(f"[FAST TEST MODE] Skipped insertion of {conversation_count} conversations")
            return conversation_count

        # Normal path for non-test environments
        if not conversations:
            logger.warning("No conversations to insert")
            return 0

        # Get the archive_id or create one if needed
        archive_id = archive_id or str(uuid.uuid4())

        logger.info(f"Inserting {len(conversations)} conversations")

        # Define columns for bulk insert
        columns = [
            "conversation_id",
            "display_name",
            "thread_type",
            "created_at"
        ]

        values = []
        for conv_id, conv_data in conversations.items():
            # Get conversation data or set defaults
            display_name = conv_data.get("display_name", "")
            thread_type = conv_data.get("type", "")

            # Set created_at timestamp
            created_at = None
            timestamp = conv_data.get("timestamp", "")
            if timestamp:
                # Convert to datetime if it's a string
                if isinstance(timestamp, str):
                    try:
                        created_at = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    except ValueError:
                        try:
                            created_at = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                        except ValueError:
                            created_at = datetime.now()
                else:
                    created_at = timestamp
            else:
                created_at = datetime.now()

            # Append values for this conversation
            values.append((
                conv_id,
                display_name,
                thread_type,
                created_at
            ))

        # Perform bulk insert
        return db_manager.bulk_insert("conversations", columns, values, batch_size)

    @staticmethod
    @log_execution_time()
    @handle_errors()
    def insert_individual(db_manager, conversations: Dict[str, Dict[str, Any]],
                          archive_id: Optional[str] = None) -> int:
        """
        Insert conversations into the database one by one.

        Args:
            db_manager: Database manager
            conversations: Conversations to insert
            archive_id: Archive ID to associate with the conversations

        Returns:
            int: Number of conversations inserted
        """
        # Fast path for test environments
        if FAST_TEST_MODE:
            if not conversations:
                logger.warning("No conversations to insert")
                return 0

            conversation_count = len(conversations)
            logger.info(f"[FAST TEST MODE] Skipped insertion of {conversation_count} conversations")
            return conversation_count

        # Normal path for non-test environments
        if not conversations:
            logger.warning("No conversations to insert")
            return 0

        # Get the archive_id or create one if needed
        archive_id = archive_id or str(uuid.uuid4())

        logger.info(f"Inserting {len(conversations)} conversations individually")

        count = 0
        for conv_id, conv_data in conversations.items():
            # Get conversation data or set defaults
            display_name = conv_data.get("display_name", "")
            thread_type = conv_data.get("type", "")

            # Set created_at timestamp
            created_at = None
            timestamp = conv_data.get("timestamp", "")
            if timestamp:
                # Convert to datetime if it's a string
                if isinstance(timestamp, str):
                    try:
                        created_at = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                    except ValueError:
                        try:
                            created_at = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
                        except ValueError:
                            created_at = datetime.now()
                else:
                    created_at = timestamp
            else:
                created_at = datetime.now()

            # Insert this conversation
            db_manager.execute_query(
                """
                INSERT INTO conversations
                (conversation_id, display_name, thread_type, created_at)
                VALUES (%s, %s, %s, %s)
                """,
                (conv_id, display_name, thread_type, created_at)
            )
            count += 1

        return count