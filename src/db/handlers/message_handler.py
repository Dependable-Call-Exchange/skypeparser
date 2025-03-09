"""
Message handler for database insertion operations.
"""
import logging
import uuid
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional, Union

from src.utils.new_structured_logging import get_logger, log_execution_time, handle_errors
from src.utils.test_utils import is_test_environment, get_fast_test_mode
from src.db.handlers.base_handler import BaseHandler

logger = get_logger(__name__)
FAST_TEST_MODE = get_fast_test_mode()


class MessageHandler(BaseHandler):
    """
    Handler for message insertion operations.
    """

    @classmethod
    def get_type(cls) -> str:
        """
        Returns the data type this handler processes.

        Returns:
            str: 'messages'
        """
        return "messages"

    @staticmethod
    @log_execution_time(level=logging.DEBUG)
    @handle_errors()
    def insert_bulk(db_manager, messages: Union[Dict[str, Any], List[Dict[str, Any]]],
                   batch_size: int, archive_id: Optional[str] = None) -> int:
        """
        Insert messages into the database in bulk.

        Args:
            db_manager: Database manager
            messages: Messages to insert (dictionary or list)
            batch_size: Size of each insertion batch
            archive_id: Archive ID to associate with the messages

        Returns:
            int: Number of messages inserted
        """
        # Fast path for test environments
        if FAST_TEST_MODE:
            message_count = 0
            if not messages:
                logger.warning("No messages to insert")
                return 0

            # Just count the messages instead of inserting them
            if isinstance(messages, dict):
                for msg_id, msg_data in messages.items():
                    if isinstance(msg_data, dict):
                        message_count += 1
                    elif isinstance(msg_data, list):
                        message_count += len(msg_data)
            elif isinstance(messages, list):
                message_count = len(messages)

            logger.info(f"[FAST TEST MODE] Skipped insertion of {message_count} messages")
            return message_count

        # Normal path for non-test environments
        if not messages:
            logger.warning("No messages to insert")
            return 0

        # Get the archive_id or create one if needed
        archive_id = archive_id or str(uuid.uuid4())

        # Convert the messages to a list if it's a dictionary
        messages_list = MessageHandler._normalize_messages(messages)

        # Count total messages
        total_messages = len(messages_list)
        logger.info(f"Inserting {total_messages} messages")

        # Prepare data for bulk insert - match the existing schema
        columns = ["id", "archive_id", "sender_name", "sender_id", "content", "timestamp", "message_type", "created_at"]
        values = []

        # Process each message
        for msg in messages_list:
            try:
                # Generate a UUID for the message ID
                message_id = msg.get("id", None) or str(uuid.uuid4())

                # Extract message data
                sender_name = msg.get("sender_name", "Unknown")
                sender_id = msg.get("sender_id", "")
                content = msg.get("content", "")

                # Handle timestamps
                timestamp = msg.get("timestamp", None)
                if isinstance(timestamp, str):
                    try:
                        timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                    except ValueError:
                        timestamp = datetime.now()
                elif timestamp is None:
                    timestamp = datetime.now()

                # Current time for created_at
                created_at = datetime.now()

                # Extract values for database insert
                values.append((
                    message_id,
                    archive_id,
                    sender_name,
                    sender_id,
                    content,
                    timestamp,
                    msg.get("message_type", "text"),
                    created_at
                ))
            except Exception as e:
                logger.error(f"Error processing message for bulk insert: {e}")
                # Continue with other messages

        # If no values to insert, return 0
        if not values:
            logger.warning("No valid messages to insert")
            return 0

        # Bulk insert
        return db_manager.bulk_insert("messages", columns, values, batch_size)

    @staticmethod
    @log_execution_time()
    @handle_errors()
    def insert_individual(db_manager, messages: Union[Dict[str, Any], List[Dict[str, Any]]],
                         archive_id: Optional[str] = None) -> int:
        """
        Insert messages into the database one by one.

        Args:
            db_manager: Database manager
            messages: Messages to insert (dictionary or list)
            archive_id: Archive ID to associate with the messages

        Returns:
            int: Number of messages inserted
        """
        # Fast path for test environments
        if FAST_TEST_MODE:
            message_count = 0
            if not messages:
                logger.warning("No messages to insert")
                return 0

            # Just count the messages instead of inserting them
            if isinstance(messages, dict):
                for msg_id, msg_data in messages.items():
                    if isinstance(msg_data, dict):
                        message_count += 1
                    elif isinstance(msg_data, list):
                        message_count += len(msg_data)
            elif isinstance(messages, list):
                message_count = len(messages)

            logger.info(f"[FAST TEST MODE] Skipped insertion of {message_count} messages")
            return message_count

        # Normal path for non-test environments
        if not messages:
            logger.warning("No messages to insert")
            return 0

        # Get the archive_id or create one if needed
        archive_id = archive_id or str(uuid.uuid4())

        # Convert the messages to a list if it's a dictionary
        messages_list = MessageHandler._normalize_messages(messages)

        # Count total messages
        total_messages = len(messages_list)
        logger.info(f"Inserting {total_messages} messages individually")

        count = 0
        for msg in messages_list:
            try:
                # Generate a UUID for the message ID
                message_id = msg.get("id", None) or str(uuid.uuid4())

                # Extract message data
                sender_name = msg.get("sender_name", "Unknown")
                sender_id = msg.get("sender_id", "")
                content = msg.get("content", "")

                # Handle timestamps
                timestamp = msg.get("timestamp", None)
                if isinstance(timestamp, str):
                    try:
                        timestamp = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                    except ValueError:
                        timestamp = datetime.now()
                elif timestamp is None:
                    timestamp = datetime.now()

                # Current time for created_at
                created_at = datetime.now()

                # Insert this message
                db_manager.execute_query(
                    """
                    INSERT INTO messages
                    (id, archive_id, sender_name, sender_id, content, timestamp, message_type, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        message_id,
                        archive_id,
                        sender_name,
                        sender_id,
                        content,
                        timestamp,
                        msg.get("message_type", "text"),
                        created_at
                    )
                )
                count += 1
            except Exception as e:
                logger.error(f"Error processing message for individual insert: {e}")
                # Continue with other messages

        return count

    @staticmethod
    def _normalize_messages(messages: Union[Dict[str, Any], List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """
        Normalize messages to a list of dictionaries.

        Args:
            messages: Messages to normalize (can be dictionary or list)

        Returns:
            List[Dict[str, Any]]: Normalized list of message dictionaries
        """
        # Convert the messages to a list if it's a dictionary
        if isinstance(messages, dict):
            logger.info(f"Converting dictionary of {len(messages)} messages to a list")
            messages_list = []
            for msg_id, msg_data in messages.items():
                # Handle the case where msg_data is a list or a dictionary
                if isinstance(msg_data, dict):
                    msg_data = msg_data.copy()
                    msg_data["id"] = msg_id
                    messages_list.append(msg_data)
                elif isinstance(msg_data, list):
                    # If it's a list, process each item
                    for item in msg_data:
                        if isinstance(item, dict):
                            # Clone the item to avoid modifying the original
                            item_copy = item.copy()
                            # Use either the item's id or the parent msg_id
                            item_copy["id"] = item.get("id", msg_id)
                            messages_list.append(item_copy)
                        else:
                            logger.warning(f"Skipping message item with unexpected type: {type(item)}")
                else:
                    logger.warning(f"Skipping message with unexpected type: {type(msg_data)}")
            return messages_list
        elif isinstance(messages, list):
            # If messages is already a list, ensure each item has an id
            messages_list = []
            for msg in messages:
                if isinstance(msg, dict):
                    # Make sure every message has an ID
                    msg_copy = msg.copy()
                    if "id" not in msg_copy:
                        msg_copy["id"] = str(uuid.uuid4())
                    messages_list.append(msg_copy)
                elif isinstance(msg, list):
                    # If an item is a list, extract dictionaries from it
                    for item in msg:
                        if isinstance(item, dict):
                            item_copy = item.copy()
                            if "id" not in item_copy:
                                item_copy["id"] = str(uuid.uuid4())
                            messages_list.append(item_copy)
                        else:
                            logger.warning(f"Skipping message item with unexpected type: {type(item)}")
                else:
                    logger.warning(f"Skipping message with unexpected type: {type(msg)}")
            return messages_list
        else:
            logger.warning(f"Unexpected messages type: {type(messages)}")
            return []