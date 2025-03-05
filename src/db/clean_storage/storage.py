"""
Database operations for storing and retrieving clean Skype data.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

from psycopg2.extras import Json
from psycopg2.pool import SimpleConnectionPool

from .models import (
    CREATE_CLEAN_TABLES_SQL,
    INSERT_CONVERSATION_SQL,
    INSERT_MESSAGE_SQL,
    GET_CONVERSATIONS_SQL,
    GET_MESSAGES_SQL,
    GET_CONVERSATION_BY_ID_SQL,
    GET_MESSAGE_BY_ID_SQL
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SkypeCleanDataStorage:
    """Handles storage of clean Skype data in PostgreSQL."""

    MIN_CONNECTIONS = 1
    MAX_CONNECTIONS = 10

    def __init__(self, connection_params: Dict[str, str]):
        """
        Initialize storage with database connection parameters.

        Args:
            connection_params: Dictionary containing database connection parameters
                             (host, database, user, password, etc.)
        """
        self.connection_params = connection_params
        self.pool = None
        self.initialize_connection_pool()
        self.ensure_tables_exist()

    def initialize_connection_pool(self) -> None:
        """Initialize the connection pool."""
        try:
            self.pool = SimpleConnectionPool(
                self.MIN_CONNECTIONS,
                self.MAX_CONNECTIONS,
                **self.connection_params
            )
            logger.info("Connection pool initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise

    def get_connection(self):
        """Get a connection from the pool."""
        if not self.pool:
            self.initialize_connection_pool()
        return self.pool.getconn()

    def return_connection(self, conn) -> None:
        """Return a connection to the pool."""
        if self.pool:
            self.pool.putconn(conn)

    def ensure_tables_exist(self) -> None:
        """Create necessary tables if they don't exist."""
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(CREATE_CLEAN_TABLES_SQL)
            conn.commit()
            logger.info("Clean data tables verified/created successfully")
        except Exception as e:
            logger.error(f"Failed to create clean data tables: {e}")
            conn.rollback()
            raise
        finally:
            self.return_connection(conn)

    def store_conversation(
        self,
        conversation_id: str,
        display_name: str,
        raw_export_id: int,
        first_message_time: Optional[datetime] = None,
        last_message_time: Optional[datetime] = None,
        message_count: int = 0
    ) -> int:
        """
        Store a conversation in the database.

        Args:
            conversation_id: Unique identifier for the conversation
            display_name: Display name of the conversation
            raw_export_id: ID of the raw export this conversation belongs to
            first_message_time: Timestamp of the first message
            last_message_time: Timestamp of the last message
            message_count: Number of messages in the conversation

        Returns:
            int: ID of the inserted/updated record
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    INSERT_CONVERSATION_SQL,
                    (
                        conversation_id,
                        display_name,
                        raw_export_id,
                        first_message_time,
                        last_message_time,
                        message_count
                    )
                )
                conversation_db_id = cur.fetchone()[0]
            conn.commit()
            logger.info(f"Stored conversation {conversation_id} with ID: {conversation_db_id}")
            return conversation_db_id
        except Exception as e:
            logger.error(f"Failed to store conversation {conversation_id}: {e}")
            conn.rollback()
            raise
        finally:
            self.return_connection(conn)

    def store_message(
        self,
        message_id: str,
        conversation_id: str,
        timestamp: datetime,
        sender_id: str,
        sender_name: Optional[str] = None,
        message_type: Optional[str] = None,
        content: Optional[str] = None,
        raw_content: Optional[str] = None,
        is_edited: bool = False
    ) -> Optional[int]:
        """
        Store a message in the database.

        Args:
            message_id: Unique identifier for the message
            conversation_id: ID of the conversation this message belongs to
            timestamp: Timestamp of the message
            sender_id: ID of the sender
            sender_name: Name of the sender
            message_type: Type of the message
            content: Processed content of the message
            raw_content: Raw content of the message
            is_edited: Whether the message has been edited

        Returns:
            Optional[int]: ID of the inserted record, or None if the message already exists
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    INSERT_MESSAGE_SQL,
                    (
                        message_id,
                        conversation_id,
                        timestamp,
                        sender_id,
                        sender_name,
                        message_type,
                        content,
                        raw_content,
                        is_edited
                    )
                )
                result = cur.fetchone()
                message_db_id = result[0] if result else None
            conn.commit()
            if message_db_id:
                logger.debug(f"Stored message {message_id} with ID: {message_db_id}")
            return message_db_id
        except Exception as e:
            logger.error(f"Failed to store message {message_id}: {e}")
            conn.rollback()
            raise
        finally:
            self.return_connection(conn)

    def get_conversations(self, raw_export_id: int) -> List[Dict[str, Any]]:
        """
        Get all conversations for a raw export.

        Args:
            raw_export_id: ID of the raw export

        Returns:
            List[Dict[str, Any]]: List of conversation data
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(GET_CONVERSATIONS_SQL, (raw_export_id,))
                columns = [desc[0] for desc in cur.description]
                conversations = [dict(zip(columns, row)) for row in cur.fetchall()]
            return conversations
        finally:
            self.return_connection(conn)

    def get_messages(self, conversation_id: str) -> List[Dict[str, Any]]:
        """
        Get all messages for a conversation.

        Args:
            conversation_id: ID of the conversation

        Returns:
            List[Dict[str, Any]]: List of message data
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(GET_MESSAGES_SQL, (conversation_id,))
                columns = [desc[0] for desc in cur.description]
                messages = [dict(zip(columns, row)) for row in cur.fetchall()]
            return messages
        finally:
            self.return_connection(conn)

    def get_conversation(self, conversation_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a conversation by ID.

        Args:
            conversation_id: ID of the conversation

        Returns:
            Optional[Dict[str, Any]]: Conversation data if found, None otherwise
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(GET_CONVERSATION_BY_ID_SQL, (conversation_id,))
                result = cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
        finally:
            self.return_connection(conn)

    def get_message(self, message_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a message by ID.

        Args:
            message_id: ID of the message

        Returns:
            Optional[Dict[str, Any]]: Message data if found, None otherwise
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(GET_MESSAGE_BY_ID_SQL, (message_id,))
                result = cur.fetchone()
                if result:
                    columns = [desc[0] for desc in cur.description]
                    return dict(zip(columns, result))
                return None
        finally:
            self.return_connection(conn)

    def store_transformed_data(
        self,
        transformed_data: Dict[str, Any],
        raw_export_id: int
    ) -> Tuple[int, int]:
        """
        Store transformed data in the database.

        Args:
            transformed_data: The transformed data structure
            raw_export_id: ID of the raw export this data belongs to

        Returns:
            Tuple[int, int]: (Number of conversations stored, Number of messages stored)
        """
        conversation_count = 0
        message_count = 0

        try:
            # Store conversations and messages
            for conv_id, conv_data in transformed_data.get('conversations', {}).items():
                # Store conversation
                self.store_conversation(
                    conversation_id=conv_id,
                    display_name=conv_data.get('displayName', ''),
                    raw_export_id=raw_export_id,
                    first_message_time=conv_data.get('firstMessageTime'),
                    last_message_time=conv_data.get('lastMessageTime'),
                    message_count=conv_data.get('messageCount', 0)
                )
                conversation_count += 1

                # Store messages for this conversation
                for msg in conv_data.get('messages', []):
                    self.store_message(
                        message_id=msg.get('id', f"{conv_id}_{msg.get('timestamp')}"),
                        conversation_id=conv_id,
                        timestamp=msg.get('timestamp'),
                        sender_id=msg.get('fromId', ''),
                        sender_name=msg.get('fromName', ''),
                        message_type=msg.get('type', ''),
                        content=msg.get('content', ''),
                        raw_content=msg.get('rawContent', ''),
                        is_edited=msg.get('isEdited', False)
                    )
                    message_count += 1

            logger.info(f"Stored {conversation_count} conversations and {message_count} messages")
            return conversation_count, message_count
        except Exception as e:
            logger.error(f"Failed to store transformed data: {e}")
            raise

    def close(self) -> None:
        """Close all database connections."""
        if self.pool:
            self.pool.closeall()
            logger.info("Closed all database connections")