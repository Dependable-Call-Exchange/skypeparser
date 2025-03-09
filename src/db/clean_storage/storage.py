"""
Database operations for storing and retrieving clean Skype data.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any

from psycopg2.extras import Json
from psycopg2.pool import SimpleConnectionPool

from src.db.models import CREATE_CLEAN_TABLES_SQL, INSERT_CONVERSATION_SQL, INSERT_MESSAGE_SQL, INSERT_MEDIA_SQL, INSERT_POLL_SQL, INSERT_POLL_OPTION_SQL, INSERT_LOCATION_SQL, GET_CONVERSATIONS_SQL, GET_MESSAGES_SQL, GET_CONVERSATION_BY_ID_SQL, GET_MESSAGE_BY_ID_SQL, GET_MEDIA_BY_MESSAGE_ID_SQL, GET_POLL_BY_MESSAGE_ID_SQL, GET_LOCATION_BY_MESSAGE_ID_SQL

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
        sender_name: str,
        message_type: str,
        content: str,
        raw_content: str,
        is_edited: bool = False,
        structured_data: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Store a message in the database.

        Args:
            message_id: Unique identifier for the message
            conversation_id: ID of the conversation this message belongs to
            timestamp: Timestamp of the message
            sender_id: ID of the sender
            sender_name: Name of the sender
            message_type: Type of message
            content: Cleaned content of the message
            raw_content: Raw content of the message
            is_edited: Whether the message has been edited
            structured_data: Structured data extracted from the message

        Returns:
            int: ID of the inserted message
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                # Insert the message
                cur.execute(
                    INSERT_MESSAGE_SQL,
                    (
                        message_id, conversation_id, timestamp, sender_id,
                        sender_name, message_type, content, raw_content, is_edited,
                        Json(structured_data) if structured_data else None
                    )
                )
                result = cur.fetchone()

                # If structured data is provided, store specific data in specialized tables
                if structured_data and result:
                    message_db_id = result[0]
                    self._store_specialized_data(cur, message_id, structured_data)

            conn.commit()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Failed to store message: {e}")
            conn.rollback()
            raise
        finally:
            self.return_connection(conn)

    def _store_specialized_data(self, cursor, message_id: str, structured_data: Dict[str, Any]) -> None:
        """
        Store specialized data in appropriate tables based on message type.

        Args:
            cursor: Database cursor
            message_id: ID of the message
            structured_data: Structured data extracted from the message
        """
        # Store media data if present
        if any(key.startswith('media_') for key in structured_data.keys()):
            self._store_media_data(cursor, message_id, structured_data)

        # Store poll data if present
        if 'poll_question' in structured_data and 'poll_options' in structured_data:
            self._store_poll_data(cursor, message_id, structured_data)

        # Store location data if present
        if any(key.startswith('location_') for key in structured_data.keys()):
            self._store_location_data(cursor, message_id, structured_data)

    def _store_media_data(self, cursor, message_id: str, structured_data: Dict[str, Any]) -> None:
        """Store media data in the media table."""
        cursor.execute(
            INSERT_MEDIA_SQL,
            (
                message_id,
                structured_data.get('media_filename', ''),
                int(structured_data.get('media_filesize', 0)) if structured_data.get('media_filesize', '').isdigit() else 0,
                structured_data.get('media_filetype', ''),
                structured_data.get('media_url', ''),
                structured_data.get('media_thumbnail_url', ''),
                int(structured_data.get('media_width', 0)) if structured_data.get('media_width', '').isdigit() else None,
                int(structured_data.get('media_height', 0)) if structured_data.get('media_height', '').isdigit() else None,
                structured_data.get('media_duration', ''),
                structured_data.get('media_description', '')
            )
        )

    def _store_poll_data(self, cursor, message_id: str, structured_data: Dict[str, Any]) -> None:
        """Store poll data in the poll tables."""
        # Insert poll
        cursor.execute(
            INSERT_POLL_SQL,
            (
                message_id,
                structured_data.get('poll_question', '')
            )
        )
        poll_result = cursor.fetchone()

        if poll_result and 'poll_options' in structured_data:
            poll_id = poll_result[0]
            # Insert poll options
            for option in structured_data['poll_options']:
                cursor.execute(
                    INSERT_POLL_OPTION_SQL,
                    (poll_id, option)
                )

    def _store_location_data(self, cursor, message_id: str, structured_data: Dict[str, Any]) -> None:
        """Store location data in the location table."""
        cursor.execute(
            INSERT_LOCATION_SQL,
            (
                message_id,
                structured_data.get('location_latitude', ''),
                structured_data.get('location_longitude', ''),
                structured_data.get('location_address', '')
            )
        )

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
        Get all messages for a conversation with structured data.

        Args:
            conversation_id: ID of the conversation

        Returns:
            list: List of message data
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(GET_MESSAGES_SQL, (conversation_id,))
                messages = []

                for row in cur.fetchall():
                    message = {
                        'id': row[0],
                        'message_id': row[1],
                        'conversation_id': row[2],
                        'timestamp': row[3],
                        'sender_id': row[4],
                        'sender_name': row[5],
                        'message_type': row[6],
                        'content': row[7],
                        'raw_content': row[8],
                        'is_edited': row[9],
                        'structured_data': row[10],
                        'created_at': row[11]
                    }

                    # Enrich with specialized data
                    message = self._enrich_message_with_specialized_data(cur, message)
                    messages.append(message)

                return messages
        except Exception as e:
            logger.error(f"Failed to get messages: {e}")
            raise
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
        Get a message by ID with all associated structured data.

        Args:
            message_id: ID of the message to retrieve

        Returns:
            dict: Message data or None if not found
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                # Get basic message data
                cur.execute(GET_MESSAGE_BY_ID_SQL, (message_id,))
                message_row = cur.fetchone()

                if not message_row:
                    return None

                # Create message dict
                message = {
                    'id': message_row[0],
                    'message_id': message_row[1],
                    'conversation_id': message_row[2],
                    'timestamp': message_row[3],
                    'sender_id': message_row[4],
                    'sender_name': message_row[5],
                    'message_type': message_row[6],
                    'content': message_row[7],
                    'raw_content': message_row[8],
                    'is_edited': message_row[9],
                    'structured_data': message_row[10],
                    'created_at': message_row[11]
                }

                # Get specialized data if available
                message = self._enrich_message_with_specialized_data(cur, message)

                return message
        except Exception as e:
            logger.error(f"Failed to get message: {e}")
            raise
        finally:
            self.return_connection(conn)

    def _enrich_message_with_specialized_data(self, cursor, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a message with specialized data from related tables.

        Args:
            cursor: Database cursor
            message: Message data

        Returns:
            dict: Enriched message data
        """
        message_id = message['message_id']

        # Get media data
        cursor.execute(GET_MEDIA_BY_MESSAGE_ID_SQL, (message_id,))
        media_row = cursor.fetchone()
        if media_row:
            message['media'] = {
                'id': media_row[0],
                'message_id': media_row[1],
                'filename': media_row[2],
                'filesize': media_row[3],
                'filetype': media_row[4],
                'url': media_row[5],
                'thumbnail_url': media_row[6],
                'width': media_row[7],
                'height': media_row[8],
                'duration': media_row[9],
                'description': media_row[10],
                'created_at': media_row[11]
            }

        # Get poll data
        cursor.execute(GET_POLL_BY_MESSAGE_ID_SQL, (message_id,))
        poll_row = cursor.fetchone()
        if poll_row:
            message['poll'] = {
                'id': poll_row[0],
                'message_id': poll_row[1],
                'question': poll_row[2],
                'created_at': poll_row[3],
                'options': poll_row[4] if poll_row[4] and poll_row[4][0] is not None else []
            }

        # Get location data
        cursor.execute(GET_LOCATION_BY_MESSAGE_ID_SQL, (message_id,))
        location_row = cursor.fetchone()
        if location_row:
            message['location'] = {
                'id': location_row[0],
                'message_id': location_row[1],
                'latitude': location_row[2],
                'longitude': location_row[3],
                'address': location_row[4],
                'created_at': location_row[5]
            }

        return message

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
                        is_edited=msg.get('isEdited', False),
                        structured_data=msg.get('structuredData', {})
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