"""
Example Queries Module for Skype Parser

This module provides example queries for common analytics tasks on Skype data
that has been processed by the ETL pipeline.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta

from src.utils.interfaces import DatabaseConnectionProtocol
from src.utils.di import get_service

logger = logging.getLogger(__name__)

class SkypeQueryExamples:
    """
    A class providing example queries for common analytics tasks on Skype data.

    This class provides methods for executing common analytics queries on Skype data
    that has been processed by the ETL pipeline and stored in a PostgreSQL database.
    """

    def __init__(self, db_connection: Optional[DatabaseConnectionProtocol] = None):
        """
        Initialize the SkypeQueryExamples.

        Args:
            db_connection: A database connection object. If None, one will be
                retrieved from the dependency injection system.
        """
        self.db_connection = db_connection or get_service(DatabaseConnectionProtocol)
        logger.info("SkypeQueryExamples initialized")

    def find_conversations_with_keyword(self, export_id: int, keyword: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Find conversations containing a specific keyword.

        Args:
            export_id: The ID of the export to search.
            keyword: The keyword to search for.
            limit: The maximum number of results to return.

        Returns:
            A list of dictionaries containing matching conversations.
        """
        query = """
            SELECT
                m.id,
                m.conversation_id,
                c.display_name as conversation_name,
                m.sender_name,
                m.timestamp,
                m.content
            FROM
                skype_messages m
            JOIN
                skype_conversations c ON m.conversation_id = c.conversation_id AND m.export_id = c.export_id
            WHERE
                m.export_id = %s
                AND m.content ILIKE %s
            ORDER BY
                m.timestamp DESC
            LIMIT %s
        """

        result = self.db_connection.execute_query(query, (export_id, f"%{keyword}%", limit))

        return result if result else []

    def get_conversation_timeline(self, export_id: int, conversation_id: str, interval: str = 'day') -> List[Dict[str, Any]]:
        """
        Get a timeline of message activity for a specific conversation.

        Args:
            export_id: The ID of the export to analyze.
            conversation_id: The ID of the conversation to analyze.
            interval: The time interval to group by ('hour', 'day', 'week', 'month').

        Returns:
            A list of dictionaries containing the message count for each time interval.
        """
        interval_sql = {
            'hour': "DATE_TRUNC('hour', timestamp)",
            'day': "DATE_TRUNC('day', timestamp)",
            'week': "DATE_TRUNC('week', timestamp)",
            'month': "DATE_TRUNC('month', timestamp)"
        }.get(interval.lower(), "DATE_TRUNC('day', timestamp)")

        query = f"""
            SELECT
                {interval_sql} as time_interval,
                COUNT(*) as message_count
            FROM
                skype_messages
            WHERE
                export_id = %s
                AND conversation_id = %s
            GROUP BY
                time_interval
            ORDER BY
                time_interval
        """

        result = self.db_connection.execute_query(query, (export_id, conversation_id))

        return result if result else []

    def get_user_activity_timeline(self, export_id: int, sender_name: str, interval: str = 'day') -> List[Dict[str, Any]]:
        """
        Get a timeline of message activity for a specific user.

        Args:
            export_id: The ID of the export to analyze.
            sender_name: The name of the sender to analyze.
            interval: The time interval to group by ('hour', 'day', 'week', 'month').

        Returns:
            A list of dictionaries containing the message count for each time interval.
        """
        interval_sql = {
            'hour': "DATE_TRUNC('hour', timestamp)",
            'day': "DATE_TRUNC('day', timestamp)",
            'week': "DATE_TRUNC('week', timestamp)",
            'month': "DATE_TRUNC('month', timestamp)"
        }.get(interval.lower(), "DATE_TRUNC('day', timestamp)")

        query = f"""
            SELECT
                {interval_sql} as time_interval,
                COUNT(*) as message_count
            FROM
                skype_messages
            WHERE
                export_id = %s
                AND sender_name = %s
            GROUP BY
                time_interval
            ORDER BY
                time_interval
        """

        result = self.db_connection.execute_query(query, (export_id, sender_name))

        return result if result else []

    def get_conversation_participants(self, export_id: int, conversation_id: str) -> List[Dict[str, Any]]:
        """
        Get a list of participants in a specific conversation.

        Args:
            export_id: The ID of the export to analyze.
            conversation_id: The ID of the conversation to analyze.

        Returns:
            A list of dictionaries containing participant information.
        """
        query = """
            SELECT
                sender_name,
                COUNT(*) as message_count,
                MIN(timestamp) as first_message,
                MAX(timestamp) as last_message
            FROM
                skype_messages
            WHERE
                export_id = %s
                AND conversation_id = %s
            GROUP BY
                sender_name
            ORDER BY
                message_count DESC
        """

        result = self.db_connection.execute_query(query, (export_id, conversation_id))

        return result if result else []

    def get_message_length_by_sender(self, export_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get average message length by sender.

        Args:
            export_id: The ID of the export to analyze.
            limit: The maximum number of results to return.

        Returns:
            A list of dictionaries containing average message length by sender.
        """
        query = """
            SELECT
                sender_name,
                AVG(LENGTH(content)) as avg_length,
                COUNT(*) as message_count
            FROM
                skype_messages
            WHERE
                export_id = %s
                AND message_type = 'RichText'
            GROUP BY
                sender_name
            HAVING
                COUNT(*) > 10
            ORDER BY
                avg_length DESC
            LIMIT %s
        """

        result = self.db_connection.execute_query(query, (export_id, limit))

        return result if result else []

    def get_most_active_conversations_by_timespan(self, export_id: int, start_date: datetime, end_date: datetime, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get the most active conversations within a specific timespan.

        Args:
            export_id: The ID of the export to analyze.
            start_date: The start date of the timespan.
            end_date: The end date of the timespan.
            limit: The maximum number of results to return.

        Returns:
            A list of dictionaries containing the most active conversations.
        """
        query = """
            SELECT
                c.conversation_id,
                c.display_name,
                COUNT(m.id) as message_count
            FROM
                skype_conversations c
            JOIN
                skype_messages m ON c.conversation_id = m.conversation_id AND c.export_id = m.export_id
            WHERE
                c.export_id = %s
                AND m.timestamp BETWEEN %s AND %s
            GROUP BY
                c.conversation_id, c.display_name
            ORDER BY
                message_count DESC
            LIMIT %s
        """

        result = self.db_connection.execute_query(query, (export_id, start_date, end_date, limit))

        return result if result else []

    def get_most_used_message_types_by_sender(self, export_id: int, sender_name: str) -> List[Dict[str, Any]]:
        """
        Get the most used message types by a specific sender.

        Args:
            export_id: The ID of the export to analyze.
            sender_name: The name of the sender to analyze.

        Returns:
            A list of dictionaries containing the most used message types.
        """
        query = """
            SELECT
                message_type,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / (
                    SELECT COUNT(*)
                    FROM skype_messages
                    WHERE export_id = %s AND sender_name = %s
                ), 2) as percentage
            FROM
                skype_messages
            WHERE
                export_id = %s
                AND sender_name = %s
            GROUP BY
                message_type
            ORDER BY
                count DESC
        """

        result = self.db_connection.execute_query(query, (export_id, sender_name, export_id, sender_name))

        return result if result else []

    def get_conversation_response_times(self, export_id: int, conversation_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get response times in a specific conversation.

        Args:
            export_id: The ID of the export to analyze.
            conversation_id: The ID of the conversation to analyze.
            limit: The maximum number of results to return.

        Returns:
            A list of dictionaries containing response time information.
        """
        query = """
            WITH ordered_messages AS (
                SELECT
                    id,
                    sender_name,
                    timestamp,
                    LAG(sender_name) OVER (ORDER BY timestamp) as prev_sender,
                    LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp
                FROM
                    skype_messages
                WHERE
                    export_id = %s
                    AND conversation_id = %s
                ORDER BY
                    timestamp
            )
            SELECT
                id,
                sender_name,
                timestamp,
                prev_sender,
                prev_timestamp,
                EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) as response_time_seconds
            FROM
                ordered_messages
            WHERE
                prev_sender IS NOT NULL
                AND prev_sender != sender_name
            ORDER BY
                response_time_seconds
            LIMIT %s
        """

        result = self.db_connection.execute_query(query, (export_id, conversation_id, limit))

        return result if result else []

    def get_average_response_times_by_sender(self, export_id: int, conversation_id: str) -> List[Dict[str, Any]]:
        """
        Get average response times by sender in a specific conversation.

        Args:
            export_id: The ID of the export to analyze.
            conversation_id: The ID of the conversation to analyze.

        Returns:
            A list of dictionaries containing average response time by sender.
        """
        query = """
            WITH ordered_messages AS (
                SELECT
                    id,
                    sender_name,
                    timestamp,
                    LAG(sender_name) OVER (ORDER BY timestamp) as prev_sender,
                    LAG(timestamp) OVER (ORDER BY timestamp) as prev_timestamp
                FROM
                    skype_messages
                WHERE
                    export_id = %s
                    AND conversation_id = %s
                ORDER BY
                    timestamp
            )
            SELECT
                sender_name,
                AVG(EXTRACT(EPOCH FROM (timestamp - prev_timestamp))) as avg_response_time_seconds,
                COUNT(*) as response_count
            FROM
                ordered_messages
            WHERE
                prev_sender IS NOT NULL
                AND prev_sender != sender_name
                AND EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) < 86400  -- Filter out responses longer than 24 hours
            GROUP BY
                sender_name
            ORDER BY
                avg_response_time_seconds
        """

        result = self.db_connection.execute_query(query, (export_id, conversation_id))

        return result if result else []

    def get_attachment_statistics(self, export_id: int) -> Dict[str, Any]:
        """
        Get statistics about attachments in a Skype export.

        Args:
            export_id: The ID of the export to analyze.

        Returns:
            A dictionary containing attachment statistics.
        """
        # Check if message_attachments table exists
        check_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'message_attachments'
            ) as table_exists
        """

        check_result = self.db_connection.execute_query(check_query)

        if not check_result or not check_result[0]["table_exists"]:
            logger.warning("message_attachments table does not exist")
            return {"error": "message_attachments table does not exist"}

        # Get attachment count by type
        type_query = """
            SELECT
                content_type,
                COUNT(*) as count
            FROM
                message_attachments
            WHERE
                export_id = %s
            GROUP BY
                content_type
            ORDER BY
                count DESC
        """

        type_result = self.db_connection.execute_query(type_query, (export_id,))

        # Get total attachment count
        count_query = """
            SELECT
                COUNT(*) as total_count,
                COUNT(DISTINCT message_id) as messages_with_attachments
            FROM
                message_attachments
            WHERE
                export_id = %s
        """

        count_result = self.db_connection.execute_query(count_query, (export_id,))

        # Get top senders of attachments
        sender_query = """
            SELECT
                m.sender_name,
                COUNT(a.id) as attachment_count
            FROM
                message_attachments a
            JOIN
                skype_messages m ON a.message_id = m.message_id AND a.export_id = m.export_id
            WHERE
                a.export_id = %s
            GROUP BY
                m.sender_name
            ORDER BY
                attachment_count DESC
            LIMIT 10
        """

        sender_result = self.db_connection.execute_query(sender_query, (export_id,))

        return {
            "attachment_types": type_result if type_result else [],
            "total_count": count_result[0]["total_count"] if count_result else 0,
            "messages_with_attachments": count_result[0]["messages_with_attachments"] if count_result else 0,
            "top_senders": sender_result if sender_result else []
        }