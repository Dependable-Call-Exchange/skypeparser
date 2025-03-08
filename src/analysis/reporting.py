"""
Reporting Module for Skype Parser

This module provides functionality for generating reports on Skype data
that has been processed by the ETL pipeline.
"""

import logging
import json
import os
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta

from src.utils.interfaces import DatabaseConnectionProtocol
from src.utils.di import get_service

logger = logging.getLogger(__name__)

# Add the generate_report function for backward compatibility
def generate_report(export_id: int, output_file: Optional[str] = None,
                    db_connection: Optional[DatabaseConnectionProtocol] = None) -> Dict[str, Any]:
    """
    Generate a report for a Skype export.

    This function is a wrapper around SkypeReportGenerator for backward compatibility.

    Args:
        export_id: The ID of the export to report on
        output_file: Optional path to save the report to
        db_connection: Optional database connection to use

    Returns:
        A dictionary containing the report data
    """
    logger.info(f"Generating report for export {export_id}")

    # Create a report generator
    report_generator = SkypeReportGenerator(db_connection)

    # Get export summary
    export_summary = report_generator.get_export_summary(export_id)

    # Get conversation statistics
    conversation_stats = report_generator.get_conversation_statistics(export_id)

    # Get message type distribution
    message_types = report_generator.get_message_type_distribution(export_id)

    # Get activity by hour
    activity_by_hour = report_generator.get_activity_by_hour(export_id)

    # Get activity by day of week
    activity_by_day = report_generator.get_activity_by_day_of_week(export_id)

    # Compile the report
    report = {
        "export_summary": export_summary,
        "conversation_statistics": conversation_stats,
        "message_type_distribution": message_types,
        "activity_by_hour": activity_by_hour,
        "activity_by_day_of_week": activity_by_day
    }

    # Save the report to a file if requested
    if output_file:
        try:
            os.makedirs(os.path.dirname(output_file), exist_ok=True)
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(report, f, indent=2, default=str)
            logger.info(f"Report saved to {output_file}")
        except Exception as e:
            logger.error(f"Error saving report to {output_file}: {e}")

    return report

class SkypeReportGenerator:
    """
    A class for generating reports on Skype data.

    This class provides methods for generating various reports on Skype data
    that has been processed by the ETL pipeline and stored in a PostgreSQL database.
    """

    def __init__(self, db_connection: Optional[DatabaseConnectionProtocol] = None):
        """
        Initialize the SkypeReportGenerator.

        Args:
            db_connection: A database connection object. If None, one will be
                retrieved from the dependency injection system.
        """
        self.db_connection = db_connection or get_service(DatabaseConnectionProtocol)
        logger.info("SkypeReportGenerator initialized")

    def get_export_summary(self, export_id: int) -> Dict[str, Any]:
        """
        Get a summary of a Skype export.

        Args:
            export_id: The ID of the export to summarize.

        Returns:
            A dictionary containing summary information about the export.
        """
        # Get export metadata
        export_query = """
            SELECT
                id,
                user_id,
                export_date,
                file_source,
                created_at
            FROM
                skype_exports
            WHERE
                id = %s
        """
        export_result = self.db_connection.execute_query(export_query, (export_id,))

        if not export_result:
            logger.warning(f"No export found with ID {export_id}")
            return {"error": f"No export found with ID {export_id}"}

        export_data = export_result[0]

        # Get conversation count
        conversation_query = """
            SELECT
                COUNT(*) as conversation_count
            FROM
                skype_conversations
            WHERE
                export_id = %s
        """
        conversation_result = self.db_connection.execute_query(conversation_query, (export_id,))
        conversation_count = conversation_result[0]["conversation_count"] if conversation_result else 0

        # Get message count
        message_query = """
            SELECT
                COUNT(*) as message_count
            FROM
                skype_messages
            WHERE
                export_id = %s
        """
        message_result = self.db_connection.execute_query(message_query, (export_id,))
        message_count = message_result[0]["message_count"] if message_result else 0

        # Get date range
        date_range_query = """
            SELECT
                MIN(timestamp) as first_message,
                MAX(timestamp) as last_message
            FROM
                skype_messages
            WHERE
                export_id = %s
        """
        date_range_result = self.db_connection.execute_query(date_range_query, (export_id,))

        first_message = date_range_result[0]["first_message"] if date_range_result else None
        last_message = date_range_result[0]["last_message"] if date_range_result else None

        # Calculate duration in days
        duration_days = None
        if first_message and last_message:
            duration = last_message - first_message
            duration_days = duration.days

        return {
            "export_id": export_id,
            "user_id": export_data["user_id"],
            "export_date": export_data["export_date"],
            "file_source": export_data["file_source"],
            "created_at": export_data["created_at"],
            "conversation_count": conversation_count,
            "message_count": message_count,
            "first_message": first_message,
            "last_message": last_message,
            "duration_days": duration_days
        }

    def get_conversation_statistics(self, export_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get statistics for conversations in a Skype export.

        Args:
            export_id: The ID of the export to analyze.
            limit: The maximum number of conversations to return.

        Returns:
            A list of dictionaries containing statistics for each conversation.
        """
        query = """
            SELECT
                c.id,
                c.conversation_id,
                c.display_name,
                c.message_count,
                c.first_message_time,
                c.last_message_time,
                EXTRACT(DAY FROM (c.last_message_time - c.first_message_time)) AS duration_days,
                (
                    SELECT COUNT(DISTINCT sender_id)
                    FROM skype_messages
                    WHERE conversation_id = c.conversation_id AND export_id = %s
                ) as participant_count
            FROM
                skype_conversations c
            WHERE
                c.export_id = %s
            ORDER BY
                c.message_count DESC
            LIMIT %s
        """

        result = self.db_connection.execute_query(query, (export_id, export_id, limit))

        return result if result else []

    def get_message_type_distribution(self, export_id: int) -> List[Dict[str, Any]]:
        """
        Get the distribution of message types in a Skype export.

        Args:
            export_id: The ID of the export to analyze.

        Returns:
            A list of dictionaries containing the count and percentage for each message type.
        """
        query = """
            SELECT
                message_type,
                COUNT(*) as count,
                ROUND(COUNT(*) * 100.0 / (
                    SELECT COUNT(*)
                    FROM skype_messages
                    WHERE export_id = %s
                ), 2) as percentage
            FROM
                skype_messages
            WHERE
                export_id = %s
            GROUP BY
                message_type
            ORDER BY
                count DESC
        """

        result = self.db_connection.execute_query(query, (export_id, export_id))

        return result if result else []

    def get_activity_by_hour(self, export_id: int) -> List[Dict[str, Any]]:
        """
        Get the distribution of messages by hour of day in a Skype export.

        Args:
            export_id: The ID of the export to analyze.

        Returns:
            A list of dictionaries containing the count for each hour of the day.
        """
        query = """
            SELECT
                EXTRACT(HOUR FROM timestamp) as hour,
                COUNT(*) as message_count
            FROM
                skype_messages
            WHERE
                export_id = %s
            GROUP BY
                hour
            ORDER BY
                hour
        """

        result = self.db_connection.execute_query(query, (export_id, export_id))

        return result if result else []

    def get_activity_by_day_of_week(self, export_id: int) -> List[Dict[str, Any]]:
        """
        Get the distribution of messages by day of week in a Skype export.

        Args:
            export_id: The ID of the export to analyze.

        Returns:
            A list of dictionaries containing the count for each day of the week.
        """
        query = """
            SELECT
                EXTRACT(DOW FROM timestamp) as day_of_week,
                COUNT(*) as message_count
            FROM
                skype_messages
            WHERE
                export_id = %s
            GROUP BY
                day_of_week
            ORDER BY
                day_of_week
        """

        result = self.db_connection.execute_query(query, (export_id, export_id))

        # Convert day_of_week number to name
        day_names = ["Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"]
        for row in result:
            row["day_name"] = day_names[int(row["day_of_week"])]

        return result if result else []

    def get_top_senders(self, export_id: int, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get the top message senders in a Skype export.

        Args:
            export_id: The ID of the export to analyze.
            limit: The maximum number of senders to return.

        Returns:
            A list of dictionaries containing the count for each sender.
        """
        query = """
            SELECT
                sender_name,
                COUNT(*) as message_count,
                ROUND(COUNT(*) * 100.0 / (
                    SELECT COUNT(*)
                    FROM skype_messages
                    WHERE export_id = %s
                ), 2) as percentage
            FROM
                skype_messages
            WHERE
                export_id = %s
            GROUP BY
                sender_name
            ORDER BY
                message_count DESC
            LIMIT %s
        """

        result = self.db_connection.execute_query(query, (export_id, export_id, limit))

        return result if result else []

    def get_message_length_statistics(self, export_id: int) -> Dict[str, Any]:
        """
        Get statistics about message lengths in a Skype export.

        Args:
            export_id: The ID of the export to analyze.

        Returns:
            A dictionary containing statistics about message lengths.
        """
        query = """
            SELECT
                AVG(LENGTH(content)) as avg_length,
                MIN(LENGTH(content)) as min_length,
                MAX(LENGTH(content)) as max_length,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY LENGTH(content)) as median_length,
                STDDEV(LENGTH(content)) as stddev_length
            FROM
                skype_messages
            WHERE
                export_id = %s
                AND message_type = 'RichText'
        """

        result = self.db_connection.execute_query(query, (export_id,))

        if not result:
            return {
                "avg_length": 0,
                "min_length": 0,
                "max_length": 0,
                "median_length": 0,
                "stddev_length": 0
            }

        return result[0]

    def generate_full_report(self, export_id: int) -> Dict[str, Any]:
        """
        Generate a full report for a Skype export.

        Args:
            export_id: The ID of the export to analyze.

        Returns:
            A dictionary containing all report data.
        """
        report = {
            "summary": self.get_export_summary(export_id),
            "conversation_statistics": self.get_conversation_statistics(export_id),
            "message_type_distribution": self.get_message_type_distribution(export_id),
            "activity_by_hour": self.get_activity_by_hour(export_id),
            "activity_by_day_of_week": self.get_activity_by_day_of_week(export_id),
            "top_senders": self.get_top_senders(export_id),
            "message_length_statistics": self.get_message_length_statistics(export_id)
        }

        return report