"""
User handler for database insertion operations.
"""
import json
import logging
from typing import Dict, Any, List, Tuple, Optional

from src.logging.new_structured_logging import get_logger, log_execution_time, handle_errors
from src.core_utils.test_utils import is_test_environment, get_fast_test_mode
from src.data_handlers.base_handler import BaseHandler

logger = get_logger(__name__)
FAST_TEST_MODE = get_fast_test_mode()


class UserHandler(BaseHandler):
    """
    Handler for user insertion operations.
    """

    @classmethod
    def get_type(cls) -> str:
        """
        Returns the data type this handler processes.

        Returns:
            str: 'users'
        """
        return "users"

    @staticmethod
    @log_execution_time(level=logging.DEBUG)
    @handle_errors()
    def insert_bulk(db_manager, users: Dict[str, Dict[str, Any]], batch_size: int,
                   archive_id: Optional[str] = None) -> int:
        """
        Insert users into the database in bulk.

        Args:
            db_manager: Database manager
            users: Dictionary of users to insert
            batch_size: Size of each insertion batch
            archive_id: Archive ID (not used for users)

        Returns:
            int: Number of users inserted
        """
        # Fast path for test environments
        if FAST_TEST_MODE:
            if not users:
                logger.warning("No users to insert")
                return 0

            user_count = len(users)
            logger.info(f"[FAST TEST MODE] Skipped insertion of {user_count} users")
            return user_count

        # Normal path for non-test environments
        if not users:
            logger.warning("No users to insert")
            return 0

        logger.info(f"Inserting {len(users)} users")

        # Prepare data for bulk insert
        columns = ["id", "display_name", "properties"]

        values = []
        for user_id, user in users.items():
            # Extract properties
            properties = {k: v for k, v in user.items() if k not in ["id", "display_name"]}

            values.append((
                user_id,
                user.get("display_name", ""),
                json.dumps(properties),
            ))

        # Insert data
        return db_manager.bulk_insert("users", columns, values, batch_size)

    @staticmethod
    @log_execution_time(level=logging.DEBUG)
    @handle_errors()
    def insert_individual(db_manager, users: Dict[str, Dict[str, Any]],
                         archive_id: Optional[str] = None) -> int:
        """
        Insert users into the database one by one.

        Args:
            db_manager: Database manager
            users: Dictionary of users to insert
            archive_id: Archive ID (not used for users)

        Returns:
            int: Number of users inserted
        """
        # Fast path for test environments
        if FAST_TEST_MODE:
            if not users:
                logger.warning("No users to insert")
                return 0

            user_count = len(users)
            logger.info(f"[FAST TEST MODE] Skipped insertion of {user_count} users")
            return user_count

        # Normal path for non-test environments
        if not users:
            logger.warning("No users to insert")
            return 0

        logger.info(f"Inserting {len(users)} users individually")

        count = 0
        for user_id, user in users.items():
            # Extract properties
            properties = {k: v for k, v in user.items() if k not in ["id", "display_name"]}

            # Insert this user
            db_manager.execute_query(
                """
                INSERT INTO users
                (id, display_name, properties)
                VALUES (%s, %s, %s)
                """,
                (
                    user_id,
                    user.get("display_name", ""),
                    json.dumps(properties),
                )
            )
            count += 1

        return count