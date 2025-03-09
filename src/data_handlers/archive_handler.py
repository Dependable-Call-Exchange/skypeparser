"""
Archive handler for database insertion operations.
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


class ArchiveHandler(BaseHandler):
    """
    Handler for archive insertion operations.
    """

    @classmethod
    def get_type(cls) -> str:
        """
        Returns the data type this handler processes.

        Returns:
            str: 'archives'
        """
        return "archives"

    @staticmethod
    @log_execution_time()
    @handle_errors()
    def insert_bulk(db_manager, data: Dict[str, Any], batch_size: int,
                   archive_id: Optional[str] = None) -> str:
        """
        Insert archive record into the database using bulk insertion.

        Args:
            db_manager: Database manager
            data: Data to insert
            batch_size: Size of each insertion batch (not used for archives)
            archive_id: Existing archive ID (if None, a new one will be generated)

        Returns:
            str: Archive ID of the inserted record
        """
        # Fast path for test environments
        if FAST_TEST_MODE:
            # Generate a fake archive ID
            archive_id = archive_id or str(uuid.uuid4())
            logger.info(f"[FAST TEST MODE] Skipped archive insertion, using ID: {archive_id}")
            return archive_id

        # Normal path for non-test environments
        # Generate a UUID for the archive if none is provided
        archive_id = archive_id or str(uuid.uuid4())

        # Get the archive name from the data or use a default
        archive_name = data.get("archive_name", "Skype Export")

        # Get the file path from the data or use a default
        file_path = data.get("file_path", "unknown_export.tar")

        # Ensure file_path ends with .tar (required by database constraint)
        if not file_path.lower().endswith('.tar'):
            logger.warning(f"File path '{file_path}' doesn't end with .tar extension, required by database constraint")
            file_path = file_path + '.tar' if '.' not in file_path else file_path.rsplit('.', 1)[0] + '.tar'
            logger.info(f"Modified file path to satisfy database constraint: {file_path}")

        # Get the file size from the data or use a default
        file_size = data.get("file_size", 0)

        # Current time for created_at and updated_at
        current_time = datetime.now()

        # Insert the archive record
        columns = ["id", "user_id", "name", "file_path", "file_size", "created_at", "updated_at"]
        values = [(
            archive_id,
            "00000000-0000-0000-0000-000000000000",  # Default user_id
            archive_name,
            file_path,
            file_size,
            current_time,
            current_time
        )]

        try:
            db_manager.bulk_insert("archives", columns, values)
            logger.info(f"Inserted archive record with ID {archive_id}")
            return archive_id
        except Exception as e:
            logger.error(f"Error inserting archive record: {str(e)}")
            raise

    @staticmethod
    @log_execution_time()
    @handle_errors()
    def insert_individual(db_manager, data: Dict[str, Any],
                         archive_id: Optional[str] = None) -> str:
        """
        Insert archive record into the database individually.

        Args:
            db_manager: Database manager
            data: Data to insert
            archive_id: Existing archive ID (if None, a new one will be generated)

        Returns:
            str: Archive ID of the inserted record
        """
        # Fast path for test environments
        if FAST_TEST_MODE:
            # Generate a fake archive ID
            archive_id = archive_id or str(uuid.uuid4())
            logger.info(f"[FAST TEST MODE] Skipped archive insertion, using ID: {archive_id}")
            return archive_id

        # Normal path for non-test environments
        # Generate a UUID for the archive if none is provided
        archive_id = archive_id or str(uuid.uuid4())

        # Get the archive name from the data or use a default
        archive_name = data.get("archive_name", "Skype Export")

        # Get the file path from the data or use a default
        file_path = data.get("file_path", "unknown_export.tar")

        # Ensure file_path ends with .tar (required by database constraint)
        if not file_path.lower().endswith('.tar'):
            logger.warning(f"File path '{file_path}' doesn't end with .tar extension, required by database constraint")
            file_path = file_path + '.tar' if '.' not in file_path else file_path.rsplit('.', 1)[0] + '.tar'
            logger.info(f"Modified file path to satisfy database constraint: {file_path}")

        # Get the file size from the data or use a default
        file_size = data.get("file_size", 0)

        # Current time for created_at and updated_at
        current_time = datetime.now()

        # Insert the archive record
        try:
            db_manager.execute_query(
                """
                INSERT INTO archives
                (id, user_id, name, file_path, file_size, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    archive_id,
                    "00000000-0000-0000-0000-000000000000",  # Default user_id
                    archive_name,
                    file_path,
                    file_size,
                    current_time,
                    current_time
                )
            )
            logger.info(f"Inserted archive record with ID {archive_id}")
            return archive_id
        except Exception as e:
            logger.error(f"Error inserting archive record: {str(e)}")
            raise