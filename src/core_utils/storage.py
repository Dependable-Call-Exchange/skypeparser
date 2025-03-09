"""
Database operations for storing raw and cleaned Skype data.
"""

import json
import hashlib
import logging
from datetime import datetime
from typing import Dict, Optional, Tuple

from psycopg2.extras import Json
from psycopg2.pool import SimpleConnectionPool

from src.db.models import CREATE_RAW_TABLES_SQL, INSERT_RAW_DATA_SQL, INSERT_CLEANED_DATA_SQL, CHECK_DUPLICATE_SQL, GET_LATEST_CLEANED_SQL

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SkypeDataStorage:
    """Handles storage of raw and cleaned Skype data in PostgreSQL."""

    CLEANING_VERSION = "1.0.0"  # Update this when cleaning logic changes
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
                cur.execute(CREATE_RAW_TABLES_SQL)
            conn.commit()
            logger.info("Database tables verified/created successfully")
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            conn.rollback()
            raise
        finally:
            self.return_connection(conn)

    def calculate_file_hash(self, data: Dict) -> str:
        """
        Calculate SHA-256 hash of the data for integrity verification.

        Args:
            data: Dictionary containing the Skype data

        Returns:
            str: Hexadecimal representation of the SHA-256 hash
        """
        data_str = json.dumps(data, sort_keys=True)
        return hashlib.sha256(data_str.encode()).hexdigest()

    def check_duplicate(self, file_hash: str) -> Optional[Dict]:
        """
        Check if a file with the given hash already exists.

        Args:
            file_hash: SHA-256 hash of the file content

        Returns:
            Optional[Dict]: Existing file info if found, None otherwise
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(CHECK_DUPLICATE_SQL, (file_hash,))
                result = cur.fetchone()
                if result:
                    return {
                        'id': result[0],
                        'file_name': result[1],
                        'export_date': result[2]
                    }
                return None
        finally:
            self.return_connection(conn)

    def verify_data_integrity(self, raw_id: int, data: Dict) -> bool:
        """
        Verify the integrity of stored data by comparing hashes.

        Args:
            raw_id: ID of the raw data record
            data: Original data to verify against

        Returns:
            bool: True if data matches, False otherwise
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT raw_data, file_hash FROM raw_skype_exports WHERE id = %s",
                    (raw_id,)
                )
                stored_data, stored_hash = cur.fetchone()

                # Calculate hash of original data
                original_hash = self.calculate_file_hash(data)

                # Compare hashes
                if original_hash != stored_hash:
                    logger.error("Data integrity check failed: hash mismatch")
                    return False

                # Compare actual data
                if stored_data != data:
                    logger.error("Data integrity check failed: content mismatch")
                    return False

                return True
        finally:
            self.return_connection(conn)

    def store_raw_data(
        self,
        data: Dict,
        file_name: str,
        export_date: Optional[datetime] = None
    ) -> int:
        """
        Store raw Skype data in the database.

        Args:
            data: Dictionary containing the raw Skype data
            file_name: Name of the original file
            export_date: Optional timestamp of when the data was exported from Skype

        Returns:
            int: ID of the inserted record
        """
        conn = self.get_connection()
        try:
            file_hash = self.calculate_file_hash(data)

            # Check for duplicates
            existing = self.check_duplicate(file_hash)
            if existing:
                logger.info(f"File already exists with ID: {existing['id']}")
                return existing['id']

            with conn.cursor() as cur:
                cur.execute(
                    INSERT_RAW_DATA_SQL,
                    (
                        Json(data),
                        file_name,
                        file_hash,
                        export_date or datetime.now()
                    )
                )
                raw_id = cur.fetchone()[0]

            conn.commit()

            # Verify data integrity
            if not self.verify_data_integrity(raw_id, data):
                raise ValueError("Data integrity verification failed")

            logger.info(f"Stored raw data with ID: {raw_id}")
            return raw_id

        except Exception as e:
            logger.error(f"Failed to store raw data: {e}")
            conn.rollback()
            raise
        finally:
            self.return_connection(conn)

    def get_latest_cleaned_version(self, raw_export_id: int) -> Optional[Dict]:
        """
        Get the latest cleaned version of a raw export.

        Args:
            raw_export_id: ID of the raw export

        Returns:
            Optional[Dict]: Latest cleaned version if found, None otherwise
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(GET_LATEST_CLEANED_SQL, (raw_export_id,))
                result = cur.fetchone()
                if result:
                    return {
                        'id': result[0],
                        'raw_export_id': result[1],
                        'cleaned_data': result[2],
                        'created_at': result[3],
                        'cleaning_version': result[4],
                        'file_name': result[5],
                        'export_date': result[6]
                    }
                return None
        finally:
            self.return_connection(conn)

    def store_cleaned_data(
        self,
        raw_export_id: int,
        cleaned_data: Dict
    ) -> int:
        """
        Store cleaned Skype data in the database.

        Args:
            raw_export_id: ID of the corresponding raw data record
            cleaned_data: Dictionary containing the cleaned Skype data

        Returns:
            int: ID of the inserted record
        """
        conn = self.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    INSERT_CLEANED_DATA_SQL,
                    (
                        raw_export_id,
                        Json(cleaned_data),
                        self.CLEANING_VERSION
                    )
                )
                cleaned_id = cur.fetchone()[0]

            conn.commit()
            logger.info(f"Stored cleaned data with ID: {cleaned_id}")
            return cleaned_id

        except Exception as e:
            logger.error(f"Failed to store cleaned data: {e}")
            conn.rollback()
            raise
        finally:
            self.return_connection(conn)

    def store_skype_export(
        self,
        raw_data: Dict,
        cleaned_data: Dict,
        file_name: str,
        export_date: Optional[datetime] = None
    ) -> Tuple[int, int]:
        """
        Store both raw and cleaned Skype data in a single transaction.

        Args:
            raw_data: Dictionary containing the raw Skype data
            cleaned_data: Dictionary containing the cleaned Skype data
            file_name: Name of the original file
            export_date: Optional timestamp of when the data was exported from Skype

        Returns:
            Tuple[int, int]: (raw_data_id, cleaned_data_id)
        """
        conn = self.get_connection()
        try:
            # Start transaction
            with conn:
                with conn.cursor() as cur:
                    # Check for duplicates
                    file_hash = self.calculate_file_hash(raw_data)
                    existing = self.check_duplicate(file_hash)
                    if existing:
                        logger.info(f"File already exists with ID: {existing['id']}")
                        # Get latest cleaned version
                        latest = self.get_latest_cleaned_version(existing['id'])
                        if latest:
                            return existing['id'], latest['id']

                    # Store raw data
                    cur.execute(
                        INSERT_RAW_DATA_SQL,
                        (
                            Json(raw_data),
                            file_name,
                            file_hash,
                            export_date or datetime.now()
                        )
                    )
                    raw_id = cur.fetchone()[0]

                    # Store cleaned data
                    cur.execute(
                        INSERT_CLEANED_DATA_SQL,
                        (
                            raw_id,
                            Json(cleaned_data),
                            self.CLEANING_VERSION
                        )
                    )
                    cleaned_id = cur.fetchone()[0]

            # Verify data integrity
            if not self.verify_data_integrity(raw_id, raw_data):
                raise ValueError("Data integrity verification failed")

            logger.info(f"Stored Skype export (raw_id: {raw_id}, cleaned_id: {cleaned_id})")
            return raw_id, cleaned_id

        except Exception as e:
            logger.error(f"Failed to store Skype export: {e}")
            raise
        finally:
            self.return_connection(conn)

    def close(self) -> None:
        """Close all database connections."""
        if self.pool:
            self.pool.closeall()
            logger.info("All database connections closed")