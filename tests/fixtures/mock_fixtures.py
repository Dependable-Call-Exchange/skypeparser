"""
Mock fixtures for ETL pipeline unit tests.

This module contains mock objects and functions to simplify unit testing
and reduce the amount of patching needed in individual tests.
"""

import json
from unittest.mock import MagicMock, patch, mock_open
from typing import Dict, Any, Optional, Callable

from .skype_data import BASIC_SKYPE_DATA, COMPLEX_SKYPE_DATA, INVALID_SKYPE_DATA
from tests.factories import SkypeDataFactory, SkypeConversationFactory, SkypeMessageFactory


class MockFileReader:
    """
    Mock file reader that returns predefined data for file paths.

    This class helps reduce the need for multiple patches in tests by providing
    a single object that can be injected into the code under test.
    """

    def __init__(self, default_data: Optional[Dict[str, Any]] = None):
        """
        Initialize the mock file reader.

        Args:
            default_data: Default data to return if path is not found
        """
        self.path_data_map = {}
        self.default_data = default_data or SkypeDataFactory.build()

    def add_file(self, path: str, data: Dict[str, Any]) -> None:
        """
        Add a file path and its corresponding data.

        Args:
            path: File path
            data: Data to return when this path is read
        """
        self.path_data_map[path] = data

    def read_file(self, path: str) -> Dict[str, Any]:
        """
        Read data from a file path.

        Args:
            path: File path

        Returns:
            Dict[str, Any]: Data for the file path
        """
        return self.path_data_map.get(path, self.default_data)

    def read_tarfile(self, path: str, **kwargs) -> Dict[str, Any]:
        """
        Read data from a tar file path.

        Args:
            path: Tar file path
            **kwargs: Additional arguments (ignored)

        Returns:
            Dict[str, Any]: Data for the tar file path
        """
        return self.path_data_map.get(path, self.default_data)

    def read_file_object(self, file_obj, **kwargs) -> Dict[str, Any]:
        """
        Read data from a file object.

        Args:
            file_obj: File object
            **kwargs: Additional arguments (ignored)

        Returns:
            Dict[str, Any]: Default data
        """
        return self.default_data


class MockDatabase:
    """
    Mock database for testing ETL pipeline loading.

    This class provides a mock database connection and cursor that can be
    injected into the code under test.
    """

    def __init__(self, should_fail: bool = False):
        """
        Initialize the mock database.

        Args:
            should_fail: Whether database operations should fail
        """
        self.should_fail = should_fail
        self.conn = MagicMock()
        self.cursor = MagicMock()
        self.conn.cursor.return_value = self.cursor

        # Set up connection encoding for psycopg2.extras.execute_values
        self.cursor.__enter__.return_value = self.cursor
        self.cursor.__enter__.return_value.connection.encoding = 'UTF8'

        # Set up mogrify to return bytes instead of a MagicMock
        def mogrify_side_effect(query, params=None):
            # Convert the query to bytes if it's not already
            if isinstance(query, str):
                query_bytes = query.encode('utf-8')
            elif isinstance(query, bytes):
                query_bytes = query
            else:
                query_bytes = str(query).encode('utf-8')

            # Return bytes as expected by execute_values
            return query_bytes

        self.cursor.mogrify.side_effect = mogrify_side_effect
        self.cursor.__enter__.return_value.mogrify.side_effect = mogrify_side_effect

        self.executed_queries = []

        # Set up cursor to record executed queries
        def execute_side_effect(query, params=None):
            self.executed_queries.append(query)  # Store just the query string for easier checking
            if self.should_fail:
                raise Exception("Database operation failed")
            return self.cursor  # Return cursor for method chaining

        self.cursor.execute.side_effect = execute_side_effect

        # Make sure fetchone returns something
        fetchone_result = MagicMock()
        fetchone_result.__getitem__.return_value = 1
        self.cursor.fetchone.return_value = fetchone_result
        self.cursor.__enter__.return_value.fetchone.return_value = fetchone_result

    def get_executed_queries(self) -> list:
        """
        Get the list of executed queries.

        Returns:
            list: List of executed query strings
        """
        return self.executed_queries


def create_mock_file_environment(file_data: Dict[str, Dict[str, Any]] = None) -> Dict[str, Callable]:
    """
    Create a set of mock functions for file operations.

    Args:
        file_data: Dictionary mapping file paths to their data

    Returns:
        Dict[str, Callable]: Dictionary of mock functions
    """
    # Use factories to create default test data if not provided
    if file_data is None:
        file_data = {
            "test.json": SkypeDataFactory.build(),
            "complex.json": SkypeDataFactory.build(
                conversations=[
                    SkypeConversationFactory.build(with_message_count=5),
                    SkypeConversationFactory.build(with_message_count=3)
                ]
            ),
            "invalid.json": SkypeDataFactory.build(
                userId=None,
                exportDate='invalid-date',
                conversations=[]
            ),
            "test.tar": SkypeDataFactory.build()
        }

    file_reader = MockFileReader()
    for path, data in file_data.items():
        file_reader.add_file(path, data)

    # Create patch functions
    mock_functions = {
        "read_file": patch("src.utils.file_handler.read_file", side_effect=file_reader.read_file),
        "read_tarfile": patch("src.utils.file_handler.read_tarfile", side_effect=file_reader.read_tarfile),
        "read_file_object": patch("src.db.etl_pipeline.read_file_object", side_effect=file_reader.read_file_object),
        "validate_file_exists": patch("src.utils.validation.validate_file_exists", return_value=True),
        "validate_path_safety": patch("src.utils.validation.validate_path_safety", side_effect=lambda path, **kwargs: path),
        "validate_json_file": patch("src.utils.validation.validate_json_file", side_effect=lambda path: file_reader.read_file(path)),
        "validate_tar_file": patch("src.utils.validation.validate_tar_file", return_value=True),
        "validate_user_display_name": patch("src.utils.validation.validate_user_display_name", return_value=True),
        "path_exists": patch("os.path.exists", return_value=True),
        "path_isfile": patch("os.path.isfile", return_value=True),
        "access": patch("os.access", return_value=True),
        "open": patch("builtins.open", new_callable=lambda: mock_open(read_data=json.dumps(SkypeDataFactory.build())))
    }

    return mock_functions