"""
Mock fixtures for ETL pipeline unit tests.

This module contains mock objects and functions to simplify unit testing
and reduce the amount of patching needed in individual tests.
"""

import json
from typing import Any, BinaryIO, Callable, Dict, Optional
from unittest.mock import MagicMock, mock_open, patch

from src.utils.interfaces import ValidationServiceProtocol
from tests.factories import (
    SkypeConversationFactory,
    SkypeDataFactory,
    SkypeMessageFactory,
)

from .skype_data import BASIC_SKYPE_DATA, COMPLEX_SKYPE_DATA, INVALID_SKYPE_DATA


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
        self.cursor.__enter__.return_value.connection.encoding = "UTF8"

        # Set up mogrify to return bytes instead of a MagicMock
        def mogrify_side_effect(query, params=None):
            # Convert the query to bytes if it's not already
            if isinstance(query, str):
                query_bytes = query.encode("utf-8")
            elif isinstance(query, bytes):
                query_bytes = query
            else:
                query_bytes = str(query).encode("utf-8")

            # Return bytes as expected by execute_values
            return query_bytes

        self.cursor.mogrify.side_effect = mogrify_side_effect
        self.cursor.__enter__.return_value.mogrify.side_effect = mogrify_side_effect

        self.executed_queries = []

        # Set up cursor to record executed queries
        def execute_side_effect(query, params=None):
            self.executed_queries.append(
                query
            )  # Store just the query string for easier checking
            if self.should_fail:
                raise Exception("Database operation failed")
            return self.cursor  # Return cursor for method chaining

        self.cursor.execute.side_effect = execute_side_effect

        # Add execute_batch method to the connection
        def execute_batch_side_effect(query, params_list=None):
            self.executed_queries.append(
                query
            )  # Store just the query string for easier checking
            if self.should_fail:
                raise Exception("Database operation failed")
            return None

        self.conn.execute_batch = MagicMock(side_effect=execute_batch_side_effect)

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


def create_mock_file_environment(
    file_data: Dict[str, Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Create a set of mock functions and objects for file operations.

    DEPRECATED: Use create_mock_functions() instead, which returns actual callable functions
    rather than patch objects.

    Args:
        file_data: Dictionary mapping file paths to their data

    Returns:
        Dict[str, Any]: Dictionary of mock functions and objects
    """
    import warnings

    warnings.warn(
        "create_mock_file_environment is deprecated. Use create_mock_functions instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    # Use factories to create default test data if not provided
    if file_data is None:
        file_data = {
            "test.json": SkypeDataFactory.build(),
            "complex.json": SkypeDataFactory.build(
                conversations=[
                    SkypeConversationFactory.build(with_message_count=5),
                    SkypeConversationFactory.build(with_message_count=3),
                ]
            ),
            "invalid.json": SkypeDataFactory.build(
                userId=None, exportDate="invalid-date", conversations=[]
            ),
            "test.tar": SkypeDataFactory.build(),
        }

    file_reader = MockFileReader()
    for path, data in file_data.items():
        file_reader.add_file(path, data)

    # Create validation service
    validation_service = MockValidationService(file_data)

    # Create patch functions
    mock_functions = {
        "read_file": patch(
            "src.utils.file_handler.read_file", side_effect=file_reader.read_file
        ),
        "read_tarfile": patch(
            "src.utils.file_handler.read_tarfile", side_effect=file_reader.read_tarfile
        ),
        "read_file_object": patch(
            "src.db.etl_pipeline.read_file_object",
            side_effect=file_reader.read_file_object,
        ),
        "path_exists": patch("os.path.exists", return_value=True),
        "path_isfile": patch("os.path.isfile", return_value=True),
        "access": patch("os.access", return_value=True),
        "open": patch(
            "builtins.open",
            new_callable=lambda: mock_open(
                read_data=json.dumps(SkypeDataFactory.build())
            ),
        ),
        "validation_service": validation_service,
    }

    return mock_functions


def create_mock_functions(
    file_data: Optional[Dict[str, Dict[str, Any]]] = None
) -> Dict[str, Any]:
    """
    Create a set of ready-to-use mock functions for file operations.

    This function returns actual callable functions rather than patch objects,
    making it easier to use in tests and reducing the need for context managers.

    Args:
        file_data: Dictionary mapping file paths to their data

    Returns:
        Dict[str, Any]: Dictionary of mock functions and objects including:
            - read_file: Function to read a file path and return data
            - read_tarfile: Function to read a tar file and return data
            - read_file_object: Function to read a file object and return data
            - validate_file_exists: Function to validate a file exists
            - validate_path_safety: Function to validate path safety
            - validate_json_file: Function to validate and parse a JSON file
            - validate_tar_file: Function to validate a tar file
            - validate_user_display_name: Function to validate a user display name
            - path_exists: Function to check if a path exists
            - path_isfile: Function to check if a path is a file
            - access: Function to check file access permissions
            - file_reader: MockFileReader instance for direct access
            - validation_service: MockValidationService instance
    """
    # Use factories to create default test data if not provided
    if file_data is None:
        file_data = {
            "test.json": SkypeDataFactory.build(),
            "complex.json": SkypeDataFactory.build(
                conversations=[
                    SkypeConversationFactory.build(with_message_count=5),
                    SkypeConversationFactory.build(with_message_count=3),
                ]
            ),
            "invalid.json": SkypeDataFactory.build(
                userId=None, exportDate="invalid-date", conversations=[]
            ),
            "test.tar": SkypeDataFactory.build(),
        }

    # Create file reader and add files
    file_reader = MockFileReader()
    for path, data in file_data.items():
        file_reader.add_file(path, data)

    # Create validation service
    validation_service = MockValidationService(file_data)

    # Configure validation service
    validation_service.file_exists_return_value = True

    # Create mock functions
    mock_functions = {
        # File reading functions
        "read_file": file_reader.read_file,
        "read_tarfile": file_reader.read_tarfile,
        "read_file_object": file_reader.read_file_object,
        # Validation functions
        "validate_file_exists": validation_service.validate_file_exists_mock,
        "validate_path_safety": lambda path, **kwargs: path,  # Just return the path unchanged
        "validate_json_file": validation_service.validate_json_file_mock,
        "validate_tar_file": lambda path, **kwargs: True,
        "validate_user_display_name": validation_service.validate_user_display_name_mock,
        # Path functions
        "path_exists": lambda path: True,
        "path_isfile": lambda path: True,
        "access": lambda path, mode: True,
        # Direct access to objects
        "file_reader": file_reader,
        "validation_service": validation_service,
    }

    return mock_functions


class MockValidationService(ValidationServiceProtocol):
    """
    Mock implementation of ValidationServiceProtocol for testing.

    This class provides mock implementations of validation methods that can be
    configured to return specific values or raise specific exceptions.
    """

    def __init__(self, file_data: Optional[Dict[str, Dict[str, Any]]] = None):
        """
        Initialize the mock validation service.

        Args:
            file_data: Dictionary mapping file paths to their data
        """
        self.file_data: Dict[str, Dict[str, Any]] = file_data or {}
        self.file_exists_return_value: bool = True
        self.user_display_name_return_value: str = "Test User"

        # Create mock functions for easier assertion in tests
        self.validate_file_exists_mock: MagicMock = MagicMock(
            side_effect=self.validate_file_exists
        )
        self.validate_json_file_mock: MagicMock = MagicMock(
            side_effect=self.validate_json_file
        )
        self.validate_user_display_name_mock: MagicMock = MagicMock(
            side_effect=self.validate_user_display_name
        )
        self.validate_file_object_mock: MagicMock = MagicMock(
            side_effect=self.validate_file_object
        )

    def validate_file_exists(
        self,
        path: str,
        base_dir: Optional[str] = None,
        allow_absolute: bool = False,
        allow_symlinks: bool = False,
    ) -> bool:
        """
        Mock implementation of validate_file_exists.

        Args:
            path: Path to validate
            base_dir: Base directory (ignored in mock)
            allow_absolute: Whether to allow absolute paths (ignored in mock)
            allow_symlinks: Whether to allow symlinks (ignored in mock)

        Returns:
            bool: The configured return value
        """
        return self.file_exists_return_value

    def validate_file_object(self, file_obj: BinaryIO) -> bool:
        """
        Mock implementation of validate_file_object.

        Args:
            file_obj: File object to validate

        Returns:
            bool: Always returns True
        """
        return True

    def validate_json_file(
        self,
        file_path: str,
        base_dir: Optional[str] = None,
        allow_absolute: bool = False,
        allow_symlinks: bool = False,
    ) -> Dict[str, Any]:
        """
        Mock implementation of validate_json_file.

        Args:
            file_path: Path to the JSON file
            base_dir: Base directory (ignored in mock)
            allow_absolute: Whether to allow absolute paths (ignored in mock)
            allow_symlinks: Whether to allow symlinks (ignored in mock)

        Returns:
            dict: The data for the file path or default data
        """
        return self.file_data.get(file_path, SkypeDataFactory.build())

    def validate_user_display_name(self, name: str) -> str:
        """
        Mock implementation of validate_user_display_name.

        Args:
            name: User display name to validate

        Returns:
            str: The configured return value
        """
        return self.user_display_name_return_value
