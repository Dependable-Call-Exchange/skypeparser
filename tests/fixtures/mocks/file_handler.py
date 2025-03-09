"""
Mock implementation of FileHandler for testing.

This module provides a mock implementation of the FileHandler class
for use in unit tests.
"""

from typing import Any, BinaryIO, Dict, Optional


class MockFileHandler:
    """
    Mock implementation of FileHandler for testing.

    This class provides a mock implementation of the FileHandler class
    with configurable return values for testing different scenarios.
    """

    def __init__(
        self,
        read_file_return=None,
        read_tarfile_return=None,
        read_file_obj_return=None,
        validate_file_exists_return=True
    ):
        """
        Initialize the mock file handler.

        Args:
            read_file_return: Value to return from read_file method
            read_tarfile_return: Value to return from read_tarfile method
            read_file_obj_return: Value to return from read_file_obj method
            validate_file_exists_return: Value to return from validate_file_exists method
        """
        self.read_file_return = read_file_return or {"data": "mock_data"}
        self.read_tarfile_return = read_tarfile_return or {"data": "mock_tar_data"}
        self.read_file_obj_return = read_file_obj_return or self.read_file_return
        self.validate_file_exists_return = validate_file_exists_return

        # Track method calls for verification in tests
        self.read_file_calls = []
        self.read_tarfile_calls = []
        self.read_file_obj_calls = []
        self.validate_file_exists_calls = []

    def read_file(self, file_path: str) -> Dict[str, Any]:
        """
        Mock implementation of read_file.

        Args:
            file_path: Path to the file

        Returns:
            Dict with mock data
        """
        self.read_file_calls.append(file_path)
        return self.read_file_return

    def read_tarfile(self, file_path: str, auto_select: bool = False) -> Dict[str, Any]:
        """
        Mock implementation of read_tarfile.

        Args:
            file_path: Path to the tar file
            auto_select: Whether to automatically select the main data file

        Returns:
            Dict with mock data
        """
        self.read_tarfile_calls.append((file_path, auto_select))
        return self.read_tarfile_return

    def read_file_obj(
        self, file_obj: BinaryIO, file_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Mock implementation of read_file_obj.

        Args:
            file_obj: File-like object to read
            file_name: Optional name of the file for logging and type detection

        Returns:
            Dict with mock data
        """
        self.read_file_obj_calls.append((file_obj, file_name))
        return self.read_file_obj_return

    def validate_file_exists(self, file_path: str) -> bool:
        """
        Mock implementation of validate_file_exists.

        Args:
            file_path: Path to the file

        Returns:
            True if the file exists, False otherwise
        """
        self.validate_file_exists_calls.append(file_path)
        return self.validate_file_exists_return