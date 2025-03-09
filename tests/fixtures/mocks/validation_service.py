"""
Mock implementation of validation service for testing.

This module provides a mock implementation of the validation service
for use in unit tests.
"""

from typing import Any, Dict, Optional


class MockValidationService:
    """
    Mock validation service for testing.

    This class provides a mock implementation of the validation service
    with configurable behavior for testing different scenarios.
    """

    def __init__(
        self,
        validate_file_exists_return=True,
        validate_file_format_return=True,
        validate_user_display_name_return=True,
        validate_data_structure_return=True,
        validate_transformed_data_return=True
    ):
        """
        Initialize the mock validation service.

        Args:
            validate_file_exists_return: Value to return from validate_file_exists
            validate_file_format_return: Value to return from validate_file_format
            validate_user_display_name_return: Value to return from validate_user_display_name
            validate_data_structure_return: Value to return from validate_data_structure
            validate_transformed_data_return: Value to return from validate_transformed_data
        """
        self.validate_file_exists_return = validate_file_exists_return
        self.validate_file_format_return = validate_file_format_return
        self.validate_user_display_name_return = validate_user_display_name_return
        self.validate_data_structure_return = validate_data_structure_return
        self.validate_transformed_data_return = validate_transformed_data_return

        # Track method calls for verification in tests
        self.validate_file_exists_calls = []
        self.validate_file_format_calls = []
        self.validate_user_display_name_calls = []
        self.validate_data_structure_calls = []
        self.validate_transformed_data_calls = []

    def validate_file_exists(self, file_path: str) -> bool:
        """
        Validate that a file exists.

        Args:
            file_path: Path to the file

        Returns:
            True if the file exists, False otherwise
        """
        self.validate_file_exists_calls.append(file_path)
        return self.validate_file_exists_return

    def validate_file_format(self, file_path: str) -> bool:
        """
        Validate that a file has the correct format.

        Args:
            file_path: Path to the file

        Returns:
            True if the file has the correct format, False otherwise
        """
        self.validate_file_format_calls.append(file_path)
        return self.validate_file_format_return

    def validate_user_display_name(self, user_display_name: Optional[str]) -> bool:
        """
        Validate that a user display name is valid.

        Args:
            user_display_name: Display name of the user

        Returns:
            True if the user display name is valid, False otherwise
        """
        self.validate_user_display_name_calls.append(user_display_name)
        return self.validate_user_display_name_return

    def validate_data_structure(self, data: Dict[str, Any]) -> bool:
        """
        Validate that data has the correct structure.

        Args:
            data: Data to validate

        Returns:
            True if the data has the correct structure, False otherwise
        """
        self.validate_data_structure_calls.append(data)
        return self.validate_data_structure_return

    def validate_transformed_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate that transformed data has the correct structure.

        Args:
            data: Transformed data to validate

        Returns:
            True if the transformed data has the correct structure, False otherwise
        """
        self.validate_transformed_data_calls.append(data)
        return self.validate_transformed_data_return