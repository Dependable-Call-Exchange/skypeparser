#!/usr/bin/env python3
"""
Example demonstrating the use of create_mock_functions for testing.

This example shows how to use the new create_mock_functions approach to create
mock functions for testing without using patch decorators or context managers.
"""

import logging
import os
import sys
from typing import Any, Dict

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tests.factories import SkypeConversationFactory, SkypeDataFactory
from tests.fixtures.mock_fixtures import create_mock_functions

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def demonstrate_basic_usage():
    """
    Demonstrate basic usage of create_mock_functions.
    """
    logger.info("=== Basic Usage Example ===")

    # Create mock functions with default data
    mocks = create_mock_functions()

    # Use the mock functions directly
    logger.info("Reading test.json file...")
    data = mocks["read_file"]("test.json")
    logger.info(f"Read data with user ID: {data.get('userId')}")

    # Validate a file path
    logger.info("Validating file path...")
    result = mocks["validate_file_exists"]("test.json")
    logger.info(f"File validation result: {result}")

    # Access the validation service directly
    logger.info("Accessing validation service directly...")
    validation_service = mocks["validation_service"]
    logger.info(
        f"User display name: {validation_service.validate_user_display_name('Test User')}"
    )


def demonstrate_custom_data():
    """
    Demonstrate using create_mock_functions with custom data.
    """
    logger.info("\n=== Custom Data Example ===")

    # Create custom test data
    custom_data = {
        "custom.json": SkypeDataFactory.build(
            userId="custom-user",
            exportDate="2023-01-01",
            conversations=[
                SkypeConversationFactory.build(
                    id="custom-conversation",
                    displayName="Custom Conversation",
                    with_message_count=2,
                )
            ],
        )
    }

    # Create mock functions with custom data
    mocks = create_mock_functions(custom_data)

    # Use the mock functions with custom data
    logger.info("Reading custom.json file...")
    data = mocks["read_file"]("custom.json")
    logger.info(f"Read data with user ID: {data.get('userId')}")
    logger.info(f"Found {len(data.get('conversations', []))} conversations")

    # Try reading a non-existent file (will return default data)
    logger.info("Reading non-existent file...")
    data = mocks["read_file"]("nonexistent.json")
    logger.info(f"Default data returned with user ID: {data.get('userId')}")


def demonstrate_in_test_function():
    """
    Demonstrate how to use create_mock_functions in a test function.
    """
    logger.info("\n=== Test Function Example ===")

    # This would be your test function
    def test_process_skype_data():
        # Create mock functions
        mocks = create_mock_functions()

        # Replace real functions with mocks in the module under test
        # This is a simplified example - in a real test, you'd use monkeypatch or similar
        import src.utils.file_handler

        original_read_file = src.utils.file_handler.read_file
        src.utils.file_handler.read_file = mocks["read_file"]

        try:
            # Call the function under test
            logger.info("Calling function under test...")

            # This is where you'd call your actual function
            # For this example, we'll just use the mock directly
            data = src.utils.file_handler.read_file("test.json")
            logger.info(f"Function returned data with user ID: {data.get('userId')}")

            # Make assertions
            assert data.get("userId") is not None, "User ID should not be None"
            logger.info("Assertion passed: User ID is not None")

        finally:
            # Restore original function
            src.utils.file_handler.read_file = original_read_file

    # Run the test function
    test_process_skype_data()


def demonstrate_with_pytest_monkeypatch():
    """
    Demonstrate how create_mock_functions would be used with pytest's monkeypatch.
    """
    logger.info("\n=== Pytest Monkeypatch Example ===")
    logger.info("In a real pytest test, you would use monkeypatch like this:")

    code_example = """
    def test_process_skype_data(monkeypatch):
        # Create mock functions
        mocks = create_mock_functions()

        # Use monkeypatch to replace real functions with mocks
        monkeypatch.setattr("src.utils.file_handler.read_file", mocks["read_file"])
        monkeypatch.setattr("src.utils.file_handler.read_tarfile", mocks["read_tarfile"])
        monkeypatch.setattr("os.path.exists", mocks["path_exists"])

        # Call the function under test
        result = process_skype_data("test.json")

        # Make assertions
        assert result.get('userId') is not None
        assert len(result.get('conversations', [])) > 0
    """

    logger.info(code_example)


if __name__ == "__main__":
    # Run all examples
    demonstrate_basic_usage()
    demonstrate_custom_data()
    demonstrate_in_test_function()
    demonstrate_with_pytest_monkeypatch()

    logger.info("\nAll examples completed successfully!")
