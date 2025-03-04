#!/usr/bin/env python3
"""
Test Helper Module

This module provides helper functions and utilities for writing tests in the SkypeParser project.
It standardizes approaches for handling path validation, mocking, and other common testing needs.
"""

import os
import tempfile
import unittest
from unittest.mock import patch, MagicMock
from pathlib import Path


class TestBase(unittest.TestCase):
    """Base class for SkypeParser tests with common setup and teardown methods."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory
        self.temp_dir = tempfile.mkdtemp()

        # Create a test directory within the tests directory to avoid absolute path issues
        self.test_dir = os.path.join(os.path.dirname(__file__), 'test_data')
        os.makedirs(self.test_dir, exist_ok=True)

    def tearDown(self):
        """Tear down test fixtures."""
        # Remove temporary directory and files
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

        # Remove test files
        if os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)


def create_test_file(directory, filename, content):
    """
    Create a test file with the given content.

    Args:
        directory (str): Directory to create the file in
        filename (str): Name of the file to create
        content (str): Content to write to the file

    Returns:
        str: Path to the created file
    """
    os.makedirs(directory, exist_ok=True)
    file_path = os.path.join(directory, filename)
    with open(file_path, 'w') as f:
        f.write(content)
    return file_path


def create_test_json_file(directory, filename, data):
    """
    Create a test JSON file with the given data.

    Args:
        directory (str): Directory to create the file in
        filename (str): Name of the file to create
        data (dict): Data to write to the file

    Returns:
        str: Path to the created file
    """
    import json
    os.makedirs(directory, exist_ok=True)
    file_path = os.path.join(directory, filename)
    with open(file_path, 'w') as f:
        json.dump(data, f)
    return file_path


def create_test_tar_file(directory, filename, files_to_add):
    """
    Create a test TAR file with the given files.

    Args:
        directory (str): Directory to create the file in
        filename (str): Name of the file to create
        files_to_add (dict): Dictionary mapping file names to content

    Returns:
        str: Path to the created TAR file
    """
    import tarfile
    os.makedirs(directory, exist_ok=True)
    tar_path = os.path.join(directory, filename)

    with tarfile.open(tar_path, 'w') as tar:
        for file_name, content in files_to_add.items():
            # Create a temporary file
            temp_file = os.path.join(directory, os.path.basename(file_name))
            with open(temp_file, 'w') as f:
                f.write(content)

            # Add the file to the TAR
            tar.add(temp_file, arcname=file_name)

            # Remove the temporary file
            os.unlink(temp_file)

    return tar_path


def patch_validation(test_method):
    """
    Decorator to patch the validation functions for testing.

    This decorator patches the validate_path_safety function to allow absolute paths in tests.

    Args:
        test_method (function): The test method to decorate

    Returns:
        function: The decorated test method
    """
    @patch('src.utils.validation.validate_path_safety')
    def wrapper(self, mock_validate_path, *args, **kwargs):
        # Configure the mock to return the path
        mock_validate_path.side_effect = lambda path, *args, **kwargs: path

        # Call the original test method
        return test_method(self, mock_validate_path, *args, **kwargs)

    return wrapper


def mock_sys_exit(test_method):
    """
    Decorator to mock sys.exit for testing.

    This decorator patches sys.exit to prevent tests from exiting prematurely.

    Args:
        test_method (function): The test method to decorate

    Returns:
        function: The decorated test method
    """
    @patch('sys.exit')
    def wrapper(self, mock_exit, *args, **kwargs):
        # Call the original test method
        return test_method(self, mock_exit, *args, **kwargs)

    return wrapper