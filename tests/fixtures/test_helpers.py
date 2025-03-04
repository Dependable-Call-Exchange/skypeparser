#!/usr/bin/env python3
"""
Test Helper Module

This module provides helper functions and utilities for writing tests in the SkypeParser project.
It standardizes approaches for handling path validation, mocking, and other common testing needs.
"""

import os
import tempfile
import unittest
from unittest.mock import patch


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
        try:
            shutil.rmtree(self.temp_dir)
        except Exception as e:
            print(f"Error removing temporary directory: {e}")

        try:
            shutil.rmtree(self.test_dir)
        except Exception as e:
            print(f"Error removing test directory: {e}")


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
        data (dict): Data to write to the file as JSON

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
        files_to_add (dict): Dictionary mapping filenames to content

    Returns:
        str: Path to the created TAR file
    """
    import tarfile
    import io

    os.makedirs(directory, exist_ok=True)
    tar_path = os.path.join(directory, filename)

    with tarfile.open(tar_path, 'w') as tar:
        for file_name, content in files_to_add.items():
            # Create a file-like object from the content
            file_data = io.BytesIO(content.encode('utf-8') if isinstance(content, str) else content)

            # Create a TarInfo object for the file
            info = tarfile.TarInfo(name=file_name)
            file_data.seek(0, os.SEEK_END)
            info.size = file_data.tell()
            file_data.seek(0)

            # Add the file to the tar archive
            tar.addfile(info, file_data)

    return tar_path


def patch_validation(test_method):
    """
    Decorator to patch validation functions for testing.

    This decorator patches the validate_path_safety and validate_file_exists functions
    to allow tests to run without actually validating paths.
    """
    @patch('src.utils.validation.validate_path_safety')
    def wrapper(self, mock_validate_path, *args, **kwargs):
        # Configure the mock to return the path, handling both path and kwargs parameters
        mock_validate_path.side_effect = lambda path, **kwargs: path

        # Call the original test method
        return test_method(self, mock_validate_path, *args, **kwargs)

    return wrapper


def mock_sys_exit(test_method):
    """
    Decorator to patch sys.exit for testing.

    This decorator patches sys.exit to prevent tests from actually exiting
    when code under test calls sys.exit.
    """
    @patch('sys.exit')
    def wrapper(self, mock_exit, *args, **kwargs):
        # Call the original test method
        return test_method(self, mock_exit, *args, **kwargs)

    return wrapper