#!/usr/bin/env python3
"""
Tests for the file_utils module.

This module contains tests for the functionality in src.utils.file_utils.
"""

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.utils.file_utils import get_file_extension, is_json_file, is_tar_file


class TestFileUtils(unittest.TestCase):
    """Test cases for the file_utils module."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.TemporaryDirectory()

        # Create sample files
        self.json_path = os.path.join(self.temp_dir.name, "test.json")
        with open(self.json_path, "w") as f:
            f.write('{"test": "data"}')

        # Create a valid tar file
        import tarfile

        self.tar_path = os.path.join(self.temp_dir.name, "test.tar")
        with tarfile.open(self.tar_path, "w") as tar:
            # Add a file to the tar archive
            file_path = os.path.join(self.temp_dir.name, "file_for_tar.txt")
            with open(file_path, "w") as f:
                f.write("This file will be added to the tar archive")
            tar.add(file_path, arcname="file_for_tar.txt")

        self.txt_path = os.path.join(self.temp_dir.name, "test.txt")
        with open(self.txt_path, "w") as f:
            f.write("This is a text file")

        self.no_extension_path = os.path.join(self.temp_dir.name, "noextension")
        with open(self.no_extension_path, "w") as f:
            f.write("This file has no extension")

    def tearDown(self):
        """Clean up test fixtures."""
        self.temp_dir.cleanup()

    def test_get_file_extension(self):
        """Test get_file_extension function."""
        # Test with a JSON file
        extension = get_file_extension(self.json_path)
        self.assertEqual(extension, ".json")

        # Test with a TAR file
        extension = get_file_extension(self.tar_path)
        self.assertEqual(extension, ".tar")

        # Test with a TXT file
        extension = get_file_extension(self.txt_path)
        self.assertEqual(extension, ".txt")

        # Test with a file that has no extension
        extension = get_file_extension(self.no_extension_path)
        self.assertEqual(extension, "")

    def test_is_json_file(self):
        """Test is_json_file function."""
        # Test with a JSON file
        self.assertTrue(is_json_file(self.json_path))

        # Test with a non-JSON file
        self.assertFalse(is_json_file(self.tar_path))
        self.assertFalse(is_json_file(self.txt_path))
        self.assertFalse(is_json_file(self.no_extension_path))

    def test_is_tar_file(self):
        """Test is_tar_file function."""
        # Test with a TAR file
        self.assertTrue(is_tar_file(self.tar_path))

        # Test with a non-TAR file
        self.assertFalse(is_tar_file(self.json_path))
        self.assertFalse(is_tar_file(self.txt_path))
        self.assertFalse(is_tar_file(self.no_extension_path))


if __name__ == "__main__":
    unittest.main()
