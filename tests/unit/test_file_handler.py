#!/usr/bin/env python3
"""
Tests for the file_handler.py module.
"""

import os
import json
import unittest
from unittest.mock import patch
from pathlib import Path

# Add the parent directory to the path so we can import from src
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.utils.file_handler import (
    read_file,
    read_file_object,
    read_tarfile,
    read_tarfile_object,
    extract_tar_contents,
    list_tar_contents
)
from src.utils.validation import ValidationError
from tests.fixtures import TestBase, patch_validation, create_test_file, create_test_json_file, create_test_tar_file


class TestFileHandler(TestBase):
    """Test the file_handler.py module."""

    def setUp(self):
        """Set up test fixtures."""
        # Call the parent setUp method
        super().setUp()

        # Create a sample JSON file
        self.json_data = {"test": "data"}
        self.json_file = create_test_json_file(self.test_dir, "test.json", self.json_data)

        # Create a sample tar file with multiple JSON files
        self.tar_files = {
            "file1.json": json.dumps({"file": "1"}),
            "file2.json": json.dumps({"file": "2"}),
            "file.txt": "test"
        }
        self.tar_file = create_test_tar_file(self.test_dir, "test.tar", self.tar_files)

    @patch_validation
    def test_read_file(self, mock_validate_path):
        """Test read_file function."""
        # Test with valid file
        data = read_file(self.json_file)
        self.assertEqual(data, self.json_data)

        # Test with non-existent file
        mock_validate_path.side_effect = ValidationError("File does not exist")
        with self.assertRaises(ValidationError):
            read_file(os.path.join(self.test_dir, "nonexistent.json"))

        # Reset the mock
        mock_validate_path.reset_mock()
        mock_validate_path.side_effect = lambda path, *args, **kwargs: path

        # Test with non-JSON file
        txt_file = create_test_file(self.test_dir, "test.txt", "test")

        with self.assertRaises(ValidationError):
            read_file(txt_file)

    def test_read_file_object(self):
        """Test read_file_object function."""
        # Test with valid file object
        with open(self.json_file, "rb") as f:
            data = read_file_object(f)
            self.assertEqual(data, self.json_data)

        # Test with invalid file object
        with self.assertRaises(ValidationError):
            read_file_object(None)

    @patch_validation
    def test_read_tarfile_auto_select(self, mock_validate_path):
        """Test read_tarfile function with auto_select=True."""
        # Test with auto_select=True
        data = read_tarfile(self.tar_file, auto_select=True)
        self.assertEqual(data, {"file": "1"})  # Should select the first file

    @patch_validation
    def test_read_tarfile_select_json(self, mock_validate_path):
        """Test read_tarfile function with select_json parameter."""
        # Test with select_json=1
        data = read_tarfile(self.tar_file, select_json=1)
        self.assertEqual(data, {"file": "2"})  # Should select the second file

    @patch_validation
    def test_read_tarfile_no_selection(self, mock_validate_path):
        """Test read_tarfile function with no selection."""
        # Test with auto_select=False and no select_json
        with self.assertRaises(ValueError):
            read_tarfile(self.tar_file, auto_select=False)

    def test_read_tarfile_object(self):
        """Test read_tarfile_object function."""
        # Create a mock for the read_tarfile function to avoid the validation issue
        with patch('src.utils.file_handler.read_tarfile') as mock_read_tarfile:
            # Configure the mock to return a known value
            mock_read_tarfile.return_value = {"file": "1"}

            # Test with valid file object
            with open(self.tar_file, "rb") as f:
                data = read_tarfile_object(f, auto_select=True)
                self.assertEqual(data, {"file": "1"})

                # Verify that read_tarfile was called with the right parameters
                mock_read_tarfile.assert_called_once()
                args, kwargs = mock_read_tarfile.call_args
                # The third positional argument should be auto_select=True
                self.assertEqual(args[2], True)

        # Test with invalid file object
        with self.assertRaises(ValidationError):
            read_tarfile_object(None)

    @patch_validation
    def test_extract_tar_contents(self, mock_validate_path):
        """Test extract_tar_contents function."""
        # Test with valid tar file
        output_dir = os.path.join(self.test_dir, "output")
        os.makedirs(output_dir, exist_ok=True)

        files = extract_tar_contents(self.tar_file, output_dir)
        self.assertEqual(len(files), 3)
        self.assertTrue(os.path.exists(os.path.join(output_dir, "file1.json")))
        self.assertTrue(os.path.exists(os.path.join(output_dir, "file2.json")))
        self.assertTrue(os.path.exists(os.path.join(output_dir, "file.txt")))

        # Test with file pattern
        files = extract_tar_contents(self.tar_file, output_dir, file_pattern=r".*\.json")
        self.assertEqual(len(files), 2)

        # Test with non-existent file
        mock_validate_path.side_effect = ValidationError("File does not exist")
        with self.assertRaises(ValidationError):
            extract_tar_contents(os.path.join(self.test_dir, "nonexistent.tar"), output_dir)

    @patch_validation
    def test_list_tar_contents(self, mock_validate_path):
        """Test list_tar_contents function."""
        # Test with valid tar file
        files = list_tar_contents(self.tar_file)
        self.assertEqual(len(files), 3)
        self.assertIn("file1.json", files)
        self.assertIn("file2.json", files)
        self.assertIn("file.txt", files)

        # Test with file pattern
        files = list_tar_contents(self.tar_file, file_pattern=r".*\.json")
        self.assertEqual(len(files), 2)
        self.assertIn("file1.json", files)
        self.assertIn("file2.json", files)

        # Test with non-existent file
        mock_validate_path.side_effect = ValidationError("File does not exist")
        with self.assertRaises(ValidationError):
            list_tar_contents(os.path.join(self.test_dir, "nonexistent.tar"))


if __name__ == "__main__":
    unittest.main()