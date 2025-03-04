#!/usr/bin/env python3
"""
Tests for the validation module.

This module contains tests for the validation functions in the src.utils.validation module.
"""

import os
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch, mock_open

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.utils.validation import (
    ValidationError,
    validate_file_exists,
    validate_directory,
    validate_file_type,
    validate_json_file,
    validate_tar_file,
    validate_file_object,
    validate_skype_data,
    validate_user_display_name,
    validate_db_config,
    validate_config,
    validate_path_safety
)

class TestValidation(unittest.TestCase):
    """Test cases for the validation module."""

    def setUp(self):
        """Set up test fixtures."""
        # Create temporary files and directories for testing
        self.temp_dir = tempfile.TemporaryDirectory()
        self.temp_file_path = os.path.join(self.temp_dir.name, 'test.txt')
        with open(self.temp_file_path, 'w') as f:
            f.write('test')

        self.temp_json_path = os.path.join(self.temp_dir.name, 'test.json')
        with open(self.temp_json_path, 'w') as f:
            json.dump({'test': 'data'}, f)

        # Sample Skype data for testing
        self.valid_skype_data = {
            'userId': 'test_user',
            'exportDate': '2023-01-01T12:00:00Z',
            'conversations': [
                {
                    'id': 'conversation1',
                    'MessageList': [
                        {'id': 'msg1', 'from': 'user1', 'content': 'Hello'}
                    ]
                }
            ]
        }

        self.invalid_skype_data = {
            'userId': 'test_user',
            'exportDate': '2023-01-01T12:00:00Z',
            # Missing conversations field
        }

    def tearDown(self):
        """Tear down test fixtures."""
        self.temp_dir.cleanup()

    def test_validate_file_exists(self):
        """Test validate_file_exists function."""
        # Test with a valid file
        self.assertTrue(validate_file_exists(self.temp_file_path, allow_absolute=True))

        # Test with a non-existent file
        with self.assertRaises(ValidationError):
            validate_file_exists(os.path.join(self.temp_dir.name, 'nonexistent.txt'), allow_absolute=True)

        # Test with an empty path
        with self.assertRaises(ValidationError):
            validate_file_exists('')

        # Test with a directory
        with self.assertRaises(ValidationError):
            validate_file_exists(self.temp_dir.name, allow_absolute=True)

    def test_validate_directory(self):
        """Test validate_directory function."""
        # Test with a valid directory
        self.assertTrue(validate_directory(self.temp_dir.name, allow_absolute=True))

        # Test with a non-existent directory
        nonexistent_dir = os.path.join(self.temp_dir.name, 'nonexistent')
        with self.assertRaises(ValidationError):
            validate_directory(nonexistent_dir, allow_absolute=True)

        # Test with create_if_missing=True
        self.assertTrue(validate_directory(nonexistent_dir, create_if_missing=True, allow_absolute=True))
        self.assertTrue(os.path.exists(nonexistent_dir))
        self.assertTrue(os.path.isdir(nonexistent_dir))

        # Test with a file
        with self.assertRaises(ValidationError):
            validate_directory(self.temp_file_path, allow_absolute=True)

        # Test with an empty path
        with self.assertRaises(ValidationError):
            validate_directory('')

    def test_validate_file_type(self):
        """Test validate_file_type function."""
        # Test with a valid file type
        self.assertTrue(validate_file_type(self.temp_json_path, ['.json'], allow_absolute=True))

        # Test with an invalid file type
        with self.assertRaises(ValidationError):
            validate_file_type(self.temp_json_path, ['.txt', '.csv'], allow_absolute=True)

        # Test with an empty path
        with self.assertRaises(ValidationError):
            validate_file_type('', ['.json'])

    @patch('src.utils.validation.validate_file_exists')
    @patch('src.utils.validation.validate_file_type')
    def test_validate_json_file(self, mock_validate_file_type, mock_validate_file_exists):
        """Test validate_json_file function."""
        # Mock the return values
        mock_validate_file_exists.return_value = True
        mock_validate_file_type.return_value = True

        # Create a mock JSON file content
        mock_data = {'test': 'data'}
        mock_file = mock_open(read_data=json.dumps(mock_data))

        # Test with a valid JSON file
        with patch('builtins.open', mock_file):
            data = validate_json_file('test.json', allow_absolute=True)
            self.assertEqual(data, mock_data)

            # Check that validate_file_exists and validate_file_type were called with the right parameters
            mock_validate_file_exists.assert_called_once_with('test.json', base_dir=None, allow_absolute=True, allow_symlinks=False)
            mock_validate_file_type.assert_called_once_with('test.json', ['.json'], base_dir=None, allow_absolute=True, allow_symlinks=False)

    def test_validate_skype_data(self):
        """Test validate_skype_data function."""
        # Test with valid Skype data
        self.assertTrue(validate_skype_data(self.valid_skype_data))

        # Test with invalid Skype data (missing conversations)
        with self.assertRaises(ValidationError):
            validate_skype_data(self.invalid_skype_data)

        # Test with non-dict input
        with self.assertRaises(ValidationError):
            validate_skype_data('not a dict')

        # Test with empty dict
        with self.assertRaises(ValidationError):
            validate_skype_data({})

        # Test with invalid userId
        invalid_data = self.valid_skype_data.copy()
        invalid_data['userId'] = ''
        with self.assertRaises(ValidationError):
            validate_skype_data(invalid_data)

        # Test with invalid exportDate
        invalid_data = self.valid_skype_data.copy()
        invalid_data['exportDate'] = ''
        with self.assertRaises(ValidationError):
            validate_skype_data(invalid_data)

        # Test with invalid conversations (not a list)
        invalid_data = self.valid_skype_data.copy()
        invalid_data['conversations'] = 'not a list'
        with self.assertRaises(ValidationError):
            validate_skype_data(invalid_data)

        # Test with invalid conversation (missing id)
        invalid_data = self.valid_skype_data.copy()
        invalid_data['conversations'] = [{'MessageList': []}]
        with self.assertRaises(ValidationError):
            validate_skype_data(invalid_data)

        # Test with invalid conversation (missing MessageList)
        invalid_data = self.valid_skype_data.copy()
        invalid_data['conversations'] = [{'id': 'conv1'}]
        with self.assertRaises(ValidationError):
            validate_skype_data(invalid_data)

        # Test with invalid MessageList (not a list)
        invalid_data = self.valid_skype_data.copy()
        invalid_data['conversations'] = [{'id': 'conv1', 'MessageList': 'not a list'}]
        with self.assertRaises(ValidationError):
            validate_skype_data(invalid_data)

    def test_validate_user_display_name(self):
        """Test validate_user_display_name function."""
        # Test with valid name
        self.assertEqual(validate_user_display_name('Test User'), 'Test User')

        # Test with name containing invalid characters
        self.assertEqual(validate_user_display_name('Test/User:123'), 'Test_User_123')

        # Test with empty name
        with self.assertRaises(ValidationError):
            validate_user_display_name('')

        # Test with None
        with self.assertRaises(ValidationError):
            validate_user_display_name(None)

    def test_validate_db_config(self):
        """Test validate_db_config function."""
        # Test with valid config
        valid_config = {
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password',
            'host': 'localhost',
            'port': 5432
        }
        self.assertTrue(validate_db_config(valid_config))

        # Test with minimal valid config
        minimal_config = {
            'dbname': 'test_db',
            'user': 'test_user'
        }
        self.assertTrue(validate_db_config(minimal_config))

        # Test with missing required fields
        invalid_config = {
            'host': 'localhost',
            'port': 5432
        }
        with self.assertRaises(ValidationError):
            validate_db_config(invalid_config)

        # Test with empty dbname
        invalid_config = {
            'dbname': '',
            'user': 'test_user'
        }
        with self.assertRaises(ValidationError):
            validate_db_config(invalid_config)

        # Test with empty user
        invalid_config = {
            'dbname': 'test_db',
            'user': ''
        }
        with self.assertRaises(ValidationError):
            validate_db_config(invalid_config)

        # Test with invalid port (string that can't be converted to int)
        invalid_config = {
            'dbname': 'test_db',
            'user': 'test_user',
            'port': 'not_an_int'
        }
        with self.assertRaises(ValidationError):
            validate_db_config(invalid_config)

        # Test with invalid port (out of range)
        invalid_config = {
            'dbname': 'test_db',
            'user': 'test_user',
            'port': 70000
        }
        with self.assertRaises(ValidationError):
            validate_db_config(invalid_config)

        # Test with non-dict input
        with self.assertRaises(ValidationError):
            validate_db_config('not a dict')

    def test_validate_config(self):
        """Test validate_config function."""
        # Test with valid config
        valid_config = {
            'database': {
                'dbname': 'test_db',
                'user': 'test_user'
            },
            'output': {
                'directory': '/tmp/output',
                'overwrite': True
            },
            'logging': {
                'level': 'INFO',
                'file': '/tmp/log.txt'
            }
        }
        self.assertTrue(validate_config(valid_config))

        # Test with minimal valid config
        minimal_config = {}
        self.assertTrue(validate_config(minimal_config))

        # Test with invalid database config
        invalid_config = {
            'database': 'not a dict'
        }
        with self.assertRaises(ValidationError):
            validate_config(invalid_config)

        # Test with invalid output config
        invalid_config = {
            'output': 'not a dict'
        }
        with self.assertRaises(ValidationError):
            validate_config(invalid_config)

        # Test with invalid logging config
        invalid_config = {
            'logging': 'not a dict'
        }
        with self.assertRaises(ValidationError):
            validate_config(invalid_config)

        # Test with invalid logging level
        invalid_config = {
            'logging': {
                'level': 'INVALID_LEVEL'
            }
        }
        with self.assertRaises(ValidationError):
            validate_config(invalid_config)

        # Test with non-dict input
        with self.assertRaises(ValidationError):
            validate_config('not a dict')

    def test_validate_path_safety(self):
        """Test validate_path_safety function."""
        # Create a test directory structure
        base_dir = os.path.join(self.temp_dir.name, "base_dir")
        os.makedirs(base_dir, exist_ok=True)

        # Create a file in the base directory
        safe_file = os.path.join(base_dir, "safe_file.txt")
        with open(safe_file, "w") as f:
            f.write("test")

        # Create a subdirectory
        sub_dir = os.path.join(base_dir, "sub_dir")
        os.makedirs(sub_dir, exist_ok=True)

        # Create a file in the subdirectory
        sub_file = os.path.join(sub_dir, "sub_file.txt")
        with open(sub_file, "w") as f:
            f.write("test")

        # Test with a safe path (allowing absolute paths)
        result = validate_path_safety(safe_file, base_dir=base_dir, allow_absolute=True)
        # Use Path.resolve() for comparison to handle symlinks on macOS
        self.assertEqual(Path(result).resolve(), Path(safe_file).resolve())

        # Test with a safe path in a subdirectory (allowing absolute paths)
        result = validate_path_safety(sub_file, base_dir=base_dir, allow_absolute=True)
        # Use Path.resolve() for comparison to handle symlinks on macOS
        self.assertEqual(Path(result).resolve(), Path(sub_file).resolve())

        # Test with a relative path
        # First, change to the base directory
        original_dir = os.getcwd()
        try:
            os.chdir(base_dir)
            # Now use a relative path
            rel_path = "sub_dir/sub_file.txt"
            result = validate_path_safety(rel_path)
            # Use Path.resolve() for comparison to handle symlinks on macOS
            self.assertEqual(Path(result).resolve(), Path(os.path.join(base_dir, rel_path)).resolve())
        finally:
            # Change back to the original directory
            os.chdir(original_dir)

        # Test with a path traversal attempt
        traversal_path = os.path.join(base_dir, "sub_dir", "..", "..", "etc", "passwd")
        with self.assertRaises(ValidationError):
            validate_path_safety(traversal_path, base_dir=base_dir, allow_absolute=True)

        # Test with an absolute path when not allowed
        with self.assertRaises(ValidationError):
            validate_path_safety(safe_file)

        # Test with an absolute path when allowed
        result = validate_path_safety(safe_file, allow_absolute=True)
        # Use Path.resolve() for comparison to handle symlinks on macOS
        self.assertEqual(Path(result).resolve(), Path(safe_file).resolve())

        # Test with a path outside the base directory
        outside_file = os.path.join(self.temp_dir.name, "outside_file.txt")
        with open(outside_file, "w") as f:
            f.write("test")

        with self.assertRaises(ValidationError):
            validate_path_safety(outside_file, base_dir=base_dir, allow_absolute=True)

if __name__ == '__main__':
    unittest.main()