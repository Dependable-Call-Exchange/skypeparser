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
    validate_config
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
        # Test with existing file
        self.assertTrue(validate_file_exists(self.temp_file_path))

        # Test with non-existent file
        with self.assertRaises(ValidationError):
            validate_file_exists(os.path.join(self.temp_dir.name, 'nonexistent.txt'))

        # Test with empty path
        with self.assertRaises(ValidationError):
            validate_file_exists('')

        # Test with directory instead of file
        with self.assertRaises(ValidationError):
            validate_file_exists(self.temp_dir.name)

    def test_validate_directory(self):
        """Test validate_directory function."""
        # Test with existing directory
        self.assertTrue(validate_directory(self.temp_dir.name))

        # Test with non-existent directory
        non_existent_dir = os.path.join(self.temp_dir.name, 'nonexistent')
        with self.assertRaises(ValidationError):
            validate_directory(non_existent_dir)

        # Test with create_if_missing=True
        self.assertTrue(validate_directory(non_existent_dir, create_if_missing=True))
        self.assertTrue(os.path.exists(non_existent_dir))

        # Test with file instead of directory
        with self.assertRaises(ValidationError):
            validate_directory(self.temp_file_path)

        # Test with empty path
        with self.assertRaises(ValidationError):
            validate_directory('')

    def test_validate_file_type(self):
        """Test validate_file_type function."""
        # Test with correct file type
        self.assertTrue(validate_file_type(self.temp_json_path, ['.json']))

        # Test with incorrect file type
        with self.assertRaises(ValidationError):
            validate_file_type(self.temp_json_path, ['.txt', '.csv'])

        # Test with empty path
        with self.assertRaises(ValidationError):
            validate_file_type('', ['.json'])

    @patch('src.utils.validation.validate_file_exists')
    @patch('src.utils.validation.validate_file_type')
    def test_validate_json_file(self, mock_validate_file_type, mock_validate_file_exists):
        """Test validate_json_file function."""
        # Mock the validation functions to avoid file system operations
        mock_validate_file_exists.return_value = True
        mock_validate_file_type.return_value = True

        # Test with valid JSON file
        with patch('builtins.open', mock_open(read_data='{"test": "data"}')):
            result = validate_json_file('test.json')
            self.assertEqual(result, {'test': 'data'})

        # Test with invalid JSON file
        with patch('builtins.open', mock_open(read_data='invalid json')):
            with self.assertRaises(ValidationError):
                validate_json_file('test.json')

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

if __name__ == '__main__':
    unittest.main()