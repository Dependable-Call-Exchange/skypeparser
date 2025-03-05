#!/usr/bin/env python3
"""
Tests for the skype_parser module.

This module contains tests for the functionality in src.parser.skype_parser.
"""

import os
import json
import tempfile
import unittest
import argparse
from pathlib import Path
from unittest.mock import patch, mock_open, MagicMock

import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.parser.skype_parser import (
    main,
    get_commandline_args
)
from src.parser.exceptions import (
    FileOperationError,
    DataExtractionError,
    ExportError,
    InvalidInputError
)


class TestSkypeParser(unittest.TestCase):
    """Test cases for the skype_parser module."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()

        # Sample Skype export data for testing
        self.sample_skype_data = {
            "userId": "test_user",
            "userDisplayName": "Test User",
            "exportDate": "2023-01-01T12:00:00Z",
            "conversations": {
                "conversation1": {
                    "id": "conversation1",
                    "displayName": "Test Conversation",
                    "messageCount": 1,
                    "firstMessageTime": "2023-01-01T12:00:00Z",
                    "lastMessageTime": "2023-01-01T12:00:00Z",
                    "messages": [
                        {
                            "timestamp": "2023-01-01T12:00:00Z",
                            "timestampFormatted": "2023-01-01 12:00:00",
                            "date": "2023-01-01",
                            "time": "12:00:00",
                            "fromId": "user1",
                            "fromName": "Test User",
                            "type": "RichText",
                            "rawContent": "Hello, world!",
                            "isEdited": False
                        }
                    ]
                }
            }
        }

        # Create a sample JSON file
        self.sample_json_path = os.path.join(self.temp_dir, 'sample.json')
        with open(self.sample_json_path, 'w') as f:
            json.dump(self.sample_skype_data, f)

        # Create a sample TAR file path (we won't actually create the file)
        self.sample_tar_path = os.path.join(self.temp_dir, 'sample.tar')

    def tearDown(self):
        """Clean up test fixtures."""
        import shutil
        shutil.rmtree(self.temp_dir)

    @patch('argparse.ArgumentParser.parse_args')
    @patch('src.parser.skype_parser.read_file')
    @patch('src.parser.skype_parser.parse_skype_data')
    @patch('src.parser.skype_parser.export_conversations')
    def test_main_with_json_file(self, mock_export, mock_parse, mock_read, mock_args):
        """Test main function with a JSON file."""
        # Mock the command line arguments
        mock_args.return_value = argparse.Namespace(
            input_file=self.sample_json_path,
            output_dir=self.temp_dir,
            format='json',
            extract_tar=False,
            store_db=False,
            db_name=None,
            db_user=None,
            db_password=None,
            db_host=None,
            db_port=None,
            username=None,
            verbose=False
        )

        # Mock the read_file function to return our sample data
        mock_read.return_value = self.sample_skype_data

        # Mock the parse_skype_data function to return structured data
        structured_data = {
            "metadata": {
                "userId": "test_user",
                "userDisplayName": "Test User",
                "exportDate": "2023-01-01T12:00:00Z"
            },
            "conversations": {
                "conversation1": {
                    "id": "conversation1",
                    "displayName": "Test Conversation",
                    "messages": []
                }
            }
        }
        mock_parse.return_value = structured_data

        # Mock the export_conversations function to return True
        mock_export.return_value = True

        # Call the main function
        with patch('sys.argv', ['skype_parser.py', self.sample_json_path, '-o', self.temp_dir, '-f', 'json']):
            main()

        # Verify that the functions were called with the correct arguments
        mock_read.assert_called_once_with(self.sample_json_path)
        mock_parse.assert_called_once_with(self.sample_skype_data, None)
        mock_export.assert_called_once_with(structured_data, 'json', self.temp_dir)

    @patch('argparse.ArgumentParser.parse_args')
    @patch('src.parser.skype_parser.read_tarfile')
    @patch('src.parser.skype_parser.parse_skype_data')
    @patch('src.parser.skype_parser.export_conversations')
    def test_main_with_tar_file(self, mock_export, mock_parse, mock_read_tar, mock_args):
        """Test main function with a TAR file."""
        # Mock the command line arguments
        mock_args.return_value = argparse.Namespace(
            input_file=self.sample_tar_path,
            output_dir=self.temp_dir,
            format='json',
            extract_tar=True,
            store_db=False,
            db_name=None,
            db_user=None,
            db_password=None,
            db_host=None,
            db_port=None,
            username=None,
            verbose=False
        )

        # Mock the read_tarfile function to return our sample data
        mock_read_tar.return_value = self.sample_skype_data

        # Mock the parse_skype_data function to return structured data
        structured_data = {
            "metadata": {
                "userId": "test_user",
                "userDisplayName": "Test User",
                "exportDate": "2023-01-01T12:00:00Z"
            },
            "conversations": {
                "conversation1": {
                    "id": "conversation1",
                    "displayName": "Test Conversation",
                    "messages": []
                }
            }
        }
        mock_parse.return_value = structured_data

        # Mock the export_conversations function to return True
        mock_export.return_value = True

        # Call the main function
        with patch('sys.argv', ['skype_parser.py', self.sample_tar_path, '-t', '-o', self.temp_dir, '-f', 'json']):
            main()

        # Verify that the functions were called with the correct arguments
        mock_read_tar.assert_called_once_with(self.sample_tar_path)
        mock_parse.assert_called_once_with(self.sample_skype_data, None)
        mock_export.assert_called_once_with(structured_data, 'json', self.temp_dir)

    @patch('argparse.ArgumentParser.parse_args')
    @patch('src.parser.skype_parser.SkypeETLPipeline')
    @patch('src.parser.skype_parser.read_file')
    @patch('src.parser.skype_parser.parse_skype_data')
    def test_main_with_db_storage(self, mock_parse, mock_read, mock_etl, mock_args):
        """Test main function with database storage."""
        # Mock the command line arguments
        mock_args.return_value = argparse.Namespace(
            input_file=self.sample_json_path,
            output_dir=None,
            format=None,
            extract_tar=False,
            store_db=True,
            db_name='test_db',
            db_user='test_user',
            db_password='test_password',
            db_host='localhost',
            db_port=5432,
            username=None,
            verbose=False
        )

        # Mock the read_file function to return our sample data
        mock_read.return_value = self.sample_skype_data

        # Mock the parse_skype_data function to return structured data
        structured_data = {
            "metadata": {
                "userId": "test_user",
                "userDisplayName": "Test User",
                "exportDate": "2023-01-01T12:00:00Z"
            },
            "conversations": {
                "conversation1": {
                    "id": "conversation1",
                    "displayName": "Test Conversation",
                    "messages": []
                }
            }
        }
        mock_parse.return_value = structured_data

        # Mock the ETL pipeline
        mock_etl_instance = MagicMock()
        mock_etl.return_value = mock_etl_instance

        # Set ETL_AVAILABLE to True for this test
        with patch('src.parser.skype_parser.ETL_AVAILABLE', True):
            # Call the main function
            with patch('sys.argv', ['skype_parser.py', self.sample_json_path, '--store-db', '--db-name', 'test_db', '--db-user', 'test_user']):
                main()

        # Verify that the functions were called with the correct arguments
        mock_read.assert_called_once_with(self.sample_json_path)
        mock_parse.assert_called_once_with(self.sample_skype_data, None)
        mock_etl.assert_called_once()
        mock_etl_instance.process.assert_called_once_with(structured_data)

    def test_get_commandline_args(self):
        """Test get_commandline_args function."""
        # Test with minimal arguments
        with patch('sys.argv', ['skype_parser.py', 'input.json']):
            args = get_commandline_args()
            self.assertEqual(args.input_file, 'input.json')
            self.assertFalse(args.extract_tar)
            self.assertIsNone(args.output_dir)
            self.assertEqual(args.format, 'all')

        # Test with more arguments
        with patch('sys.argv', ['skype_parser.py', 'input.tar', '-t', '-o', 'output_dir', '-f', 'json']):
            args = get_commandline_args()
            self.assertEqual(args.input_file, 'input.tar')
            self.assertTrue(args.extract_tar)
            self.assertEqual(args.output_dir, 'output_dir')
            self.assertEqual(args.format, 'json')

        # Test with database arguments
        with patch('sys.argv', ['skype_parser.py', 'input.json', '--store-db', '--db-name', 'test_db', '--db-user', 'test_user']):
            args = get_commandline_args()
            self.assertEqual(args.input_file, 'input.json')
            self.assertTrue(args.store_db)
            self.assertEqual(args.db_name, 'test_db')
            self.assertEqual(args.db_user, 'test_user')


if __name__ == '__main__':
    unittest.main()