#!/usr/bin/env python3
"""
Tests for the tar_extractor module.

This module contains tests for the functionality in src.utils.tar_extractor.
"""

import os
import sys
import unittest
import tempfile
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add the parent directory to the path so we can import from src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import the functions from tar_extractor
from src.utils.tar_extractor import main, parse_args


class TestTarExtractor(unittest.TestCase):
    """Test cases for the tar_extractor module."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.TemporaryDirectory()
        self.output_dir = os.path.join(self.temp_dir.name, 'output')
        os.makedirs(self.output_dir, exist_ok=True)

        # Create a test tar file to avoid validation errors with absolute paths
        self.tar_path = os.path.join(os.path.dirname(__file__), 'test_data', 'test.tar')
        os.makedirs(os.path.dirname(self.tar_path), exist_ok=True)
        if not os.path.exists(self.tar_path):
            import tarfile
            with tarfile.open(self.tar_path, 'w') as tar:
                # Create a test file
                test_file = os.path.join(self.temp_dir.name, 'test.txt')
                with open(test_file, 'w') as f:
                    f.write('Test content')
                tar.add(test_file, arcname='test.txt')
                os.unlink(test_file)  # Remove the file after adding to tar

    def tearDown(self):
        """Clean up test fixtures."""
        self.temp_dir.cleanup()
        # Clean up the test tar file if it exists
        if os.path.exists(self.tar_path):
            os.unlink(self.tar_path)
        if os.path.exists(os.path.dirname(self.tar_path)):
            os.rmdir(os.path.dirname(self.tar_path))

    @patch('src.utils.tar_extractor.list_tar_contents')
    def test_list_command(self, mock_list_tar_contents):
        """Test the list command."""
        # Mock the list_tar_contents function
        mock_list_tar_contents.return_value = [
            'file1.txt',
            'file2.json',
            'nested/file3.txt'
        ]

        # Mock sys.argv
        with patch('sys.argv', ['tar_extractor.py', self.tar_path, '-l']):
            # Mock logger to capture output
            with patch('src.utils.tar_extractor.logger') as mock_logger:
                # Mock sys.exit to prevent the test from exiting
                with patch('sys.exit'):
                    # Call the main function
                    main()

                # Check that list_tar_contents was called with the correct arguments
                mock_list_tar_contents.assert_called_once_with(self.tar_path, None)

                # Check that the logger was called with the expected messages
                mock_logger.info.assert_any_call(f"Contents of {self.tar_path}:")
                for i, item in enumerate(['file1.txt', 'file2.json', 'nested/file3.txt'], 1):
                    mock_logger.info.assert_any_call(f"{i}: {item}")
                mock_logger.info.assert_any_call("Total: 3 items")

    @patch('src.utils.tar_extractor.extract_tar_contents')
    def test_extract_command(self, mock_extract_tar_contents):
        """Test the extract command."""
        # Mock the extract_tar_contents function
        mock_extract_tar_contents.return_value = [
            os.path.join(self.output_dir, 'file1.txt'),
            os.path.join(self.output_dir, 'file2.json'),
            os.path.join(self.output_dir, 'nested/file3.txt')
        ]

        # Mock sys.argv
        with patch('sys.argv', ['tar_extractor.py', self.tar_path, '-o', self.output_dir]):
            # Mock logger to capture output
            with patch('src.utils.tar_extractor.logger') as mock_logger:
                # Mock sys.exit to prevent the test from exiting
                with patch('sys.exit'):
                    # Call the main function
                    main()

                # Check that extract_tar_contents was called with the correct arguments
                mock_extract_tar_contents.assert_called_once_with(self.tar_path, self.output_dir, None)

                # Check that the logger was called with the expected message
                mock_logger.info.assert_called_with(f"Extracted 3 files to {self.output_dir}")

    @patch('src.utils.tar_extractor.read_tarfile')
    def test_json_command(self, mock_read_tarfile):
        """Test the json command."""
        # Mock the read_tarfile function
        mock_read_tarfile.return_value = {
            "key1": "value1",
            "key2": "value2"
        }

        # Mock sys.argv
        with patch('sys.argv', ['tar_extractor.py', self.tar_path, '-j']):
            # Mock logger to capture output
            with patch('src.utils.tar_extractor.logger') as mock_logger:
                # Mock sys.exit to prevent the test from exiting
                with patch('sys.exit'):
                    # Call the main function
                    main()

                # Check that read_tarfile was called with the correct arguments
                mock_read_tarfile.assert_called_once_with(self.tar_path, None)

                # Check that the logger was called with the expected messages
                mock_logger.info.assert_any_call(f"Successfully extracted and parsed JSON from {self.tar_path}")
                mock_logger.info.assert_any_call("JSON contains 2 top-level keys")
                mock_logger.info.assert_any_call("Top-level keys: key1, key2")

    @patch('src.utils.tar_extractor.read_tarfile')
    def test_json_command_with_select(self, mock_read_tarfile):
        """Test the json command with select_json parameter."""
        # Mock the read_tarfile function
        mock_read_tarfile.return_value = {
            "key1": "value1",
            "key2": "value2"
        }

        # Mock sys.argv
        with patch('sys.argv', ['tar_extractor.py', self.tar_path, '-j', '-s', '1']):
            # Mock logger to capture output
            with patch('src.utils.tar_extractor.logger') as mock_logger:
                # Mock sys.exit to prevent the test from exiting
                with patch('sys.exit'):
                    # Call the main function
                    main()

                # Check that read_tarfile was called with the correct arguments
                mock_read_tarfile.assert_called_once_with(self.tar_path, 1)

                # Check that the logger was called with the expected messages
                mock_logger.info.assert_any_call(f"Successfully extracted and parsed JSON from {self.tar_path}")

    @patch('src.utils.tar_extractor.os.path.exists')
    def test_file_not_found(self, mock_exists):
        """Test handling of non-existent files."""
        # Mock os.path.exists to return False
        mock_exists.return_value = False

        # Use a relative path to avoid validation errors
        tar_file = 'nonexistent.tar'

        # Mock sys.argv
        with patch('sys.argv', ['tar_extractor.py', tar_file, '-l']):
            # Mock logger to capture output
            with patch('src.utils.tar_extractor.logger') as mock_logger:
                # Mock sys.exit to prevent the test from exiting
                with patch('sys.exit') as mock_exit:
                    # Call the main function
                    main()

                    # Check that the logger was called with the expected error message
                    mock_logger.error.assert_called_with(f"File not found: {tar_file}")

                    # Check that sys.exit was called with exit code 1
                    mock_exit.assert_called_with(1)

    @patch('src.utils.tar_extractor.extract_tar_contents')
    def test_missing_output_dir(self, mock_extract_tar_contents):
        """Test handling of missing output directory."""
        # Mock sys.argv without output directory
        with patch('sys.argv', ['tar_extractor.py', self.tar_path]):
            # Mock logger to capture output
            with patch('src.utils.tar_extractor.logger') as mock_logger:
                # Mock sys.exit to prevent the test from exiting
                with patch('sys.exit') as mock_exit:
                    # Call the main function
                    main()

                    # Check that the logger was called with the expected error message
                    mock_logger.error.assert_called_with("Output directory is required for extraction")

                    # Check that sys.exit was called with exit code 1
                    mock_exit.assert_called_with(1)

    def test_parse_args(self):
        """Test parsing command line arguments."""
        # Test with just the tar file
        with patch('sys.argv', ['tar_extractor.py', 'test.tar']):
            args = parse_args()
            self.assertEqual(args.tar_file, 'test.tar')
            self.assertIsNone(args.output_dir)
            self.assertIsNone(args.pattern)
            self.assertFalse(args.list)
            self.assertFalse(args.json)
            self.assertIsNone(args.select_json)

        # Test with output directory
        with patch('sys.argv', ['tar_extractor.py', 'test.tar', '-o', 'output']):
            args = parse_args()
            self.assertEqual(args.tar_file, 'test.tar')
            self.assertEqual(args.output_dir, 'output')

        # Test with pattern
        with patch('sys.argv', ['tar_extractor.py', 'test.tar', '-p', '*.txt']):
            args = parse_args()
            self.assertEqual(args.pattern, '*.txt')

        # Test with list flag
        with patch('sys.argv', ['tar_extractor.py', 'test.tar', '-l']):
            args = parse_args()
            self.assertTrue(args.list)

        # Test with json flag
        with patch('sys.argv', ['tar_extractor.py', 'test.tar', '-j']):
            args = parse_args()
            self.assertTrue(args.json)

        # Test with select_json
        with patch('sys.argv', ['tar_extractor.py', 'test.tar', '-s', '1']):
            args = parse_args()
            self.assertEqual(args.select_json, 1)


if __name__ == '__main__':
    unittest.main()