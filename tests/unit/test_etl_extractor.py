#!/usr/bin/env python3
"""
Unit tests for the ETL Extractor class.
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock, mock_open
from io import BytesIO

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl.extractor import Extractor

class TestExtractor(unittest.TestCase):
    """Test cases for the Extractor class."""

    def setUp(self):
        """Set up test fixtures."""
        self.extractor = Extractor(output_dir='test_output')

        # Sample data for testing
        self.sample_data = {
            'conversations': [
                {
                    'id': 'conv1',
                    'displayName': 'Test Conversation',
                    'MessageList': [
                        {'id': 'msg1', 'content': 'Hello'}
                    ]
                }
            ]
        }

    def tearDown(self):
        """Clean up after tests."""
        # Remove test output directory if it exists
        if os.path.exists('test_output'):
            import shutil
            shutil.rmtree('test_output')

    @patch('src.db.etl.extractor.validate_file_exists')
    @patch('src.db.etl.extractor.validate_tar_file')
    @patch('src.db.etl.extractor.read_tarfile')
    @patch('src.db.etl.extractor.validate_skype_data')
    def test_extract_from_tar_file(self, mock_validate_skype_data, mock_read_tarfile,
                                 mock_validate_tar_file, mock_validate_file_exists):
        """Test extracting data from a tar file."""
        # Set up mocks
        mock_read_tarfile.return_value = self.sample_data

        # Call the method
        result = self.extractor.extract(file_path='test.tar')

        # Verify the result
        self.assertEqual(result, self.sample_data)

        # Verify the mocks were called
        mock_validate_file_exists.assert_called_once_with('test.tar')
        mock_validate_tar_file.assert_called_once_with('test.tar')
        mock_read_tarfile.assert_called_once_with('test.tar')
        mock_validate_skype_data.assert_called_once_with(self.sample_data)

    @patch('src.db.etl.extractor.validate_file_exists')
    @patch('src.db.etl.extractor.validate_json_file')
    @patch('src.db.etl.extractor.read_file_object')
    @patch('src.db.etl.extractor.validate_skype_data')
    @patch('builtins.open', new_callable=mock_open)
    def test_extract_from_json_file(self, mock_file, mock_validate_skype_data,
                                  mock_read_file_object, mock_validate_json_file,
                                  mock_validate_file_exists):
        """Test extracting data from a JSON file."""
        # Set up mocks
        mock_read_file_object.return_value = self.sample_data

        # Call the method
        result = self.extractor.extract(file_path='test.json')

        # Verify the result
        self.assertEqual(result, self.sample_data)

        # Verify the mocks were called
        mock_validate_file_exists.assert_called_once_with('test.json')
        mock_validate_json_file.assert_called_once_with('test.json')
        mock_file.assert_called_once_with('test.json', 'r')
        mock_validate_skype_data.assert_called_once_with(self.sample_data)

    @patch('src.db.etl.extractor.validate_file_object')
    @patch('src.db.etl.extractor.read_file_object')
    @patch('src.db.etl.extractor.validate_skype_data')
    def test_extract_from_file_object(self, mock_validate_skype_data,
                                    mock_read_file_object, mock_validate_file_object):
        """Test extracting data from a file object."""
        # Set up mocks
        mock_read_file_object.return_value = self.sample_data
        file_obj = BytesIO(b'{"test": "data"}')

        # Call the method
        result = self.extractor.extract(file_obj=file_obj)

        # Verify the result
        self.assertEqual(result, self.sample_data)

        # Verify the mocks were called
        mock_validate_file_object.assert_called_once_with(file_obj)
        mock_read_file_object.assert_called_once_with(file_obj)
        mock_validate_skype_data.assert_called_once_with(self.sample_data)

    def test_extract_with_no_input(self):
        """Test extracting data with no input."""
        # Call the method with no input
        with self.assertRaises(ValueError):
            self.extractor.extract()

    @patch('src.db.etl.extractor.validate_file_exists')
    @patch('src.db.etl.extractor.validate_tar_file')
    @patch('src.db.etl.extractor.read_tarfile')
    @patch('src.db.etl.extractor.validate_skype_data')
    @patch('os.makedirs')
    @patch('json.dump')
    @patch('builtins.open', new_callable=mock_open)
    def test_save_raw_data(self, mock_file, mock_json_dump, mock_makedirs,
                         mock_validate_skype_data, mock_read_tarfile,
                         mock_validate_tar_file, mock_validate_file_exists):
        """Test saving raw data to a file."""
        # Set up mocks
        mock_read_tarfile.return_value = self.sample_data

        # Call the method
        self.extractor.extract(file_path='test.tar')

        # Verify the mocks were called
        mock_makedirs.assert_called_once_with('test_output', exist_ok=True)
        mock_file.assert_called_once()
        mock_json_dump.assert_called_once()

if __name__ == '__main__':
    unittest.main()