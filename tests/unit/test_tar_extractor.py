#!/usr/bin/env python3
"""
Tests for the tar_extractor module.

This module contains tests for the functionality in src.utils.tar_extractor.
"""

import os
import sys
import json
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Add the parent directory to the path so we can import from src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

# Import the functions from tar_extractor
from src.utils.tar_extractor import main, parse_args
from tests.fixtures import create_test_tar_file


@pytest.fixture
def test_dir():
    """Create a temporary directory for test files."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Clean up
    import shutil
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)


@pytest.fixture
def tar_setup(test_dir):
    """Set up test tar file and output directory."""
    # Create a test tar file
    tar_files = {
        'test.txt': 'Test content',
        'file1.txt': 'File 1 content',
        'file2.json': '{"key": "value"}',
        'nested/file3.txt': 'Nested file content'
    }
    tar_path = create_test_tar_file(test_dir, 'test.tar', tar_files)

    # Create an output directory
    output_dir = os.path.join(test_dir, 'output')
    os.makedirs(output_dir, exist_ok=True)

    return {
        'tar_path': tar_path,
        'output_dir': output_dir,
        'tar_files': tar_files
    }


@pytest.fixture
def mock_tar_extractor():
    """Create mock functions for tar_extractor module."""
    mock_list_tar_contents = MagicMock()
    mock_extract_tar_contents = MagicMock()
    mock_read_tarfile = MagicMock()
    mock_logger = MagicMock()
    mock_exists = MagicMock()

    # Set up default return values
    mock_list_tar_contents.return_value = [
        'file1.txt',
        'file2.json',
        'nested/file3.txt'
    ]
    mock_extract_tar_contents.return_value = [
        'output/file1.txt',
        'output/file2.json',
        'output/nested/file3.txt'
    ]
    mock_read_tarfile.return_value = {'key': 'value'}
    mock_exists.return_value = True

    return {
        'list_tar_contents': mock_list_tar_contents,
        'extract_tar_contents': mock_extract_tar_contents,
        'read_tarfile': mock_read_tarfile,
        'logger': mock_logger,
        'exists': mock_exists
    }


def test_list_command(tar_setup, mock_tar_extractor, monkeypatch):
    """Test the list command."""
    # Set up mocks
    monkeypatch.setattr('src.utils.tar_extractor.list_tar_contents', mock_tar_extractor['list_tar_contents'])
    monkeypatch.setattr('src.utils.tar_extractor.logger', mock_tar_extractor['logger'])
    monkeypatch.setattr('sys.argv', ['tar_extractor.py', tar_setup['tar_path'], '-l'])

    # Call the main function
    main()

    # Check that list_tar_contents was called with the correct arguments
    mock_tar_extractor['list_tar_contents'].assert_called_once_with(tar_setup['tar_path'], None)

    # Check that the logger was called with the expected messages
    mock_tar_extractor['logger'].info.assert_any_call(f"Contents of {tar_setup['tar_path']}:")
    for i, item in enumerate(['file1.txt', 'file2.json', 'nested/file3.txt'], 1):
        mock_tar_extractor['logger'].info.assert_any_call(f"{i}: {item}")
    mock_tar_extractor['logger'].info.assert_any_call("Total: 3 items")


def test_extract_command(tar_setup, mock_tar_extractor, monkeypatch):
    """Test the extract command."""
    # Set up mocks
    monkeypatch.setattr('src.utils.tar_extractor.extract_tar_contents', mock_tar_extractor['extract_tar_contents'])
    monkeypatch.setattr('src.utils.tar_extractor.logger', mock_tar_extractor['logger'])
    monkeypatch.setattr('sys.argv', ['tar_extractor.py', tar_setup['tar_path'], '-o', tar_setup['output_dir']])

    # Update the return value to use the actual output directory
    mock_tar_extractor['extract_tar_contents'].return_value = [
        os.path.join(tar_setup['output_dir'], 'file1.txt'),
        os.path.join(tar_setup['output_dir'], 'file2.json'),
        os.path.join(tar_setup['output_dir'], 'nested/file3.txt')
    ]

    # Call the main function
    main()

    # Check that extract_tar_contents was called with the correct arguments
    mock_tar_extractor['extract_tar_contents'].assert_called_once_with(
        tar_setup['tar_path'], tar_setup['output_dir'], None
    )

    # Check that the logger was called with the expected message
    mock_tar_extractor['logger'].info.assert_called_with(f"Extracted 3 files to {tar_setup['output_dir']}")


def test_json_command(tar_setup, mock_tar_extractor, monkeypatch):
    """Test the json command."""
    # Set up mocks
    monkeypatch.setattr('src.utils.tar_extractor.read_tarfile', mock_tar_extractor['read_tarfile'])
    monkeypatch.setattr('src.utils.tar_extractor.logger', mock_tar_extractor['logger'])
    monkeypatch.setattr('sys.argv', ['tar_extractor.py', tar_setup['tar_path'], '-j'])

    # Set up mock return value
    mock_tar_extractor['read_tarfile'].return_value = {'key': 'value'}

    # Call the main function
    main()

    # Check that read_tarfile was called with the correct arguments
    mock_tar_extractor['read_tarfile'].assert_called_once_with(tar_setup['tar_path'], False)

    # Check that the logger was called with the expected message
    mock_tar_extractor['logger'].info.assert_called_with(json.dumps({'key': 'value'}, indent=2))


def test_json_command_with_select(tar_setup, mock_tar_extractor, monkeypatch):
    """Test the json command with select option."""
    # Set up mocks
    monkeypatch.setattr('src.utils.tar_extractor.read_tarfile', mock_tar_extractor['read_tarfile'])
    monkeypatch.setattr('src.utils.tar_extractor.logger', mock_tar_extractor['logger'])
    monkeypatch.setattr('sys.argv', ['tar_extractor.py', tar_setup['tar_path'], '-j', '-s'])

    # Set up mock return value
    mock_tar_extractor['read_tarfile'].return_value = {'key': 'value'}

    # Call the main function
    main()

    # Check that read_tarfile was called with the correct arguments
    mock_tar_extractor['read_tarfile'].assert_called_once_with(tar_setup['tar_path'], True)

    # Check that the logger was called with the expected message
    mock_tar_extractor['logger'].info.assert_called_with(json.dumps({'key': 'value'}, indent=2))


def test_file_not_found(tar_setup, mock_tar_extractor, monkeypatch):
    """Test handling of file not found."""
    # Set up mocks
    monkeypatch.setattr('os.path.exists', mock_tar_extractor['exists'])
    monkeypatch.setattr('src.utils.tar_extractor.logger', mock_tar_extractor['logger'])
    monkeypatch.setattr('sys.argv', ['tar_extractor.py', 'nonexistent.tar', '-l'])

    # Set up mock return value
    mock_tar_extractor['exists'].return_value = False

    # Call the main function and check for SystemExit
    with pytest.raises(SystemExit):
        main()

    # Check that the logger was called with the expected error message
    mock_tar_extractor['logger'].error.assert_called_with("File not found: nonexistent.tar")


def test_missing_output_dir(tar_setup, mock_tar_extractor, monkeypatch):
    """Test handling of missing output directory argument."""
    # Set up mocks
    monkeypatch.setattr('src.utils.tar_extractor.extract_tar_contents', mock_tar_extractor['extract_tar_contents'])
    monkeypatch.setattr('src.utils.tar_extractor.logger', mock_tar_extractor['logger'])
    monkeypatch.setattr('sys.argv', ['tar_extractor.py', tar_setup['tar_path'], '-e'])

    # Call the main function and check for SystemExit
    with pytest.raises(SystemExit):
        main()

    # Check that the logger was called with the expected error message
    mock_tar_extractor['logger'].error.assert_called_with("Output directory must be specified with -o/--output-dir")


def test_parse_args():
    """Test the parse_args function."""
    # Test list command
    with patch('sys.argv', ['tar_extractor.py', 'test.tar', '-l']):
        args = parse_args()
        assert args.tar_file == 'test.tar'
        assert args.list is True
        assert args.extract is False
        assert args.json is False
        assert args.output_dir is None
        assert args.select is False

    # Test extract command
    with patch('sys.argv', ['tar_extractor.py', 'test.tar', '-e', '-o', 'output']):
        args = parse_args()
        assert args.tar_file == 'test.tar'
        assert args.list is False
        assert args.extract is True
        assert args.json is False
        assert args.output_dir == 'output'
        assert args.select is False

    # Test json command
    with patch('sys.argv', ['tar_extractor.py', 'test.tar', '-j']):
        args = parse_args()
        assert args.tar_file == 'test.tar'
        assert args.list is False
        assert args.extract is False
        assert args.json is True
        assert args.output_dir is None
        assert args.select is False

    # Test json command with select
    with patch('sys.argv', ['tar_extractor.py', 'test.tar', '-j', '-s']):
        args = parse_args()
        assert args.tar_file == 'test.tar'
        assert args.list is False
        assert args.extract is False
        assert args.json is True
        assert args.output_dir is None
        assert args.select is True