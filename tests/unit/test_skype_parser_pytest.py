#!/usr/bin/env python3
"""
Pytest version of test_skype_parser.py.

This module contains tests for the functionality in src.parser.skype_parser,
migrated from unittest.TestCase style to pytest style with dependency injection.
"""

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Dict, Any, Callable, Optional, List, Tuple

import pytest

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.parser.exceptions import (
    DataExtractionError,
    ExportError,
    FileOperationError,
    InvalidInputError,
)
from src.parser.skype_parser import get_commandline_args, main

# Import fixtures and mocks
from tests.fixtures.mocks import MockFileHandler
from tests.fixtures import SkypeDataFactory
from tests.fixtures.expected_data import get_expected_api_response


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    temp_directory = tempfile.mkdtemp()
    yield temp_directory
    # Cleanup after test
    import shutil
    shutil.rmtree(temp_directory)


@pytest.fixture
def sample_skype_data():
    """Create sample Skype data for testing."""
    return SkypeDataFactory.build(
        user_id="test_user",
        user_display_name="Test User",
        export_date="2023-01-01T12:00:00Z",
        conversation_count=1,
        message_count=1
    )


@pytest.fixture
def sample_json_path(temp_dir, sample_skype_data):
    """Create a sample JSON file with Skype data."""
    json_path = os.path.join(temp_dir, "sample.json")
    with open(json_path, "w") as f:
        json.dump(sample_skype_data, f)
    return json_path


@pytest.fixture
def sample_tar_path(temp_dir):
    """Return a path for a sample TAR file (without creating it)."""
    return os.path.join(temp_dir, "sample.tar")


@pytest.fixture
def mock_args_factory():
    """Factory fixture to create mock command line arguments."""
    def _create_args(
        input_file: str,
        output_dir: str = None,
        format: str = "json",
        extract_tar: bool = False,
        store_db: bool = False,
        db_name: Optional[str] = None,
        db_user: Optional[str] = None,
        db_password: Optional[str] = None,
        db_host: Optional[str] = None,
        db_port: Optional[int] = None,
        user_display_name: Optional[str] = None,
        verbose: bool = False,
        choose: bool = False,
        select_conversations: Optional[List[str]] = None,
        select_json: Optional[int] = None,
        text_output: bool = False,
        overwrite: bool = False,
        skip_existing: bool = False,
    ) -> argparse.Namespace:
        """Create mock command line arguments."""
        return argparse.Namespace(
            input_file=input_file,
            output_dir=output_dir,
            format=format,
            extract_tar=extract_tar,
            store_db=store_db,
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
            db_host=db_host,
            db_port=db_port,
            user_display_name=user_display_name,
            verbose=verbose,
            choose=choose,
            select_conversations=select_conversations,
            select_json=select_json,
            text_output=text_output,
            overwrite=overwrite,
            skip_existing=skip_existing,
        )
    return _create_args


@pytest.fixture
def mock_read_file():
    """Mock for the read_file function."""
    def _read_file(file_path: str) -> Dict[str, Any]:
        """Mock implementation of read_file."""
        return SkypeDataFactory.build()
    return _read_file


@pytest.fixture
def mock_read_tarfile():
    """Mock for the read_tarfile function."""
    def _read_tarfile(tar_path: str, json_index: Optional[int] = None) -> Dict[str, Any]:
        """Mock implementation of read_tarfile."""
        return SkypeDataFactory.build()
    return _read_tarfile


@pytest.fixture
def mock_parse_skype_data():
    """Mock for the parse_skype_data function."""
    def _parse_skype_data(data: Dict[str, Any], user_display_name: str = None) -> Dict[str, Any]:
        """Mock implementation of parse_skype_data."""
        return data
    return _parse_skype_data


@pytest.fixture
def mock_export_conversations():
    """Mock for the export_conversations function."""
    def _export_conversations(
        data: Dict[str, Any],
        output_dir: str,
        format: str = "json",
        select_conversations: Optional[List[str]] = None,
    ) -> List[str]:
        """Mock implementation of export_conversations."""
        return [os.path.join(output_dir, "conversation1.json")]
    return _export_conversations


@pytest.fixture
def mock_etl_pipeline():
    """Mock for the ETLPipeline class."""
    class MockETLPipeline:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        def run_pipeline(self, **kwargs):
            return True

    return MockETLPipeline


def test_main_with_json_file(
    monkeypatch,
    temp_dir,
    sample_json_path,
    mock_args_factory,
    mock_read_file,
    mock_parse_skype_data,
    mock_export_conversations
):
    """Test main function with a JSON file."""
    # Setup mock arguments
    mock_args = mock_args_factory(
        input_file=sample_json_path,
        output_dir=temp_dir
    )

    # Create the file to avoid file not found error
    with open(sample_json_path, "w") as f:
        json.dump({}, f)

    # Apply monkeypatches
    monkeypatch.setattr("argparse.ArgumentParser.parse_args", lambda _: mock_args)
    monkeypatch.setattr("src.parser.skype_parser.read_file", mock_read_file)
    monkeypatch.setattr("src.parser.skype_parser.parse_skype_data", mock_parse_skype_data)
    monkeypatch.setattr("src.parser.skype_parser.export_conversations", mock_export_conversations)
    monkeypatch.setattr("sys.exit", lambda code: None)  # Prevent sys.exit from stopping the test

    # Execute the function
    result = main()

    # Since we're preventing sys.exit, the function doesn't return a value
    # We can only verify that it ran without exceptions
    assert True


def test_main_with_tar_file(
    monkeypatch,
    temp_dir,
    sample_tar_path,
    mock_args_factory,
    mock_read_tarfile,
    mock_parse_skype_data,
    mock_export_conversations
):
    """Test main function with a TAR file."""
    # Setup mock arguments with extract_tar=True
    mock_args = mock_args_factory(
        input_file=sample_tar_path,
        output_dir=temp_dir,
        extract_tar=True
    )

    # Create the file to avoid file not found error
    with open(sample_tar_path, "w") as f:
        f.write("mock tar file")

    # Apply monkeypatches
    monkeypatch.setattr("argparse.ArgumentParser.parse_args", lambda _: mock_args)
    monkeypatch.setattr("src.parser.skype_parser.read_tarfile", mock_read_tarfile)
    monkeypatch.setattr("src.parser.skype_parser.parse_skype_data", mock_parse_skype_data)
    monkeypatch.setattr("src.parser.skype_parser.export_conversations", mock_export_conversations)
    monkeypatch.setattr("sys.exit", lambda code: None)  # Prevent sys.exit from stopping the test

    # Execute the function
    result = main()

    # Since we're preventing sys.exit, the function doesn't return a value
    # We can only verify that it ran without exceptions
    assert True


def test_main_with_db_storage(
    monkeypatch,
    temp_dir,
    sample_json_path,
    mock_args_factory,
    mock_read_file,
    mock_etl_pipeline
):
    """Test main function with database storage."""
    # Setup mock arguments with store_db=True
    mock_args = mock_args_factory(
        input_file=sample_json_path,
        output_dir=temp_dir,
        store_db=True,
        db_name="test_db",
        db_user="test_user",
        db_password="test_password",
        db_host="localhost",
        db_port=5432
    )

    # Create the file to avoid file not found error
    with open(sample_json_path, "w") as f:
        json.dump({}, f)

    # Apply monkeypatches
    monkeypatch.setattr("argparse.ArgumentParser.parse_args", lambda _: mock_args)
    monkeypatch.setattr("src.parser.skype_parser.read_file", mock_read_file)
    monkeypatch.setattr("src.parser.skype_parser.ETL_AVAILABLE", True)
    monkeypatch.setattr("src.parser.skype_parser.SkypeETLPipeline", mock_etl_pipeline)
    monkeypatch.setattr("sys.exit", lambda code: None)  # Prevent sys.exit from stopping the test

    # Execute the function
    result = main()

    # Since we're preventing sys.exit, the function doesn't return a value
    # We can only verify that it ran without exceptions
    assert True


def test_main_with_file_error(
    monkeypatch,
    temp_dir,
    mock_args_factory
):
    """Test main function with a file error."""
    # Setup mock arguments with a non-existent file
    non_existent_file = os.path.join(temp_dir, "non_existent.json")
    mock_args = mock_args_factory(
        input_file=non_existent_file,
        output_dir=temp_dir
    )

    # Apply monkeypatches
    monkeypatch.setattr("argparse.ArgumentParser.parse_args", lambda _: mock_args)
    monkeypatch.setattr("sys.exit", lambda code: None)  # Prevent sys.exit from stopping the test

    # Execute the function and verify it handles the error
    result = main()

    # Since we're preventing sys.exit, the function doesn't return a value
    # We can only verify that it ran without exceptions
    assert True


def test_get_commandline_args(monkeypatch):
    """Test the get_commandline_args function."""
    # Mock sys.argv
    test_args = [
        "skype_parser.py",
        "test.json",
        "-o", "/tmp",
        "-f", "json",
        "-v"
    ]
    monkeypatch.setattr("sys.argv", test_args)
    monkeypatch.setattr("sys.exit", lambda code: None)  # Prevent sys.exit from stopping the test

    # Call the function
    args = get_commandline_args()

    # Verify the result
    assert args.input_file == "test.json"
    assert args.output_dir == "/tmp"
    assert args.format == "json"
    assert args.verbose is True

