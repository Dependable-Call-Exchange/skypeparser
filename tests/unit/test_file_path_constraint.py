#!/usr/bin/env python3
"""
Unit tests for the file path constraint fix in ETL pipeline components.

This module tests that the fix for handling file paths that must end with .tar
is properly implemented in both the Loader and DataInserter classes.
"""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock, Mock

# Add the src directory to the path so we can import the modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl.loader import Loader
from src.db.data_inserter import BulkInsertionStrategy, IndividualInsertionStrategy
from src.db.etl.context import ETLContext
from tests.fixtures.mocks import MockDatabase


# Fixtures
@pytest.fixture
def mock_context():
    """Create a mock ETL context."""
    mock_context = MagicMock(spec=ETLContext)
    mock_context.db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_password"
    }
    mock_context.batch_size = 100
    mock_context.file_path = None  # Will be set in individual tests
    return mock_context


@pytest.fixture
def mock_db_manager():
    """Create a mock database manager."""
    mock_db = MockDatabase()
    mock_db.connection = MagicMock()  # Ensure connection attribute exists
    return mock_db


@pytest.fixture
def loader(mock_context):
    """Create a loader instance with mocked dependencies."""
    # Create mock database connection directly
    mock_db_connection = MagicMock()
    return Loader(
        context=mock_context,
        db_connection=mock_db_connection
    )


@pytest.fixture
def bulk_strategy():
    """Create a bulk insertion strategy instance."""
    return BulkInsertionStrategy(batch_size=10)


@pytest.fixture
def individual_strategy():
    """Create an individual insertion strategy instance."""
    return IndividualInsertionStrategy()


@pytest.fixture
def transformed_data():
    """Sample transformed data for testing."""
    return {
        'user': {
            'id': 'test-user-id',
            'display_name': 'Test User'
        },
        'conversations': {},
        'messages': {},
        'metadata': {
            'conversation_count': 0,
            'message_count': 0
        }
    }


@pytest.fixture
def raw_data():
    """Sample raw data for testing."""
    return {
        'userId': 'test-user-id',
        'exportDate': '2023-01-01T00:00:00Z',
        'conversations': {}
    }


# Tests for Loader
def test_loader_with_tar_file_path(loader, raw_data, transformed_data):
    """Test that loader correctly handles a file path ending with .tar."""
    # Set up
    loader.context.file_path = "/path/to/skype_export.tar"

    # Execute with patch to avoid actual database operations
    with patch.object(loader, 'data_inserter'):
        loader.load(raw_data, transformed_data)

        # Verify that file_path is used as is (not modified)
        args, kwargs = loader.data_inserter.insert.call_args
        assert kwargs.get('file_path', args[0].get('file_path')) == "/path/to/skype_export.tar"


def test_loader_with_non_tar_file_path(loader, raw_data, transformed_data):
    """Test that loader correctly appends .tar to a file path not ending with .tar."""
    # Set up
    loader.context.file_path = "/path/to/skype_export.json"

    # Execute with patch to avoid actual database operations
    with patch.object(loader, 'data_inserter'):
        loader.load(raw_data, transformed_data)

        # Verify that .tar extension was added
        args, kwargs = loader.data_inserter.insert.call_args
        inserted_data = args[0] if args else kwargs.get('data')
        assert inserted_data.get('file_path').endswith('.tar')
        assert inserted_data.get('file_path') == "/path/to/skype_export.tar"


def test_loader_with_no_file_path(loader, raw_data, transformed_data):
    """Test that loader creates a valid fallback file path when none is provided."""
    # Set up
    loader.context.file_path = None

    # Execute with patch to avoid actual database operations
    with patch.object(loader, 'data_inserter'):
        loader.load(raw_data, transformed_data)

        # Verify that a fallback path was created with .tar extension
        args, kwargs = loader.data_inserter.insert.call_args
        inserted_data = args[0] if args else kwargs.get('data')
        assert inserted_data.get('file_path').endswith('.tar')
        assert "unknown_export_" in inserted_data.get('file_path')


# Tests for BulkInsertionStrategy
def test_bulk_strategy_insert_archive_with_tar_file_path(bulk_strategy):
    """Test that _insert_archive directly handles a file path ending with .tar."""
    db_manager = MagicMock()
    db_manager.bulk_insert.return_value = 1

    # Call _insert_archive directly with .tar file path
    data = {"file_path": "/path/to/skype_export.tar"}
    result = bulk_strategy._insert_archive(db_manager, data)

    # Verify the file path wasn't modified
    call_args = db_manager.bulk_insert.call_args
    values = call_args[0][2][0]  # Extract values from bulk_insert call
    assert values[3] == "/path/to/skype_export.tar"  # file_path is the 4th item


def test_bulk_strategy_insert_archive_with_non_tar_file_path(bulk_strategy):
    """Test that _insert_archive directly modifies a file path not ending with .tar."""
    db_manager = MagicMock()
    db_manager.bulk_insert.return_value = 1

    # Call _insert_archive directly with non-.tar file path
    data = {"file_path": "/path/to/skype_export.json"}
    result = bulk_strategy._insert_archive(db_manager, data)

    # Verify the file path was modified to end with .tar
    call_args = db_manager.bulk_insert.call_args
    values = call_args[0][2][0]  # Extract values from bulk_insert call
    assert values[3].endswith('.tar')
    assert values[3] == "/path/to/skype_export.tar"


# Tests for IndividualInsertionStrategy
def test_individual_strategy_insert_archive_with_tar_file_path(individual_strategy):
    """Test that _insert_archive directly handles a file path ending with .tar."""
    db_manager = MagicMock()
    db_manager.execute_query.return_value = None
    db_manager.cursor = MagicMock()

    # Call _insert_archive directly with .tar file path
    data = {"file_path": "/path/to/skype_export.tar"}
    result = individual_strategy._insert_archive(db_manager, data)

    # Verify the file path wasn't modified
    call_args = db_manager.execute_query.call_args
    params = call_args[0][1]  # Extract params from execute_query call
    assert params[3] == "/path/to/skype_export.tar"  # file_path is the 4th item


def test_individual_strategy_insert_archive_with_non_tar_file_path(individual_strategy):
    """Test that _insert_archive directly modifies a file path not ending with .tar."""
    db_manager = MagicMock()
    db_manager.execute_query.return_value = None
    db_manager.cursor = MagicMock()

    # Call _insert_archive directly with non-.tar file path
    data = {"file_path": "/path/to/skype_export.json"}
    result = individual_strategy._insert_archive(db_manager, data)

    # Verify the file path was modified to end with .tar
    call_args = db_manager.execute_query.call_args
    params = call_args[0][1]  # Extract params from execute_query call
    assert params[3].endswith('.tar')
    assert params[3] == "/path/to/skype_export.tar"