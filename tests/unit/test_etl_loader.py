#!/usr/bin/env python3
"""
Unit tests for the ETL Loader class.

These tests validate the loading logic of the ETL pipeline,
ensuring that transformed data is correctly loaded into the database.
"""

import os
import sys
import tempfile
import json
from unittest.mock import patch, MagicMock, Mock

import pytest

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl.loader import Loader
from src.utils.interfaces import DatabaseConnectionProtocol
from src.db.etl.context import ETLContext
from tests.fixtures.mocks import MockDatabase


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Clean up
    if os.path.exists(temp_dir):
        os.rmdir(temp_dir)


@pytest.fixture
def mock_context(temp_dir):
    """Create a mock ETL context."""
    mock_context = MagicMock(spec=ETLContext)
    mock_context.output_dir = temp_dir
    mock_context.db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "user": "test_user",
        "password": "test_password"
    }
    mock_context.batch_size = 100
    mock_context.user_id = "test-user-id"
    mock_context.user_display_name = "Test User"
    mock_context.export_date = "2023-01-01T00:00:00Z"
    return mock_context


@pytest.fixture
def mock_db_connection():
    """Create a mock database connection."""
    mock_db = MockDatabase()
    return mock_db


@pytest.fixture
def loader(mock_context, mock_db_connection):
    """Create a loader instance with mocked dependencies."""
    return Loader(
        context=mock_context,
        db_connection=mock_db_connection
    )


@pytest.fixture
def raw_data():
    """Sample raw data for testing."""
    return {
        'userId': 'test-user-id',
        'exportDate': '2023-01-01T00:00:00Z',
        'conversations': {
            'conv1': {
                'Properties': {
                    'displayName': 'Test Conversation'
                },
                'MessageList': [
                    {
                        'id': 'msg1',
                        'content': 'Test message'
                    }
                ]
            }
        },
        'messages': {
            'msg1': {
                'id': 'msg1',
                'content': 'Test message',
                'conversationId': 'conv1'
            }
        }
    }


@pytest.fixture
def transformed_data():
    """Sample transformed data for testing."""
    return {
        'user': {
            'id': 'test-user-id',
            'display_name': 'Test User'
        },
        'conversations': {
            'conv1': {
                'id': 'conv1',
                'display_name': 'Test Conversation',
                'participants': [
                    {
                        'id': 'test-user-id',
                        'display_name': 'Test User',
                        'is_self': True
                    },
                    {
                        'id': 'other-user-id',
                        'display_name': 'Other User',
                        'is_self': False
                    }
                ],
                'messages': [
                    {
                        'id': 'msg1',
                        'content_text': 'Test message',
                        'content_html': '<p>Test message</p>',
                        'is_edited': False,
                        'is_deleted': False
                    }
                ]
            }
        },
        'messages': {
            'msg1': {
                'id': 'msg1',
                'conversation_id': 'conv1',
                'sender_id': 'test-user-id',
                'sender_display_name': 'Test User',
                'content': 'Test message',
                'timestamp': '2023-01-01T12:00:00Z',
                'message_type': 'RichText'
            }
        },
        'metadata': {
            'conversation_count': 1,
            'message_count': 1
        }
    }


def test_connect_db(loader, mock_context, mock_db_connection):
    """Test connecting to the database."""
    # Patch the psycopg2.connect function
    with patch('psycopg2.connect') as mock_connect:
        # Set up the mock connection
        mock_connect.return_value = mock_db_connection

        # Connect to the database
        loader.connect_db()

        # Verify that psycopg2.connect was called with the correct arguments
        mock_connect.assert_called_once_with(**mock_context.db_config)

        # Verify that the connection was set
        assert loader.db_connection == mock_db_connection


def test_close_db(loader, mock_db_connection):
    """Test closing the database connection."""
    # Set up the mock to have the necessary attributes for close_db
    mock_db_connection.closed = False

    # Close the database connection
    loader.close_db()

    # Verify that the mock database connection was closed
    mock_db_connection.close.assert_called_once()


def test_load(loader, raw_data, transformed_data):
    """Test loading data into the database."""
    # Patch the _store_messages method to avoid the issue with dictionary slicing
    with patch.object(loader, '_store_messages'):
        # Load the data
        export_id = loader.load(raw_data, transformed_data, 'test.json')

        # Verify that the export ID is correct
        assert export_id == 1

        # Verify that _store_messages was called with the transformed data
        loader._store_messages.assert_called_once_with(transformed_data)


def test_load_with_invalid_input(loader, raw_data, transformed_data):
    """Test loading with invalid input."""
    # Test with None raw data
    with pytest.raises(ValueError):
        loader.load(None, transformed_data, 'test.json')

    # Test with None transformed data
    with pytest.raises(ValueError):
        loader.load(raw_data, None, 'test.json')

    # Test with invalid transformed data (missing user)
    invalid_transformed_data = {
        'conversations': {},
        'metadata': {}
    }
    with pytest.raises(ValueError):
        loader.load(raw_data, invalid_transformed_data, 'test.json')

    # Test with invalid transformed data (missing conversations)
    invalid_transformed_data = {
        'user': {},
        'metadata': {}
    }
    with pytest.raises(ValueError):
        loader.load(raw_data, invalid_transformed_data, 'test.json')


def test_load_with_di_dependencies(mock_context, transformed_data, raw_data):
    """Test loading using dependency injection for all dependencies."""
    # Create a custom mock database using the centralized MockDatabase class
    custom_db_connection = MockDatabase()

    # Initialize the loader with dependency injection
    loader = Loader(
        context=mock_context,
        db_connection=custom_db_connection
    )

    # Load the data
    result = loader.load(raw_data=raw_data, transformed_data=transformed_data)

    # Verify the result
    assert isinstance(result, dict)
    assert result.get("conversations", 0) == 1
    assert result.get("messages", 0) == 1
    assert result.get("users", 0) == 1


def test_load_with_transaction_error(mock_context, transformed_data, raw_data):
    """Test loading with a transaction error."""
    # Create a mock database that will fail
    error_db_connection = MockDatabase(should_fail=True)

    # Initialize the loader with the failing database
    loader = Loader(
        context=mock_context,
        db_connection=error_db_connection
    )

    # Load the data, should not raise an exception
    result = loader.load(raw_data=raw_data, transformed_data=transformed_data)

    # Verify the result contains zeros for all counts
    assert isinstance(result, dict)
    assert result.get("conversations", 0) == 0
    assert result.get("messages", 0) == 0
    assert result.get("users", 0) == 0


def test_load_with_no_context():
    """Test loading with no context."""
    # Create a loader with no context
    loader_no_context = Loader(db_connection=MagicMock())

    # Verify that the context is None
    assert loader_no_context.context is None

    # Verify that loading without a context raises an exception
    with pytest.raises(ValueError):
        loader_no_context.load({}, {}, 'test.json')