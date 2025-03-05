"""
Tests for ETL validation utilities.

This module contains tests for the ETL validation utilities in src/utils/etl_validation.py.
"""

import os
import unittest
import tempfile
import json
import psycopg2
from unittest.mock import patch, MagicMock, mock_open

import sys
from pathlib import Path
# Add the project root to the Python path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.utils.etl_validation import (
    validate_supabase_config,
    validate_database_schema,
    validate_checkpoint_data,
    validate_transformed_data_structure,
    validate_connection_string,
    ETLValidationError
)


class TestSupabaseConfigValidation(unittest.TestCase):
    """Tests for validate_supabase_config function."""

    def test_valid_config(self):
        """Test validation of a valid Supabase configuration."""
        config = {
            'host': 'db.abcdefg.supabase.co',
            'port': 5432,
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'password',
            'sslmode': 'require'
        }
        self.assertTrue(validate_supabase_config(config))

    def test_invalid_config_type(self):
        """Test validation with an invalid configuration type."""
        with self.assertRaises(ETLValidationError):
            validate_supabase_config("not a dict")

    def test_missing_required_fields(self):
        """Test validation with missing required fields."""
        config = {
            'host': 'db.abcdefg.supabase.co',
            'port': 5432,
            # Missing dbname, user, password, sslmode
        }
        with self.assertRaises(ETLValidationError):
            validate_supabase_config(config)

    def test_empty_host(self):
        """Test validation with an empty host."""
        config = {
            'host': '',
            'port': 5432,
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'password',
            'sslmode': 'require'
        }
        with self.assertRaises(ETLValidationError):
            validate_supabase_config(config)

    def test_invalid_port_type(self):
        """Test validation with an invalid port type."""
        config = {
            'host': 'db.abcdefg.supabase.co',
            'port': 'not an int',
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'password',
            'sslmode': 'require'
        }
        with self.assertRaises(ETLValidationError):
            validate_supabase_config(config)

    @patch('src.utils.etl_validation.logger')
    def test_non_standard_port(self, mock_logger):
        """Test validation with a non-standard port."""
        config = {
            'host': 'db.abcdefg.supabase.co',
            'port': 1234,  # Non-standard port
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'password',
            'sslmode': 'require'
        }
        self.assertTrue(validate_supabase_config(config))
        mock_logger.warning.assert_called_once()

    @patch('src.utils.etl_validation.logger')
    def test_non_standard_sslmode(self, mock_logger):
        """Test validation with a non-standard SSL mode."""
        config = {
            'host': 'db.abcdefg.supabase.co',
            'port': 5432,
            'dbname': 'postgres',
            'user': 'postgres',
            'password': 'password',
            'sslmode': 'prefer'  # Non-standard SSL mode
        }
        self.assertTrue(validate_supabase_config(config))
        mock_logger.warning.assert_called_once()


class TestDatabaseSchemaValidation(unittest.TestCase):
    """Tests for validate_database_schema function."""

    def test_null_connection(self):
        """Test validation with a null connection."""
        with self.assertRaises(ETLValidationError):
            validate_database_schema(None)

    @patch('psycopg2.extensions.connection')
    def test_valid_schema(self, mock_conn):
        """Test validation with a valid schema."""
        # Mock cursor and fetchone
        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.fetchone.return_value = (True,)  # All tables exist

        # Mock connection
        mock_conn.cursor.return_value = mock_cursor

        is_valid, missing_tables = validate_database_schema(mock_conn)
        self.assertTrue(is_valid)
        self.assertEqual(missing_tables, [])

    @patch('psycopg2.extensions.connection')
    def test_missing_tables(self, mock_conn):
        """Test validation with missing tables."""
        # Mock cursor and fetchone
        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        # First table exists, second and third don't
        mock_cursor.fetchone.side_effect = [(True,), (False,), (False,)]

        # Mock connection
        mock_conn.cursor.return_value = mock_cursor

        is_valid, missing_tables = validate_database_schema(mock_conn)
        self.assertFalse(is_valid)
        self.assertEqual(len(missing_tables), 2)

    @patch('psycopg2.extensions.connection')
    def test_database_error(self, mock_conn):
        """Test validation with a database error."""
        # Mock cursor to raise an exception
        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.execute.side_effect = Exception("Database error")

        # Mock connection
        mock_conn.cursor.return_value = mock_cursor

        with self.assertRaises(ETLValidationError):
            validate_database_schema(mock_conn)


class TestCheckpointDataValidation(unittest.TestCase):
    """Tests for validate_checkpoint_data function."""

    def test_valid_checkpoint_data(self):
        """Test validation with valid checkpoint data."""
        checkpoint_data = {
            'checkpoint_version': '1.0',
            'serialized_at': '2023-01-01T00:00:00',
            'context': {
                'db_config': {'host': 'localhost'},
                'task_id': 'test-task'
            },
            'available_checkpoints': ['extract', 'transform'],
            'data_files': {}
        }
        self.assertTrue(validate_checkpoint_data(checkpoint_data))

    def test_invalid_checkpoint_data_type(self):
        """Test validation with an invalid checkpoint data type."""
        with self.assertRaises(ETLValidationError):
            validate_checkpoint_data("not a dict")

    def test_missing_required_fields(self):
        """Test validation with missing required fields."""
        checkpoint_data = {
            'serialized_at': '2023-01-01T00:00:00',
            # Missing checkpoint_version and context
        }
        with self.assertRaises(ETLValidationError):
            validate_checkpoint_data(checkpoint_data)

    def test_invalid_context_type(self):
        """Test validation with an invalid context type."""
        checkpoint_data = {
            'checkpoint_version': '1.0',
            'serialized_at': '2023-01-01T00:00:00',
            'context': "not a dict"  # Invalid context type
        }
        with self.assertRaises(ETLValidationError):
            validate_checkpoint_data(checkpoint_data)

    def test_missing_context_fields(self):
        """Test validation with missing context fields."""
        checkpoint_data = {
            'checkpoint_version': '1.0',
            'serialized_at': '2023-01-01T00:00:00',
            'context': {
                # Missing db_config and task_id
            }
        }
        with self.assertRaises(ETLValidationError):
            validate_checkpoint_data(checkpoint_data)

    def test_invalid_available_checkpoints_type(self):
        """Test validation with an invalid available_checkpoints type."""
        checkpoint_data = {
            'checkpoint_version': '1.0',
            'serialized_at': '2023-01-01T00:00:00',
            'context': {
                'db_config': {'host': 'localhost'},
                'task_id': 'test-task'
            },
            'available_checkpoints': "not a list"  # Invalid type
        }
        with self.assertRaises(ETLValidationError):
            validate_checkpoint_data(checkpoint_data)

    def test_invalid_data_files_type(self):
        """Test validation with an invalid data_files type."""
        checkpoint_data = {
            'checkpoint_version': '1.0',
            'serialized_at': '2023-01-01T00:00:00',
            'context': {
                'db_config': {'host': 'localhost'},
                'task_id': 'test-task'
            },
            'available_checkpoints': [],
            'data_files': "not a dict"  # Invalid type
        }
        with self.assertRaises(ETLValidationError):
            validate_checkpoint_data(checkpoint_data)

    @patch('os.path.exists')
    @patch('src.utils.etl_validation.logger')
    def test_missing_data_files(self, mock_logger, mock_exists):
        """Test validation with missing data files."""
        mock_exists.return_value = False

        checkpoint_data = {
            'checkpoint_version': '1.0',
            'serialized_at': '2023-01-01T00:00:00',
            'context': {
                'db_config': {'host': 'localhost'},
                'task_id': 'test-task'
            },
            'available_checkpoints': [],
            'data_files': {
                'raw_data': '/path/to/nonexistent/file.json'
            }
        }
        self.assertTrue(validate_checkpoint_data(checkpoint_data))
        mock_logger.warning.assert_called_once()


class TestTransformedDataValidation(unittest.TestCase):
    """Tests for validate_transformed_data_structure function."""

    def test_valid_transformed_data(self):
        """Test validation with valid transformed data."""
        transformed_data = {
            'metadata': {
                'user_id': 'test-user',
                'export_date': '2023-01-01'
            },
            'conversations': {
                'conv1': {
                    'display_name': 'Conversation 1',
                    'messages': [
                        {
                            'id': 'msg1',
                            'timestamp': '2023-01-01T00:00:00',
                            'raw_content': 'Hello',
                            'cleaned_content': 'Hello'
                        }
                    ]
                }
            }
        }
        sanitized_data = validate_transformed_data_structure(transformed_data)
        self.assertEqual(sanitized_data['conversations']['conv1']['display_name'], 'Conversation 1')
        self.assertEqual(len(sanitized_data['conversations']['conv1']['messages']), 1)

    def test_invalid_transformed_data_type(self):
        """Test validation with an invalid transformed data type."""
        with self.assertRaises(ETLValidationError):
            validate_transformed_data_structure("not a dict")

    def test_missing_required_fields(self):
        """Test validation with missing required fields."""
        transformed_data = {
            'metadata': {
                'user_id': 'test-user',
                'export_date': '2023-01-01'
            }
            # Missing conversations
        }
        with self.assertRaises(ETLValidationError):
            validate_transformed_data_structure(transformed_data)

    def test_invalid_metadata_type(self):
        """Test validation with an invalid metadata type."""
        transformed_data = {
            'metadata': "not a dict",  # Invalid type
            'conversations': {}
        }
        with self.assertRaises(ETLValidationError):
            validate_transformed_data_structure(transformed_data)

    def test_invalid_conversations_type(self):
        """Test validation with an invalid conversations type."""
        transformed_data = {
            'metadata': {},
            'conversations': "not a dict"  # Invalid type
        }
        with self.assertRaises(ETLValidationError):
            validate_transformed_data_structure(transformed_data)

    @patch('src.utils.etl_validation.logger')
    def test_invalid_conversation_entry(self, mock_logger):
        """Test validation with an invalid conversation entry."""
        transformed_data = {
            'metadata': {},
            'conversations': {
                'conv1': "not a dict"  # Invalid conversation
            }
        }
        sanitized_data = validate_transformed_data_structure(transformed_data)
        self.assertEqual(len(sanitized_data['conversations']), 0)
        mock_logger.warning.assert_called_once()

    @patch('src.utils.etl_validation.logger')
    def test_sanitize_conversation_id(self, mock_logger):
        """Test sanitization of conversation IDs."""
        transformed_data = {
            'metadata': {},
            'conversations': {
                'conv1/with/invalid/chars': {  # Invalid ID with slashes
                    'display_name': 'Conversation 1',
                    'messages': []
                }
            }
        }
        sanitized_data = validate_transformed_data_structure(transformed_data)
        self.assertIn('conv1_with_invalid_chars', sanitized_data['conversations'])
        mock_logger.warning.assert_called_once()

    @patch('src.utils.etl_validation.logger')
    def test_invalid_message_entry(self, mock_logger):
        """Test validation with an invalid message entry."""
        transformed_data = {
            'metadata': {},
            'conversations': {
                'conv1': {
                    'display_name': 'Conversation 1',
                    'messages': [
                        "not a dict"  # Invalid message
                    ]
                }
            }
        }
        sanitized_data = validate_transformed_data_structure(transformed_data)
        self.assertEqual(len(sanitized_data['conversations']['conv1']['messages']), 0)
        mock_logger.warning.assert_called_once()

    def test_sanitize_content_fields(self):
        """Test sanitization of content fields."""
        transformed_data = {
            'metadata': {},
            'conversations': {
                'conv1': {
                    'display_name': 'Conversation 1',
                    'messages': [
                        {
                            'id': 'msg1',
                            'raw_content': 123,  # Non-string content
                            'cleaned_content': None  # None content
                        }
                    ]
                }
            }
        }
        sanitized_data = validate_transformed_data_structure(transformed_data)
        self.assertEqual(sanitized_data['conversations']['conv1']['messages'][0]['raw_content'], '123')
        self.assertEqual(sanitized_data['conversations']['conv1']['messages'][0]['cleaned_content'], 'None')


class TestConnectionStringValidation(unittest.TestCase):
    """Tests for validate_connection_string function."""

    def test_valid_uri_connection_string(self):
        """Test validation with a valid URI connection string."""
        connection_string = "postgresql://user:password@localhost:5432/dbname"
        params = validate_connection_string(connection_string)
        self.assertEqual(params['user'], 'user')
        self.assertEqual(params['password'], 'password')
        self.assertEqual(params['host'], 'localhost')
        self.assertEqual(params['port'], '5432')
        self.assertEqual(params['dbname'], 'dbname')

    def test_valid_keyword_connection_string(self):
        """Test validation with a valid keyword connection string."""
        connection_string = "host=localhost port=5432 dbname=mydb user=postgres password=secret"
        params = validate_connection_string(connection_string)
        self.assertEqual(params['host'], 'localhost')
        self.assertEqual(params['port'], '5432')
        self.assertEqual(params['dbname'], 'mydb')
        self.assertEqual(params['user'], 'postgres')
        self.assertEqual(params['password'], 'secret')

    def test_empty_connection_string(self):
        """Test validation with an empty connection string."""
        with self.assertRaises(ETLValidationError):
            validate_connection_string("")

    def test_invalid_uri_format(self):
        """Test validation with an invalid URI format."""
        connection_string = "invalid://user:password@localhost:5432/dbname"
        with self.assertRaises(ETLValidationError):
            validate_connection_string(connection_string)

    def test_missing_required_params(self):
        """Test validation with missing required parameters."""
        connection_string = "postgresql://user@localhost/dbname"
        params = validate_connection_string(connection_string)
        self.assertEqual(params['user'], 'user')
        self.assertEqual(params['host'], 'localhost')
        self.assertEqual(params['dbname'], 'dbname')
        self.assertNotIn('password', params)

    def test_invalid_port(self):
        """Test validation with an invalid port."""
        connection_string = "postgresql://user:password@localhost:invalid/dbname"
        with self.assertRaises(ETLValidationError):
            validate_connection_string(connection_string)

    def test_quoted_values(self):
        """Test validation with quoted values."""
        connection_string = "host=localhost port=5432 dbname=mydb user='postgres' password='sec\"ret'"
        params = validate_connection_string(connection_string)
        self.assertEqual(params['user'], 'postgres')
        self.assertEqual(params['password'], 'sec\"ret')

    @patch('src.utils.etl_validation.logger')
    def test_invalid_parameter_format(self, mock_logger):
        """Test validation with an invalid parameter format."""
        connection_string = "host=localhost port=5432 dbname=mydb invalid_param user=postgres"
        params = validate_connection_string(connection_string)
        self.assertEqual(params['host'], 'localhost')
        self.assertEqual(params['port'], '5432')
        self.assertEqual(params['dbname'], 'mydb')
        self.assertEqual(params['user'], 'postgres')
        mock_logger.warning.assert_called_once()


if __name__ == '__main__':
    unittest.main()