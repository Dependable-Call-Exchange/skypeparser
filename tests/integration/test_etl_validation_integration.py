"""
Integration tests for ETL validation utilities.

This module contains integration tests for the ETL validation utilities
in src/utils/etl_validation.py, testing their interaction with real data
and database connections.
"""

import os
import unittest
import tempfile
import json
import psycopg2
from unittest.mock import patch, MagicMock

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
from src.db.etl.context import ETLContext
from tests.fixtures.skype_data import get_sample_skype_data


class TestETLValidationIntegration(unittest.TestCase):
    """Integration tests for ETL validation utilities."""

    def setUp(self):
        """Set up test environment."""
        # Create a temporary directory for test files
        self.temp_dir = tempfile.mkdtemp()

        # Sample database configuration
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password',
            'sslmode': 'prefer'
        }

        # Sample checkpoint data
        self.checkpoint_data = {
            'checkpoint_version': '1.0',
            'serialized_at': '2023-01-01T00:00:00',
            'context': {
                'db_config': self.db_config,
                'task_id': 'test-task',
                'output_dir': self.temp_dir,
                'memory_limit_mb': 1024,
                'parallel_processing': True,
                'chunk_size': 1000,
                'batch_size': 100,
                'max_workers': 4
            },
            'available_checkpoints': ['extract', 'transform'],
            'data_files': {}
        }

        # Sample transformed data
        self.transformed_data = {
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

    def tearDown(self):
        """Clean up test environment."""
        # Remove temporary directory and files
        for root, dirs, files in os.walk(self.temp_dir, topdown=False):
            for file in files:
                os.remove(os.path.join(root, file))
            for dir in dirs:
                os.rmdir(os.path.join(root, dir))
        os.rmdir(self.temp_dir)

    @patch('psycopg2.connect')
    def test_database_schema_validation_integration(self, mock_connect):
        """Test database schema validation with a mocked database connection."""
        # Mock cursor and fetchone
        mock_cursor = MagicMock()
        mock_cursor.__enter__.return_value = mock_cursor
        mock_cursor.fetchone.side_effect = [(True,), (True,), (True,)]  # All tables exist

        # Mock connection
        mock_conn = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Connect to the database
        conn = psycopg2.connect(
            host=self.db_config['host'],
            port=self.db_config['port'],
            dbname=self.db_config['dbname'],
            user=self.db_config['user'],
            password=self.db_config['password']
        )

        # Validate the database schema
        is_valid, missing_tables = validate_database_schema(conn)

        # Check the results
        self.assertTrue(is_valid)
        self.assertEqual(missing_tables, [])

        # Verify that the cursor was called with the expected SQL queries
        expected_calls = [
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s)",
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s)",
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s)"
        ]
        for i, call in enumerate(mock_cursor.execute.call_args_list):
            self.assertEqual(call[0][0].strip(), expected_calls[i])

    def test_checkpoint_data_validation_integration(self):
        """Test checkpoint data validation with real file operations."""
        # Create a checkpoint file
        checkpoint_file = os.path.join(self.temp_dir, 'checkpoint.json')
        with open(checkpoint_file, 'w') as f:
            json.dump(self.checkpoint_data, f)

        # Add the checkpoint file to the data_files
        self.checkpoint_data['data_files'] = {
            'raw_data': checkpoint_file
        }

        # Validate the checkpoint data
        self.assertTrue(validate_checkpoint_data(self.checkpoint_data))

        # Test with a non-existent file
        non_existent_file = os.path.join(self.temp_dir, 'non_existent.json')
        self.checkpoint_data['data_files'] = {
            'raw_data': non_existent_file
        }

        # Should still validate but log a warning
        with patch('src.utils.etl_validation.logger') as mock_logger:
            self.assertTrue(validate_checkpoint_data(self.checkpoint_data))
            mock_logger.warning.assert_called_once()

    def test_transformed_data_validation_integration(self):
        """Test transformed data validation with sample Skype data."""
        # Get sample Skype data
        sample_data = get_sample_skype_data()

        # Create a simple transformer to convert the sample data
        transformed_data = {
            'metadata': {
                'user_id': sample_data.get('userId', 'unknown'),
                'export_date': sample_data.get('exportDate', '2023-01-01')
            },
            'conversations': {}
        }

        # Add a conversation with some problematic data
        if 'conversations' in sample_data:
            for i, conv in enumerate(sample_data['conversations']):
                conv_id = f"conv{i}"
                transformed_data['conversations'][conv_id] = {
                    'display_name': f"Conversation {i}",
                    'messages': []
                }

                # Add some messages with problematic content
                if 'MessageList' in conv:
                    for j, msg in enumerate(conv['MessageList']):
                        transformed_data['conversations'][conv_id]['messages'].append({
                            'id': f"msg{j}",
                            'timestamp': msg.get('originalarrivaltime', '2023-01-01T00:00:00'),
                            'raw_content': 123 if j % 2 == 0 else "Hello",  # Non-string content
                            'cleaned_content': None if j % 3 == 0 else "Cleaned content"  # None content
                        })

        # Validate and sanitize the transformed data
        sanitized_data = validate_transformed_data_structure(transformed_data)

        # Check that the sanitized data has the expected structure
        self.assertIn('metadata', sanitized_data)
        self.assertIn('conversations', sanitized_data)

        # Check that all conversations have been sanitized
        for conv_id, conv in sanitized_data['conversations'].items():
            self.assertIn('display_name', conv)
            self.assertIn('messages', conv)

            # Check that all messages have been sanitized
            for msg in conv['messages']:
                self.assertIn('id', msg)
                self.assertIn('timestamp', msg)

                # Check that content fields are strings
                if 'raw_content' in msg:
                    self.assertIsInstance(msg['raw_content'], str)
                if 'cleaned_content' in msg:
                    self.assertIsInstance(msg['cleaned_content'], str)

    def test_connection_string_validation_integration(self):
        """Test connection string validation with various formats."""
        # Test URI format
        uri_string = f"postgresql://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
        params = validate_connection_string(uri_string)
        self.assertEqual(params['user'], self.db_config['user'])
        self.assertEqual(params['host'], self.db_config['host'])
        self.assertEqual(params['port'], str(self.db_config['port']))  # Convert to string for comparison

        # Test keyword format
        keyword_string = f"host={self.db_config['host']} port={self.db_config['port']} dbname={self.db_config['dbname']} user={self.db_config['user']} password={self.db_config['password']}"
        params = validate_connection_string(keyword_string)
        self.assertEqual(params['host'], self.db_config['host'])
        self.assertEqual(params['port'], str(self.db_config['port']))  # Convert to string for comparison
        self.assertEqual(params['dbname'], self.db_config['dbname'])

    def test_etl_context_integration(self):
        """Test integration with ETLContext."""
        # Create an ETLContext
        context = ETLContext(
            db_config=self.db_config,
            output_dir=self.temp_dir,
            memory_limit_mb=1024,
            parallel_processing=True,
            chunk_size=1000,
            batch_size=100,
            max_workers=4,
            task_id='test-task'
        )

        # Serialize the context to a checkpoint
        checkpoint_data = context.serialize_checkpoint()

        # Validate the checkpoint data
        self.assertTrue(validate_checkpoint_data(checkpoint_data))

        # Save the checkpoint to a file
        checkpoint_file = context.save_checkpoint_to_file(self.temp_dir)

        # Check that the file exists
        self.assertTrue(os.path.exists(checkpoint_file))

        # Load the checkpoint data from the file
        with open(checkpoint_file, 'r') as f:
            loaded_checkpoint = json.load(f)

        # Validate the loaded checkpoint data
        self.assertTrue(validate_checkpoint_data(loaded_checkpoint))


if __name__ == '__main__':
    unittest.main()