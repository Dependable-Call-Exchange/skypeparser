#!/usr/bin/env python3
"""
Tests for the database connection module.

This module contains test cases for the DatabaseConnection class in src.db.connection.
"""

import unittest
from unittest.mock import Mock, patch
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.db.connection import DatabaseConnection
from src.utils.interfaces import DatabaseConnectionProtocol


class TestDatabaseConnection(unittest.TestCase):
    """Test cases for the DatabaseConnection class."""

    def setUp(self):
        """Set up test fixtures."""
        self.db_config = {
            'host': 'localhost',
            'port': 5432,
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password'
        }

        # Create a mock cursor and connection
        self.mock_cursor = Mock()
        self.mock_connection = Mock()
        self.mock_connection.cursor.return_value = self.mock_cursor

    @patch('psycopg2.connect')
    def test_initialization(self, mock_connect):
        """Test initialization of DatabaseConnection."""
        # Create a DatabaseConnection instance
        db_connection = DatabaseConnection(self.db_config)

        # Verify that the db_config is stored correctly
        self.assertEqual(db_connection.db_config, self.db_config)

        # Verify that connection and cursor are initially None
        self.assertIsNone(db_connection.connection)
        self.assertIsNone(db_connection.cursor)

        # Verify that the class implements the DatabaseConnectionProtocol
        self.assertIsInstance(db_connection, DatabaseConnectionProtocol)

    @patch('psycopg2.connect')
    def test_connect(self, mock_connect):
        """Test connect method."""
        # Set up the mock
        mock_connect.return_value = self.mock_connection

        # Create a DatabaseConnection instance and connect
        db_connection = DatabaseConnection(self.db_config)
        db_connection.connect()

        # Verify that connect was called with the correct arguments
        mock_connect.assert_called_once_with(
            host=self.db_config['host'],
            port=self.db_config['port'],
            dbname=self.db_config['dbname'],
            user=self.db_config['user'],
            password=self.db_config['password']
        )

        # Verify that connection and cursor are set
        self.assertEqual(db_connection.connection, self.mock_connection)
        self.assertEqual(db_connection.cursor, self.mock_cursor)

    @patch('psycopg2.connect')
    def test_disconnect(self, mock_connect):
        """Test disconnect method."""
        # Set up the mock
        mock_connect.return_value = self.mock_connection

        # Create a DatabaseConnection instance, connect, and disconnect
        db_connection = DatabaseConnection(self.db_config)
        db_connection.connect()
        db_connection.disconnect()

        # Verify that cursor and connection are closed
        self.mock_cursor.close.assert_called_once()
        self.mock_connection.close.assert_called_once()

        # Verify that connection and cursor are set to None
        self.assertIsNone(db_connection.connection)
        self.assertIsNone(db_connection.cursor)

    @patch('psycopg2.connect')
    def test_execute(self, mock_connect):
        """Test execute method."""
        # Set up the mock
        mock_connect.return_value = self.mock_connection

        # Create a DatabaseConnection instance and connect
        db_connection = DatabaseConnection(self.db_config)
        db_connection.connect()

        # Mock the execute_query method
        db_connection.execute_query = Mock(return_value=[{'id': 1, 'name': 'test'}])

        # Call the execute method
        result = db_connection.execute('SELECT * FROM test', {'param': 'value'})

        # Verify that execute_query was called with the correct arguments
        db_connection.execute_query.assert_called_once_with('SELECT * FROM test', {'param': 'value'})

        # Verify that the result is correct
        self.assertEqual(result, [{'id': 1, 'name': 'test'}])

    @patch('psycopg2.connect')
    def test_execute_query(self, mock_connect):
        """Test execute_query method."""
        # Set up the mock
        mock_connect.return_value = self.mock_connection

        # Set up the cursor mock to return some rows
        self.mock_cursor.description = [('id',), ('name',)]
        self.mock_cursor.fetchall.return_value = [(1, 'test'), (2, 'test2')]

        # Create a DatabaseConnection instance and connect
        db_connection = DatabaseConnection(self.db_config)
        db_connection.connect()

        # Call the execute_query method
        result = db_connection.execute_query('SELECT * FROM test', {'param': 'value'})

        # Verify that cursor.execute was called with the correct arguments
        self.mock_cursor.execute.assert_called_once_with('SELECT * FROM test', {'param': 'value'})

        # Verify that the result is correct
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['id'], 1)
        self.assertEqual(result[0]['name'], 'test')
        self.assertEqual(result[1]['id'], 2)
        self.assertEqual(result[1]['name'], 'test2')

    @patch('psycopg2.connect')
    def test_execute_batch(self, mock_connect):
        """Test execute_batch method."""
        # Set up the mock
        mock_connect.return_value = self.mock_connection

        # Create a DatabaseConnection instance and connect
        db_connection = DatabaseConnection(self.db_config)
        db_connection.connect()

        # Call the execute_batch method
        with patch('psycopg2.extras.execute_values') as mock_execute_values:
            db_connection.execute_batch(
                'INSERT INTO test (id, name) VALUES %s',
                [{'id': 1, 'name': 'test'}, {'id': 2, 'name': 'test2'}]
            )

            # Verify that execute_values was called with the correct arguments
            mock_execute_values.assert_called_once_with(
                self.mock_cursor,
                'INSERT INTO test (id, name) VALUES %s',
                [{'id': 1, 'name': 'test'}, {'id': 2, 'name': 'test2'}]
            )

    @patch('psycopg2.connect')
    def test_commit(self, mock_connect):
        """Test commit method."""
        # Set up the mock
        mock_connect.return_value = self.mock_connection

        # Create a DatabaseConnection instance and connect
        db_connection = DatabaseConnection(self.db_config)
        db_connection.connect()

        # Call the commit method
        db_connection.commit()

        # Verify that connection.commit was called
        self.mock_connection.commit.assert_called_once()

    @patch('psycopg2.connect')
    def test_rollback(self, mock_connect):
        """Test rollback method."""
        # Set up the mock
        mock_connect.return_value = self.mock_connection

        # Create a DatabaseConnection instance and connect
        db_connection = DatabaseConnection(self.db_config)
        db_connection.connect()

        # Call the rollback method
        db_connection.rollback()

        # Verify that connection.rollback was called
        self.mock_connection.rollback.assert_called_once()


if __name__ == '__main__':
    unittest.main()