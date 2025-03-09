"""
Mock implementation of database connection for testing.

This module provides a mock implementation of the database connection
for use in unit tests.
"""

from unittest.mock import MagicMock


class MockDatabase:
    """
    Mock database for testing.

    This class provides a mock implementation of a database connection
    with configurable behavior for testing different scenarios.
    """

    def __init__(self, should_fail=False):
        """
        Initialize the mock database.

        Args:
            should_fail: Whether database operations should fail
        """
        self.should_fail = should_fail
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.execute_count = 0
        self.queries = []
        self.params = []

        # Create a mock cursor with a connection attribute that has encoding
        self.mock_cursor = MagicMock()
        self.mock_cursor.connection = MagicMock()
        self.mock_cursor.connection.encoding = "UTF8"

        # Set up fetchone to return a mock result with __getitem__ implemented
        mock_result = MagicMock()
        mock_result.__getitem__.side_effect = lambda key: "mock_value"
        self.mock_cursor.fetchone.return_value = mock_result

        # Set up mogrify to return bytes instead of a MagicMock
        self.mock_cursor.mogrify.side_effect = (
            lambda template, args: b"MOCK_MOGRIFIED_SQL"
        )

        # Set up execute to increment the execute_count and track queries
        original_execute = self.mock_cursor.execute

        def execute_with_count(query, params=None):
            self.execute_count += 1
            self.queries.append(query)
            self.params.append(params)
            return original_execute(query, params)

        self.mock_cursor.execute = execute_with_count

        # Set up __enter__ and __exit__ for cursor context manager support
        self.mock_cursor.__enter__ = MagicMock(return_value=self.mock_cursor)
        self.mock_cursor.__exit__ = MagicMock(return_value=None)

        # Make self a context manager too
        self.__enter__ = MagicMock(return_value=self)
        self.__exit__ = MagicMock(return_value=None)

    def cursor(self):
        """
        Return a mock cursor.

        Returns:
            Mock cursor object
        """
        return self.mock_cursor

    def execute(self, query, params=None):
        """
        Execute a query.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            Mock cursor

        Raises:
            Exception: If should_fail is True
        """
        if self.should_fail:
            raise Exception("Mock database error")
        self.execute_count += 1
        self.queries.append(query)
        self.params.append(params)
        return self.mock_cursor

    def commit(self):
        """Commit the transaction."""
        self.committed = True

    def rollback(self):
        """Rollback the transaction."""
        self.rolled_back = True

    def close(self):
        """Close the connection."""
        self.closed = True

    def begin(self):
        """Begin a transaction."""
        # Just record that a transaction was begun
        self.transaction_begun = True

    def begin_transaction(self):
        """Begin a transaction (alias for begin)."""
        # This is the method BulkInsertionStrategy calls
        return self.begin()

    def bulk_insert(self, table: str, columns: list, values: list, batch_size: int = 1000) -> int:
        """
        Insert multiple rows into a table.

        Args:
            table: Table name
            columns: Column names
            values: Values to insert
            batch_size: Batch size for bulk inserts

        Returns:
            Number of rows inserted or MagicMock representing success
        """
        if self.should_fail:
            raise Exception(f"Mock database error during bulk insert to {table}")

        # Record the operation
        self.queries.append(f"BULK INSERT INTO {table} ({', '.join(columns)}) VALUES ...")
        self.params.append(values)

        # Return a MagicMock that can be used in further operations
        # This allows code that uses the return value to continue functioning
        return MagicMock()

    def get_executed_queries(self):
        """
        Get all executed queries.

        Returns:
            List of executed queries
        """
        return self.queries