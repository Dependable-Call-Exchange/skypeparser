#!/usr/bin/env python3
"""
Integration tests for database schema and data integrity.

This test suite focuses on validating the database schema and ensuring
data integrity after ETL operations.
"""

import os
import sys
import unittest
import tempfile
import json
import pytest
import psycopg2
from psycopg2.extras import RealDictCursor

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl import ETLPipeline
from src.utils.config import get_db_config
from tests.fixtures import (
    BASIC_SKYPE_DATA,
    COMPLEX_SKYPE_DATA,
    test_db_connection,
    is_db_available
)


@pytest.mark.integration
class TestDatabaseSchemaIntegrity(unittest.TestCase):
    """Integration tests for database schema and data integrity."""

    def setUp(self):
        """Set up the test environment."""
        # Skip integration tests unless database is available
        if not is_db_available():
            self.skipTest("Integration tests disabled. Database not available.")

        # Set up the test environment
        self.temp_dir = tempfile.mkdtemp()
        self.test_dir = os.path.join(self.temp_dir, 'test_output')
        os.makedirs(self.test_dir, exist_ok=True)

        # Get database configuration
        self.db_config = get_test_db_config()

        # Create a sample Skype export data
        self.sample_data = BASIC_SKYPE_DATA

        # Create a file with the sample data
        self.sample_file = os.path.join(self.temp_dir, 'sample.json')
        with open(self.sample_file, 'w') as f:
            json.dump(self.sample_data, f)

        # Run the pipeline to populate the database
        self.pipeline = ETLPipeline(db_config=self.db_config, output_dir=self.test_dir)
        self.result = self.pipeline.run_pipeline(file_path=self.sample_file)

    def tearDown(self):
        """Clean up after the test."""
        # Clean up temporary files
        import shutil
        shutil.rmtree(self.temp_dir)

        # Clean up database tables
        with test_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DROP TABLE IF EXISTS messages CASCADE")
            cursor.execute("DROP TABLE IF EXISTS conversations CASCADE")
            cursor.execute("DROP TABLE IF EXISTS participants CASCADE")
            cursor.execute("DROP TABLE IF EXISTS message_content CASCADE")
            cursor.execute("DROP TABLE IF EXISTS message_metadata CASCADE")
            conn.commit()

    def test_schema_creation(self):
        """Test that the database schema is created correctly."""
        # Connect to the database
        with test_db_connection() as conn:
            cursor = conn.cursor()

            # Check if tables exist
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
            """)
            tables = [row[0] for row in cursor.fetchall()]

            # Verify required tables exist
            required_tables = ['messages', 'conversations', 'participants',
                              'message_content', 'message_metadata']
            for table in required_tables:
                self.assertIn(table, tables, f"Table {table} does not exist")

            # Check messages table structure
            cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'messages'
            """)
            columns = {row[0]: row[1] for row in cursor.fetchall()}

            # Verify required columns exist with correct types
            self.assertIn('id', columns, "Column 'id' missing from messages table")
            self.assertIn('conversation_id', columns, "Column 'conversation_id' missing from messages table")
            self.assertIn('timestamp', columns, "Column 'timestamp' missing from messages table")
            self.assertIn('message_type', columns, "Column 'message_type' missing from messages table")

            # Check foreign key constraints
            cursor.execute("""
                SELECT tc.constraint_name, tc.table_name, kcu.column_name,
                       ccu.table_name AS foreign_table_name,
                       ccu.column_name AS foreign_column_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                JOIN information_schema.constraint_column_usage AS ccu
                  ON ccu.constraint_name = tc.constraint_name
                WHERE constraint_type = 'FOREIGN KEY'
            """)
            foreign_keys = cursor.fetchall()

            # Verify at least one foreign key exists
            self.assertTrue(len(foreign_keys) > 0, "No foreign key constraints found")

    def test_data_integrity(self):
        """Test that data is loaded correctly and maintains integrity."""
        # Connect to the database
        with test_db_connection() as conn:
            cursor = conn.cursor(cursor_factory=RealDictCursor)

            # Count messages
            cursor.execute("SELECT COUNT(*) as count FROM messages")
            message_count = cursor.fetchone()['count']
            self.assertEqual(message_count, len(self.sample_data.get('messages', [])),
                            "Message count in database does not match input data")

            # Check conversation data
            cursor.execute("SELECT * FROM conversations LIMIT 1")
            conversation = cursor.fetchone()
            self.assertIsNotNone(conversation, "No conversation data found")
            self.assertIn('id', conversation, "Conversation missing 'id' field")
            self.assertIn('display_name', conversation, "Conversation missing 'display_name' field")

            # Check message content
            cursor.execute("""
                SELECT m.id, mc.content
                FROM messages m
                JOIN message_content mc ON m.id = mc.message_id
                LIMIT 5
            """)
            message_contents = cursor.fetchall()
            self.assertTrue(len(message_contents) > 0, "No message content found")

            # Verify content is not empty for text messages
            cursor.execute("""
                SELECT m.id, m.message_type, mc.content
                FROM messages m
                JOIN message_content mc ON m.id = mc.message_id
                WHERE m.message_type = 'Text'
                LIMIT 5
            """)
            text_messages = cursor.fetchall()
            for msg in text_messages:
                self.assertTrue(msg['content'], f"Empty content for text message {msg['id']}")

    def test_referential_integrity(self):
        """Test referential integrity between tables."""
        # Connect to the database
        with test_db_connection() as conn:
            cursor = conn.cursor()

            # Check message to message_content relationship
            cursor.execute("""
                SELECT COUNT(*) FROM messages m
                LEFT JOIN message_content mc ON m.id = mc.message_id
                WHERE mc.message_id IS NULL
            """)
            orphaned_messages = cursor.fetchone()[0]
            self.assertEqual(orphaned_messages, 0,
                            f"Found {orphaned_messages} messages without content")

            # Check message to conversation relationship
            cursor.execute("""
                SELECT COUNT(*) FROM messages m
                LEFT JOIN conversations c ON m.conversation_id = c.id
                WHERE c.id IS NULL
            """)
            orphaned_conversations = cursor.fetchone()[0]
            self.assertEqual(orphaned_conversations, 0,
                            f"Found {orphaned_conversations} messages with invalid conversation_id")

            # Check participant relationships
            cursor.execute("""
                SELECT COUNT(*) FROM participants p
                LEFT JOIN conversations c ON p.conversation_id = c.id
                WHERE c.id IS NULL
            """)
            orphaned_participants = cursor.fetchone()[0]
            self.assertEqual(orphaned_participants, 0,
                            f"Found {orphaned_participants} participants with invalid conversation_id")

    def test_data_types_and_constraints(self):
        """Test that data types and constraints are enforced."""
        # Connect to the database
        with test_db_connection() as conn:
            cursor = conn.cursor()

            # Check primary key constraints
            cursor.execute("""
                SELECT tc.table_name, kcu.column_name
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu
                  ON tc.constraint_name = kcu.constraint_name
                WHERE tc.constraint_type = 'PRIMARY KEY'
            """)
            primary_keys = cursor.fetchall()

            # Verify each table has a primary key
            tables_with_pk = set(pk[0] for pk in primary_keys)
            required_tables = ['messages', 'conversations', 'participants',
                              'message_content', 'message_metadata']
            for table in required_tables:
                self.assertIn(table, tables_with_pk, f"Table {table} missing primary key")

            # Check not null constraints
            cursor.execute("""
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE is_nullable = 'NO' AND table_schema = 'public'
            """)
            not_null_columns = cursor.fetchall()

            # Verify essential columns have not null constraints
            essential_columns = [
                ('messages', 'id'),
                ('messages', 'conversation_id'),
                ('conversations', 'id'),
                ('message_content', 'message_id')
            ]
            for table, column in essential_columns:
                self.assertIn((table, column), not_null_columns,
                             f"Column {table}.{column} should have NOT NULL constraint")

    def test_incremental_data_integrity(self):
        """Test data integrity with incremental data loading."""
        # Create additional data file
        additional_file = os.path.join(self.temp_dir, 'additional.json')
        with open(additional_file, 'w') as f:
            json.dump(COMPLEX_SKYPE_DATA, f)

        # Run the pipeline with additional data
        additional_result = self.pipeline.run_pipeline(file_path=additional_file)
        self.assertEqual(additional_result['status'], 'completed',
                         "Failed to process additional data")

        # Connect to the database
        with test_db_connection() as conn:
            cursor = conn.cursor()

            # Count total messages
            cursor.execute("SELECT COUNT(*) FROM messages")
            total_count = cursor.fetchone()[0]

            # Calculate expected count
            expected_count = (len(self.sample_data["conversations"][0]["MessageList"]) +
                             len(COMPLEX_SKYPE_DATA["conversations"][0]["MessageList"]))

            # Verify total count
            self.assertEqual(total_count, expected_count,
                            f"Expected {expected_count} messages, found {total_count}")


def get_test_db_config():
    """Get database configuration for tests."""
    # Try to get from environment variables first
    db_config = get_db_config()

    # If not available, use test defaults
    if not db_config:
        db_config = {
            'host': 'localhost',
            'port': 5432,
            'database': 'test_db',
            'user': 'test_user',
            'password': 'test_password'
        }

    return db_config


if __name__ == '__main__':
    unittest.main()