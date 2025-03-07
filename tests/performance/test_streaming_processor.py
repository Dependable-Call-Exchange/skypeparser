#!/usr/bin/env python3
"""
Performance tests for the streaming processor.

This test suite focuses on testing the streaming processor's performance
with very large datasets.
"""

import os
import sys
import unittest
import tempfile
import json
import time
import psutil
import pytest
import threading
from unittest.mock import patch, MagicMock

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl import ETLContext
from src.db.etl.streaming_processor import StreamingProcessor
from src.utils.config import get_db_config
from src.db.etl.utils import MemoryMonitor
from tests.fixtures import (
    BASIC_SKYPE_DATA,
    COMPLEX_SKYPE_DATA,
    test_db_connection,
    is_db_available
)


@pytest.mark.performance
class TestStreamingProcessor(unittest.TestCase):
    """Performance tests for the streaming processor."""

    def setUp(self):
        """Set up the test environment."""
        # Skip performance tests unless database is available
        if not is_db_available():
            self.skipTest("Performance tests disabled. Database not available.")

        # Set up the test environment
        self.temp_dir = tempfile.mkdtemp()
        self.test_dir = os.path.join(self.temp_dir, 'test_output')
        os.makedirs(self.test_dir, exist_ok=True)

        # Get database configuration
        self.db_config = get_test_db_config()

        # Create a large dataset for performance testing
        self.large_data = {
            "userId": "test-user-id",
            "exportDate": "2023-01-01T12:00:00Z",
            "conversations": []
        }

        # Add 10 conversations with 1000 messages each
        for c in range(10):
            conversation = {
                "id": f"conversation{c}",
                "displayName": f"Test Conversation {c}",
                "MessageList": []
            }

            # Add 1000 messages to the conversation
            for i in range(1000):
                message = {
                    "id": f"message{c}_{i}",
                    "content": f"Test message {i} in conversation {c}",
                    "from": "user1",
                    "displayName": "Test User",
                    "originalarrivaltime": f"2023-01-01T{i//60:02d}:{i%60:02d}:00Z",
                    "messagetype": "RichText" if i % 2 == 0 else "Text"
                }
                conversation["MessageList"].append(message)

            self.large_data["conversations"].append(conversation)

        # Create a file with the large data
        self.large_file = os.path.join(self.temp_dir, 'large_sample.json')
        with open(self.large_file, 'w') as f:
            json.dump(self.large_data, f)

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

    def test_streaming_extraction(self):
        """Test streaming extraction performance."""
        # Create ETL context
        context = ETLContext(
            db_config=self.db_config,
            output_dir=self.test_dir,
            memory_limit_mb=1024
        )

        # Create streaming processor
        processor = StreamingProcessor(context=context)

        # Measure memory usage during streaming extraction
        memory_monitor = MemoryMonitor()
        memory_monitor.start()

        # Start timing
        start_time = time.time()

        # Stream extract
        conversation_count = 0
        message_count = 0
        for conversation in processor.stream_extract(self.large_file):
            conversation_count += 1
            message_count += len(conversation.get("MessageList", []))

        # End timing
        end_time = time.time()
        extraction_time = end_time - start_time

        # Stop memory monitoring
        memory_monitor.stop()
        peak_memory = memory_monitor.peak_memory_mb

        # Verify all conversations and messages were extracted
        self.assertEqual(conversation_count, 10, "Not all conversations were extracted")
        self.assertEqual(message_count, 10000, "Not all messages were extracted")

        # Log performance metrics
        print(f"\nStreaming extraction time: {extraction_time:.2f}s")
        print(f"Peak memory usage: {peak_memory:.2f}MB")
        print(f"Extraction rate: {message_count / extraction_time:.2f} messages per second")

        # Verify memory usage is reasonable
        # For a 10,000 message dataset, memory usage should be much less than loading the entire dataset
        self.assertLess(peak_memory, 200, "Memory usage is too high for streaming extraction")

    def test_streaming_transform_load(self):
        """Test streaming transform and load performance."""
        # Create ETL context
        context = ETLContext(
            db_config=self.db_config,
            output_dir=self.test_dir,
            memory_limit_mb=1024
        )

        # Create database connection mock
        db_connection = MagicMock()
        db_connection.execute_batch = MagicMock()

        # Create streaming processor
        processor = StreamingProcessor(context=context, db_connection=db_connection)

        # Measure memory usage during streaming transform and load
        memory_monitor = MemoryMonitor()
        memory_monitor.start()

        # Start timing
        start_time = time.time()

        # Stream extract
        conversation_iterator = processor.stream_extract(self.large_file)

        # Stream transform and load
        result = processor.stream_transform_load(
            conversation_iterator=conversation_iterator,
            batch_size=100
        )

        # End timing
        end_time = time.time()
        transform_load_time = end_time - start_time

        # Stop memory monitoring
        memory_monitor.stop()
        peak_memory = memory_monitor.peak_memory_mb

        # Verify all conversations and messages were processed
        self.assertEqual(result['conversations_processed'], 10, "Not all conversations were processed")
        self.assertEqual(result['messages_processed'], 10000, "Not all messages were processed")

        # Verify database batch insert was called
        self.assertTrue(db_connection.execute_batch.called, "Database batch insert was not called")

        # Log performance metrics
        print(f"\nStreaming transform and load time: {transform_load_time:.2f}s")
        print(f"Peak memory usage: {peak_memory:.2f}MB")
        print(f"Processing rate: {result['messages_processed'] / result['duration_seconds']:.2f} messages per second")

        # Verify memory usage is reasonable
        # For a 10,000 message dataset, memory usage should be much less than loading the entire dataset
        self.assertLess(peak_memory, 200, "Memory usage is too high for streaming transform and load")

    def test_compare_with_standard_pipeline(self):
        """Compare streaming processor with standard ETL pipeline."""
        from src.db.etl import ETLPipeline

        # Create standard ETL pipeline
        standard_pipeline = ETLPipeline(
            db_config=self.db_config,
            output_dir=self.test_dir,
            memory_limit_mb=1024
        )

        # Measure memory usage during standard pipeline execution
        memory_monitor = MemoryMonitor()
        memory_monitor.start()

        # Start timing
        start_time = time.time()

        # Run standard pipeline
        standard_result = standard_pipeline.run_pipeline(file_path=self.large_file)

        # End timing
        end_time = time.time()
        standard_time = end_time - start_time

        # Stop memory monitoring
        memory_monitor.stop()
        standard_peak_memory = memory_monitor.peak_memory_mb

        # Create ETL context for streaming processor
        context = ETLContext(
            db_config=self.db_config,
            output_dir=self.test_dir,
            memory_limit_mb=1024
        )

        # Create database connection mock
        db_connection = MagicMock()
        db_connection.execute_batch = MagicMock()

        # Create streaming processor
        processor = StreamingProcessor(context=context, db_connection=db_connection)

        # Measure memory usage during streaming processor execution
        memory_monitor = MemoryMonitor()
        memory_monitor.start()

        # Start timing
        start_time = time.time()

        # Stream extract
        conversation_iterator = processor.stream_extract(self.large_file)

        # Stream transform and load
        streaming_result = processor.stream_transform_load(
            conversation_iterator=conversation_iterator,
            batch_size=100
        )

        # End timing
        end_time = time.time()
        streaming_time = end_time - start_time

        # Stop memory monitoring
        memory_monitor.stop()
        streaming_peak_memory = memory_monitor.peak_memory_mb

        # Log performance comparison
        print(f"\nStandard pipeline time: {standard_time:.2f}s")
        print(f"Streaming processor time: {streaming_time:.2f}s")
        print(f"Time difference: {standard_time - streaming_time:.2f}s ({(1 - streaming_time / standard_time) * 100:.2f}%)")
        print(f"Standard pipeline peak memory: {standard_peak_memory:.2f}MB")
        print(f"Streaming processor peak memory: {streaming_peak_memory:.2f}MB")
        print(f"Memory difference: {standard_peak_memory - streaming_peak_memory:.2f}MB ({(1 - streaming_peak_memory / standard_peak_memory) * 100:.2f}%)")

        # Verify streaming processor uses less memory
        self.assertLess(streaming_peak_memory, standard_peak_memory, "Streaming processor should use less memory than standard pipeline")

        # Verify streaming processor processes all messages
        self.assertEqual(streaming_result['messages_processed'], 10000, "Not all messages were processed by streaming processor")


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