#!/usr/bin/env python3
"""
Extended performance tests for the ETL pipeline.

This test suite provides comprehensive performance testing for the ETL pipeline,
measuring memory usage, processing time, and database operations for large datasets.
"""

import json
import os
import sys
import tempfile
import time
import unittest
from datetime import datetime
from unittest.mock import patch

import psutil
import pytest

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.db.etl import ETLPipeline
from src.db.etl.streaming_processor import StreamingProcessor
from src.utils.config import get_db_config
from tests.fixtures import (
    SkypeConversationFactory,
    SkypeDataFactory,
    SkypeMessageFactory,
    is_db_available,
    test_db_connection,
)


def get_memory_usage():
    """Get current memory usage in bytes."""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss


class PerformanceMetrics:
    """Class to track and report performance metrics."""

    def __init__(self, name):
        """Initialize with a test name."""
        self.name = name
        self.start_time = None
        self.end_time = None
        self.start_memory = None
        self.peak_memory = None
        self.end_memory = None
        self.metrics = {}

    def start(self):
        """Start tracking performance."""
        self.start_time = time.time()
        self.start_memory = get_memory_usage()
        self.peak_memory = self.start_memory
        return self

    def update_peak_memory(self):
        """Update peak memory usage."""
        current = get_memory_usage()
        if current > self.peak_memory:
            self.peak_memory = current

    def stop(self):
        """Stop tracking performance."""
        self.end_time = time.time()
        self.end_memory = get_memory_usage()
        self.calculate_metrics()
        return self

    def calculate_metrics(self):
        """Calculate performance metrics."""
        self.metrics = {
            "test_name": self.name,
            "duration_seconds": self.end_time - self.start_time,
            "memory_start_mb": self.start_memory / (1024 * 1024),
            "memory_peak_mb": self.peak_memory / (1024 * 1024),
            "memory_end_mb": self.end_memory / (1024 * 1024),
            "memory_used_mb": (self.peak_memory - self.start_memory) / (1024 * 1024),
            "timestamp": datetime.now().isoformat(),
        }

    def log_metrics(self, output_file=None):
        """Log performance metrics to console and optionally to a file."""
        print(f"\n--- Performance Metrics for {self.name} ---")
        print(f"Duration: {self.metrics['duration_seconds']:.2f} seconds")
        print(f"Memory Start: {self.metrics['memory_start_mb']:.2f} MB")
        print(f"Memory Peak: {self.metrics['memory_peak_mb']:.2f} MB")
        print(f"Memory End: {self.metrics['memory_end_mb']:.2f} MB")
        print(f"Memory Used: {self.metrics['memory_used_mb']:.2f} MB")

        if output_file:
            with open(output_file, "a") as f:
                f.write(json.dumps(self.metrics) + "\n")


def create_large_test_file(
    file_path, conversation_count=10, messages_per_conversation=100
):
    """Create a large test file with the specified number of conversations and messages."""
    large_data = SkypeDataFactory.build(
        userId="performance-test-user",
        exportDate=datetime.now().isoformat(),
        conversations=[
            SkypeConversationFactory.build(
                id=f"conversation{i}",
                displayName=f"Performance Test Conversation {i}",
                MessageList=[
                    SkypeMessageFactory.build(
                        id=f"msg_{i}_{j}",
                        from_id=f"user{j % 5}",
                        from_name=f"User {j % 5}",
                        content=f"Test message {j} in conversation {i}. This message has some additional content to make it more realistic.",
                    )
                    for j in range(messages_per_conversation)
                ],
            )
            for i in range(conversation_count)
        ],
    )

    with open(file_path, "w") as f:
        json.dump(large_data, f)

    return file_path


@pytest.mark.performance
class TestETLPerformanceExtended(unittest.TestCase):
    """Extended performance tests for the ETL pipeline."""

    @classmethod
    def setUpClass(cls):
        """Set up class-level test environment."""
        # Create performance results directory
        cls.results_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "results"
        )
        os.makedirs(cls.results_dir, exist_ok=True)

        # Create results file
        cls.results_file = os.path.join(
            cls.results_dir,
            f'performance_results_{datetime.now().strftime("%Y%m%d_%H%M%S")}.jsonl',
        )

    def setUp(self):
        """Set up the test environment."""
        # Skip performance tests unless database is available
        if not is_db_available():
            self.skipTest("Performance tests disabled. Database not available.")

        # Set up temporary directories
        self.temp_dir = tempfile.mkdtemp()
        self.test_dir = os.path.join(self.temp_dir, "test_output")
        os.makedirs(self.test_dir, exist_ok=True)

        # Get database configuration
        self.db_config = get_test_db_config()

    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary directory
        import shutil

        shutil.rmtree(self.temp_dir)

    def test_medium_dataset_performance(self):
        """Test performance with a medium-sized dataset (10 conversations, 100 messages each)."""
        # Create test file
        test_file = os.path.join(self.temp_dir, "medium_data.json")
        create_large_test_file(
            test_file, conversation_count=10, messages_per_conversation=100
        )

        # Create ETL pipeline
        pipeline = ETLPipeline(db_config=self.db_config, output_dir=self.test_dir)

        # Track performance
        metrics = PerformanceMetrics("medium_dataset_performance").start()

        # Run pipeline
        result = pipeline.run_pipeline(
            file_path=test_file, user_display_name="Performance Test User"
        )

        # Periodically update peak memory
        metrics.update_peak_memory()

        # Stop tracking and log results
        metrics.stop().log_metrics(self.results_file)

        # Verify pipeline success
        self.assertTrue(
            result["success"],
            f"Pipeline failed: {result.get('error', 'Unknown error')}",
        )

        # Performance assertions
        self.assertLess(
            metrics.metrics["duration_seconds"],
            30,
            "Processing took too long (>30 seconds)",
        )
        self.assertLess(
            metrics.metrics["memory_used_mb"], 200, "Memory usage too high (>200 MB)"
        )

    def test_large_dataset_performance(self):
        """Test performance with a large dataset (50 conversations, 200 messages each)."""
        # Create test file
        test_file = os.path.join(self.temp_dir, "large_data.json")
        create_large_test_file(
            test_file, conversation_count=50, messages_per_conversation=200
        )

        # Create ETL pipeline
        pipeline = ETLPipeline(db_config=self.db_config, output_dir=self.test_dir)

        # Track performance
        metrics = PerformanceMetrics("large_dataset_performance").start()

        # Run pipeline
        result = pipeline.run_pipeline(
            file_path=test_file, user_display_name="Performance Test User"
        )

        # Periodically update peak memory
        metrics.update_peak_memory()

        # Stop tracking and log results
        metrics.stop().log_metrics(self.results_file)

        # Verify pipeline success
        self.assertTrue(
            result["success"],
            f"Pipeline failed: {result.get('error', 'Unknown error')}",
        )

        # Performance assertions (adjust thresholds as needed)
        self.assertLess(
            metrics.metrics["duration_seconds"],
            120,
            "Processing took too long (>120 seconds)",
        )
        self.assertLess(
            metrics.metrics["memory_used_mb"], 500, "Memory usage too high (>500 MB)"
        )

    def test_streaming_processor_performance(self):
        """Test performance of the streaming processor with a large dataset."""
        # Create test file
        test_file = os.path.join(self.temp_dir, "streaming_data.json")
        create_large_test_file(
            test_file, conversation_count=30, messages_per_conversation=300
        )

        # Create streaming processor
        processor = StreamingProcessor(
            db_config=self.db_config,
            output_dir=self.test_dir,
            batch_size=100,  # Process in batches of 100 messages
            checkpoint_interval=1000,  # Create checkpoint every 1000 messages
        )

        # Track performance
        metrics = PerformanceMetrics("streaming_processor_performance").start()

        # Process file
        result = processor.process_file(
            file_path=test_file, user_display_name="Streaming Performance Test"
        )

        # Periodically update peak memory during processing
        checkpoint_times = []
        for i in range(10):
            checkpoint_start = time.time()
            metrics.update_peak_memory()
            time.sleep(0.5)  # Sleep to allow processing to continue
            checkpoint_times.append(time.time() - checkpoint_start)

        # Stop tracking and log results
        metrics.stop().log_metrics(self.results_file)

        # Add checkpoint timing to metrics
        metrics.metrics["checkpoint_times"] = checkpoint_times

        # Verify processing success
        self.assertTrue(
            result["success"],
            f"Streaming processing failed: {result.get('error', 'Unknown error')}",
        )

        # Performance assertions for streaming processor
        self.assertLess(
            metrics.metrics["memory_used_mb"],
            300,
            "Streaming processor memory usage too high (>300 MB)",
        )

    def test_database_operation_performance(self):
        """Test performance of database operations during ETL processing."""
        # Create test file with moderately large dataset
        test_file = os.path.join(self.temp_dir, "db_perf_data.json")
        create_large_test_file(
            test_file, conversation_count=20, messages_per_conversation=150
        )

        # Create ETL pipeline
        pipeline = ETLPipeline(db_config=self.db_config, output_dir=self.test_dir)

        # Connect to database for verification
        pipeline.loader.connect_db()

        # Track overall performance
        overall_metrics = PerformanceMetrics("overall_db_operations").start()

        # Track extraction phase
        extract_metrics = PerformanceMetrics("extraction_phase").start()
        raw_data = pipeline._run_extraction_phase(test_file, None, {})
        extract_metrics.stop().log_metrics(self.results_file)

        # Track transformation phase
        transform_metrics = PerformanceMetrics("transformation_phase").start()
        transformed_data = pipeline._run_transformation_phase(
            raw_data, "DB Performance Test", {}
        )
        transform_metrics.stop().log_metrics(self.results_file)

        # Track loading phase
        load_metrics = PerformanceMetrics("loading_phase").start()
        export_id = pipeline._run_loading_phase(
            raw_data, transformed_data, test_file, {}
        )
        load_metrics.stop().log_metrics(self.results_file)

        # Stop overall tracking
        overall_metrics.stop().log_metrics(self.results_file)

        # Verify database content
        cursor = pipeline.loader.conn.cursor()

        # Check for conversations
        cursor.execute(
            "SELECT COUNT(*) FROM skype_conversations WHERE export_id = %s",
            (export_id,),
        )
        conversation_count = cursor.fetchone()[0]

        # Check for messages
        cursor.execute(
            """
            SELECT COUNT(*) FROM skype_messages
            WHERE conversation_id IN (
                SELECT conversation_id FROM skype_conversations
                WHERE export_id = %s
            )
        """,
            (export_id,),
        )
        message_count = cursor.fetchone()[0]

        # Close database connection
        pipeline.loader.close_db()

        # Verify counts
        self.assertEqual(
            conversation_count, 20, "Expected 20 conversations in database"
        )
        self.assertEqual(message_count, 20 * 150, "Expected 3000 messages in database")

        # Performance assertions
        self.assertGreater(
            load_metrics.metrics["duration_seconds"],
            extract_metrics.metrics["duration_seconds"],
            "Database loading should take longer than extraction",
        )


# Helper function to get test database configuration
def get_test_db_config():
    """Get database configuration for testing."""
    from tests.fixtures import get_test_db_config

    return get_test_db_config()


if __name__ == "__main__":
    unittest.main()
