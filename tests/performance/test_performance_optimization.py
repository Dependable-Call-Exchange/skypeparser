#!/usr/bin/env python3
"""
Performance tests for the ETL pipeline.

This test suite focuses on testing performance optimization aspects,
including parallel processing, memory usage, and database batch operations.
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

from src.db.etl import ETLPipeline
from src.utils.config import get_db_config
from src.db.etl.utils import MemoryMonitor
from tests.fixtures import (
    BASIC_SKYPE_DATA,
    COMPLEX_SKYPE_DATA,
    test_db_connection,
    is_db_available
)


@pytest.mark.performance
class TestPerformanceOptimization(unittest.TestCase):
    """Performance tests for the ETL pipeline."""

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
        # Create a copy of COMPLEX_SKYPE_DATA with more messages
        self.large_data = {
            "userId": "test-user-id",
            "exportDate": "2023-01-01T12:00:00Z",
            "conversations": [
                {
                    "id": "conversation1",
                    "displayName": "Test Conversation",
                    "MessageList": []
                }
            ]
        }

        # Add 1000 messages to the conversation
        message_list = []
        for i in range(1000):
            message = {
                "id": f"message{i}",
                "content": f"Test message {i}",
                "from": "user1",
                "originalarrivaltime": f"2023-01-01T{i//60:02d}:{i%60:02d}:00Z",
                "messagetype": "RichText" if i % 2 == 0 else "Text"
            }
            message_list.append(message)

        self.large_data["conversations"][0]["MessageList"] = message_list

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

    def test_parallel_processing(self):
        """Test parallel processing functionality."""
        # Create a pipeline with parallel processing enabled
        pipeline = ETLPipeline(
            db_config=self.db_config,
            output_dir=self.test_dir,
            parallel_processing=True,
            num_workers=4
        )

        # Measure execution time with parallel processing
        start_time = time.time()
        parallel_result = pipeline.run_pipeline(file_path=self.large_file)
        parallel_execution_time = time.time() - start_time

        # Create a pipeline with parallel processing disabled
        pipeline_sequential = ETLPipeline(
            db_config=self.db_config,
            output_dir=self.test_dir,
            parallel_processing=False
        )

        # Measure execution time with sequential processing
        start_time = time.time()
        sequential_result = pipeline_sequential.run_pipeline(file_path=self.large_file)
        sequential_execution_time = time.time() - start_time

        # Verify both pipelines completed successfully
        self.assertEqual(parallel_result['status'], 'completed',
                         "Parallel pipeline did not complete successfully")
        self.assertEqual(sequential_result['status'], 'completed',
                         "Sequential pipeline did not complete successfully")

        # Verify parallel processing is faster (with some tolerance for test variability)
        # Note: This test may be flaky depending on the test environment
        # We allow for a small margin where parallel might not be faster due to overhead
        self.assertLessEqual(
            parallel_execution_time,
            sequential_execution_time * 1.1,  # Allow 10% tolerance
            f"Parallel processing ({parallel_execution_time:.2f}s) not faster than "
            f"sequential processing ({sequential_execution_time:.2f}s)"
        )

        # Log performance metrics
        print(f"\nParallel processing time: {parallel_execution_time:.2f}s")
        print(f"Sequential processing time: {sequential_execution_time:.2f}s")
        print(f"Speedup: {sequential_execution_time / parallel_execution_time:.2f}x")

    def test_memory_usage_optimization(self):
        """Test memory usage optimization."""
        # Create a memory monitor
        memory_monitor = MemoryMonitor()

        # Create a pipeline with memory optimization enabled
        pipeline_optimized = ETLPipeline(
            db_config=self.db_config,
            output_dir=self.test_dir,
            memory_optimization=True,
            batch_size=100  # Process in smaller batches
        )

        # Start memory monitoring
        memory_monitor.start()

        # Run the pipeline with memory optimization
        optimized_result = pipeline_optimized.run_pipeline(file_path=self.large_file)

        # Stop memory monitoring and get peak memory usage
        memory_monitor.stop()
        optimized_peak_memory = memory_monitor.peak_memory_mb

        # Create a new memory monitor
        memory_monitor = MemoryMonitor()

        # Create a pipeline without memory optimization
        pipeline_standard = ETLPipeline(
            db_config=self.db_config,
            output_dir=self.test_dir,
            memory_optimization=False,
            batch_size=1000  # Process in larger batches
        )

        # Start memory monitoring
        memory_monitor.start()

        # Run the pipeline without memory optimization
        standard_result = pipeline_standard.run_pipeline(file_path=self.large_file)

        # Stop memory monitoring and get peak memory usage
        memory_monitor.stop()
        standard_peak_memory = memory_monitor.peak_memory_mb

        # Verify both pipelines completed successfully
        self.assertEqual(optimized_result['status'], 'completed',
                         "Optimized pipeline did not complete successfully")
        self.assertEqual(standard_result['status'], 'completed',
                         "Standard pipeline did not complete successfully")

        # Verify memory optimization reduces peak memory usage
        # Note: This test may be flaky depending on the test environment
        # We allow for a small margin where optimized might use more memory due to test variability
        self.assertLessEqual(
            optimized_peak_memory,
            standard_peak_memory * 1.1,  # Allow 10% tolerance
            f"Memory-optimized pipeline ({optimized_peak_memory:.2f}MB) used more memory than "
            f"standard pipeline ({standard_peak_memory:.2f}MB)"
        )

        # Log memory usage metrics
        print(f"\nOptimized peak memory usage: {optimized_peak_memory:.2f}MB")
        print(f"Standard peak memory usage: {standard_peak_memory:.2f}MB")
        print(f"Memory reduction: {(standard_peak_memory - optimized_peak_memory):.2f}MB "
              f"({(1 - optimized_peak_memory / standard_peak_memory) * 100:.2f}%)")

    def test_checkpoint_resumption_performance(self):
        """Test performance of checkpoint resumption."""
        # Create a pipeline
        pipeline = ETLPipeline(db_config=self.db_config, output_dir=self.test_dir)

        # Patch the transformer to raise an error after processing half the messages
        half_point = len(self.large_data["conversations"][0]["MessageList"]) // 2

        def transform_with_error(data):
            """Transform function that raises an error after processing half the messages."""
            # Process only half the messages
            if len(data["conversations"][0]["MessageList"]) > half_point:
                data["conversations"][0]["MessageList"] = data["conversations"][0]["MessageList"][:half_point]
            return pipeline.transformer.transform(data)

        with patch.object(pipeline.transformer, 'transform', side_effect=transform_with_error):
            try:
                # Run the pipeline, which should process half the messages
                pipeline.run_pipeline(file_path=self.large_file)
            except Exception:
                pass  # Expected error

        # Get the checkpoint
        checkpoints = pipeline.get_available_checkpoints()
        self.assertTrue(len(checkpoints) > 0, "No checkpoint was created")

        # Measure time to resume from checkpoint
        start_time = time.time()
        resume_pipeline = ETLPipeline.load_from_checkpoint(
            checkpoint_file=checkpoints[0],
            db_config=self.db_config
        )
        resume_result = resume_pipeline.run_pipeline(resume_from_checkpoint=True)
        resume_time = time.time() - start_time

        # Measure time to process the entire file from scratch
        start_time = time.time()
        full_pipeline = ETLPipeline(db_config=self.db_config, output_dir=self.test_dir)
        full_result = full_pipeline.run_pipeline(file_path=self.large_file)
        full_time = time.time() - start_time

        # Verify both pipelines completed successfully
        self.assertEqual(resume_result['status'], 'completed',
                         "Resume pipeline did not complete successfully")
        self.assertEqual(full_result['status'], 'completed',
                         "Full pipeline did not complete successfully")

        # Verify resuming from checkpoint is faster than processing from scratch
        self.assertLess(resume_time, full_time,
                        f"Resuming from checkpoint ({resume_time:.2f}s) not faster than "
                        f"processing from scratch ({full_time:.2f}s)")

        # Log performance metrics
        print(f"\nResume from checkpoint time: {resume_time:.2f}s")
        print(f"Full processing time: {full_time:.2f}s")
        print(f"Time saved: {full_time - resume_time:.2f}s "
              f"({(1 - resume_time / full_time) * 100:.2f}%)")

    def test_database_batch_operations(self):
        """Test performance of database batch operations."""
        # Test different batch sizes
        batch_sizes = [10, 50, 100, 500]
        execution_times = {}

        for batch_size in batch_sizes:
            # Create a pipeline with the specified batch size
            pipeline = ETLPipeline(
                db_config=self.db_config,
                output_dir=self.test_dir,
                batch_size=batch_size
            )

            # Measure execution time
            start_time = time.time()
            result = pipeline.run_pipeline(file_path=self.large_file)
            execution_time = time.time() - start_time

            # Store execution time
            execution_times[batch_size] = execution_time

            # Verify pipeline completed successfully
            self.assertEqual(result['status'], 'completed',
                             f"Pipeline with batch size {batch_size} did not complete successfully")

            # Clean up database tables for the next test
            with test_db_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DROP TABLE IF EXISTS messages CASCADE")
                cursor.execute("DROP TABLE IF EXISTS conversations CASCADE")
                cursor.execute("DROP TABLE IF EXISTS participants CASCADE")
                cursor.execute("DROP TABLE IF EXISTS message_content CASCADE")
                cursor.execute("DROP TABLE IF EXISTS message_metadata CASCADE")
                conn.commit()

        # Log performance metrics
        print("\nDatabase batch operation performance:")
        for batch_size, execution_time in execution_times.items():
            print(f"Batch size {batch_size}: {execution_time:.2f}s")

        # Find the optimal batch size (minimum execution time)
        optimal_batch_size = min(execution_times, key=execution_times.get)
        print(f"Optimal batch size: {optimal_batch_size}")

        # Verify larger batch sizes generally perform better than very small ones
        # This is a general trend, but may not always hold true depending on the environment
        self.assertLessEqual(
            execution_times[100],
            execution_times[10],
            "Larger batch sizes should generally be more efficient than very small ones"
        )


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