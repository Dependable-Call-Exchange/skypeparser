#!/usr/bin/env python3
"""
Unit tests for the ETLContext class.
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock
import tempfile
import time
from datetime import datetime

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl import ETLContext

class TestETLContext(unittest.TestCase):
    """Test cases for the ETLContext class."""

    def setUp(self):
        """Set up test fixtures."""
        self.db_config = {
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password',
            'host': 'localhost',
            'port': 5432
        }
        self.temp_dir = tempfile.mkdtemp()
        self.task_id = "test-task-123"

    def tearDown(self):
        """Clean up after tests."""
        # Remove temporary directory
        if os.path.exists(self.temp_dir):
            import shutil
            shutil.rmtree(self.temp_dir)

    def test_init(self):
        """Test initialization of ETLContext."""
        context = ETLContext(
            db_config=self.db_config,
            output_dir=self.temp_dir,
            memory_limit_mb=512,
            parallel_processing=True,
            chunk_size=100,
            batch_size=50,
            max_workers=4,
            task_id=self.task_id
        )

        # Check that attributes are set correctly
        self.assertEqual(context.db_config, self.db_config)
        self.assertEqual(context.output_dir, self.temp_dir)
        self.assertEqual(context.memory_limit_mb, 512)
        self.assertTrue(context.parallel_processing)
        self.assertEqual(context.chunk_size, 100)
        self.assertEqual(context.batch_size, 50)
        self.assertEqual(context.max_workers, 4)
        self.assertEqual(context.task_id, self.task_id)

        # Check that state attributes are initialized
        self.assertIsNone(context.current_phase)
        self.assertEqual(context.phase_results, {})
        self.assertEqual(context.checkpoints, {})
        self.assertEqual(context.errors, [])
        self.assertIsNone(context.raw_data)
        self.assertIsNone(context.transformed_data)
        self.assertIsNotNone(context.start_time)
        self.assertIsNone(context.file_source)

    def test_start_phase(self):
        """Test starting a phase."""
        context = ETLContext(
            db_config=self.db_config,
            task_id=self.task_id
        )

        # Start a phase
        context.start_phase("extract", total_conversations=10, total_messages=100)

        # Check that phase is set correctly
        self.assertEqual(context.current_phase, "extract")
        # The phase_results are populated after end_phase is called, not during start_phase
        self.assertIn("extract", context.metrics['duration'])
        self.assertIsNotNone(context.metrics['duration']["extract"]["start"])

    def test_end_phase(self):
        """Test ending a phase."""
        context = ETLContext(
            db_config=self.db_config,
            task_id=self.task_id
        )

        # Start and end a phase
        context.start_phase("extract", total_conversations=10, total_messages=100)

        # Update progress
        context.update_progress(conversations=5, messages=50)

        # Sleep a bit to ensure duration is measurable
        time.sleep(0.1)

        # End the phase
        context.end_phase()

        # Check that phase is ended correctly
        self.assertIsNone(context.current_phase)
        self.assertIn("extract", context.phase_results)
        self.assertGreater(context.phase_results["extract"]["duration_seconds"], 0)
        self.assertEqual(context.phase_results["extract"]["processed_conversations"], 5)
        self.assertEqual(context.phase_results["extract"]["processed_messages"], 50)
        self.assertGreater(context.phase_results["extract"]["messages_per_second"], 0)

    def test_update_progress(self):
        """Test updating progress."""
        context = ETLContext(
            db_config=self.db_config,
            task_id=self.task_id
        )

        # Start a phase
        context.start_phase("transform", total_conversations=20, total_messages=200)

        # Update progress
        context.update_progress(conversations=5, messages=50)

        # End the phase to populate phase_results
        context.end_phase()

        # Check the results
        self.assertEqual(context.phase_results["transform"]["processed_conversations"], 5)
        self.assertEqual(context.phase_results["transform"]["processed_messages"], 50)

    @patch('psutil.Process')
    def test_check_memory(self, mock_process):
        """Test memory checking."""
        # Mock the memory_info method
        mock_memory_info = MagicMock()
        mock_memory_info.rss = 100 * 1024 * 1024  # 100 MB
        mock_process.return_value.memory_info.return_value = mock_memory_info

        context = ETLContext(
            db_config=self.db_config,
            memory_limit_mb=200,
            task_id=self.task_id
        )

        # We need to patch the memory_monitor's last_memory_mb attribute
        # since it's used in the check_memory method
        context.memory_monitor.last_memory_mb = 100

        # Check memory usage
        context.check_memory()

        # Verify that memory usage was recorded
        self.assertEqual(len(context.metrics['memory_usage']), 1)
        self.assertEqual(context.metrics['memory_usage'][0]['memory_mb'], 100)

        # Set memory usage above warning threshold and check again
        mock_memory_info.rss = 160 * 1024 * 1024  # 160 MB (80% of limit)
        context.memory_monitor.last_memory_mb = 160
        context.check_memory()

        # Verify that memory usage was recorded again
        self.assertEqual(len(context.metrics['memory_usage']), 2)
        self.assertEqual(context.metrics['memory_usage'][1]['memory_mb'], 160)

    def test_record_error(self):
        """Test recording errors."""
        context = ETLContext(
            db_config=self.db_config,
            task_id=self.task_id
        )

        # Record a non-fatal error
        error = ValueError("Test error")
        context.record_error("extract", error, fatal=False)
        self.assertEqual(len(context.errors), 1)
        self.assertEqual(context.errors[0]["phase"], "extract")
        self.assertEqual(context.errors[0]["error_message"], "Test error")
        self.assertFalse(context.errors[0]["fatal"])

        # Record a fatal error
        error = RuntimeError("Fatal error")
        context.record_error("transform", error, fatal=True)
        self.assertEqual(len(context.errors), 2)
        self.assertEqual(context.errors[1]["phase"], "transform")
        self.assertEqual(context.errors[1]["error_message"], "Fatal error")
        self.assertTrue(context.errors[1]["fatal"])

    def test_create_checkpoint(self):
        """Test creating checkpoints."""
        context = ETLContext(
            db_config=self.db_config,
            output_dir=self.temp_dir,
            task_id=self.task_id
        )

        # Start a phase
        context.start_phase("extract")

        # Set raw_data to simulate extraction
        context.raw_data = {"test": "data"}

        # Create a checkpoint
        context._create_checkpoint("extract")

        # Check that checkpoint is created
        self.assertIn("extract", context.checkpoints)
        self.assertTrue(context.checkpoints["extract"]["raw_data_available"])
        self.assertIsNotNone(context.checkpoints["extract"]["timestamp"])

    def test_can_resume_from_phase(self):
        """Test checking if can resume from a phase."""
        context = ETLContext(
            db_config=self.db_config,
            task_id=self.task_id
        )

        # Create checkpoints with the correct structure
        context.checkpoints = {
            "extract": {
                "timestamp": datetime.now(),
                "raw_data_available": True
            },
            "transform": {
                "timestamp": datetime.now(),
                "transformed_data_available": True
            }
        }

        # Check if can resume from phases
        self.assertTrue(context.can_resume_from_phase("extract"))
        self.assertTrue(context.can_resume_from_phase("transform"))
        self.assertFalse(context.can_resume_from_phase("load"))

    def test_get_summary(self):
        """Test getting summary."""
        context = ETLContext(
            db_config=self.db_config,
            task_id=self.task_id
        )

        # Add some phase results
        context.start_phase("extract", total_conversations=10, total_messages=100)
        context.update_progress(conversations=10, messages=100)
        context.end_phase()

        context.start_phase("transform", total_conversations=10, total_messages=100)
        context.update_progress(conversations=10, messages=100)
        context.end_phase()

        # Record an error
        error = ValueError("Test error")
        context.record_error("extract", error, fatal=False)

        # Get summary
        summary = context.get_summary()

        # Check summary contents
        self.assertEqual(summary["task_id"], self.task_id)
        self.assertIsNotNone(summary["start_time"])
        self.assertIsNotNone(summary["end_time"])
        self.assertGreater(summary["total_duration_seconds"], 0)
        self.assertEqual(len(summary["phases"]), 2)
        self.assertEqual(summary["error_count"], 1)

if __name__ == '__main__':
    unittest.main()