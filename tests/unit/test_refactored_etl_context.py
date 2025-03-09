#!/usr/bin/env python3
"""
Unit tests for the refactored ETLContext class.
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

from src.db.etl.context import ETLContext
from src.utils.checkpoint_manager import CheckpointManager
from src.utils.error_logger import ErrorLogger
from src.utils.phase_manager import PhaseManager
from src.utils.progress_tracker import ProgressTracker
from src.utils.memory_monitor import MemoryMonitor


class TestRefactoredETLContext(unittest.TestCase):
    """Test cases for the refactored ETLContext class."""

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

        # Mock the component managers
        self.mock_progress_tracker = MagicMock(spec=ProgressTracker)
        self.mock_memory_monitor = MagicMock(spec=MemoryMonitor)
        self.mock_phase_manager = MagicMock(spec=PhaseManager)
        self.mock_error_logger = MagicMock(spec=ErrorLogger)
        self.mock_checkpoint_manager = MagicMock(spec=CheckpointManager)

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
            task_id=self.task_id,
            progress_tracker=self.mock_progress_tracker,
            memory_monitor=self.mock_memory_monitor,
            phase_manager=self.mock_phase_manager,
            error_logger=self.mock_error_logger,
            checkpoint_manager=self.mock_checkpoint_manager
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

        # Check that component managers are set correctly
        self.assertEqual(context.progress_tracker, self.mock_progress_tracker)
        self.assertEqual(context.memory_monitor, self.mock_memory_monitor)
        self.assertEqual(context.phase_manager, self.mock_phase_manager)
        self.assertEqual(context.error_logger, self.mock_error_logger)
        self.assertEqual(context.checkpoint_manager, self.mock_checkpoint_manager)

        # Check that data references are initialized
        self.assertIsNone(context.raw_data)
        self.assertIsNone(context.transformed_data)
        self.assertIsNone(context.file_source)
        self.assertIsNone(context.export_id)
        self.assertEqual(context.custom_metadata, {})

    def test_init_with_defaults(self):
        """Test initialization of ETLContext with default values."""
        context = ETLContext(
            db_config=self.db_config,
            task_id=self.task_id
        )

        # Check that default values are set correctly
        self.assertEqual(context.memory_limit_mb, 1024)
        self.assertTrue(context.parallel_processing)
        self.assertEqual(context.chunk_size, 1000)
        self.assertEqual(context.batch_size, 100)
        self.assertIsNone(context.max_workers)

        # Check that component managers are created
        self.assertIsInstance(context.progress_tracker, ProgressTracker)
        self.assertIsInstance(context.memory_monitor, MemoryMonitor)
        self.assertIsInstance(context.phase_manager, PhaseManager)
        self.assertIsInstance(context.error_logger, ErrorLogger)
        self.assertIsInstance(context.checkpoint_manager, CheckpointManager)

    def test_start_phase(self):
        """Test starting a phase."""
        context = ETLContext(
            db_config=self.db_config,
            task_id=self.task_id,
            phase_manager=self.mock_phase_manager,
            memory_monitor=self.mock_memory_monitor
        )

        # Set up mock for memory_monitor.check_memory
        self.mock_memory_monitor.check_memory.return_value = {
            "used_mb": 100.0,
            "peak_mb": 100.0,
            "limit_mb": 1024.0,
            "percent": 9.8,
            "rss_bytes": 104857600,
            "vms_bytes": 209715200
        }

        # Start a phase
        context.start_phase("extract", total_conversations=10, total_messages=100)

        # Check that phase_manager.start_phase was called with the correct arguments
        self.mock_phase_manager.start_phase.assert_called_once_with(
            "extract", 10, 100
        )

        # Check that memory_monitor.check_memory was called
        self.mock_memory_monitor.check_memory.assert_called_once()

    def test_end_phase(self):
        """Test ending a phase."""
        context = ETLContext(
            db_config=self.db_config,
            task_id=self.task_id,
            phase_manager=self.mock_phase_manager,
            memory_monitor=self.mock_memory_monitor
        )

        # Set up mock for memory_monitor.check_memory
        self.mock_memory_monitor.check_memory.return_value = {
            "used_mb": 100.0,
            "peak_mb": 100.0,
            "limit_mb": 1024.0,
            "percent": 9.8,
            "rss_bytes": 104857600,
            "vms_bytes": 209715200
        }

        # End a phase
        context.end_phase("extract", "completed")

        # Check that phase_manager.end_phase was called with the correct arguments
        self.mock_phase_manager.end_phase.assert_called_once_with(
            "extract", "completed"
        )

        # Check that memory_monitor.check_memory was called
        self.mock_memory_monitor.check_memory.assert_called_once()

    def test_record_error(self):
        """Test recording errors."""
        context = ETLContext(
            db_config=self.db_config,
            task_id=self.task_id,
            phase_manager=self.mock_phase_manager,
            error_logger=self.mock_error_logger
        )

        # Set up mock for phase_manager.get_phase_status
        self.mock_phase_manager.get_phase_status.return_value = "in_progress"

        # Record a non-fatal error
        error_details = {"line": 42, "file": "test.py"}
        context.record_error("extract", "Test error", error_details, fatal=False)

        # Check that error_logger.record_error was called with the correct arguments
        self.mock_error_logger.record_error.assert_called_once_with(
            "extract", "Test error", error_details, False
        )

        # Check that phase_manager.end_phase was called with "warning"
        self.mock_phase_manager.end_phase.assert_called_once_with(
            "extract", "warning"
        )

        # Reset mocks
        self.mock_error_logger.reset_mock()
        self.mock_phase_manager.reset_mock()

        # Record a fatal error
        context.record_error("transform", "Fatal error", None, fatal=True)

        # Check that error_logger.record_error was called with the correct arguments
        self.mock_error_logger.record_error.assert_called_once_with(
            "transform", "Fatal error", None, True
        )

        # Check that phase_manager.end_phase was called with "failed"
        self.mock_phase_manager.end_phase.assert_called_once_with(
            "transform", "failed"
        )

    def test_create_checkpoint(self):
        """Test creating checkpoints."""
        context = ETLContext(
            db_config=self.db_config,
            output_dir=self.temp_dir,
            task_id=self.task_id,
            checkpoint_manager=self.mock_checkpoint_manager,
            phase_manager=self.mock_phase_manager
        )

        # Set up mock for phase_manager attributes
        self.mock_phase_manager.current_phase = "extract"
        self.mock_phase_manager.phase_statuses = {"extract": "in_progress"}
        self.mock_phase_manager.phase_results = {"extract": {"start_time": datetime.now().isoformat()}}

        # Set raw_data to simulate extraction
        context.raw_data = {"test": "data"}

        # Set up mock for checkpoint_manager.create_checkpoint
        self.mock_checkpoint_manager.create_checkpoint.return_value = "checkpoint-123"

        # Create a checkpoint
        checkpoint_id = context.create_checkpoint()

        # Check that checkpoint_manager.create_checkpoint was called with the correct arguments
        self.mock_checkpoint_manager.create_checkpoint.assert_called_once()

        # Check that the checkpoint ID is returned
        self.assertEqual(checkpoint_id, "checkpoint-123")

    def test_restore_checkpoint(self):
        """Test restoring checkpoints."""
        context = ETLContext(
            db_config=self.db_config,
            output_dir=self.temp_dir,
            task_id=self.task_id,
            checkpoint_manager=self.mock_checkpoint_manager,
            phase_manager=self.mock_phase_manager
        )

        # Set up mock for checkpoint_manager.restore_checkpoint
        checkpoint_data = {
            "task_id": self.task_id,
            "current_phase": "extract",
            "phase_statuses": {"extract": "completed"},
            "phase_results": {"extract": {"start_time": datetime.now().isoformat()}},
            "raw_data": {"test": "data"},
            "transformed_data": None,
            "file_source": "test.json",
            "export_id": 123,
            "user_id": "test-user",
            "user_display_name": "Test User",
            "export_date": datetime.now().isoformat(),
            "custom_metadata": {"test": "metadata"}
        }
        self.mock_checkpoint_manager.restore_checkpoint.return_value = checkpoint_data

        # Restore a checkpoint
        result = context.restore_checkpoint("checkpoint-123")

        # Check that checkpoint_manager.restore_checkpoint was called with the correct arguments
        self.mock_checkpoint_manager.restore_checkpoint.assert_called_once_with("checkpoint-123")

        # Check that the result is True
        self.assertTrue(result)

        # Check that the context attributes were updated
        self.assertEqual(context.raw_data, {"test": "data"})
        self.assertEqual(context.file_source, "test.json")
        self.assertEqual(context.export_id, 123)
        self.assertEqual(context.user_id, "test-user")
        self.assertEqual(context.user_display_name, "Test User")
        self.assertEqual(context.custom_metadata, {"test": "metadata"})

        # Check that the phase manager attributes were updated
        self.assertEqual(self.mock_phase_manager.phase_statuses, {"extract": "completed"})
        self.assertEqual(self.mock_phase_manager.phase_results, {"extract": {"start_time": checkpoint_data["phase_results"]["extract"]["start_time"]}})
        self.assertEqual(self.mock_phase_manager.current_phase, "extract")

    def test_update_progress(self):
        """Test updating progress."""
        context = ETLContext(
            db_config=self.db_config,
            task_id=self.task_id,
            progress_tracker=self.mock_progress_tracker,
            phase_manager=self.mock_phase_manager
        )

        # Update progress
        context.update_progress("transform", 5, 20, "conversations")

        # Check that progress_tracker.update was called with the correct arguments
        self.mock_progress_tracker.update.assert_called_once_with(5, 20)

        # Check that phase_manager.update_phase_metric was called with the correct arguments
        self.mock_phase_manager.update_phase_metric.assert_any_call(
            "transform", "processed_conversations", 5
        )
        self.mock_phase_manager.update_phase_metric.assert_any_call(
            "transform", "total_conversations", 20
        )

        # Check that metrics were updated
        self.assertEqual(context.metrics["processed_items"]["transform"]["conversations"], 5)

    def test_get_summary(self):
        """Test getting summary."""
        context = ETLContext(
            db_config=self.db_config,
            task_id=self.task_id,
            phase_manager=self.mock_phase_manager,
            error_logger=self.mock_error_logger
        )

        # Set up mock for phase_manager.get_phase_summary
        phase_summary = {
            "phases": {"extract": {"status": "completed"}},
            "current_phase": None,
            "completed_phases": ["extract"],
            "failed_phases": [],
            "in_progress_phases": [],
            "warning_phases": []
        }
        self.mock_phase_manager.get_phase_summary.return_value = phase_summary

        # Set up mock for error_logger.errors and error_logger.get_errors
        self.mock_error_logger.errors = []
        self.mock_error_logger.get_errors.return_value = []

        # Get summary
        summary = context.get_summary()

        # Check that phase_manager.get_phase_summary was called
        self.mock_phase_manager.get_phase_summary.assert_called_once()

        # Check that error_logger.get_errors was called
        self.mock_error_logger.get_errors.assert_called_once_with(fatal_only=True)

        # Check that the summary contains the expected keys
        self.assertIn("task_id", summary)
        self.assertIn("start_time", summary)
        self.assertIn("end_time", summary)
        self.assertIn("duration", summary)
        self.assertIn("user_id", summary)
        self.assertIn("phases", summary)
        self.assertIn("errors", summary)
        self.assertIn("metrics", summary)

        # Check that the summary values are correct
        self.assertEqual(summary["task_id"], self.task_id)
        self.assertEqual(summary["phases"], phase_summary)
        self.assertEqual(summary["errors"]["count"], 0)
        self.assertEqual(summary["errors"]["fatal_count"], 0)
        self.assertEqual(summary["errors"]["details"], [])

    def test_save_summary(self):
        """Test saving summary."""
        context = ETLContext(
            db_config=self.db_config,
            output_dir=self.temp_dir,
            task_id=self.task_id,
            phase_manager=self.mock_phase_manager,
            error_logger=self.mock_error_logger
        )

        # Set up mock for phase_manager.get_phase_summary
        phase_summary = {
            "phases": {"extract": {"status": "completed"}},
            "current_phase": None,
            "completed_phases": ["extract"],
            "failed_phases": [],
            "in_progress_phases": [],
            "warning_phases": []
        }
        self.mock_phase_manager.get_phase_summary.return_value = phase_summary

        # Set up mock for error_logger.errors and error_logger.get_errors
        self.mock_error_logger.errors = []
        self.mock_error_logger.get_errors.return_value = []

        # Save summary
        output_path = context.save_summary()

        # Check that the output file exists
        self.assertTrue(os.path.exists(output_path))

        # Check that the output file contains the expected data
        with open(output_path, "r") as f:
            import json
            summary = json.load(f)
            self.assertEqual(summary["task_id"], self.task_id)
            self.assertEqual(summary["phases"], phase_summary)
            self.assertEqual(summary["errors"]["count"], 0)

    def test_can_resume_from_phase(self):
        """Test checking if can resume from a phase."""
        context = ETLContext(
            db_config=self.db_config,
            task_id=self.task_id,
            phase_manager=self.mock_phase_manager
        )

        # Set up mock for phase_manager.can_resume_from_phase
        self.mock_phase_manager.can_resume_from_phase.return_value = True

        # Check if can resume from a phase
        result = context.can_resume_from_phase("extract")

        # Check that phase_manager.can_resume_from_phase was called with the correct arguments
        self.mock_phase_manager.can_resume_from_phase.assert_called_once_with("extract")

        # Check that the result is True
        self.assertTrue(result)

    def test_has_checkpoint(self):
        """Test checking if has checkpoint."""
        context = ETLContext(
            db_config=self.db_config,
            task_id=self.task_id,
            checkpoint_manager=self.mock_checkpoint_manager
        )

        # Set up mock for checkpoint_manager.list_checkpoints
        self.mock_checkpoint_manager.list_checkpoints.return_value = ["checkpoint-123"]

        # Check if has checkpoint
        result = context.has_checkpoint

        # Check that checkpoint_manager.list_checkpoints was called
        self.mock_checkpoint_manager.list_checkpoints.assert_called_once()

        # Check that the result is True
        self.assertTrue(result)

    def test_serialize_checkpoint(self):
        """Test serializing checkpoint."""
        context = ETLContext(
            db_config=self.db_config,
            task_id=self.task_id,
            checkpoint_manager=self.mock_checkpoint_manager
        )

        # Set up mock for checkpoint_manager.get_checkpoint
        checkpoint_data = {
            "id": "checkpoint-123",
            "timestamp": datetime.now().isoformat(),
            "task_id": self.task_id
        }
        self.mock_checkpoint_manager.get_checkpoint.return_value = checkpoint_data

        # Serialize checkpoint
        result = context.serialize_checkpoint("checkpoint-123")

        # Check that checkpoint_manager.get_checkpoint was called with the correct arguments
        self.mock_checkpoint_manager.get_checkpoint.assert_called_once_with("checkpoint-123")

        # Check that the result contains the expected keys
        self.assertIn("id", result)
        self.assertIn("timestamp", result)
        self.assertIn("task_id", result)
        self.assertIn("serialized_at", result)
        self.assertIn("context_task_id", result)

        # Check that the result values are correct
        self.assertEqual(result["id"], "checkpoint-123")
        self.assertEqual(result["task_id"], self.task_id)
        self.assertEqual(result["context_task_id"], self.task_id)

    def test_check_memory(self):
        """Test checking memory."""
        context = ETLContext(
            db_config=self.db_config,
            task_id=self.task_id,
            memory_monitor=self.mock_memory_monitor
        )

        # Set up mock for memory_monitor.check_memory
        memory_info = {
            "used_mb": 100,
            "total_mb": 1000,
            "percent": 10.0
        }
        self.mock_memory_monitor.check_memory.return_value = memory_info

        # Check memory
        result = context.check_memory()

        # Check that memory_monitor.check_memory was called
        self.mock_memory_monitor.check_memory.assert_called_once()

        # Check that the result is the memory info
        self.assertEqual(result, memory_info)


if __name__ == '__main__':
    unittest.main()