#!/usr/bin/env python3
"""
Integration tests for error handling and recovery in the ETL pipeline.

This test suite focuses on testing error handling and recovery mechanisms,
including checkpoint creation and resumption after errors.
"""

import os
import sys
import unittest
import tempfile
import json
import pytest
from unittest.mock import patch, MagicMock

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl import ETLPipeline, ETLContext
from src.utils.config import get_db_config
from tests.fixtures import (
    BASIC_SKYPE_DATA,
    test_db_connection,
    is_db_available
)


@pytest.mark.integration
class TestErrorHandlingRecovery(unittest.TestCase):
    """Integration tests for error handling and recovery in the ETL pipeline."""

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

    def tearDown(self):
        """Clean up after the test."""
        # Clean up temporary files
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_checkpoint_creation_on_error(self):
        """Test that a checkpoint is created when an error occurs."""
        # Create a pipeline
        pipeline = ETLPipeline(db_config=self.db_config, output_dir=self.test_dir)

        # Patch the transformer to raise an error
        with patch.object(pipeline.transformer, 'transform', side_effect=ValueError("Test error")):
            try:
                # Run the pipeline, which should fail during transformation
                pipeline.run_pipeline(file_path=self.sample_file)
            except ValueError:
                pass  # Expected error

        # Verify checkpoint was created
        checkpoints = pipeline.get_available_checkpoints()
        self.assertTrue(len(checkpoints) > 0, "No checkpoint was created after error")

        # Verify checkpoint file exists
        self.assertTrue(os.path.exists(checkpoints[0]), f"Checkpoint file {checkpoints[0]} does not exist")

        # Verify checkpoint file contains expected data
        with open(checkpoints[0], 'r') as f:
            checkpoint_data = json.load(f)
            self.assertIn('context', checkpoint_data, "Checkpoint data missing context")
            self.assertIn('checkpoint_version', checkpoint_data, "Checkpoint data missing version")
            self.assertIn('serialized_at', checkpoint_data, "Checkpoint data missing timestamp")

    def test_resumption_from_checkpoint(self):
        """Test resuming the pipeline from a checkpoint."""
        # Create a pipeline
        pipeline = ETLPipeline(db_config=self.db_config, output_dir=self.test_dir)

        # Patch the transformer to raise an error
        with patch.object(pipeline.transformer, 'transform', side_effect=ValueError("Test error")):
            try:
                # Run the pipeline, which should fail during transformation
                pipeline.run_pipeline(file_path=self.sample_file)
            except ValueError:
                pass  # Expected error

        # Get the checkpoint
        checkpoints = pipeline.get_available_checkpoints()
        self.assertTrue(len(checkpoints) > 0, "No checkpoint was created after error")

        # Create a new pipeline from the checkpoint
        resume_pipeline = ETLPipeline.load_from_checkpoint(
            checkpoint_file=checkpoints[0],
            db_config=self.db_config
        )

        # Run the pipeline from the checkpoint
        result = resume_pipeline.run_pipeline(resume_from_checkpoint=True)

        # Verify the pipeline completed successfully
        self.assertEqual(result['status'], 'completed',
                         f"Pipeline did not complete successfully: {result}")
        self.assertTrue(result['resumed_from_checkpoint'],
                        "Pipeline was not resumed from checkpoint")

    def test_multiple_error_recovery(self):
        """Test recovery from multiple errors in sequence."""
        # Create a pipeline
        pipeline = ETLPipeline(db_config=self.db_config, output_dir=self.test_dir)

        # First error during extraction
        with patch.object(pipeline.extractor, 'extract', side_effect=ValueError("Extraction error")):
            try:
                pipeline.run_pipeline(file_path=self.sample_file)
            except ValueError:
                pass  # Expected error

        # Get the checkpoint
        checkpoints = pipeline.get_available_checkpoints()
        self.assertTrue(len(checkpoints) > 0, "No checkpoint was created after extraction error")

        # Resume from checkpoint
        resume_pipeline = ETLPipeline.load_from_checkpoint(
            checkpoint_file=checkpoints[0],
            db_config=self.db_config
        )

        # Second error during transformation
        with patch.object(resume_pipeline.transformer, 'transform', side_effect=ValueError("Transform error")):
            try:
                resume_pipeline.run_pipeline(resume_from_checkpoint=True)
            except ValueError:
                pass  # Expected error

        # Get the new checkpoint
        new_checkpoints = resume_pipeline.get_available_checkpoints()
        self.assertTrue(len(new_checkpoints) > len(checkpoints),
                        "No new checkpoint was created after transform error")

        # Resume from the new checkpoint
        final_pipeline = ETLPipeline.load_from_checkpoint(
            checkpoint_file=new_checkpoints[-1],
            db_config=self.db_config
        )

        # Run the pipeline from the checkpoint
        result = final_pipeline.run_pipeline(resume_from_checkpoint=True)

        # Verify the pipeline completed successfully
        self.assertEqual(result['status'], 'completed',
                         f"Pipeline did not complete successfully: {result}")

    def test_checkpoint_data_integrity(self):
        """Test that checkpoint data maintains integrity across resumptions."""
        # Create a pipeline with custom context data
        context = ETLContext(output_dir=self.test_dir)
        context.user_id = "test_user_123"
        context.export_date = "2023-01-01"
        context.custom_metadata = {"test_key": "test_value"}

        pipeline = ETLPipeline(db_config=self.db_config, output_dir=self.test_dir, context=context)

        # Patch the transformer to raise an error
        with patch.object(pipeline.transformer, 'transform', side_effect=ValueError("Test error")):
            try:
                pipeline.run_pipeline(file_path=self.sample_file)
            except ValueError:
                pass  # Expected error

        # Get the checkpoint
        checkpoints = pipeline.get_available_checkpoints()
        self.assertTrue(len(checkpoints) > 0, "No checkpoint was created after error")

        # Create a new pipeline from the checkpoint
        resume_pipeline = ETLPipeline.load_from_checkpoint(
            checkpoint_file=checkpoints[0],
            db_config=self.db_config
        )

        # Verify context data was preserved
        self.assertEqual(resume_pipeline.context.user_id, "test_user_123",
                         "User ID was not preserved in checkpoint")
        self.assertEqual(resume_pipeline.context.export_date, "2023-01-01",
                         "Export date was not preserved in checkpoint")
        self.assertEqual(resume_pipeline.context.custom_metadata, {"test_key": "test_value"},
                         "Custom metadata was not preserved in checkpoint")


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