#!/usr/bin/env python3
"""
Integration tests for handling various Skype export formats and sizes.

This test suite focuses on testing the ETL pipeline with different
Skype export formats (JSON, TAR) and various data sizes.
"""

import os
import sys
import unittest
import tempfile
import json
import tarfile
import pytest
import shutil
from unittest.mock import patch

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl import ETLPipeline
from src.utils.config import get_db_config
from tests.fixtures import (
    BASIC_SKYPE_DATA,
    COMPLEX_SKYPE_DATA,
    test_db_connection,
    is_db_available,
    create_test_json_file,
    create_test_tar_file
)


@pytest.mark.integration
class TestExportFormats(unittest.TestCase):
    """Integration tests for handling various Skype export formats and sizes."""

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

    def tearDown(self):
        """Clean up after the test."""
        # Clean up temporary files
        shutil.rmtree(self.temp_dir)

    def test_json_format(self):
        """Test processing a JSON format Skype export."""
        # Create a sample JSON file
        sample_file = os.path.join(self.temp_dir, 'sample.json')
        with open(sample_file, 'w') as f:
            json.dump(BASIC_SKYPE_DATA, f)

        # Create and run the pipeline
        pipeline = ETLPipeline(db_config=self.db_config, output_dir=self.test_dir)
        result = pipeline.run_pipeline(file_path=sample_file)

        # Verify the pipeline completed successfully
        self.assertEqual(result['status'], 'completed',
                         f"Pipeline did not complete successfully: {result}")
        self.assertTrue(result['extraction']['success'],
                        "Extraction phase failed for JSON format")

    def test_tar_format(self):
        """Test processing a TAR format Skype export."""
        # Create a sample JSON file inside a TAR archive
        json_file = os.path.join(self.temp_dir, 'messages.json')
        with open(json_file, 'w') as f:
            json.dump(BASIC_SKYPE_DATA, f)

        # Create a TAR archive containing the JSON file
        tar_file = os.path.join(self.temp_dir, 'sample.tar')
        with tarfile.open(tar_file, 'w') as tar:
            tar.add(json_file, arcname='messages.json')

        # Create and run the pipeline
        pipeline = ETLPipeline(db_config=self.db_config, output_dir=self.test_dir)
        result = pipeline.run_pipeline(file_path=tar_file)

        # Verify the pipeline completed successfully
        self.assertEqual(result['status'], 'completed',
                         f"Pipeline did not complete successfully: {result}")
        self.assertTrue(result['extraction']['success'],
                        "Extraction phase failed for TAR format")

    def test_large_dataset(self):
        """Test processing a large dataset."""
        # Create a sample large JSON file
        sample_file = os.path.join(self.temp_dir, 'large_sample.json')
        with open(sample_file, 'w') as f:
            json.dump(COMPLEX_SKYPE_DATA, f)

        # Create and run the pipeline
        pipeline = ETLPipeline(db_config=self.db_config, output_dir=self.test_dir)
        result = pipeline.run_pipeline(file_path=sample_file)

        # Verify the pipeline completed successfully
        self.assertEqual(result['status'], 'completed',
                         f"Pipeline did not complete successfully: {result}")
        self.assertTrue(result['extraction']['success'],
                        "Extraction phase failed for large dataset")

        # Verify all messages were processed
        self.assertEqual(result['transformation']['messages_processed'],
                         len(COMPLEX_SKYPE_DATA.get('messages', [])),
                         "Not all messages were processed")

    def test_mixed_message_types(self):
        """Test processing a dataset with mixed message types."""
        # Create a sample JSON file with mixed message types
        sample_file = os.path.join(self.temp_dir, 'mixed_types.json')
        with open(sample_file, 'w') as f:
            json.dump(COMPLEX_SKYPE_DATA, f)

        # Create and run the pipeline
        pipeline = ETLPipeline(db_config=self.db_config, output_dir=self.test_dir)
        result = pipeline.run_pipeline(file_path=sample_file)

        # Verify the pipeline completed successfully
        self.assertEqual(result['status'], 'completed',
                         f"Pipeline did not complete successfully: {result}")

        # Verify all message types were processed
        message_types = set(msg.get('messagetype') for msg in COMPLEX_SKYPE_DATA.get('messages', []))
        self.assertTrue(len(message_types) > 1,
                        "Test data does not contain mixed message types")

        # Verify transformation details
        self.assertTrue(result['transformation']['success'],
                        "Transformation phase failed for mixed message types")
        self.assertEqual(result['transformation']['messages_processed'],
                         len(COMPLEX_SKYPE_DATA.get('messages', [])),
                         "Not all messages were processed")

    def test_incremental_processing(self):
        """Test incremental processing of data."""
        # Create initial dataset
        initial_file = os.path.join(self.temp_dir, 'initial.json')
        with open(initial_file, 'w') as f:
            json.dump(BASIC_SKYPE_DATA, f)

        # Process initial dataset
        pipeline = ETLPipeline(db_config=self.db_config, output_dir=self.test_dir)
        initial_result = pipeline.run_pipeline(file_path=initial_file)

        # Verify initial processing
        self.assertEqual(initial_result['status'], 'completed',
                         "Initial processing failed")

        # Create additional dataset
        additional_file = os.path.join(self.temp_dir, 'additional.json')
        with open(additional_file, 'w') as f:
            json.dump(COMPLEX_SKYPE_DATA, f)

        # Process additional dataset
        additional_result = pipeline.run_pipeline(file_path=additional_file)

        # Verify additional processing
        self.assertEqual(additional_result['status'], 'completed',
                         "Additional processing failed")

        # Verify total messages processed
        total_messages = (len(BASIC_SKYPE_DATA.get('messages', [])) +
                          len(COMPLEX_SKYPE_DATA.get('messages', [])))

        # Check database for total messages
        with test_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM messages")
            count = cursor.fetchone()[0]
            self.assertEqual(count, total_messages,
                             f"Expected {total_messages} messages in database, found {count}")


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