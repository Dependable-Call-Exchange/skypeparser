#!/usr/bin/env python3
"""
Integration tests for the modular ETL pipeline.

This test suite provides integration testing for the modular ETL pipeline,
verifying its functionality with real database connections and file operations.
"""

import os
import sys
import unittest
import tempfile
import json
import pytest

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl import ETLPipeline
from src.utils.config import get_db_config, load_config
from tests.fixtures import (
    BASIC_SKYPE_DATA,
    test_db_connection,
    is_db_available
)


@pytest.mark.integration
@pytest.mark.modular_etl
class TestModularETLPipelineIntegration(unittest.TestCase):
    """Integration tests for the modular ETL pipeline with a real database."""

    def setUp(self):
        """Set up the test environment."""
        # Skip integration tests unless database is available
        if not is_db_available():
            self.skipTest("Integration tests disabled. Database not available.")

        # Set up the test environment
        self.temp_dir = tempfile.mkdtemp()
        self.test_dir = os.path.join(self.temp_dir, 'test_output')
        os.makedirs(self.test_dir, exist_ok=True)

        # Get database configuration from fixtures
        self.db_config = get_test_db_config()

        # Create a sample Skype export data
        self.sample_data = BASIC_SKYPE_DATA

        # Create a file with the sample data
        self.sample_file = os.path.join(self.temp_dir, 'sample.json')
        with open(self.sample_file, 'w') as f:
            json.dump(self.sample_data, f)

        # Create the ETL pipeline
        self.pipeline = ETLPipeline(
            db_config=self.db_config,
            output_dir=self.test_dir
        )

    def tearDown(self):
        """Clean up after tests."""
        # Remove temporary directory
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_integration_run_pipeline(self):
        """Test running the complete pipeline with a real file and database."""
        # Run the pipeline
        result = self.pipeline.run_pipeline(
            file_path=self.sample_file,
            user_display_name='Test User'
        )

        # Verify the result
        self.assertTrue(result['success'], f"Pipeline failed: {result.get('error', 'Unknown error')}")
        self.assertIn('export_id', result, "Result should contain export_id")
        self.assertIsNotNone(result['export_id'], "Export ID should not be None")

        # Verify phases
        self.assertIn('phases', result, "Result should contain phases")
        self.assertIn('extract', result['phases'], "Phases should include extract")
        self.assertIn('transform', result['phases'], "Phases should include transform")
        self.assertIn('load', result['phases'], "Phases should include load")

        # Verify the data was loaded into the database
        with test_db_connection(self.db_config) as conn:
            with conn.cursor() as cursor:
                # Check for the export
                cursor.execute("SELECT COUNT(*) FROM skype_raw_exports WHERE export_id = %s",
                             (result['export_id'],))
                export_count = cursor.fetchone()[0]
                self.assertEqual(export_count, 1, "Export should be in the database")

                # Check for conversations
                cursor.execute("SELECT COUNT(*) FROM skype_conversations WHERE export_id = %s",
                             (result['export_id'],))
                conversation_count = cursor.fetchone()[0]
                self.assertGreater(conversation_count, 0, "Conversations should be in the database")

                # Check for messages
                cursor.execute("""
                    SELECT COUNT(*) FROM skype_messages
                    WHERE conversation_id IN (
                        SELECT conversation_id FROM skype_conversations
                        WHERE export_id = %s
                    )
                """, (result['export_id'],))
                message_count = cursor.fetchone()[0]
                self.assertGreater(message_count, 0, "Messages should be in the database")

    def test_integration_extract_transform_load(self):
        """Test each phase of the pipeline individually."""
        # Extract
        results = {}
        raw_data = self.pipeline._run_extraction_phase(self.sample_file, None, results)
        self.assertIsNotNone(raw_data, "Extraction should return data")
        self.assertIn('conversations', raw_data, "Raw data should contain conversations")
        self.assertIn('extract', results['phases'], "Results should include extract phase")

        # Transform
        transformed_data = self.pipeline._run_transformation_phase(raw_data, 'Test User', results)
        self.assertIsNotNone(transformed_data, "Transformation should return data")
        self.assertIn('metadata', transformed_data, "Transformed data should contain metadata")
        self.assertIn('conversations', transformed_data, "Transformed data should contain conversations")
        self.assertIn('transform', results['phases'], "Results should include transform phase")

        # Load
        self.pipeline.loader.connect_db()
        try:
            export_id = self.pipeline._run_loading_phase(raw_data, transformed_data, self.sample_file, results)
            self.assertIsNotNone(export_id, "Loading should return an export ID")
            self.assertIn('load', results['phases'], "Results should include load phase")

            # Verify the data was loaded into the database
            with self.pipeline.loader.conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM skype_raw_exports WHERE export_id = %s",
                             (export_id,))
                export_count = cursor.fetchone()[0]
                self.assertEqual(export_count, 1, "Export should be in the database")
        finally:
            self.pipeline.loader.close_db()

    def test_integration_output_files(self):
        """Test that output files are created correctly."""
        # Run the pipeline
        result = self.pipeline.run_pipeline(
            file_path=self.sample_file,
            user_display_name='Test User'
        )

        # Verify the result
        self.assertTrue(result['success'], f"Pipeline failed: {result.get('error', 'Unknown error')}")

        # Check for output files
        output_files = os.listdir(self.test_dir)
        self.assertGreater(len(output_files), 0, "Output directory should contain files")

        # Check for raw data file
        raw_files = [f for f in output_files if f.startswith('raw_')]
        self.assertGreater(len(raw_files), 0, "Output directory should contain raw data files")

        # Verify the content of the raw data file
        raw_file_path = os.path.join(self.test_dir, raw_files[0])
        with open(raw_file_path, 'r') as f:
            raw_data = json.load(f)
        self.assertIn('conversations', raw_data, "Raw data file should contain conversations")


def get_test_db_config():
    """Get database configuration for testing."""
    try:
        # Try to load from config file
        config = load_config('config/config.json')
        db_config = get_db_config(config)
    except Exception:
        # Fall back to default test configuration
        db_config = {
            'dbname': 'test_skype_logs',
            'user': 'postgres',
            'password': '',
            'host': 'localhost',
            'port': 5432
        }
    return db_config


if __name__ == '__main__':
    unittest.main()