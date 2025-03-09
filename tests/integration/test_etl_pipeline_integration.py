#!/usr/bin/env python3
"""
DEPRECATED: This file is being migrated to pytest style in test_etl_pipeline_integration_pytest.py.
Please use the pytest version instead. This file will be removed after migration verification is complete.

Integration tests for the ETL pipeline module.

This test suite provides integration testing for the ETL pipeline,
verifying its functionality with real database connections.
"""

import os
import sys
import unittest
import tempfile
import json
import pytest

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.testable_etl_pipeline import TestableETLPipeline
from src.utils.config import get_db_config, load_config
from tests.fixtures import (
    BASIC_SKYPE_DATA,
    test_db_connection,
    is_db_available
)


@pytest.mark.integration
class TestETLPipelineIntegration(unittest.TestCase):
    """Integration tests for the ETL pipeline with a real database."""

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

        # Create a pipeline instance with a test database connection
        with test_db_connection() as conn:
            self.pipeline = TestableETLPipeline(
                output_dir=self.test_dir,
                db_connection=conn
            )

    def tearDown(self):
        """Tear down test fixtures."""
        # Clean up temporary directory
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_integration_run_pipeline(self):
        """Test running the complete pipeline with a real database."""
        # Run the pipeline
        result = self.pipeline.run_pipeline(
            file_path=self.sample_file,
            user_display_name="Integration Test User"
        )

        # Verify the results
        self.assertIn('extraction', result)
        self.assertIn('transformation', result)
        self.assertIn('loading', result)
        self.assertTrue(result['extraction']['success'])
        self.assertTrue(result['transformation']['success'])
        self.assertTrue(result['loading']['success'])
        self.assertIn('exportId', result['loading'])

        # Verify the data was stored in the database
        with self.pipeline.conn.cursor() as cur:
            # Check raw exports
            cur.execute("SELECT COUNT(*) FROM skype_raw_exports")
            count = cur.fetchone()[0]
            self.assertGreaterEqual(count, 1)

            # Check conversations
            cur.execute("SELECT COUNT(*) FROM skype_conversations")
            count = cur.fetchone()[0]
            self.assertGreaterEqual(count, 1)

            # Check messages
            cur.execute("SELECT COUNT(*) FROM skype_messages")
            count = cur.fetchone()[0]
            self.assertGreaterEqual(count, 1)

    def test_integration_extract_transform_load(self):
        """Test each phase of the pipeline separately with a real database."""
        # Extract phase
        raw_data = self.pipeline.extract(file_path=self.sample_file)
        self.assertEqual(raw_data['userId'], self.sample_data['userId'])
        self.assertEqual(raw_data['exportDate'], self.sample_data['exportDate'])

        # Transform phase
        transformed_data = self.pipeline.transform(raw_data, user_display_name="Integration Test User")
        self.assertIn('metadata', transformed_data)
        self.assertIn('conversations', transformed_data)
        self.assertEqual(transformed_data['metadata']['userId'], raw_data['userId'])

        # Load phase
        export_id = self.pipeline.load(raw_data, transformed_data, file_source=self.sample_file)
        self.assertIsNotNone(export_id)

        # Verify the data was stored in the database
        with self.pipeline.conn.cursor() as cur:
            # Check the specific export
            cur.execute("SELECT user_id FROM skype_raw_exports WHERE export_id = %s", (export_id,))
            user_id = cur.fetchone()[0]
            self.assertEqual(user_id, raw_data['userId'])

    def test_integration_output_files(self):
        """Test that output files are created correctly."""
        # Run the pipeline
        self.pipeline.run_pipeline(
            file_path=self.sample_file,
            user_display_name="Integration Test User"
        )

        # Check that output files were created
        raw_output_path = os.path.join(self.test_dir, 'raw_data.json')
        transformed_output_path = os.path.join(self.test_dir, 'transformed_data.json')

        self.assertTrue(os.path.exists(raw_output_path))
        self.assertTrue(os.path.exists(transformed_output_path))

        # Verify the contents of the output files
        with open(raw_output_path, 'r') as f:
            raw_data = json.load(f)
            self.assertEqual(raw_data['userId'], self.sample_data['userId'])

        with open(transformed_output_path, 'r') as f:
            transformed_data = json.load(f)
            self.assertIn('metadata', transformed_data)
            self.assertIn('conversations', transformed_data)


# Helper function to get test database configuration
def get_test_db_config():
    """Get database configuration for testing."""
    from tests.fixtures import get_test_db_config
    return get_test_db_config()


if __name__ == '__main__':
    unittest.main()