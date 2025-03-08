#!/usr/bin/env python3
"""
End-to-End workflow tests for the SkypeParser project.

This test suite focuses on testing complete user workflows from file upload
through ETL processing to data analysis, ensuring all components work together.
"""

import json
import os
import sys
import tempfile
import time
import unittest

import pytest

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.analysis.reporting import SkypeReportGenerator
from src.api.skype_api import create_app
from src.db.etl import ETLPipeline
from tests.fixtures import (
    BASIC_SKYPE_DATA,
    COMPLEX_SKYPE_DATA,
    is_db_available,
    test_db_connection,
)

# Conditionally import FastAPI TestClient
try:
    from fastapi.testclient import TestClient

    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False


@pytest.mark.integration
@pytest.mark.e2e
class TestCompleteWorkflow(unittest.TestCase):
    """Tests for complete end-to-end workflows."""

    def setUp(self):
        """Set up the test environment."""
        # Skip integration tests unless database is available
        if not is_db_available():
            self.skipTest("Integration tests disabled. Database not available.")

        # Set up temporary directories
        self.temp_dir = tempfile.mkdtemp()
        self.test_dir = os.path.join(self.temp_dir, "test_output")
        self.upload_dir = os.path.join(self.temp_dir, "uploads")
        os.makedirs(self.test_dir, exist_ok=True)
        os.makedirs(self.upload_dir, exist_ok=True)

        # Get database configuration
        self.db_config = get_test_db_config()

        # Create sample Skype export data files
        self.sample_file = os.path.join(self.temp_dir, "sample.json")
        with open(self.sample_file, "w", encoding="utf-8") as f:
            json.dump(BASIC_SKYPE_DATA, f)

        self.complex_file = os.path.join(self.temp_dir, "complex.json")
        with open(self.complex_file, "w", encoding="utf-8") as f:
            json.dump(COMPLEX_SKYPE_DATA, f)

    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary directory
        import shutil

        shutil.rmtree(self.temp_dir)

    def test_cli_workflow(self):
        """Test complete workflow using CLI commands."""
        # Set up the ETL pipeline
        pipeline = ETLPipeline(db_config=self.db_config, output_dir=self.test_dir)

        # Step 1: Run ETL pipeline on the sample file
        etl_result = pipeline.run_pipeline(
            file_path=self.sample_file, user_display_name="CLI Test User"
        )

        # Verify ETL success
        self.assertTrue(
            etl_result["success"],
            f"ETL pipeline failed: {etl_result.get('error', 'Unknown error')}",
        )
        self.assertIn("export_id", etl_result, "Export ID not found in ETL result")
        export_id = etl_result["export_id"]

        # Step 2: Generate a report on the processed data
        report_path = os.path.join(self.test_dir, f"report_{export_id}.html")
        report_result = generate_report(
            export_id=export_id, output_file=report_path, db_config=self.db_config
        )

        # Verify report generation
        self.assertTrue(os.path.exists(report_path), "Report file was not created")
        self.assertIn(
            "conversation_count",
            report_result,
            "Report result missing conversation count",
        )
        self.assertIn(
            "message_count", report_result, "Report result missing message count"
        )
        self.assertGreater(
            report_result["message_count"], 0, "Report shows no messages"
        )

        # Step 3: Verify data in the database
        with test_db_connection(self.db_config) as conn:
            with conn.cursor() as cursor:
                # Verify export record
                cursor.execute(
                    "SELECT COUNT(*) FROM skype_raw_exports WHERE export_id = %s",
                    (export_id,),
                )
                export_count = cursor.fetchone()[0]
                self.assertEqual(export_count, 1, "Export record not found in database")

                # Verify conversations
                cursor.execute(
                    "SELECT COUNT(*) FROM skype_conversations WHERE export_id = %s",
                    (export_id,),
                )
                conversation_count = cursor.fetchone()[0]
                self.assertGreater(
                    conversation_count, 0, "No conversations found in database"
                )

                # Verify messages
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
                self.assertGreater(message_count, 0, "No messages found in database")

    @pytest.mark.skipif(not HAS_FASTAPI, reason="FastAPI not installed")
    def test_api_workflow(self):
        """Test complete workflow using the API."""
        # Create API app and test client
        app = create_app(
            db_config=self.db_config,
            upload_dir=self.upload_dir,
            output_dir=self.test_dir,
        )
        client = TestClient(app)

        # Step 1: Upload file
        with open(self.sample_file, "rb") as f:
            upload_response = client.post(
                "/api/upload",
                files={"file": ("skype_export.json", f, "application/json")},
            )

        # Verify upload success
        self.assertEqual(
            upload_response.status_code, 200, f"Upload failed: {upload_response.text}"
        )
        upload_data = upload_response.json()
        self.assertIn("task_id", upload_data, "Task ID not found in upload response")
        task_id = upload_data["task_id"]

        # Step 2: Check processing status
        status = "pending"
        retry_count = 0
        max_retries = 10

        while status in ("pending", "processing") and retry_count < max_retries:
            time.sleep(1)
            status_response = client.get(f"/api/status/{task_id}")
            self.assertEqual(
                status_response.status_code,
                200,
                f"Status check failed: {status_response.text}",
            )
            status_data = status_response.json()
            status = status_data.get("status", "unknown")
            retry_count += 1

        # Verify processing completed successfully
        self.assertEqual(
            status,
            "completed",
            f"Processing failed or timed out. Final status: {status}",
        )
        self.assertIn(
            "export_id", status_data, "Export ID not found in status response"
        )
        export_id = status_data["export_id"]

        # Step 3: Get analysis results
        analysis_response = client.get(f"/api/analysis/{export_id}")
        self.assertEqual(
            analysis_response.status_code,
            200,
            f"Analysis failed: {analysis_response.text}",
        )
        analysis_data = analysis_response.json()

        # Verify analysis data
        self.assertIn("message_count", analysis_data, "Analysis missing message count")
        self.assertIn(
            "conversation_count", analysis_data, "Analysis missing conversation count"
        )
        self.assertGreater(
            analysis_data["message_count"], 0, "Analysis shows no messages"
        )

        # Step 4: Generate report
        report_response = client.get(f"/api/report/{export_id}")
        self.assertEqual(
            report_response.status_code,
            200,
            f"Report generation failed: {report_response.text}",
        )

        # Check report content type
        self.assertEqual(
            report_response.headers.get("content-type"),
            "text/html",
            "Report is not in HTML format",
        )
        self.assertIn(
            b"<html", report_response.content, "Response does not contain HTML content"
        )


# Helper function to get test database configuration
def get_test_db_config():
    """Get database configuration for testing."""
    from tests.fixtures import get_test_db_config

    return get_test_db_config()


if __name__ == "__main__":
    unittest.main()
