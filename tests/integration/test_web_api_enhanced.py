#!/usr/bin/env python3
"""
Enhanced tests for the Web API.

This test suite provides comprehensive testing for the Skype Parser Web API,
including authentication, error handling, and edge cases.
"""

import json
import os
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import pytest

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Conditionally import FastAPI TestClient
try:
    from fastapi.testclient import TestClient

    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

from src.api.skype_api import create_app
from src.utils.config import get_db_config
from tests.fixtures import (
    BASIC_SKYPE_DATA,
    COMPLEX_SKYPE_DATA,
    SkypeDataFactory,
    is_db_available,
    test_db_connection,
)


@pytest.mark.skipif(not HAS_FASTAPI, reason="FastAPI not installed")
@pytest.mark.integration
@pytest.mark.api
class TestWebAPIEnhanced(unittest.TestCase):
    """Enhanced tests for the Web API."""

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

        # Create test files
        self.sample_file = os.path.join(self.temp_dir, "sample.json")
        with open(self.sample_file, "w") as f:
            json.dump(BASIC_SKYPE_DATA, f)

        self.complex_file = os.path.join(self.temp_dir, "complex.json")
        with open(self.complex_file, "w") as f:
            json.dump(COMPLEX_SKYPE_DATA, f)

        # Create corrupted file
        self.corrupted_file = os.path.join(self.temp_dir, "corrupted.json")
        with open(self.corrupted_file, "w") as f:
            f.write('{"this_is_not": "valid_json')

        # Create empty file
        self.empty_file = os.path.join(self.temp_dir, "empty.json")
        with open(self.empty_file, "w") as f:
            f.write("")

        # Create app with authentication enabled
        self.app = create_app(
            db_config=self.db_config,
            upload_dir=self.upload_dir,
            output_dir=self.test_dir,
            enable_auth=True,
            api_key="test_api_key_123",
            require_auth=True,
        )

        # Create app without authentication for some tests
        self.app_no_auth = create_app(
            db_config=self.db_config,
            upload_dir=self.upload_dir,
            output_dir=self.test_dir,
            enable_auth=False,
            require_auth=False,
        )

        # Create test clients
        self.client = TestClient(self.app)
        self.client_no_auth = TestClient(self.app_no_auth)

    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary directory
        import shutil

        shutil.rmtree(self.temp_dir)

    def test_health_check_endpoints(self):
        """Test health check endpoints."""
        # Health check should work without authentication
        response = self.client.get("/api/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

        # Version endpoint should also work without authentication
        response = self.client.get("/api/version")
        self.assertEqual(response.status_code, 200)
        self.assertIn("version", response.json())

    def test_authentication_required(self):
        """Test that protected endpoints require authentication."""
        # Attempt to access protected endpoint without authentication
        response = self.client.get("/api/exports")
        self.assertEqual(response.status_code, 401)
        self.assertIn("error", response.json())
        self.assertIn("Authentication", response.json()["error"])

        # Attempt to upload without authentication
        with open(self.sample_file, "rb") as f:
            response = self.client.post(
                "/api/upload", files={"file": ("test.json", f, "application/json")}
            )
        self.assertEqual(response.status_code, 401)

    def test_authentication_success(self):
        """Test successful authentication."""
        # Access protected endpoint with valid API key
        response = self.client.get(
            "/api/exports", headers={"X-API-Key": "test_api_key_123"}
        )
        self.assertEqual(response.status_code, 200)

        # Upload with valid API key
        with open(self.sample_file, "rb") as f:
            response = self.client.post(
                "/api/upload",
                files={"file": ("test.json", f, "application/json")},
                headers={"X-API-Key": "test_api_key_123"},
            )
        self.assertEqual(response.status_code, 200)
        self.assertIn("task_id", response.json())

    def test_authentication_invalid_key(self):
        """Test authentication with invalid API key."""
        # Access protected endpoint with invalid API key
        response = self.client.get("/api/exports", headers={"X-API-Key": "invalid_key"})
        self.assertEqual(response.status_code, 401)
        self.assertIn("error", response.json())
        self.assertIn("Invalid API key", response.json()["error"])

    def test_file_upload_and_processing(self):
        """Test successful file upload and processing."""
        # Upload file with authentication
        with open(self.sample_file, "rb") as f:
            response = self.client.post(
                "/api/upload",
                files={"file": ("test.json", f, "application/json")},
                headers={"X-API-Key": "test_api_key_123"},
            )

        # Verify upload success
        self.assertEqual(response.status_code, 200)
        self.assertIn("task_id", response.json())
        task_id = response.json()["task_id"]

        # Check processing status
        max_retries = 10
        retry_count = 0
        status = None

        while retry_count < max_retries:
            status_response = self.client.get(
                f"/api/status/{task_id}", headers={"X-API-Key": "test_api_key_123"}
            )
            self.assertEqual(status_response.status_code, 200)
            status = status_response.json().get("status")

            if status == "completed":
                break

            retry_count += 1
            time.sleep(1)

        # Verify processing completed
        self.assertEqual(status, "completed", "Processing did not complete in time")

        # Verify export ID is available
        self.assertIn("export_id", status_response.json())
        export_id = status_response.json()["export_id"]

        # Get analysis data
        analysis_response = self.client.get(
            f"/api/analysis/{export_id}", headers={"X-API-Key": "test_api_key_123"}
        )
        self.assertEqual(analysis_response.status_code, 200)
        self.assertIn("message_count", analysis_response.json())

        # Get report
        report_response = self.client.get(
            f"/api/report/{export_id}", headers={"X-API-Key": "test_api_key_123"}
        )
        self.assertEqual(report_response.status_code, 200)
        self.assertEqual(report_response.headers.get("content-type"), "text/html")

    def test_upload_invalid_file(self):
        """Test upload of invalid files."""
        # Upload corrupted JSON file
        with open(self.corrupted_file, "rb") as f:
            response = self.client_no_auth.post(
                "/api/upload", files={"file": ("corrupted.json", f, "application/json")}
            )

        # Verify upload succeeds (validation happens during processing)
        self.assertEqual(response.status_code, 200)
        self.assertIn("task_id", response.json())
        task_id = response.json()["task_id"]

        # Check processing status - should eventually fail
        max_retries = 10
        retry_count = 0
        status = None

        while retry_count < max_retries:
            status_response = self.client_no_auth.get(f"/api/status/{task_id}")
            status = status_response.json().get("status")

            if status in ("failed", "error"):
                break

            retry_count += 1
            time.sleep(1)

        # Verify processing failed
        self.assertIn(
            status,
            ("failed", "error"),
            "Processing should have failed with corrupted file",
        )
        self.assertIn("error", status_response.json())

    def test_upload_empty_file(self):
        """Test upload of empty file."""
        # Upload empty file
        with open(self.empty_file, "rb") as f:
            response = self.client_no_auth.post(
                "/api/upload", files={"file": ("empty.json", f, "application/json")}
            )

        # Verify upload succeeds (validation happens during processing)
        self.assertEqual(response.status_code, 200)
        self.assertIn("task_id", response.json())
        task_id = response.json()["task_id"]

        # Check processing status - should fail
        max_retries = 10
        retry_count = 0
        status = None

        while retry_count < max_retries:
            status_response = self.client_no_auth.get(f"/api/status/{task_id}")
            status = status_response.json().get("status")

            if status in ("failed", "error"):
                break

            retry_count += 1
            time.sleep(1)

        # Verify processing failed
        self.assertIn(
            status, ("failed", "error"), "Processing should have failed with empty file"
        )

    def test_error_handling_invalid_task_id(self):
        """Test error handling for invalid task ID."""
        # Check status with invalid task ID
        response = self.client_no_auth.get("/api/status/invalid-task-id")
        self.assertEqual(response.status_code, 404)
        self.assertIn("error", response.json())

    def test_error_handling_invalid_export_id(self):
        """Test error handling for invalid export ID."""
        # Get analysis with invalid export ID
        response = self.client_no_auth.get("/api/analysis/invalid-export-id")
        self.assertEqual(response.status_code, 404)
        self.assertIn("error", response.json())

        # Get report with invalid export ID
        response = self.client_no_auth.get("/api/report/invalid-export-id")
        self.assertEqual(response.status_code, 404)
        self.assertIn("error", response.json())

    def test_exports_listing(self):
        """Test listing of exports."""
        # Upload file to create an export
        with open(self.sample_file, "rb") as f:
            response = self.client_no_auth.post(
                "/api/upload", files={"file": ("test.json", f, "application/json")}
            )

        # Get task ID
        task_id = response.json()["task_id"]

        # Wait for processing to complete
        max_retries = 10
        retry_count = 0

        while retry_count < max_retries:
            status_response = self.client_no_auth.get(f"/api/status/{task_id}")
            status = status_response.json().get("status")

            if status == "completed":
                break

            retry_count += 1
            time.sleep(1)

        # Get exports list
        exports_response = self.client_no_auth.get("/api/exports")
        self.assertEqual(exports_response.status_code, 200)

        # Verify that the list contains at least one export
        exports = exports_response.json()
        self.assertIsInstance(exports, list)
        self.assertGreaterEqual(len(exports), 1)

        # Verify export structure
        if len(exports) > 0:
            export = exports[0]
            self.assertIn("export_id", export)
            self.assertIn("timestamp", export)
            self.assertIn("user_id", export)

    def test_file_upload_with_metadata(self):
        """Test file upload with additional metadata."""
        # Upload file with metadata
        with open(self.sample_file, "rb") as f:
            response = self.client_no_auth.post(
                "/api/upload",
                files={"file": ("test.json", f, "application/json")},
                data={
                    "user_display_name": "API Test User",
                    "description": "Test upload with metadata",
                    "tags": "test,api,metadata",
                },
            )

        # Verify upload success
        self.assertEqual(response.status_code, 200)
        self.assertIn("task_id", response.json())
        task_id = response.json()["task_id"]

        # Wait for processing to complete
        max_retries = 10
        retry_count = 0
        export_id = None

        while retry_count < max_retries:
            status_response = self.client_no_auth.get(f"/api/status/{task_id}")
            status = status_response.json().get("status")

            if status == "completed":
                export_id = status_response.json().get("export_id")
                break

            retry_count += 1
            time.sleep(1)

        # Verify processing completed
        self.assertIsNotNone(
            export_id, "Export ID should be available after processing"
        )

        # Get export details
        export_response = self.client_no_auth.get(f"/api/export/{export_id}")
        self.assertEqual(export_response.status_code, 200)

        # Verify metadata was saved
        export_data = export_response.json()
        self.assertEqual(export_data.get("user_display_name"), "API Test User")
        self.assertEqual(export_data.get("description"), "Test upload with metadata")

    def test_concurrent_uploads(self):
        """Test handling of concurrent uploads."""
        # Upload multiple files concurrently
        task_ids = []

        # File 1
        with open(self.sample_file, "rb") as f:
            response = self.client_no_auth.post(
                "/api/upload",
                files={"file": ("file1.json", f, "application/json")},
                data={"user_display_name": "Concurrent User 1"},
            )
            self.assertEqual(response.status_code, 200)
            task_ids.append(response.json()["task_id"])

        # File 2
        with open(self.complex_file, "rb") as f:
            response = self.client_no_auth.post(
                "/api/upload",
                files={"file": ("file2.json", f, "application/json")},
                data={"user_display_name": "Concurrent User 2"},
            )
            self.assertEqual(response.status_code, 200)
            task_ids.append(response.json()["task_id"])

        # Wait for all tasks to complete
        max_retries = 20
        completed_count = 0

        for _ in range(max_retries):
            completed_count = 0

            for task_id in task_ids:
                status_response = self.client_no_auth.get(f"/api/status/{task_id}")
                status = status_response.json().get("status")

                if status == "completed":
                    completed_count += 1

            if completed_count == len(task_ids):
                break

            time.sleep(1)

        # Verify all tasks completed
        self.assertEqual(
            completed_count,
            len(task_ids),
            f"Not all tasks completed. Completed: {completed_count}, Total: {len(task_ids)}",
        )

    def test_api_rate_limiting(self):
        """Test API rate limiting."""
        # Make multiple rapid requests to test rate limiting
        responses = []

        for _ in range(10):
            response = self.client_no_auth.get("/api/health")
            responses.append(response.status_code)

        # All responses should be successful (rate limit should be reasonable)
        self.assertTrue(
            all(status == 200 for status in responses),
            "Rate limiting should not affect reasonable request rates",
        )

    def test_api_input_validation(self):
        """Test API input validation."""
        # Test with missing file
        response = self.client_no_auth.post("/api/upload")
        self.assertEqual(response.status_code, 422)  # Unprocessable Entity

        # Test with empty file field
        response = self.client_no_auth.post(
            "/api/upload", files={"file": ("", "", "application/json")}
        )
        self.assertEqual(response.status_code, 400)  # Bad Request

        # Test with wrong file type
        with open(self.sample_file, "rb") as f:
            response = self.client_no_auth.post(
                "/api/upload", files={"file": ("test.txt", f, "text/plain")}
            )

        # File type validation may or may not be enforced, depends on implementation
        # But the request should be processed
        self.assertIn(response.status_code, (200, 400, 415))

    def test_error_response_format(self):
        """Test error response format for consistency."""
        # Get a 404 error
        response = self.client_no_auth.get("/api/nonexistent-endpoint")
        self.assertEqual(response.status_code, 404)

        # Verify error response format
        self.assertIn("error", response.json())
        self.assertIsInstance(response.json()["error"], str)

        # Get authentication error
        response = self.client.get("/api/exports", headers={"X-API-Key": "wrong-key"})
        self.assertEqual(response.status_code, 401)

        # Verify error response format
        self.assertIn("error", response.json())
        self.assertIsInstance(response.json()["error"], str)


# Helper function to get test database configuration
def get_test_db_config():
    """Get database configuration for testing."""
    from tests.fixtures import get_test_db_config

    return get_test_db_config()


if __name__ == "__main__":
    unittest.main()
