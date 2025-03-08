#!/usr/bin/env python3
"""
Tests for API versioning.

This test suite verifies that both the original and versioned API endpoints
work correctly.
"""

import io
import json
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pytest

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# Conditionally import FastAPI TestClient
try:
    from fastapi.testclient import TestClient as FastAPITestClient

    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

# Import Flask's test client
from flask.testing import FlaskClient

from src.api.skype_api import create_app
from src.utils.config import get_db_config
from tests.fixtures import is_db_available
from tests.fixtures import test_db_connection as db_connection_fixture


# Define a proper test function for database connection
def test_db_connection():
    """Test that the database connection works."""
    # Skip if database is not available
    if not is_db_available():
        pytest.skip("Database not available for testing")

    # Use the fixture to get a connection and assert it works
    with db_connection_fixture() as conn:
        assert conn is not None
        # Verify we can execute a simple query
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            result = cur.fetchone()
            assert result[0] == 1


@pytest.mark.skipif(not HAS_FASTAPI, reason="FastAPI not installed")
@pytest.mark.integration
@pytest.mark.api
class TestAPIVersioning(unittest.TestCase):
    """Tests for API versioning."""

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

        # Create app with authentication enabled
        self.app = create_app(
            db_config=None,  # No database needed for these tests
            upload_dir=self.upload_dir,
            output_dir=self.test_dir,
            enable_auth=True,
            api_key="test_api_key_123",
            require_auth=True,
        )

        # Create test client using Flask's test client instead of FastAPI's
        self.app.testing = True
        self.client = self.app.test_client()

        # Mock the user manager to authenticate with our test API key
        self.user_manager_patcher = patch(
            "src.api.user_management.UserManager.get_user_by_api_key"
        )
        self.mock_get_user_by_api_key = self.user_manager_patcher.start()
        self.mock_get_user_by_api_key.return_value = {
            "username": "testuser",
            "email": "test@example.com",
            "display_name": "Test User",
            "api_key": "test_api_key_123",
        }

    def tearDown(self):
        """Clean up test fixtures."""
        # Stop the user manager patcher
        self.user_manager_patcher.stop()

        # Remove temporary directory
        import shutil

        shutil.rmtree(self.temp_dir)

    def test_health_endpoints(self):
        """Test that both original and versioned health endpoints work."""
        # Test original endpoint
        response = self.client.get("/api/health")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertEqual(data["status"], "ok")
        self.assertIn("timestamp", data)
        self.assertIn("version", data)

        # Test versioned endpoint
        response = self.client.get("/api/v1/health")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertEqual(data["status"], "ok")
        self.assertIn("timestamp", data)
        self.assertIn("version", data)

    def test_version_endpoints(self):
        """Test that both original and versioned version endpoints work."""
        # Test original endpoint
        response = self.client.get("/api/version")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn("version", data)

        # Test versioned endpoint
        response = self.client.get("/api/v1/version")
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn("version", data)

    def test_authentication_endpoints(self):
        """Test that both original and versioned authentication endpoints work."""
        # Test register endpoints
        user_data = {
            "username": "testuser",
            "password": "testpassword",
            "email": "test@example.com",
            "display_name": "Test User",
        }

        # Mock the user manager to avoid file operations
        with patch(
            "src.api.user_management.UserManager.register_user"
        ) as mock_register:
            mock_register.return_value = True
            with patch("src.api.user_management.UserManager.get_user") as mock_get_user:
                mock_get_user.return_value = {
                    "username": "testuser",
                    "email": "test@example.com",
                    "display_name": "Test User",
                    "api_key": "test_api_key_123",
                }

                # Test original endpoint
                response = self.client.post("/api/register", json=user_data)
                self.assertEqual(response.status_code, 200)
                data = response.get_json()
                self.assertEqual(data["username"], "testuser")
                self.assertEqual(data["email"], "test@example.com")
                self.assertEqual(data["display_name"], "Test User")
                self.assertIn("api_key", data)

                # Test versioned endpoint
                response = self.client.post("/api/v1/register", json=user_data)
                self.assertEqual(response.status_code, 200)
                data = response.get_json()
                self.assertEqual(data["username"], "testuser")
                self.assertEqual(data["email"], "test@example.com")
                self.assertEqual(data["display_name"], "Test User")
                self.assertEqual(data["api_key"], "test_api_key_123")

        # Test login endpoints
        login_data = {"username": "testuser", "password": "testpassword"}

        # Mock the user manager to avoid file operations
        with patch(
            "src.api.user_management.UserManager.authenticate_user"
        ) as mock_auth:
            mock_auth.return_value = True
            with patch("src.api.user_management.UserManager.get_user") as mock_get_user:
                mock_get_user.return_value = {
                    "username": "testuser",
                    "email": "test@example.com",
                    "display_name": "Test User",
                    "api_key": "test_api_key_123",
                }

                # Test original endpoint
                response = self.client.post("/api/login", json=login_data)
                self.assertEqual(response.status_code, 200)
                data = response.get_json()
                self.assertEqual(data["username"], "testuser")
                self.assertEqual(data["email"], "test@example.com")
                self.assertEqual(data["display_name"], "Test User")
                self.assertEqual(data["api_key"], "test_api_key_123")

                # Test versioned endpoint
                response = self.client.post("/api/v1/login", json=login_data)
                self.assertEqual(response.status_code, 200)
                data = response.get_json()
                self.assertEqual(data["username"], "testuser")
                self.assertEqual(data["email"], "test@example.com")
                self.assertEqual(data["display_name"], "Test User")
                self.assertEqual(data["api_key"], "test_api_key_123")

    def test_authenticated_endpoints(self):
        """Test that both original and versioned authenticated endpoints work."""
        # Test profile endpoints
        # Test original endpoint
        response = self.client.get(
            "/api/profile", headers={"X-API-Key": "test_api_key_123"}
        )
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn("username", data)
        self.assertIn("email", data)
        self.assertIn("display_name", data)
        self.assertIn("api_key", data)

        # Test versioned endpoint
        response = self.client.get(
            "/api/v1/profile", headers={"X-API-Key": "test_api_key_123"}
        )
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn("username", data)
        self.assertIn("email", data)
        self.assertIn("display_name", data)
        self.assertIn("api_key", data)

        # Test regenerate API key endpoints
        # Test original endpoint
        response = self.client.post(
            "/api/regenerate-api-key", headers={"X-API-Key": "test_api_key_123"}
        )
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn("api_key", data)

        # Test versioned endpoint
        response = self.client.post(
            "/api/v1/regenerate-api-key", headers={"X-API-Key": "test_api_key_123"}
        )
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn("api_key", data)

    def test_upload_endpoints(self):
        """Test that both original and versioned upload endpoints work."""
        # Create a test file
        test_file_path = os.path.join(self.temp_dir, "test.json")
        with open(test_file_path, "w") as f:
            json.dump({"test": "data"}, f)

        # Mock the ETL pipeline and ETLContext to avoid actual processing
        with patch("src.api.skype_api.ETLPipeline") as mock_pipeline, patch(
            "src.api.skype_api.ETLContext"
        ) as mock_context:
            # Configure the mock ETLContext
            mock_context_instance = MagicMock()
            mock_context.return_value = mock_context_instance

            # Configure the mock ETLPipeline
            mock_pipeline_instance = MagicMock()
            mock_pipeline_instance.run_pipeline.return_value = {
                "success": True,
                "export_id": 123,
                "phases": {
                    "extract": {
                        "status": "completed",
                        "processed_conversations": 10,
                        "processed_messages": 100,
                    },
                    "transform": {
                        "status": "completed",
                        "processed_conversations": 10,
                        "processed_messages": 100,
                    },
                    "load": {
                        "status": "completed",
                        "processed_conversations": 10,
                        "processed_messages": 100,
                        "export_id": 123,
                    },
                },
            }
            mock_pipeline.return_value = mock_pipeline_instance

            # Test original endpoint
            with open(test_file_path, "rb") as f:
                # Use Flask's test client file upload format
                response = self.client.post(
                    "/api/upload",
                    data={
                        "file": (io.BytesIO(f.read()), "test.json", "application/json")
                    },
                    headers={"X-API-Key": "test_api_key_123"},
                )
                self.assertEqual(response.status_code, 200)
                data = response.get_json()
                self.assertTrue(data["success"])
                self.assertEqual(data["export_id"], 123)

            # Test versioned endpoint
            with open(test_file_path, "rb") as f:
                response = self.client.post(
                    "/api/v1/upload",
                    data={
                        "file": (io.BytesIO(f.read()), "test.json", "application/json")
                    },
                    headers={"X-API-Key": "test_api_key_123"},
                )
                self.assertEqual(response.status_code, 200)
                data = response.get_json()
                self.assertTrue(data["success"])
                self.assertEqual(data["export_id"], 123)

    def test_task_status_endpoints(self):
        """Test that both original and versioned task status endpoints work."""
        # Mock the progress tracker to avoid actual tracking
        with patch("src.api.skype_api.get_tracker") as mock_get_tracker:
            mock_tracker = MagicMock()
            mock_tracker.get_status.return_value = {
                "status": "completed",
                "progress": 100,
                "message": "Processing completed successfully",
                "export_id": 123,
            }
            mock_get_tracker.return_value = mock_tracker

            # Test original endpoint
            response = self.client.get(
                "/api/status/test_task_id", headers={"X-API-Key": "test_api_key_123"}
            )
            self.assertEqual(response.status_code, 200)
            data = response.get_json()
            self.assertEqual(data["status"], "completed")
            self.assertEqual(data["progress"], 100)
            self.assertEqual(data["message"], "Processing completed successfully")
            self.assertEqual(data["export_id"], 123)

            # Test versioned endpoint
            response = self.client.get(
                "/api/v1/status/test_task_id", headers={"X-API-Key": "test_api_key_123"}
            )
            self.assertEqual(response.status_code, 200)
            data = response.get_json()
            self.assertEqual(data["status"], "completed")
            self.assertEqual(data["progress"], 100)
            self.assertEqual(data["message"], "Processing completed successfully")
            self.assertEqual(data["export_id"], 123)

    def test_analysis_endpoints(self):
        """Test that both original and versioned analysis endpoints work."""
        # Test original endpoint
        response = self.client.get(
            "/api/analysis/123", headers={"X-API-Key": "test_api_key_123"}
        )
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn("message_count", data)
        self.assertIn("conversation_count", data)
        self.assertIn("date_range", data)
        self.assertIn("top_contacts", data)

        # Test versioned endpoint
        response = self.client.get(
            "/api/v1/analysis/123", headers={"X-API-Key": "test_api_key_123"}
        )
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIn("message_count", data)
        self.assertIn("conversation_count", data)
        self.assertIn("date_range", data)
        self.assertIn("top_contacts", data)

    def test_report_endpoints(self):
        """Test that both original and versioned report endpoints work."""
        # Test original endpoint
        response = self.client.get(
            "/api/report/123", headers={"X-API-Key": "test_api_key_123"}
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "text/html; charset=utf-8")
        self.assertIn(b"Skype Export Report", response.data)
        self.assertIn(b"Export ID: 123", response.data)

        # Test versioned endpoint
        response = self.client.get(
            "/api/v1/report/123", headers={"X-API-Key": "test_api_key_123"}
        )
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.headers["content-type"], "text/html; charset=utf-8")
        self.assertIn(b"Skype Export Report", response.data)
        self.assertIn(b"Export ID: 123", response.data)

    def test_exports_endpoints(self):
        """Test that both original and versioned exports endpoints work."""
        # Test original endpoint
        response = self.client.get(
            "/api/exports", headers={"X-API-Key": "test_api_key_123"}
        )
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["export_id"], 123)

        # Test versioned endpoint
        response = self.client.get(
            "/api/v1/exports", headers={"X-API-Key": "test_api_key_123"}
        )
        self.assertEqual(response.status_code, 200)
        data = response.get_json()
        self.assertIsInstance(data, list)
        self.assertEqual(len(data), 1)
        self.assertEqual(data[0]["export_id"], 123)


if __name__ == "__main__":
    unittest.main()
