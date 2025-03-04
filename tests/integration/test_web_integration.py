#!/usr/bin/env python3
"""
Integration tests for the web application with the ETL pipeline.
"""

import os
import sys
import unittest
import json
import io
from unittest.mock import patch, MagicMock

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import the Flask app from the example
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..', 'examples')))
from web_etl_example import app


class TestWebIntegration(unittest.TestCase):
    """Test cases for the web application integration with the ETL pipeline."""

    def setUp(self):
        """Set up test fixtures."""
        # Configure the Flask app for testing
        app.config['TESTING'] = True
        app.config['WTF_CSRF_ENABLED'] = False
        self.client = app.test_client()

        # Create a sample Skype export data
        self.sample_data = {
            "userId": "test_user",
            "exportDate": "2023-01-01T12:00:00Z",
            "conversations": [
                {
                    "id": "conversation1",
                    "displayName": "Test Conversation 1",
                    "messages": [
                        {
                            "id": "msg1",
                            "timestamp": "2023-01-01T12:30:00Z",
                            "from": {"id": "user1", "displayName": "User 1"},
                            "content": "Hello, world!",
                            "messageType": "RichText"
                        },
                        {
                            "id": "msg2",
                            "timestamp": "2023-01-01T12:35:00Z",
                            "from": {"id": "user2", "displayName": "User 2"},
                            "content": "Hi there!",
                            "messageType": "RichText"
                        }
                    ]
                }
            ]
        }

        # Create a mock file for upload
        self.mock_file = io.BytesIO(json.dumps(self.sample_data).encode('utf-8'))
        self.mock_file.name = 'test.json'

    def test_login_page(self):
        """Test the login page."""
        # Send a GET request to the login page
        response = self.client.get('/login')

        # Verify the response
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Login', response.data)
        self.assertIn(b'<form', response.data)
        self.assertIn(b'username', response.data)
        self.assertIn(b'password', response.data)

    def test_upload_endpoint_redirects_to_login(self):
        """Test that the upload endpoint redirects to login when not authenticated."""
        # Create a test file for upload
        data = {
            'file': (self.mock_file, 'test.json'),
            'user_display_name': 'Test User',
            'csrf_token': 'test_csrf_token'
        }

        # Send a POST request to the upload endpoint
        response = self.client.post('/upload', data=data, content_type='multipart/form-data')

        # Verify the response is a redirect to login
        self.assertEqual(response.status_code, 302)
        self.assertTrue('/login' in response.location)

    def test_api_upload_endpoint_redirects_to_login(self):
        """Test that the API upload endpoint redirects to login when not authenticated."""
        # Create a test file for upload
        data = {
            'file': (self.mock_file, 'test.json'),
            'user_display_name': 'Test User'
        }

        # Send a POST request to the API upload endpoint
        response = self.client.post(
            '/api/upload',
            data=data,
            content_type='multipart/form-data',
            headers={'X-API-Key': 'test_api_key'}
        )

        # Verify the response is a redirect to login
        self.assertEqual(response.status_code, 302)
        self.assertTrue('/login' in response.location)

    def test_authentication_flow(self):
        """Test the authentication flow."""
        # Test login with valid credentials
        with patch('examples.web_etl_example.check_password_hash', return_value=True):
            response = self.client.post('/login', data={
                'username': 'admin',
                'password': 'admin'
            }, follow_redirects=True)

            # Verify successful login redirects to index and shows the upload form
            self.assertEqual(response.status_code, 200)
            self.assertIn(b'Skype ETL Pipeline', response.data)
            self.assertIn(b'<form', response.data)


if __name__ == '__main__':
    unittest.main()