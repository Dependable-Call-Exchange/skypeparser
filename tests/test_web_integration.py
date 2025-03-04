#!/usr/bin/env python3
"""
Integration tests for the web application with the ETL pipeline.
"""

import os
import sys
import unittest
import json
import io
from unittest.mock import patch

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the Flask app from the example
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'examples')))
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

    @patch('src.db.etl_pipeline.SkypeETLPipeline.run_pipeline')
    def test_upload_endpoint(self, mock_run_pipeline):
        """Test the upload endpoint."""
        # Set up the mock
        mock_run_pipeline.return_value = {
            'export_id': 1,
            'conversations': [{'id': 'conversation1', 'display_name': 'Test Conversation 1'}],
            'message_count': 2
        }

        # Create a test file for upload
        data = {
            'file': (self.mock_file, 'test.json'),
            'user_name': 'Test User'
        }

        # Send a POST request to the upload endpoint
        response = self.client.post('/upload', data=data, content_type='multipart/form-data')

        # Verify the response
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Upload successful', response.data)
        self.assertIn(b'Test Conversation 1', response.data)
        self.assertIn(b'2 messages', response.data)

        # Verify that the run_pipeline method was called with the correct arguments
        mock_run_pipeline.assert_called_once()
        args, kwargs = mock_run_pipeline.call_args
        self.assertEqual(kwargs['user_display_name'], 'Test User')

    @patch('src.db.etl_pipeline.SkypeETLPipeline.run_pipeline')
    def test_api_upload_endpoint(self, mock_run_pipeline):
        """Test the API upload endpoint."""
        # Set up the mock
        mock_run_pipeline.return_value = {
            'export_id': 1,
            'conversations': [{'id': 'conversation1', 'display_name': 'Test Conversation 1'}],
            'message_count': 2
        }

        # Create a test file for upload
        data = {
            'file': (self.mock_file, 'test.json'),
            'user_name': 'Test User'
        }

        # Send a POST request to the API upload endpoint
        response = self.client.post('/api/upload', data=data, content_type='multipart/form-data')

        # Verify the response
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.content_type, 'application/json')

        # Parse the JSON response
        response_data = json.loads(response.data)

        # Verify the response data
        self.assertEqual(response_data['status'], 'success')
        self.assertEqual(response_data['export_id'], 1)
        self.assertEqual(len(response_data['conversations']), 1)
        self.assertEqual(response_data['message_count'], 2)

        # Verify that the run_pipeline method was called with the correct arguments
        mock_run_pipeline.assert_called_once()
        args, kwargs = mock_run_pipeline.call_args
        self.assertEqual(kwargs['user_display_name'], 'Test User')

    def test_index_page(self):
        """Test the index page."""
        # Send a GET request to the index page
        response = self.client.get('/')

        # Verify the response
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Upload Skype Export', response.data)
        self.assertIn(b'<form', response.data)
        self.assertIn(b'<input type="file"', response.data)

    @patch('src.db.etl_pipeline.SkypeETLPipeline.run_pipeline')
    def test_upload_error_handling(self, mock_run_pipeline):
        """Test error handling in the upload endpoint."""
        # Set up the mock to raise an exception
        mock_run_pipeline.side_effect = Exception("Test error")

        # Create a test file for upload
        data = {
            'file': (self.mock_file, 'test.json'),
            'user_name': 'Test User'
        }

        # Send a POST request to the upload endpoint
        response = self.client.post('/upload', data=data, content_type='multipart/form-data')

        # Verify the response
        self.assertEqual(response.status_code, 200)
        self.assertIn(b'Error processing file', response.data)
        self.assertIn(b'Test error', response.data)


if __name__ == '__main__':
    unittest.main()