#!/usr/bin/env python3
"""
Unit tests for the AttachmentHandler class.
"""

import os
import unittest
import tempfile
import shutil
import json
from unittest.mock import patch, MagicMock
from io import BytesIO
from PIL import Image

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.utils.attachment_handler import AttachmentHandler

class TestAttachmentHandler(unittest.TestCase):
    """Test cases for the AttachmentHandler class."""

    def setUp(self):
        """Set up test environment."""
        # Create temporary directory for test data
        self.temp_dir = tempfile.mkdtemp()
        self.attachment_handler = AttachmentHandler(storage_dir=self.temp_dir)

        # Create a test image
        self.test_image_path = os.path.join(self.temp_dir, 'test_image.jpg')
        img = Image.new('RGB', (100, 100), color='red')
        img.save(self.test_image_path)

        # Sample attachment data
        self.image_attachment = {
            'type': 'image',
            'name': 'test_image.jpg',
            'url': 'http://example.com/test_image.jpg',
            'content_type': 'image/jpeg',
            'size': 1024
        }

        self.document_attachment = {
            'type': 'file',
            'name': 'test_document.pdf',
            'url': 'http://example.com/test_document.pdf',
            'content_type': 'application/pdf',
            'size': 2048
        }

        # Sample message with attachments
        self.test_message = {
            'id': 'msg123',
            'content': 'Test message with attachments',
            'attachments': [
                self.image_attachment,
                self.document_attachment
            ]
        }

    def tearDown(self):
        """Clean up after tests."""
        shutil.rmtree(self.temp_dir)

    def test_init(self):
        """Test initialization of AttachmentHandler."""
        # Verify directories are created
        self.assertTrue(os.path.exists(self.temp_dir))
        self.assertTrue(os.path.exists(os.path.join(self.temp_dir, 'images')))
        self.assertTrue(os.path.exists(os.path.join(self.temp_dir, 'videos')))
        self.assertTrue(os.path.exists(os.path.join(self.temp_dir, 'audio')))
        self.assertTrue(os.path.exists(os.path.join(self.temp_dir, 'documents')))
        self.assertTrue(os.path.exists(os.path.join(self.temp_dir, 'other')))
        self.assertTrue(os.path.exists(os.path.join(self.temp_dir, 'thumbnails')))

    def test_get_storage_path(self):
        """Test getting the appropriate storage path for attachments."""
        # Test image path
        image_path = self.attachment_handler._get_storage_path(self.image_attachment)
        self.assertTrue(image_path.startswith(os.path.join(self.temp_dir, 'images')))
        self.assertTrue(image_path.endswith('test_image.jpg'))

        # Test document path
        document_path = self.attachment_handler._get_storage_path(self.document_attachment)
        self.assertTrue(document_path.startswith(os.path.join(self.temp_dir, 'documents')))
        self.assertTrue(document_path.endswith('test_document.pdf'))

        # Test with no name but URL
        no_name_attachment = {
            'url': 'http://example.com/unnamed.txt',
            'content_type': 'text/plain'
        }
        no_name_path = self.attachment_handler._get_storage_path(no_name_attachment)
        self.assertTrue(no_name_path.startswith(os.path.join(self.temp_dir, 'documents')))
        self.assertTrue('unnamed.txt' in no_name_path)

        # Test with no name and no URL
        empty_attachment = {
            'content_type': 'application/octet-stream'
        }
        empty_path = self.attachment_handler._get_storage_path(empty_attachment)
        self.assertTrue(empty_path.startswith(os.path.join(self.temp_dir, 'documents')))
        self.assertTrue('attachment-' in empty_path)

    @patch('requests.get')
    def test_download_attachment(self, mock_get):
        """Test downloading an attachment."""
        # Mock the response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.iter_content.return_value = [b'test content']
        mock_response.headers = {'content-type': 'image/jpeg'}
        mock_get.return_value = mock_response

        # Test successful download
        path = self.attachment_handler.download_attachment(self.image_attachment)
        self.assertIsNotNone(path)
        self.assertTrue(os.path.exists(path))
        mock_get.assert_called_once_with(
            'http://example.com/test_image.jpg',
            stream=True,
            timeout=30
        )

        # Test with no URL
        no_url_attachment = {'name': 'test.txt', 'content_type': 'text/plain'}
        path = self.attachment_handler.download_attachment(no_url_attachment)
        self.assertIsNone(path)

        # Test with exception
        mock_get.side_effect = Exception('Download error')
        path = self.attachment_handler.download_attachment(self.image_attachment)
        self.assertIsNone(path)

    def test_generate_thumbnail(self):
        """Test generating a thumbnail for an image."""
        # Generate thumbnail
        thumbnail_path = self.attachment_handler.generate_thumbnail(self.test_image_path)
        self.assertIsNotNone(thumbnail_path)
        self.assertTrue(os.path.exists(thumbnail_path))

        # Verify thumbnail dimensions
        thumb_img = Image.open(thumbnail_path)
        self.assertLessEqual(thumb_img.width, 200)
        self.assertLessEqual(thumb_img.height, 200)

        # Test with non-existent file
        thumbnail_path = self.attachment_handler.generate_thumbnail('nonexistent.jpg')
        self.assertIsNone(thumbnail_path)

    def test_extract_image_metadata(self):
        """Test extracting metadata from an image."""
        # Extract metadata
        metadata = self.attachment_handler.extract_image_metadata(self.test_image_path)
        self.assertIsInstance(metadata, dict)
        self.assertEqual(metadata['format'], 'JPEG')
        self.assertEqual(metadata['width'], 100)
        self.assertEqual(metadata['height'], 100)

        # Test with non-existent file
        metadata = self.attachment_handler.extract_image_metadata('nonexistent.jpg')
        self.assertIsInstance(metadata, dict)
        self.assertEqual(len(metadata), 0)

    @patch.object(AttachmentHandler, 'download_attachment')
    @patch.object(AttachmentHandler, 'extract_image_metadata')
    @patch.object(AttachmentHandler, 'generate_thumbnail')
    def test_enrich_attachment_data(self, mock_generate_thumbnail, mock_extract_metadata, mock_download):
        """Test enriching attachment data."""
        # Mock download to return a path
        mock_download.return_value = self.test_image_path

        # Mock metadata
        mock_extract_metadata.return_value = {
            'format': 'JPEG',
            'width': 100,
            'height': 100
        }

        # Mock thumbnail
        mock_generate_thumbnail.return_value = os.path.join(self.temp_dir, 'thumbnails', 'thumb_test.jpg')

        # Test enriching an image attachment
        enriched = self.attachment_handler.enrich_attachment_data(self.image_attachment)
        self.assertIn('local_path', enriched)
        self.assertIn('metadata', enriched)
        self.assertIn('thumbnail_path', enriched)

        # Verify download was called
        mock_download.assert_called_once_with(self.image_attachment)

        # Reset the mock for the next test
        mock_download.reset_mock()

        # Test with pre-downloaded file
        enriched = self.attachment_handler.enrich_attachment_data(
            self.image_attachment,
            file_path=self.test_image_path
        )
        self.assertIn('local_path', enriched)

        # Now verify download was not called when we provide the file path
        mock_download.assert_not_called()

        # Reset the mock again for the next test
        mock_download.reset_mock()

        # Test with download failure
        mock_download.return_value = None
        enriched = self.attachment_handler.enrich_attachment_data(self.image_attachment)
        self.assertNotIn('local_path', enriched)
        self.assertNotIn('metadata', enriched)

    @patch.object(AttachmentHandler, 'enrich_attachment_data')
    def test_process_message_attachments(self, mock_enrich):
        """Test processing all attachments in a message."""
        # Mock enrichment
        mock_enrich.side_effect = [
            {**self.image_attachment, 'local_path': 'path/to/image.jpg', 'metadata': {'format': 'JPEG'}},
            {**self.document_attachment, 'local_path': 'path/to/document.pdf'}
        ]

        # Process message
        processed = self.attachment_handler.process_message_attachments(self.test_message)
        self.assertEqual(len(processed['attachments']), 2)
        self.assertIn('local_path', processed['attachments'][0])
        self.assertIn('metadata', processed['attachments'][0])
        self.assertIn('local_path', processed['attachments'][1])

        # Test with no attachments
        msg_no_attachments = {'id': 'msg456', 'content': 'No attachments'}
        processed = self.attachment_handler.process_message_attachments(msg_no_attachments)
        self.assertEqual(processed, msg_no_attachments)


if __name__ == '__main__':
    unittest.main()