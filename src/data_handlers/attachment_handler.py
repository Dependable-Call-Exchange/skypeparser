#!/usr/bin/env python3
"""
Attachment Handler

This module provides functionality for handling attachments in Skype messages,
including downloading, metadata extraction, and thumbnail generation.
"""

import os
import logging
import mimetypes
import requests
import hashlib
from typing import Dict, Any, Optional, List, Tuple, BinaryIO
from PIL import Image
import io
import json
from pathlib import Path
from datetime import datetime

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class AttachmentHandler:
    """Class for handling message attachments."""

    def __init__(self, storage_dir: str = "attachments"):
        """Initialize the attachment handler.

        Args:
            storage_dir: Directory to store downloaded attachments
        """
        self.storage_dir = storage_dir
        os.makedirs(storage_dir, exist_ok=True)

        # Create subdirectories for different attachment types
        self.image_dir = os.path.join(storage_dir, "images")
        self.video_dir = os.path.join(storage_dir, "videos")
        self.audio_dir = os.path.join(storage_dir, "audio")
        self.document_dir = os.path.join(storage_dir, "documents")
        self.other_dir = os.path.join(storage_dir, "other")

        for directory in [self.image_dir, self.video_dir, self.audio_dir,
                          self.document_dir, self.other_dir]:
            os.makedirs(directory, exist_ok=True)

        # Initialize thumbnail directory
        self.thumbnail_dir = os.path.join(storage_dir, "thumbnails")
        os.makedirs(self.thumbnail_dir, exist_ok=True)

    def _get_storage_path(self, attachment: Dict[str, Any]) -> str:
        """Get the appropriate storage path for an attachment.

        Args:
            attachment: Attachment information

        Returns:
            Path where the attachment should be stored
        """
        content_type = attachment.get('content_type', '')

        if content_type.startswith('image/'):
            base_dir = self.image_dir
        elif content_type.startswith('video/'):
            base_dir = self.video_dir
        elif content_type.startswith('audio/'):
            base_dir = self.audio_dir
        elif content_type.startswith(('application/', 'text/')):
            base_dir = self.document_dir
        else:
            base_dir = self.other_dir

        # Generate a filename based on the attachment name and url
        original_name = attachment.get('name', '')
        url = attachment.get('url', '')

        if not original_name and url:
            # Extract filename from URL if name is not provided
            original_name = os.path.basename(url.split('?')[0])

        if not original_name:
            # Generate a unique name based on content and timestamp
            hash_input = f"{url}-{datetime.now().isoformat()}"
            hash_value = hashlib.md5(hash_input.encode()).hexdigest()[:10]

            # Try to determine extension from content type
            ext = mimetypes.guess_extension(content_type) or ''
            original_name = f"attachment-{hash_value}{ext}"

        # Ensure the filename is safe for the filesystem
        safe_name = ''.join(c for c in original_name if c.isalnum() or c in '._- ')

        return os.path.join(base_dir, safe_name)

    def download_attachment(self, attachment: Dict[str, Any]) -> Optional[str]:
        """Download an attachment from the given URL.

        Args:
            attachment: Attachment information including URL

        Returns:
            Path to the downloaded file or None if download failed
        """
        url = attachment.get('url', '')
        if not url:
            logger.warning("No URL provided for attachment")
            return None

        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()

            # Get the storage path for this attachment
            storage_path = self._get_storage_path(attachment)

            # Save the file
            with open(storage_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

            logger.info(f"Downloaded attachment to {storage_path}")

            # Update content type if not already set
            if not attachment.get('content_type'):
                content_type = response.headers.get('content-type')
                if content_type:
                    attachment['content_type'] = content_type

            # Generate thumbnail if it's an image
            if attachment.get('content_type', '').startswith('image/'):
                self.generate_thumbnail(storage_path)

            return storage_path

        except Exception as e:
            logger.error(f"Error downloading attachment: {e}")
            return None

    def generate_thumbnail(self, image_path: str, size: Tuple[int, int] = (200, 200)) -> Optional[str]:
        """Generate a thumbnail for an image.

        Args:
            image_path: Path to the image file
            size: Thumbnail size (width, height)

        Returns:
            Path to the thumbnail or None if generation failed
        """
        try:
            img = Image.open(image_path)
            img.thumbnail(size)

            # Create thumbnail filename
            base_name = os.path.basename(image_path)
            thumbnail_name = f"thumb_{base_name}"
            thumbnail_path = os.path.join(self.thumbnail_dir, thumbnail_name)

            # Save thumbnail
            img.save(thumbnail_path)
            logger.info(f"Generated thumbnail: {thumbnail_path}")

            return thumbnail_path

        except Exception as e:
            logger.error(f"Error generating thumbnail: {e}")
            return None

    def extract_image_metadata(self, image_path: str) -> Dict[str, Any]:
        """Extract metadata from an image file.

        Args:
            image_path: Path to the image file

        Returns:
            Dictionary containing image metadata
        """
        metadata = {}

        try:
            img = Image.open(image_path)

            # Basic image properties
            metadata['format'] = img.format
            metadata['mode'] = img.mode
            metadata['width'], metadata['height'] = img.size

            # Extract EXIF data if available
            if hasattr(img, '_getexif') and callable(img._getexif):
                exif = img._getexif()
                if exif:
                    metadata['exif'] = {}
                    for tag_id, value in exif.items():
                        # Convert binary data to string representation
                        if isinstance(value, bytes):
                            try:
                                value = value.decode('utf-8', errors='replace')
                            except:
                                value = str(value)

                        # Store the EXIF tag and value
                        metadata['exif'][f"tag_{tag_id}"] = str(value)

        except Exception as e:
            logger.error(f"Error extracting image metadata: {e}")

        return metadata

    def enrich_attachment_data(self, attachment: Dict[str, Any], file_path: Optional[str] = None) -> Dict[str, Any]:
        """Enrich attachment data with additional information.

        Args:
            attachment: Original attachment data
            file_path: Path to the downloaded file (if already downloaded)

        Returns:
            Enriched attachment data
        """
        # Create a copy of the original attachment data
        enriched = attachment.copy()

        # Download the file if not already downloaded
        local_file_path = file_path
        if not local_file_path:
            local_file_path = self.download_attachment(attachment)

        if not local_file_path or not os.path.exists(local_file_path):
            return enriched

        # Add file size if not already present
        if 'size' not in enriched:
            enriched['size'] = os.path.getsize(local_file_path)

        # Add file path
        enriched['local_path'] = local_file_path

        # Add metadata based on file type
        content_type = enriched.get('content_type', '')

        if content_type.startswith('image/'):
            # For images, add metadata and thumbnail
            enriched['metadata'] = self.extract_image_metadata(local_file_path)
            thumbnail_path = self.generate_thumbnail(local_file_path)
            if thumbnail_path:
                enriched['thumbnail_path'] = thumbnail_path

        # Add additional metadata for other file types as needed

        return enriched

    def process_message_attachments(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Process all attachments in a message.

        Args:
            message: Message data containing attachments

        Returns:
            Updated message with enriched attachment data
        """
        # Create a copy of the original message
        processed_message = message.copy()

        # Process each attachment
        if 'attachments' in processed_message:
            enriched_attachments = []

            for attachment in processed_message['attachments']:
                enriched_attachment = self.enrich_attachment_data(attachment)
                enriched_attachments.append(enriched_attachment)

            processed_message['attachments'] = enriched_attachments

        return processed_message