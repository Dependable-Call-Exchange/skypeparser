"""
Extractor module for the ETL pipeline.

This module handles the extraction of data from Skype export files,
including validation and initial processing.
"""

import logging
import os
import json
from typing import Dict, Any, Optional, BinaryIO

from src.utils.file_handler import read_file_object, read_tarfile, read_tarfile_object
from src.utils.validation import (
    validate_file_exists,
    validate_json_file,
    validate_tar_file,
    validate_file_object,
    validate_skype_data
)
from .context import ETLContext

logger = logging.getLogger(__name__)

class Extractor:
    """Handles extraction of data from Skype export files."""

    def __init__(self, context: ETLContext = None, output_dir: Optional[str] = None):
        """Initialize the Extractor.

        Args:
            context: Shared ETL context object
            output_dir: Optional directory to save extracted data (used if context not provided)
        """
        self.context = context
        self.output_dir = output_dir if context is None else context.output_dir

    def extract(self, file_path: Optional[str] = None, file_obj: Optional[BinaryIO] = None) -> Dict[str, Any]:
        """Extract data from a Skype export file.

        Args:
            file_path: Path to the file to extract from
            file_obj: File-like object to extract from

        Returns:
            Dict containing the extracted data

        Raises:
            ValueError: If neither file_path nor file_obj is provided
        """
        if not file_path and not file_obj:
            raise ValueError("Either file_path or file_obj must be provided")

        # Log extraction start
        logger.info("Starting data extraction")

        # Update context if available
        if self.context:
            self.context.set_file_source(file_path, file_obj)

        # Extract data from source
        raw_data = self._extract_data_from_source(file_path, file_obj)

        # Validate the extracted data
        validate_skype_data(raw_data)

        # Save raw data if output directory is specified
        self._save_raw_data(raw_data, file_path)

        # Update context if available
        if self.context:
            self.context.raw_data = raw_data
            self.context.check_memory()

            # Update progress
            conversation_count = len(raw_data.get('conversations', []))
            self.context.update_progress(conversations=conversation_count)

        logger.info(f"Extracted data with {len(raw_data.get('conversations', []))} conversations")
        return raw_data

    def _extract_data_from_source(self, file_path: Optional[str] = None, file_obj: Optional[BinaryIO] = None) -> Dict[str, Any]:
        """Extract data from either a file path or a file object.

        Args:
            file_path: Path to the file to extract from
            file_obj: File-like object to extract from

        Returns:
            Dict containing the extracted data
        """
        if file_path:
            return self._extract_from_file_path(file_path)
        elif file_obj:
            return self._extract_from_file_object(file_obj)
        else:
            raise ValueError("Either file_path or file_obj must be provided")

    def _extract_from_file_path(self, file_path: str) -> Dict[str, Any]:
        """Extract data from a file path.

        Args:
            file_path: Path to the file to extract from

        Returns:
            Dict containing the extracted data
        """
        # Validate file exists
        validate_file_exists(file_path)

        # Process based on file type
        if file_path.endswith('.tar'):
            logger.info(f"Extracting data from TAR file: {file_path}")
            return self._extract_from_tar_file(file_path)
        else:
            logger.info(f"Extracting data from JSON file: {file_path}")
            return self._extract_from_json_file(file_path)

    def _extract_from_tar_file(self, file_path: str) -> Dict[str, Any]:
        """Extract data from a tar file.

        Args:
            file_path: Path to the tar file

        Returns:
            Dict containing the extracted data
        """
        validate_tar_file(file_path)
        raw_data = read_tarfile(file_path, auto_select=True)
        logger.info(f"Successfully extracted data from TAR file: {file_path}")
        return raw_data

    def _extract_from_json_file(self, file_path: str) -> Dict[str, Any]:
        """Extract data from a JSON file.

        Args:
            file_path: Path to the JSON file

        Returns:
            Dict containing the extracted data
        """
        raw_data = validate_json_file(file_path)
        logger.info(f"Successfully read data from JSON file: {file_path}")
        return raw_data

    def _extract_from_file_object(self, file_obj: BinaryIO) -> Dict[str, Any]:
        """Extract data from a file object.

        Args:
            file_obj: File-like object to extract from

        Returns:
            Dict containing the extracted data
        """
        # Validate file object
        validate_file_object(file_obj, allowed_extensions=['.json', '.tar'])

        # Try to determine file type from name if available
        if hasattr(file_obj, 'name') and file_obj.name.endswith('.tar'):
            logger.info("Extracting data from uploaded TAR file")
            raw_data = read_tarfile_object(file_obj, auto_select=True)
        else:
            # Assume JSON if not a tar file
            logger.info("Reading data from uploaded JSON file")
            raw_data = read_file_object(file_obj)

        return raw_data

    def _save_raw_data(self, raw_data: Dict[str, Any], file_path: Optional[str] = None) -> None:
        """Save the raw data to a file if output directory is specified.

        Args:
            raw_data: The raw data to save
            file_path: The original file path, used for logging
        """
        if not self.output_dir:
            return

        try:
            # Ensure output directory exists
            os.makedirs(self.output_dir, exist_ok=True)

            # Create output file path
            raw_output_path = os.path.join(self.output_dir, 'raw_data.json')

            # Write data to file
            with open(raw_output_path, 'w', encoding='utf-8') as f:
                json.dump(raw_data, f, indent=2)

            logger.info(f"Raw data saved to {raw_output_path}")

        except Exception as e:
            logger.warning(f"Failed to save raw data: {e}")

            # Record error in context if available
            if self.context:
                self.context.record_error("extract", e, fatal=False)
