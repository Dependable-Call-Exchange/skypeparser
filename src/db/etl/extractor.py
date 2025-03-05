"""
Extractor module for the ETL pipeline.

This module handles the extraction of data from Skype export files,
including validation and initial processing.
"""

import logging
import os
import json
from typing import Dict, Any, Optional, BinaryIO
import datetime

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
        # Validate input parameters
        self._validate_input_parameters(file_path, file_obj)

        # Log extraction start
        logger.info("Starting data extraction")

        # Update context if available
        if self.context:
            self.context.set_file_source(file_path, file_obj)

        # Extract data from source
        raw_data = self._extract_data_from_source(file_path, file_obj)

        # Validate the extracted data
        self._validate_extracted_data(raw_data)

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

    def _validate_input_parameters(self, file_path: Optional[str], file_obj: Optional[BinaryIO]) -> None:
        """Validate input parameters for extraction.

        Args:
            file_path: Path to the file to extract from
            file_obj: File-like object to extract from

        Raises:
            ValueError: If input parameters are invalid
        """
        if not file_path and not file_obj:
            raise ValueError("Either file_path or file_obj must be provided")

        if file_path and not isinstance(file_path, str):
            raise ValueError(f"file_path must be a string, got {type(file_path).__name__}")

        if file_path and file_obj:
            logger.warning("Both file_path and file_obj provided, using file_path")

        if file_path:
            # Check file extension
            _, ext = os.path.splitext(file_path)
            if ext.lower() not in ['.tar', '.json']:
                raise ValueError(f"Unsupported file extension: {ext}. Supported extensions: .tar, .json")

    def _validate_extracted_data(self, raw_data: Dict[str, Any]) -> None:
        """Validate the extracted data structure with enhanced checks.

        Args:
            raw_data: The raw data to validate

        Raises:
            ValueError: If the data is invalid
        """
        try:
            # Use the existing validation function
            validate_skype_data(raw_data)

            # Additional validation checks
            if not raw_data.get('conversations'):
                logger.warning("No conversations found in the extracted data")

            # Check for empty conversations
            empty_conversations = [i for i, conv in enumerate(raw_data.get('conversations', []))
                                  if not conv.get('MessageList')]
            if empty_conversations:
                logger.warning(f"Found {len(empty_conversations)} empty conversations without messages")

            # Check for conversations with missing IDs
            missing_ids = [i for i, conv in enumerate(raw_data.get('conversations', []))
                          if not conv.get('id')]
            if missing_ids:
                logger.warning(f"Found {len(missing_ids)} conversations with missing IDs")

            # Validate message timestamps if present
            invalid_timestamps = 0
            for conv in raw_data.get('conversations', []):
                for msg in conv.get('MessageList', []):
                    if 'originalarrivaltime' in msg and not self._is_valid_timestamp(msg['originalarrivaltime']):
                        invalid_timestamps += 1

            if invalid_timestamps > 0:
                logger.warning(f"Found {invalid_timestamps} messages with invalid timestamps")

            logger.info("Extracted data validation completed successfully")

        except Exception as e:
            logger.error(f"Data validation error: {e}")
            raise ValueError(f"Invalid Skype data: {e}")

    def _is_valid_timestamp(self, timestamp: str) -> bool:
        """Check if a timestamp string is valid.

        Args:
            timestamp: Timestamp string to validate

        Returns:
            bool: True if the timestamp is valid
        """
        if not timestamp or not isinstance(timestamp, str):
            return False

        # Try to parse the timestamp
        try:
            # Check if it's in ISO format
            datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return True
        except (ValueError, TypeError):
            return False

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
