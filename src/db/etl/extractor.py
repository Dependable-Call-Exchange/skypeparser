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

from src.utils.interfaces import ExtractorProtocol, FileHandlerProtocol
from src.utils.di import get_service
from src.utils.validation import (
    validate_file_exists,
    validate_json_file,
    validate_tar_file,
    validate_file_object,
    validate_skype_data
)
from .context import ETLContext

logger = logging.getLogger(__name__)

class Extractor(ExtractorProtocol):
    """Handles extraction of data from Skype export files."""

    def __init__(self,
                 context: ETLContext = None,
                 output_dir: Optional[str] = None,
                 file_handler: Optional[FileHandlerProtocol] = None):
        """Initialize the Extractor.

        Args:
            context: Shared ETL context object
            output_dir: Optional directory to save extracted data (used if context not provided)
            file_handler: Optional file handler for reading files
        """
        self.context = context
        self.output_dir = output_dir if context is None else context.output_dir
        self.file_handler = file_handler or get_service(FileHandlerProtocol)

    def extract(self, file_path: Optional[str] = None, file_obj: Optional[BinaryIO] = None) -> Dict[str, Any]:
        """Extract data from a Skype export file.

        Args:
            file_path: Path to the file to extract from
            file_obj: File-like object to extract from

        Returns:
            Dict containing the extracted data

        Raises:
            ValueError: If neither file_path nor file_obj is provided
            ValueError: If the file is not a valid Skype export
        """
        # Validate input parameters
        self._validate_input_parameters(file_path, file_obj)

        # Extract data from source
        raw_data = self._extract_data_from_source(file_path, file_obj)

        # Validate extracted data
        self._validate_extracted_data(raw_data)

        # Save raw data if output directory is specified
        if self.output_dir:
            self._save_raw_data(raw_data, file_path)

        # Update context if available
        if self.context:
            self.context.set_raw_data(raw_data)
            self.context.set_phase_status('extract', 'completed')

        return raw_data

    def _validate_input_parameters(self, file_path: Optional[str], file_obj: Optional[BinaryIO]) -> None:
        """Validate input parameters.

        Args:
            file_path: Path to the file to extract from
            file_obj: File-like object to extract from

        Raises:
            ValueError: If neither file_path nor file_obj is provided
        """
        if file_path is None and file_obj is None:
            error_msg = "Either file_path or file_obj must be provided"
            logger.error(error_msg)
            raise ValueError(error_msg)

        if file_path is not None:
            try:
                validate_file_exists(file_path)
            except Exception as e:
                logger.error(f"Invalid file path: {e}")
                raise ValueError(f"Invalid file path: {e}")

        if file_obj is not None:
            try:
                validate_file_object(file_obj)
            except Exception as e:
                logger.error(f"Invalid file object: {e}")
                raise ValueError(f"Invalid file object: {e}")

    def _validate_extracted_data(self, raw_data: Dict[str, Any]) -> None:
        """Validate extracted data.

        Args:
            raw_data: Extracted data to validate

        Raises:
            ValueError: If the data is not a valid Skype export
        """
        try:
            # Validate that the data is a valid Skype export
            validate_skype_data(raw_data)
        except Exception as e:
            logger.error(f"Invalid Skype export data: {e}")
            raise ValueError(f"Invalid Skype export data: {e}")

        # Additional validation for message timestamps
        if 'conversations' in raw_data:
            for conv_id, conv_data in raw_data['conversations'].items():
                if 'MessageList' in conv_data:
                    for message in conv_data['MessageList']:
                        if 'originalarrivaltime' in message:
                            timestamp = message['originalarrivaltime']
                            if not self._is_valid_timestamp(timestamp):
                                logger.warning(f"Invalid timestamp in message: {timestamp}")

        logger.info("Extracted data validation completed successfully")

    def _is_valid_timestamp(self, timestamp: str) -> bool:
        """Check if a timestamp is valid.

        Args:
            timestamp: Timestamp string to validate

        Returns:
            True if the timestamp is valid, False otherwise
        """
        try:
            # Try to parse the timestamp
            datetime.datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            return True
        except (ValueError, TypeError):
            return False

    def _extract_data_from_source(self, file_path: Optional[str] = None, file_obj: Optional[BinaryIO] = None) -> Dict[str, Any]:
        """Extract data from the source.

        Args:
            file_path: Path to the file to extract from
            file_obj: File-like object to extract from

        Returns:
            Dict containing the extracted data
        """
        if file_path is not None:
            return self._extract_from_file_path(file_path)
        else:
            return self._extract_from_file_object(file_obj)

    def _extract_from_file_path(self, file_path: str) -> Dict[str, Any]:
        """Extract data from a file path.

        Args:
            file_path: Path to the file to extract from

        Returns:
            Dict containing the extracted data
        """
        logger.info(f"Extracting data from file: {file_path}")

        if file_path.endswith('.tar'):
            return self._extract_from_tar_file(file_path)
        elif file_path.endswith('.json'):
            return self._extract_from_json_file(file_path)
        else:
            error_msg = f"Unsupported file format: {file_path}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    def _extract_from_tar_file(self, file_path: str) -> Dict[str, Any]:
        """Extract data from a tar file.

        Args:
            file_path: Path to the tar file

        Returns:
            Dict containing the extracted data
        """
        logger.info(f"Extracting data from tar file: {file_path}")
        return self.file_handler.read_tarfile(file_path, auto_select=True)

    def _extract_from_json_file(self, file_path: str) -> Dict[str, Any]:
        """Extract data from a JSON file.

        Args:
            file_path: Path to the JSON file

        Returns:
            Dict containing the extracted data
        """
        logger.info(f"Extracting data from JSON file: {file_path}")
        return self.file_handler.read_file(file_path)

    def _extract_from_file_object(self, file_obj: BinaryIO) -> Dict[str, Any]:
        """Extract data from a file object.

        Args:
            file_obj: File-like object to extract from

        Returns:
            Dict containing the extracted data
        """
        logger.info("Extracting data from file object")

        # Try to determine file type from name if available
        if hasattr(file_obj, 'name'):
            if file_obj.name.endswith('.tar'):
                return self.file_handler.read_file_object(file_obj)
            elif file_obj.name.endswith('.json'):
                return self.file_handler.read_file_object(file_obj)

        # If we can't determine the type, try to read it as a file object
        return self.file_handler.read_file_object(file_obj)

    def _save_raw_data(self, raw_data: Dict[str, Any], file_path: Optional[str] = None) -> None:
        """Save raw data to a file.

        Args:
            raw_data: Raw data to save
            file_path: Original file path (used for naming)
        """
        if not self.output_dir:
            return

        os.makedirs(self.output_dir, exist_ok=True)

        # Generate a filename based on the original file or a timestamp
        if file_path:
            base_name = os.path.basename(file_path)
            file_name = f"raw_{os.path.splitext(base_name)[0]}.json"
        else:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"raw_data_{timestamp}.json"

        output_path = os.path.join(self.output_dir, file_name)

        logger.info(f"Saving raw data to: {output_path}")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f, ensure_ascii=False, indent=2)
