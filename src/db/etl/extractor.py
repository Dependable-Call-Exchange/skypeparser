"""
Extractor module for the ETL pipeline.

This module handles the extraction of data from Skype export files,
including validation and initial processing.
"""

import datetime
import json
import logging
import os
import time
from typing import Any, BinaryIO, Dict, Optional

from src.utils.di import get_service
from src.utils.interfaces import (
    ExtractorProtocol,
    FileHandlerProtocol,
    ValidationServiceProtocol,
)
from src.utils.validation import (
    validate_file_exists,
    validate_file_object,
    validate_json_file,
    validate_skype_data,
    validate_tar_file,
)
from src.utils.new_structured_logging import (
    get_logger,
    log_execution_time,
    log_call,
    handle_errors,
    with_context,
    LogContext,
    log_metrics
)

from .context import ETLContext

logger = get_logger(__name__)


class Extractor(ExtractorProtocol):
    """Handles extraction of data from Skype export files."""

    def __init__(
        self,
        context: ETLContext = None,
        output_dir: Optional[str] = None,
        file_handler: Optional[FileHandlerProtocol] = None,
        validation_service: Optional[ValidationServiceProtocol] = None,
    ):
        """
        Initialize the extractor.

        Args:
            context: ETL context
            output_dir: Output directory for extracted data
            file_handler: File handler for reading files
            validation_service: Validation service for validating inputs
        """
        # Initialize metrics
        self._metrics = {
            "start_time": None,
            "end_time": None,
            "extraction_time_ms": 0,
            "validation_time_ms": 0,
            "file_size_bytes": 0,
            "message_count": 0,
            "conversation_count": 0,
        }

        # Set context
        self.context = context

        # Set output directory
        if output_dir is None and context is not None and hasattr(context, "output_dir"):
            output_dir = context.output_dir
        self.output_dir = output_dir

        # Create output directory if it doesn't exist
        if self.output_dir and not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)
            logger.info(f"Created output directory: {self.output_dir}")

        # Set file handler
        self.file_handler = file_handler
        if self.file_handler is None:
            # Get file handler from service registry if available
            try:
                self.file_handler = get_service("file_handler")
            except (ImportError, KeyError):
                self.file_handler = None

        # Set validation service
        self.validation_service = validation_service
        if self.validation_service is None:
            # Get validation service from service registry if available
            try:
                self.validation_service = get_service("validation_service")
            except (ImportError, KeyError):
                self.validation_service = None

        # Log initialization
        logger.info(
            "Initialized Extractor",
            extra={
                "output_dir": self.output_dir,
                "has_file_handler": self.file_handler is not None,
                "has_validation_service": self.validation_service is not None,
            }
        )

    @log_execution_time(level=logging.INFO)
    @with_context(operation="extract")
    def extract(self, file_path: str) -> Dict[str, Any]:
        """
        Extract data from a Skype export file.

        Args:
            file_path: Path to the Skype export file

        Returns:
            Extracted data
        """
        # Start timer
        self._metrics["start_time"] = time.time()

        try:
            # Validate file path
            logger.info(f"Extracting data from file: {file_path}")
            validate_file_exists(file_path)

            # Determine file type and extract accordingly
            if file_path.endswith(".tar"):
                with LogContext(file_type="tar"):
                    data = self._extract_tar_file(file_path)
            elif file_path.endswith(".json"):
                with LogContext(file_type="json"):
                    data = self._extract_json_file(file_path)
            else:
                raise ValueError(f"Unsupported file type: {file_path}")

            # Validate extracted data
            validation_start = time.time()
            self._validate_extracted_data(data)
            self._metrics["validation_time_ms"] = (time.time() - validation_start) * 1000

            # Update context if available
            if self.context is not None:
                self.context.raw_data = data
                self.context.file_source = file_path

            # End timer
            self._metrics["end_time"] = time.time()
            self._metrics["extraction_time_ms"] = (self._metrics["end_time"] - self._metrics["start_time"]) * 1000

            # Count messages and conversations
            if "messages" in data:
                self._metrics["message_count"] = len(data["messages"])
            if "conversations" in data:
                self._metrics["conversation_count"] = len(data["conversations"])

            # Log success
            logger.info(
                f"Data extracted successfully from {file_path}",
                extra={
                    "metrics": self._metrics,
                    "file_path": file_path,
                    "data_keys": list(data.keys()),
                }
            )

            return data

        except Exception as e:
            # Log error
            logger.error(
                f"Error extracting data from {file_path}: {e}",
                exc_info=True,
                extra={
                    "file_path": file_path,
                    "error": str(e),
                }
            )
            raise

    @handle_errors(log_level="ERROR", default_message="Error extracting TAR file")
    def _extract_tar_file(self, file_path: str) -> Dict[str, Any]:
        """
        Extract data from a TAR file.

        Args:
            file_path: Path to the TAR file

        Returns:
            Extracted data
        """
        # Validate TAR file
        validate_tar_file(file_path)

        # Get file size
        self._metrics["file_size_bytes"] = os.path.getsize(file_path)

        # Log extraction start
        logger.info(
            f"Extracting TAR file: {file_path}",
            extra={
                "file_path": file_path,
                "file_size_bytes": self._metrics["file_size_bytes"],
            }
        )

        # Extract TAR file
        start_time = time.time()

        # Use file handler if available
        if self.file_handler:
            data = self.file_handler.extract_tar(file_path, self.output_dir)
        else:
            # Fallback to direct extraction
            from src.utils.tar_extractor import extract_tar_file
            temp_dir = os.path.join(self.output_dir or ".", "temp_extract")
            os.makedirs(temp_dir, exist_ok=True)
            extract_tar_file(file_path, temp_dir)

            # Read extracted files
            messages_path = os.path.join(temp_dir, "messages.json")
            endpoints_path = os.path.join(temp_dir, "endpoints.json")

            # Load messages
            with open(messages_path, "r", encoding="utf-8") as f:
                messages = json.load(f)

            # Load endpoints if available
            endpoints = {}
            if os.path.exists(endpoints_path):
                with open(endpoints_path, "r", encoding="utf-8") as f:
                    endpoints = json.load(f)

            # Combine data
            data = {
                "messages": messages,
                "endpoints": endpoints,
                "export_date": datetime.datetime.now().isoformat(),
            }

        # Calculate extraction time
        extraction_time_ms = (time.time() - start_time) * 1000

        # Log extraction completion
        logger.info(
            f"TAR file extracted successfully: {file_path}",
            extra={
                "file_path": file_path,
                "extraction_time_ms": extraction_time_ms,
                "data_keys": list(data.keys()),
            }
        )

        return data

    @handle_errors(log_level="ERROR", default_message="Error extracting JSON file")
    def _extract_json_file(self, file_path: str) -> Dict[str, Any]:
        """
        Extract data from a JSON file.

        Args:
            file_path: Path to the JSON file

        Returns:
            Extracted data
        """
        # Validate JSON file
        validate_json_file(file_path)

        # Get file size
        self._metrics["file_size_bytes"] = os.path.getsize(file_path)

        # Log extraction start
        logger.info(
            f"Extracting JSON file: {file_path}",
            extra={
                "file_path": file_path,
                "file_size_bytes": self._metrics["file_size_bytes"],
            }
        )

        # Extract JSON file
        start_time = time.time()

        # Use file handler if available
        if self.file_handler:
            data = self.file_handler.read_json(file_path)
        else:
            # Fallback to direct reading
            with open(file_path, "r", encoding="utf-8") as f:
                data = json.load(f)

        # Calculate extraction time
        extraction_time_ms = (time.time() - start_time) * 1000

        # Log extraction completion
        logger.info(
            f"JSON file extracted successfully: {file_path}",
            extra={
                "file_path": file_path,
                "extraction_time_ms": extraction_time_ms,
                "data_keys": list(data.keys()),
            }
        )

        return data

    @handle_errors(log_level="ERROR", default_message="Error extracting from file object")
    def extract_from_file_object(self, file_obj: BinaryIO, file_type: str) -> Dict[str, Any]:
        """
        Extract data from a file object.

        Args:
            file_obj: File object
            file_type: Type of file (tar, json)

        Returns:
            Extracted data
        """
        # Validate file object
        validate_file_object(file_obj)

        # Log extraction start
        logger.info(
            f"Extracting from file object of type: {file_type}",
            extra={"file_type": file_type}
        )

        # Extract based on file type
        start_time = time.time()

        with LogContext(file_type=file_type):
            if file_type.lower() == "tar":
                # Use file handler if available
                if self.file_handler:
                    data = self.file_handler.extract_tar_from_file_object(file_obj, self.output_dir)
                else:
                    raise ValueError("File handler is required for extracting TAR from file object")
            elif file_type.lower() == "json":
                # Use file handler if available
                if self.file_handler:
                    data = self.file_handler.read_json_from_file_object(file_obj)
                else:
                    # Fallback to direct reading
                    data = json.load(file_obj)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")

        # Calculate extraction time
        extraction_time_ms = (time.time() - start_time) * 1000

        # Validate extracted data
        validation_start = time.time()
        self._validate_extracted_data(data)
        validation_time_ms = (time.time() - validation_start) * 1000

        # Update metrics
        self._metrics["extraction_time_ms"] = extraction_time_ms
        self._metrics["validation_time_ms"] = validation_time_ms

        # Count messages and conversations
        if "messages" in data:
            self._metrics["message_count"] = len(data["messages"])
        if "conversations" in data:
            self._metrics["conversation_count"] = len(data["conversations"])

        # Log extraction completion
        logger.info(
            f"Data extracted successfully from file object",
            extra={
                "file_type": file_type,
                "metrics": {
                    "extraction_time_ms": extraction_time_ms,
                    "validation_time_ms": validation_time_ms,
                    "message_count": self._metrics["message_count"],
                    "conversation_count": self._metrics["conversation_count"],
                },
                "data_keys": list(data.keys()),
            }
        )

        return data

    @handle_errors(log_level="ERROR", default_message="Error validating extracted data")
    def _validate_extracted_data(self, data: Dict[str, Any]) -> None:
        """
        Validate extracted data.

        Args:
            data: Extracted data

        Raises:
            ValueError: If data is invalid
        """
        # Basic validation
        if not isinstance(data, dict):
            raise ValueError("Extracted data must be a dictionary")

        # Check for required keys
        if "messages" not in data:
            raise ValueError("Extracted data must contain 'messages' key")

        # Use validation service if available
        if self.validation_service:
            self.validation_service.validate_skype_data(data)
        else:
            # Fallback to direct validation
            validate_skype_data(data)

        # Log validation success
        logger.debug(
            "Extracted data validated successfully",
            extra={
                "data_keys": list(data.keys()),
                "message_count": len(data.get("messages", [])),
            }
        )

    @handle_errors(log_level="ERROR", default_message="Error saving extracted data")
    def save_extracted_data(self, data: Dict[str, Any], output_path: Optional[str] = None) -> str:
        """
        Save extracted data to a file.

        Args:
            data: Extracted data
            output_path: Path to save the data (generated if not provided)

        Returns:
            Path to the saved file
        """
        # Generate output path if not provided
        if output_path is None:
            if self.output_dir is None:
                raise ValueError("Output directory is required when output_path is not provided")

            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(self.output_dir, f"raw_data_{timestamp}.json")

        # Log save start
        logger.info(
            f"Saving extracted data to: {output_path}",
            extra={"output_path": output_path}
        )

        # Save data
        start_time = time.time()

        # Use file handler if available
        if self.file_handler:
            self.file_handler.write_json(data, output_path)
        else:
            # Fallback to direct writing
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

        # Calculate save time
        save_time_ms = (time.time() - start_time) * 1000

        # Log save completion
        logger.info(
            f"Extracted data saved successfully to: {output_path}",
            extra={
                "output_path": output_path,
                "save_time_ms": save_time_ms,
                "file_size_bytes": os.path.getsize(output_path),
            }
        )

        return output_path
