#!/usr/bin/env python3
"""
File Handler Module

This module provides utilities for reading and processing Skype export files,
including JSON and TAR archives.
"""

import os
import json
import tarfile
import logging
from typing import Dict, Any, BinaryIO, Optional, List

from src.utils.interfaces import FileHandlerProtocol
from src.utils.validation import (
    validate_file_exists,
    validate_json_file,
    validate_tar_file,
    validate_file_object
)

# Set up logging
logger = logging.getLogger(__name__)


class FileHandler(FileHandlerProtocol):
    """
    Handles reading and processing of Skype export files.

    This class implements the FileHandlerProtocol interface and provides
    methods for reading data from various file formats.
    """

    def read_file(self, file_path: str) -> Dict[str, Any]:
        """
        Read data from a file.

        Args:
            file_path: Path to the file

        Returns:
            The data read from the file

        Raises:
            ValueError: If the file doesn't exist or has an unsupported format
        """
        logger.info(f"Reading file: {file_path}")

        # Validate file path
        if not os.path.exists(file_path):
            error_msg = f"File does not exist: {file_path}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        if not os.path.isfile(file_path):
            error_msg = f"Path is not a file: {file_path}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Determine file type based on extension
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()

        try:
            if ext == '.json':
                # Read JSON file
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                logger.info(f"Successfully read JSON file: {file_path}")
                return data
            elif ext == '.tar':
                # Read TAR file
                return self.read_tar_file(file_path)
            else:
                error_msg = f"Unsupported file extension: {ext}. Supported extensions: .json, .tar"
                logger.error(error_msg)
                raise ValueError(error_msg)
        except json.JSONDecodeError as e:
            error_msg = f"Error decoding JSON file {file_path}: {e}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = f"Error reading file {file_path}: {e}"
            logger.error(error_msg)
            raise

    def read_file_obj(self, file_obj: BinaryIO, file_name: Optional[str] = None) -> Dict[str, Any]:
        """Read data from a file-like object.

        Args:
            file_obj: File-like object to read
            file_name: Optional name of the file for logging and type detection

        Returns:
            Data read from the file object

        Raises:
            ValueError: If the file format is not supported
            Exception: If an error occurs during file reading
        """
        logger.info(f"Reading from file object{' ' + file_name if file_name else ''}")

        # Determine file type based on name if provided
        file_type = None
        if file_name:
            _, ext = os.path.splitext(file_name)
            file_type = ext.lower()

        try:
            if file_type == '.json' or not file_type:
                # Try to read as JSON
                file_obj.seek(0)
                content = file_obj.read()
                if isinstance(content, bytes):
                    content = content.decode('utf-8')
                data = json.loads(content)
                logger.info("Successfully read JSON from file object")
                return data
            elif file_type == '.tar':
                # Read as TAR file
                return self.read_tar_file_obj(file_obj)
            else:
                error_msg = f"Unsupported file type: {file_type}. Supported types: .json, .tar"
                logger.error(error_msg)
                raise ValueError(error_msg)
        except json.JSONDecodeError as e:
            error_msg = f"Error decoding JSON from file object: {e}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = f"Error reading from file object: {e}"
            logger.error(error_msg)
            raise

    def read_tar_file(self, file_path: str) -> Dict[str, Any]:
        """Read data from a TAR file.

        Args:
            file_path: Path to the TAR file

        Returns:
            Data read from the TAR file

        Raises:
            ValueError: If the TAR file does not contain a valid Skype export
            Exception: If an error occurs during TAR file reading
        """
        logger.info(f"Reading TAR file: {file_path}")

        try:
            with tarfile.open(file_path, 'r') as tar:
                # Find the messages.json file
                messages_file = None
                for member in tar.getmembers():
                    if member.name.endswith('messages.json'):
                        messages_file = member
                        break

                if not messages_file:
                    error_msg = f"No messages.json file found in TAR archive: {file_path}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                # Extract and read the messages.json file
                f = tar.extractfile(messages_file)
                if f is None:
                    error_msg = f"Failed to extract messages.json from TAR archive: {file_path}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                content = f.read().decode('utf-8')
                data = json.loads(content)
                logger.info(f"Successfully read messages.json from TAR file: {file_path}")
                return data
        except tarfile.ReadError as e:
            error_msg = f"Error reading TAR file {file_path}: {e}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = f"Error processing TAR file {file_path}: {e}"
            logger.error(error_msg)
            raise

    def read_tar_file_obj(self, file_obj: BinaryIO) -> Dict[str, Any]:
        """Read data from a TAR file-like object.

        Args:
            file_obj: File-like object containing a TAR archive

        Returns:
            Data read from the TAR file object

        Raises:
            ValueError: If the TAR file does not contain a valid Skype export
            Exception: If an error occurs during TAR file reading
        """
        logger.info("Reading from TAR file object")

        try:
            # Ensure we're at the beginning of the file
            file_obj.seek(0)

            with tarfile.open(fileobj=file_obj, mode='r') as tar:
                # Find the messages.json file
                messages_file = None
                for member in tar.getmembers():
                    if member.name.endswith('messages.json'):
                        messages_file = member
                        break

                if not messages_file:
                    error_msg = "No messages.json file found in TAR archive"
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                # Extract and read the messages.json file
                f = tar.extractfile(messages_file)
                if f is None:
                    error_msg = "Failed to extract messages.json from TAR archive"
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                content = f.read().decode('utf-8')
                data = json.loads(content)
                logger.info("Successfully read messages.json from TAR file object")
                return data
        except tarfile.ReadError as e:
            error_msg = f"Error reading TAR file object: {e}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        except Exception as e:
            error_msg = f"Error processing TAR file object: {e}"
            logger.error(error_msg)
            raise


# Legacy function wrappers for backward compatibility
def read_file(file_path: str) -> Dict[str, Any]:
    """Legacy wrapper for FileHandler.read_file."""
    handler = FileHandler()
    return handler.read_file(file_path)

def read_file_obj(file_obj: BinaryIO, file_name: Optional[str] = None) -> Dict[str, Any]:
    """Legacy wrapper for FileHandler.read_file_obj."""
    handler = FileHandler()
    return handler.read_file_obj(file_obj, file_name)

def read_tar_file(file_path: str) -> Dict[str, Any]:
    """Legacy wrapper for FileHandler.read_tar_file."""
    handler = FileHandler()
    return handler.read_tar_file(file_path)

def read_tar_file_obj(file_obj: BinaryIO) -> Dict[str, Any]:
    """Legacy wrapper for FileHandler.read_tar_file_obj."""
    handler = FileHandler()
    return handler.read_tar_file_obj(file_obj)