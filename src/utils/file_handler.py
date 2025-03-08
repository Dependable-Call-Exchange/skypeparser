#!/usr/bin/env python3
"""
File Handler Module

This module provides utilities for reading and processing Skype export files,
including JSON and TAR archives.
"""

import json
import logging
import os
import tarfile
from typing import Any, BinaryIO, Dict, Iterator, List, Optional, Tuple

from src.utils.interfaces import FileHandlerProtocol
from src.utils.validation import (
    validate_file_exists,
    validate_file_object,
    validate_json_file,
    validate_tar_file,
)

# Set up logging
logger = logging.getLogger(__name__)

# Try to import ijson for streaming JSON processing
try:
    import ijson

    IJSON_AVAILABLE = True
except ImportError:
    IJSON_AVAILABLE = False
    logger.debug(
        "ijson library not available. Streaming JSON processing will not be supported."
    )


# Add extract_tar_contents function for backward compatibility
def extract_tar_contents(
    tar_path: str, output_dir: str, file_pattern: str = None
) -> List[str]:
    """
    Extract the contents of a TAR file to a directory.

    This function is a wrapper around tarfile.extractall for backward compatibility.

    Args:
        tar_path: Path to the TAR file
        output_dir: Directory to extract to
        file_pattern: Optional regex pattern to filter files by name

    Returns:
        List of extracted file paths
    """
    logger.info(f"Extracting TAR file {tar_path} to {output_dir}")

    # Validate the TAR file
    validate_file_exists(tar_path)
    validate_tar_file(tar_path)

    # Create the output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Extract the TAR file
    with tarfile.open(tar_path, "r") as tar:
        # Filter files if a pattern is provided
        if file_pattern:
            import re

            pattern = re.compile(file_pattern)
            members_to_extract = [m for m in tar.getmembers() if pattern.match(m.name)]
            tar.extractall(path=output_dir, members=members_to_extract)
            extracted_files = [
                os.path.join(output_dir, member.name) for member in members_to_extract
            ]
        else:
            tar.extractall(path=output_dir)
            extracted_files = [
                os.path.join(output_dir, member.name) for member in tar.getmembers()
            ]

    logger.info(f"Extracted {len(extracted_files)} files from {tar_path}")
    return extracted_files


# Add list_tar_contents function for backward compatibility
def list_tar_contents(tar_path: str, file_pattern: str = None) -> List[str]:
    """
    List the contents of a TAR file.

    Args:
        tar_path: Path to the TAR file
        file_pattern: Optional regex pattern to filter files by name

    Returns:
        List of file names in the TAR file
    """
    logger.info(f"Listing contents of TAR file {tar_path}")

    # Validate the TAR file
    validate_file_exists(tar_path)
    validate_tar_file(tar_path)

    # List the contents of the TAR file
    with tarfile.open(tar_path, "r") as tar:
        if file_pattern:
            import re

            pattern = re.compile(file_pattern)
            file_names = [
                member.name for member in tar.getmembers() if pattern.match(member.name)
            ]
        else:
            file_names = [member.name for member in tar.getmembers()]

    logger.info(f"Found {len(file_names)} files in {tar_path}")
    return file_names


# Add read_tarfile function for backward compatibility
def read_tarfile(
    file_path: str, auto_select: bool = False, select_json: Optional[int] = None
) -> Dict[str, Any]:
    """
    Read data from a tar file.

    Args:
        file_path: Path to the tar file
        auto_select: Whether to automatically select the main data file
        select_json: Index of the JSON file to select (0-based)

    Returns:
        The data read from the tar file

    Raises:
        ValueError: If the tar file doesn't exist or is invalid
        Exception: If an error occurs during file reading
    """
    logger.info(f"Reading tar file: {file_path}")

    # Validate tar file
    validate_file_exists(file_path)
    validate_tar_file(file_path)

    try:
        # Open TAR file
        with tarfile.open(file_path, "r") as tar:
            # List all files in the archive
            members = tar.getmembers()
            logger.debug(f"Files in archive: {[m.name for m in members]}")

            # Look for JSON files
            json_files = [m for m in members if m.name.lower().endswith(".json")]

            if not json_files:
                error_msg = "No JSON files found in TAR archive"
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Select the JSON file based on the parameters
            if select_json is not None:
                # Use the specified JSON file index
                if select_json < 0 or select_json >= len(json_files):
                    error_msg = f"Invalid JSON file index {select_json}. Only {len(json_files)} JSON files available."
                    logger.error(error_msg)
                    raise ValueError(error_msg)
                selected_file = json_files[select_json]
            elif auto_select:
                # Try to find the main data file (usually messages.json or similar)
                # This is a simplified heuristic and might need adjustment
                main_file_candidates = [
                    m
                    for m in json_files
                    if "message" in m.name.lower() or "export" in m.name.lower()
                ]

                if main_file_candidates:
                    selected_file = main_file_candidates[0]
                else:
                    selected_file = json_files[0]
            else:
                # If neither select_json nor auto_select is specified, raise an error
                error_msg = "No selection method specified. Use auto_select=True or provide a select_json index."
                logger.error(error_msg)
                raise ValueError(error_msg)

            logger.info(f"Selected JSON file from archive: {selected_file.name}")

            # Extract and read the selected file
            f = tar.extractfile(selected_file)
            if f is None:
                error_msg = f"Failed to extract {selected_file.name} from TAR archive"
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Read JSON data
            data = json.load(f)
            logger.info(
                f"Successfully read JSON from TAR archive: {selected_file.name}"
            )
            return data
    except Exception as e:
        error_msg = f"Error reading TAR file {file_path}: {e}"
        logger.error(error_msg)
        raise


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
            if ext == ".json":
                # Read JSON file
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                logger.info(f"Successfully read JSON file: {file_path}")
                return data
            elif ext == ".tar":
                # Read TAR file
                return self.read_tarfile(file_path)
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

    def read_file_object(
        self, file_obj: BinaryIO, file_name: Optional[str] = None
    ) -> Dict[str, Any]:
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
        logger.info("Reading from file object")

        # Validate file object
        validate_file_object(file_obj)

        try:
            # Determine file type based on file_name if provided
            if file_name:
                _, ext = os.path.splitext(file_name)
                ext = ext.lower()

                if ext == ".json":
                    # Read JSON from file object
                    file_obj.seek(0)
                    data = json.load(file_obj)
                    logger.info("Successfully read JSON from file object")
                    return data
                elif ext == ".tar":
                    # Read TAR from file object
                    return self.read_tar_file_obj(file_obj)

            # If file_name is not provided or doesn't have a recognized extension,
            # try to determine content type based on content
            # First try JSON
            try:
                file_obj.seek(0)
                data = json.load(file_obj)
                logger.info("Successfully read JSON from file object")
                return data
            except json.JSONDecodeError:
                # Not JSON, try TAR
                return self.read_tar_file_obj(file_obj)
        except Exception as e:
            error_msg = f"Error reading from file object: {e}"
            logger.error(error_msg)
            raise

    def read_tarfile(
        self,
        file_path: str,
        auto_select: bool = False,
        select_json: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Read data from a tar file.

        Args:
            file_path: Path to the tar file
            auto_select: Whether to automatically select the main data file
            select_json: Index of the JSON file to select (0-based)

        Returns:
            The data read from the tar file

        Raises:
            ValueError: If the tar file doesn't exist or is invalid
            Exception: If an error occurs during file reading
        """
        logger.info(f"Reading tar file: {file_path}")

        # Validate tar file
        validate_file_exists(file_path)
        validate_tar_file(file_path)

        try:
            # Open TAR file
            with tarfile.open(file_path, "r") as tar:
                # List all files in the archive
                members = tar.getmembers()
                logger.debug(f"Files in archive: {[m.name for m in members]}")

                # Look for JSON files
                json_files = [m for m in members if m.name.lower().endswith(".json")]

                if not json_files:
                    error_msg = "No JSON files found in TAR archive"
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                # Select the JSON file based on the parameters
                if select_json is not None:
                    # Use the specified JSON file index
                    if select_json < 0 or select_json >= len(json_files):
                        error_msg = f"Invalid JSON file index {select_json}. Only {len(json_files)} JSON files available."
                        logger.error(error_msg)
                        raise ValueError(error_msg)
                    selected_file = json_files[select_json]
                elif auto_select:
                    # Try to find the main data file (usually messages.json or similar)
                    # This is a simplified heuristic and might need adjustment
                    main_file_candidates = [
                        m
                        for m in json_files
                        if "message" in m.name.lower() or "export" in m.name.lower()
                    ]

                    if main_file_candidates:
                        selected_file = main_file_candidates[0]
                    else:
                        selected_file = json_files[0]
                else:
                    # If neither select_json nor auto_select is specified, raise an error
                    error_msg = "No selection method specified. Use auto_select=True or provide a select_json index."
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                logger.info(f"Selected JSON file from archive: {selected_file.name}")

                # Extract and read the selected file
                f = tar.extractfile(selected_file)
                if f is None:
                    error_msg = (
                        f"Failed to extract {selected_file.name} from TAR archive"
                    )
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                # Read JSON data
                data = json.load(f)
                logger.info(
                    f"Successfully read JSON from TAR archive: {selected_file.name}"
                )
                return data
        except Exception as e:
            error_msg = f"Error reading TAR file {file_path}: {e}"
            logger.error(error_msg)
            raise

    def read_tarfile_streaming(
        self, file_path: str, auto_select: bool = False
    ) -> Iterator[Tuple[str, Any]]:
        """
        Read data from a tar file using streaming JSON processing.

        This method uses ijson for memory-efficient processing of large JSON files.
        It yields (path, item) tuples for each item in the JSON file.

        Args:
            file_path: Path to the tar file
            auto_select: Whether to automatically select the main data file

        Yields:
            Tuples of (path, item) where path is the JSON path and item is the value

        Raises:
            ValueError: If the tar file doesn't exist, is invalid, or ijson is not available
            Exception: If an error occurs during file reading
        """
        if not IJSON_AVAILABLE:
            raise ValueError(
                "ijson library is required for streaming JSON processing. Please install it with 'pip install ijson'"
            )

        logger.info(f"Reading tar file with streaming: {file_path}")

        # Validate tar file
        validate_file_exists(file_path)
        validate_tar_file(file_path)

        try:
            # Open TAR file
            with tarfile.open(file_path, "r") as tar:
                # List all files in the archive
                members = tar.getmembers()
                logger.debug(f"Files in archive: {[m.name for m in members]}")

                # Look for JSON files
                json_files = [m for m in members if m.name.lower().endswith(".json")]

                if not json_files:
                    error_msg = "No JSON files found in TAR archive"
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                # Either auto-select or use the first JSON file
                if auto_select:
                    # Try to find the main data file (usually messages.json or similar)
                    main_file_candidates = [
                        m
                        for m in json_files
                        if "message" in m.name.lower() or "export" in m.name.lower()
                    ]

                    if main_file_candidates:
                        selected_file = main_file_candidates[0]
                    else:
                        selected_file = json_files[0]
                else:
                    selected_file = json_files[0]

                logger.info(
                    f"Selected JSON file from archive for streaming: {selected_file.name}"
                )

                # Extract and read the selected file
                f = tar.extractfile(selected_file)
                if f is None:
                    error_msg = (
                        f"Failed to extract {selected_file.name} from TAR archive"
                    )
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                # Stream JSON data with ijson
                logger.debug(f"Starting streaming processing of {selected_file.name}")
                for path, event, value in ijson.parse(f):
                    yield (path, value)

                logger.info(f"Completed streaming processing of {selected_file.name}")

        except Exception as e:
            error_msg = f"Error streaming TAR file {file_path}: {e}"
            logger.error(error_msg)
            raise

    # Keep the original method names as aliases for backward compatibility
    def read_file_obj(
        self, file_obj: BinaryIO, file_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Alias for read_file_object for backward compatibility."""
        return self.read_file_object(file_obj, file_name)

    def read_tar_file(self, file_path: str) -> Dict[str, Any]:
        """Alias for read_tarfile for backward compatibility."""
        return self.read_tarfile(file_path)

    def read_tar_file_obj(self, file_obj: BinaryIO) -> Dict[str, Any]:
        """Alias for read_tarfile_object for backward compatibility."""
        return self.read_tarfile_object(file_obj, auto_select=True)

    def read_tarfile_object(
        self,
        file_obj: BinaryIO,
        auto_select: bool = False,
        select_json: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Read data from a tar file object.

        Args:
            file_obj: File object for the tar file
            auto_select: Whether to automatically select the main data file
            select_json: Index of the JSON file to select (0-based)

        Returns:
            The data read from the tar file

        Raises:
            ValueError: If the tar file is invalid
            Exception: If an error occurs during file reading
        """
        logger.info("Reading from tar file object")

        try:
            # Open TAR file from file object
            with tarfile.open(fileobj=file_obj, mode="r") as tar:
                # List all files in the archive
                members = tar.getmembers()
                logger.debug(f"Files in archive: {[m.name for m in members]}")

                # Look for JSON files
                json_files = [m for m in members if m.name.lower().endswith(".json")]

                if not json_files:
                    error_msg = "No JSON files found in TAR archive"
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                # Select the JSON file based on the parameters
                if select_json is not None:
                    # Use the specified JSON file index
                    if select_json < 0 or select_json >= len(json_files):
                        error_msg = f"Invalid JSON file index {select_json}. Only {len(json_files)} JSON files available."
                        logger.error(error_msg)
                        raise ValueError(error_msg)
                    selected_file = json_files[select_json]
                elif auto_select:
                    # Try to find the main data file (usually messages.json or similar)
                    main_file_candidates = [
                        m
                        for m in json_files
                        if "message" in m.name.lower() or "export" in m.name.lower()
                    ]

                    if main_file_candidates:
                        selected_file = main_file_candidates[0]
                    else:
                        selected_file = json_files[0]
                else:
                    # If neither select_json nor auto_select is specified, raise an error
                    error_msg = "No selection method specified. Use auto_select=True or provide a select_json index."
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                logger.info(f"Selected JSON file from archive: {selected_file.name}")

                # Extract and read the selected file
                f = tar.extractfile(selected_file)
                if f is None:
                    error_msg = (
                        f"Failed to extract {selected_file.name} from TAR archive"
                    )
                    logger.error(error_msg)
                    raise ValueError(error_msg)

                # Read JSON data
                data = json.load(f)
                logger.info(
                    f"Successfully read JSON from TAR archive: {selected_file.name}"
                )
                return data
        except Exception as e:
            error_msg = f"Error reading from tar file object: {e}"
            logger.error(error_msg)
            raise


# Helper functions that use a singleton FileHandler instance


def read_file(file_path: str) -> Dict[str, Any]:
    """Helper function to read a file using the FileHandler."""
    from src.utils.di import get_service

    return get_service(FileHandlerProtocol).read_file(file_path)


def read_file_obj(
    file_obj: BinaryIO, file_name: Optional[str] = None
) -> Dict[str, Any]:
    """Helper function to read a file object using the FileHandler."""
    from src.utils.di import get_service

    return get_service(FileHandlerProtocol).read_file_object(file_obj, file_name)


def read_tarfile(
    file_path: str, auto_select: bool = False, select_json: Optional[int] = None
) -> Dict[str, Any]:
    """Helper function to read a tar file using the FileHandler."""
    from src.utils.di import get_service

    return get_service(FileHandlerProtocol).read_tarfile(
        file_path, auto_select, select_json
    )


def read_tar_file_obj(
    file_obj: BinaryIO, auto_select: bool = False, select_json: Optional[int] = None
) -> Dict[str, Any]:
    """Helper function to read a tar file object using the FileHandler."""
    from src.utils.di import get_service

    return get_service(FileHandlerProtocol).read_tarfile_object(
        file_obj, auto_select, select_json
    )
