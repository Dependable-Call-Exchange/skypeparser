"""
File handling utilities for the Skype Parser project.

This module provides functions for reading and extracting data from various file formats,
including JSON files and TAR archives. It's designed to work with uploaded files in
web applications or other automated processing systems.

This module serves as the foundation for the Extraction phase of the ETL pipeline,
providing robust functionality for extracting data from Skype export archives.
"""

import os
import json
import tarfile
import re
import logging
import tempfile
from typing import Dict, List, Optional, Any, BinaryIO

# Import validation functions
from .validation import (
    ValidationError,
    validate_file_exists,
    validate_file_type,
    validate_file_object
)

# Set up logging
logger = logging.getLogger(__name__)

def read_file(filename: str) -> Dict[str, Any]:
    """
    Read and parse a JSON file.

    Args:
        filename (str): Path to the JSON file

    Returns:
        dict: Parsed JSON data

    Raises:
        ValidationError: If the file is invalid
        json.JSONDecodeError: If the file is not valid JSON
        FileNotFoundError: If the file does not exist
    """
    try:
        # Validate file exists and is readable
        validate_file_exists(filename)
        validate_file_type(filename, ['.json'])

        with open(filename, encoding='utf-8') as f:
            data = json.load(f)
        return data
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON file: {e}")
        raise
    except Exception as e:
        logger.error(f"Error reading file {filename}: {e}")
        raise

def read_file_object(file_obj: BinaryIO) -> Dict[str, Any]:
    """
    Read and parse a JSON file from a file-like object.

    Args:
        file_obj (BinaryIO): File-like object containing JSON data

    Returns:
        dict: Parsed JSON data

    Raises:
        ValidationError: If the file object is invalid
        json.JSONDecodeError: If the content is not valid JSON
    """
    try:
        # Validate file object
        validate_file_object(file_obj)

        # Reset file pointer to beginning
        file_obj.seek(0)

        # Read and parse JSON
        data = json.loads(file_obj.read().decode('utf-8'))
        return data
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in file object: {e}")
        raise
    except Exception as e:
        logger.error(f"Error reading file object: {e}")
        raise

def read_tarfile(filename: str, select_json: Optional[int] = None,
                 auto_select: bool = True) -> Dict[str, Any]:
    """
    Extract and read a JSON file from a tar archive.

    Args:
        filename (str): Path to the tar archive
        select_json (int, optional): Index of the JSON file to use if multiple are found
        auto_select (bool): If True and multiple JSON files are found, automatically select
                           the first one without prompting. Default is True.

    Returns:
        dict: Contents of the JSON file

    Raises:
        ValidationError: If the file is invalid
        tarfile.ReadError: If the file is not a valid tar archive
        KeyError: If the specified JSON file is not found in the archive
        IndexError: If no JSON files are found in the tar
        json.JSONDecodeError: If the file is not valid JSON
        ValueError: If multiple JSON files are found and neither select_json nor auto_select is provided
    """
    try:
        # Validate file exists and is a tar file
        validate_file_exists(filename)
        validate_file_type(filename, ['.tar'])

        with tarfile.open(filename) as tar:
            # Find files inside the tar
            tar_contents = tar.getnames()

            # Only get the files with .json extension
            pattern = re.compile(r'.*\.json')
            tar_files = list(filter(pattern.match, tar_contents))

            if not tar_files:
                error_msg = "No JSON files found in the tar archive"
                logger.error(error_msg)
                raise IndexError(error_msg)

            # If multiple JSON files are found, handle selection
            if len(tar_files) > 1:
                if select_json is not None and 0 <= select_json < len(tar_files):
                    selected_index = select_json
                elif auto_select:
                    selected_index = 0
                    logger.info(f"Multiple JSON files found. Auto-selecting: {tar_files[selected_index]}")
                else:
                    # Instead of interactive prompt, raise an exception with available options
                    available_files = "\n".join([f"{i+1}: {file}" for i, file in enumerate(tar_files)])
                    error_msg = f"Multiple JSON files found in the tar archive. Please specify which one to use with select_json parameter:\n{available_files}"
                    logger.error(error_msg)
                    raise ValueError(error_msg)
            else:
                selected_index = 0

            # Read that file and parse it
            file_obj = tar.extractfile(tar.getmember(tar_files[selected_index]))
            if file_obj is None:
                raise KeyError(f"File {tar_files[selected_index]} could not be extracted")

            data = json.loads(file_obj.read().decode('utf-8'))
            return data
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        raise
    except tarfile.ReadError as e:
        logger.error(f"Invalid tar file: {e}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in tar file: {e}")
        raise
    except Exception as e:
        logger.error(f"Error reading tar file {filename}: {e}")
        raise

def read_tarfile_object(file_obj: BinaryIO, select_json: Optional[int] = None,
                       auto_select: bool = True) -> Dict[str, Any]:
    """
    Extract and read a JSON file from a tar archive provided as a file-like object.
    This is useful for processing uploaded files without saving them to disk.

    Args:
        file_obj (BinaryIO): File-like object containing a tar archive
        select_json (int, optional): Index of the JSON file to use if multiple are found
        auto_select (bool): If True and multiple JSON files are found, automatically select
                           the first one without prompting. Default is True.

    Returns:
        dict: Contents of the JSON file

    Raises:
        ValidationError: If the file object is invalid
        tarfile.ReadError: If the content is not a valid tar archive
        KeyError: If the specified JSON file is not found in the archive
        IndexError: If no JSON files are found in the tar
        json.JSONDecodeError: If the file is not valid JSON
        ValueError: If multiple JSON files are found and neither select_json nor auto_select is provided
    """
    # Validate file object
    try:
        validate_file_object(file_obj)
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        raise

    # Create a temporary file to store the tar content
    with tempfile.NamedTemporaryFile(suffix='.tar', delete=False) as temp_file:
        try:
            # Reset file pointer to beginning
            file_obj.seek(0)

            # Write content to temporary file
            temp_file.write(file_obj.read())
            temp_file.flush()

            # Use the existing read_tarfile function with the temporary file
            return read_tarfile(temp_file.name, select_json, auto_select)
        finally:
            # Clean up the temporary file
            try:
                os.unlink(temp_file.name)
            except Exception as e:
                logger.warning(f"Failed to delete temporary file {temp_file.name}: {e}")

def extract_tar_contents(tar_filename: str, output_dir: Optional[str] = None,
                        file_pattern: Optional[str] = None) -> List[str]:
    """
    Extract contents from a tar file based on an optional pattern.

    Args:
        tar_filename (str): Path to the tar file
        output_dir (str, optional): Directory to extract files to. If None, files are not extracted.
        file_pattern (str, optional): Regex pattern to match filenames

    Returns:
        list: List of extracted file paths or tar members if output_dir is None

    Raises:
        ValidationError: If the file is invalid
        tarfile.ReadError: If the file is not a valid tar archive
    """
    try:
        # Validate file exists and is a tar file
        validate_file_exists(tar_filename)
        validate_file_type(tar_filename, ['.tar'])

        with tarfile.open(tar_filename) as tar:
            # Get all members
            members = tar.getmembers()

            # Filter members if pattern is provided
            if file_pattern:
                pattern = re.compile(file_pattern)
                members = [m for m in members if pattern.match(m.name)]

            # Extract if output_dir is provided
            if output_dir:
                os.makedirs(output_dir, exist_ok=True)
                tar.extractall(path=output_dir, members=members)
                return [os.path.join(output_dir, m.name) for m in members]
            else:
                return [m.name for m in members]
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        raise
    except tarfile.ReadError as e:
        logger.error(f"Invalid tar file: {e}")
        raise
    except Exception as e:
        logger.error(f"Error extracting from tar file {tar_filename}: {e}")
        raise

def extract_tar_object(file_obj: BinaryIO, output_dir: str,
                      file_pattern: Optional[str] = None) -> List[str]:
    """
    Extract contents from a tar file provided as a file-like object.
    This is useful for processing uploaded files without saving them to disk.

    Args:
        file_obj (BinaryIO): File-like object containing a tar archive
        output_dir (str): Directory to extract files to
        file_pattern (str, optional): Regex pattern to match filenames

    Returns:
        list: List of extracted file paths

    Raises:
        ValidationError: If the file object is invalid
        tarfile.ReadError: If the content is not a valid tar archive
    """
    # Validate file object
    _validate_tar_file_object(file_obj)

    # Create a temporary file and process the tar content
    temp_file_path = _create_temp_file_from_object(file_obj)

    try:
        # Use the existing extract_tar_contents function with the temporary file
        return extract_tar_contents(temp_file_path, output_dir, file_pattern)
    finally:
        # Clean up the temporary file
        _cleanup_temp_file(temp_file_path)

def _validate_tar_file_object(file_obj: BinaryIO) -> None:
    """
    Validate that the file object is valid for tar extraction.

    Args:
        file_obj (BinaryIO): File-like object to validate

    Raises:
        ValidationError: If the file object is invalid
    """
    try:
        validate_file_object(file_obj)
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        raise

def _create_temp_file_from_object(file_obj: BinaryIO) -> str:
    """
    Create a temporary file from a file-like object.

    Args:
        file_obj (BinaryIO): File-like object containing content

    Returns:
        str: Path to the temporary file
    """
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        # Reset file pointer to beginning
        file_obj.seek(0)

        # Write content to temporary file
        temp_file.write(file_obj.read())
        temp_file.flush()

        return temp_file.name

def _cleanup_temp_file(file_path: str) -> None:
    """
    Clean up a temporary file.

    Args:
        file_path (str): Path to the temporary file to delete
    """
    try:
        os.unlink(file_path)
    except Exception as e:
        logger.warning(f"Failed to delete temporary file {file_path}: {e}")

def list_tar_contents(tar_filename: str, file_pattern: Optional[str] = None) -> List[str]:
    """
    List contents of a tar file, optionally filtered by a pattern.

    Args:
        tar_filename (str): Path to the tar file
        file_pattern (str, optional): Regex pattern to match filenames

    Returns:
        list: List of file names in the tar archive

    Raises:
        ValidationError: If the file is invalid
        tarfile.ReadError: If the file is not a valid tar archive
    """
    try:
        # Validate file exists and is a tar file
        validate_file_exists(tar_filename)
        validate_file_type(tar_filename, ['.tar'])

        with tarfile.open(tar_filename) as tar:
            contents = tar.getnames()

            # Filter contents if pattern is provided
            if file_pattern:
                pattern = re.compile(file_pattern)
                contents = [name for name in contents if pattern.match(name)]

            return contents
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        raise
    except tarfile.ReadError as e:
        logger.error(f"Invalid tar file: {e}")
        raise
    except Exception as e:
        logger.error(f"Error listing tar file {tar_filename}: {e}")
        raise

def list_tar_object(file_obj: BinaryIO, file_pattern: Optional[str] = None) -> List[str]:
    """
    List contents of a tar file provided as a file-like object.
    This is useful for processing uploaded files without saving them to disk.

    Args:
        file_obj (BinaryIO): File-like object containing a tar archive
        file_pattern (str, optional): Regex pattern to match filenames

    Returns:
        list: List of file names in the tar archive

    Raises:
        ValidationError: If the file object is invalid
        tarfile.ReadError: If the content is not a valid tar archive
    """
    # Validate file object
    try:
        validate_file_object(file_obj)
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
        raise

    # Create a temporary file to store the tar content
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        try:
            # Reset file pointer to beginning
            file_obj.seek(0)

            # Write content to temporary file
            temp_file.write(file_obj.read())
            temp_file.flush()

            # Use the existing list_tar_contents function with the temporary file
            return list_tar_contents(temp_file.name, file_pattern)
        finally:
            # Clean up the temporary file
            try:
                os.unlink(temp_file.name)
            except Exception as e:
                logger.warning(f"Failed to delete temporary file {temp_file.name}: {e}")