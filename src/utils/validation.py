"""
Input Validation Module

This module provides comprehensive validation functions for all input data
used throughout the Skype Parser project. It centralizes validation logic
to ensure consistency and robustness.
"""

import os
import re
import json
import logging
import tarfile
from typing import Dict, List, Any, Optional, Union, BinaryIO
from datetime import datetime

# Set up logging
logger = logging.getLogger(__name__)

class ValidationError(Exception):
    """Exception raised for validation errors."""
    pass

def validate_file_exists(file_path: str) -> bool:
    """
    Validate that a file exists and is accessible.

    Args:
        file_path (str): Path to the file to validate

    Returns:
        bool: True if the file exists and is accessible

    Raises:
        ValidationError: If the file does not exist or is not accessible
    """
    if not file_path:
        raise ValidationError("File path cannot be empty")

    if not os.path.exists(file_path):
        raise ValidationError(f"File does not exist: {file_path}")

    if not os.path.isfile(file_path):
        raise ValidationError(f"Path is not a file: {file_path}")

    if not os.access(file_path, os.R_OK):
        raise ValidationError(f"File is not readable: {file_path}")

    return True

def validate_directory(directory: str, create_if_missing: bool = False) -> bool:
    """
    Validate that a directory exists and is accessible.

    Args:
        directory (str): Path to the directory to validate
        create_if_missing (bool): If True, create the directory if it doesn't exist

    Returns:
        bool: True if the directory exists and is accessible

    Raises:
        ValidationError: If the directory does not exist or is not accessible
    """
    if not directory:
        raise ValidationError("Directory path cannot be empty")

    if not os.path.exists(directory):
        if create_if_missing:
            try:
                os.makedirs(directory, exist_ok=True)
                logger.info(f"Created directory: {directory}")
            except Exception as e:
                raise ValidationError(f"Failed to create directory {directory}: {e}")
        else:
            raise ValidationError(f"Directory does not exist: {directory}")

    if not os.path.isdir(directory):
        raise ValidationError(f"Path is not a directory: {directory}")

    if not os.access(directory, os.R_OK | os.W_OK):
        raise ValidationError(f"Directory is not accessible (read/write): {directory}")

    return True

def validate_file_type(file_path: str, allowed_extensions: List[str]) -> bool:
    """
    Validate that a file has an allowed extension.

    Args:
        file_path (str): Path to the file to validate
        allowed_extensions (list): List of allowed file extensions (e.g., ['.json', '.tar'])

    Returns:
        bool: True if the file has an allowed extension

    Raises:
        ValidationError: If the file does not have an allowed extension
    """
    if not file_path:
        raise ValidationError("File path cannot be empty")

    _, ext = os.path.splitext(file_path.lower())
    if ext not in allowed_extensions:
        raise ValidationError(
            f"Invalid file type: {ext}. Allowed types: {', '.join(allowed_extensions)}"
        )

    return True

def validate_json_file(file_path: str) -> Dict[str, Any]:
    """
    Validate that a file contains valid JSON.

    Args:
        file_path (str): Path to the JSON file to validate

    Returns:
        dict: The parsed JSON data

    Raises:
        ValidationError: If the file does not contain valid JSON
    """
    validate_file_exists(file_path)
    validate_file_type(file_path, ['.json'])

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        if not isinstance(data, dict):
            raise ValidationError(f"File does not contain a JSON object: {file_path}")

        return data
    except json.JSONDecodeError as e:
        raise ValidationError(f"Invalid JSON in file {file_path}: {e}")
    except Exception as e:
        raise ValidationError(f"Error reading JSON file {file_path}: {e}")

def validate_tar_file(file_path: str) -> bool:
    """
    Validate that a file is a valid TAR archive.

    Args:
        file_path (str): Path to the TAR file to validate

    Returns:
        bool: True if the file is a valid TAR archive

    Raises:
        ValidationError: If the file is not a valid TAR archive
    """
    validate_file_exists(file_path)
    validate_file_type(file_path, ['.tar'])

    try:
        with tarfile.open(file_path) as tar:
            # Check if we can list the contents
            tar.getnames()
        return True
    except tarfile.ReadError as e:
        raise ValidationError(f"Invalid TAR file {file_path}: {e}")
    except Exception as e:
        raise ValidationError(f"Error reading TAR file {file_path}: {e}")

def validate_file_object(file_obj: BinaryIO, allowed_extensions: Optional[List[str]] = None) -> bool:
    """
    Validate a file-like object.

    Args:
        file_obj (BinaryIO): File-like object to validate
        allowed_extensions (list, optional): List of allowed file extensions

    Returns:
        bool: True if the file object is valid

    Raises:
        ValidationError: If the file object is invalid
    """
    if file_obj is None:
        raise ValidationError("File object cannot be None")

    if not hasattr(file_obj, 'read') or not callable(file_obj.read):
        raise ValidationError("Object does not have a 'read' method")

    if allowed_extensions and hasattr(file_obj, 'name'):
        _, ext = os.path.splitext(file_obj.name.lower())
        if ext not in allowed_extensions:
            raise ValidationError(
                f"Invalid file type: {ext}. Allowed types: {', '.join(allowed_extensions)}"
            )

    return True

def validate_skype_data(data: Dict[str, Any]) -> bool:
    """
    Validate the structure of Skype export data.

    Args:
        data (dict): Dictionary containing Skype data

    Returns:
        bool: True if valid

    Raises:
        ValidationError: If the data is invalid
    """
    if not isinstance(data, dict):
        raise ValidationError("Data must be a dictionary")

    # Check required top-level fields
    required_fields = ['userId', 'exportDate', 'conversations']
    missing_fields = [f for f in required_fields if f not in data]
    if missing_fields:
        raise ValidationError(f"Missing required fields: {missing_fields}")

    # Validate userId
    if not isinstance(data['userId'], str) or not data['userId'].strip():
        raise ValidationError("userId must be a non-empty string")

    # Validate exportDate
    if not isinstance(data['exportDate'], str) or not data['exportDate'].strip():
        raise ValidationError("exportDate must be a non-empty string")

    # Try to parse the exportDate
    try:
        # Check if it's in ISO format
        datetime.fromisoformat(data['exportDate'].replace('Z', '+00:00'))
    except ValueError:
        logger.warning(f"exportDate is not in ISO format: {data['exportDate']}")

    # Validate conversations
    if not isinstance(data['conversations'], list):
        raise ValidationError("conversations must be a list")

    # Validate each conversation
    for i, conv in enumerate(data['conversations']):
        if not isinstance(conv, dict):
            raise ValidationError(f"Conversation at index {i} must be a dictionary")

        if 'id' not in conv:
            raise ValidationError(f"Conversation at index {i} is missing 'id' field")

        if not isinstance(conv['id'], str) or not conv['id'].strip():
            raise ValidationError(f"Conversation id at index {i} must be a non-empty string")

        if 'MessageList' not in conv:
            raise ValidationError(f"Conversation at index {i} is missing 'MessageList' field")

        if not isinstance(conv['MessageList'], list):
            raise ValidationError(f"MessageList for conversation {conv['id']} must be a list")

        # Validate each message (basic structure only)
        for j, msg in enumerate(conv['MessageList']):
            if not isinstance(msg, dict):
                raise ValidationError(f"Message at index {j} in conversation {conv['id']} must be a dictionary")

    return True

def validate_user_display_name(name: str) -> str:
    """
    Validate and sanitize a user display name.

    Args:
        name (str): User display name to validate

    Returns:
        str: Sanitized user display name

    Raises:
        ValidationError: If the name is invalid
    """
    if name is None:
        raise ValidationError("User display name cannot be None")

    # Convert to string if it's not already
    name = str(name).strip()

    if not name:
        raise ValidationError("User display name cannot be empty")

    # Remove any potentially dangerous characters
    sanitized = re.sub(r'[<>:"/\\|?*]', '_', name)

    return sanitized

def validate_db_config(config: Dict[str, Any]) -> bool:
    """
    Validate database configuration.

    Args:
        config (dict): Database configuration dictionary

    Returns:
        bool: True if the configuration is valid

    Raises:
        ValidationError: If the configuration is invalid
    """
    if not isinstance(config, dict):
        raise ValidationError("Database configuration must be a dictionary")

    # Check required fields
    required_fields = ['dbname', 'user']
    missing_fields = [f for f in required_fields if f not in config]
    if missing_fields:
        raise ValidationError(f"Missing required database configuration fields: {missing_fields}")

    # Validate field types
    if not isinstance(config['dbname'], str) or not config['dbname'].strip():
        raise ValidationError("Database name must be a non-empty string")

    if not isinstance(config['user'], str) or not config['user'].strip():
        raise ValidationError("Database user must be a non-empty string")

    # Validate optional fields if present
    if 'password' in config and not isinstance(config['password'], str):
        raise ValidationError("Database password must be a string")

    if 'host' in config and not isinstance(config['host'], str):
        raise ValidationError("Database host must be a string")

    if 'port' in config:
        if not isinstance(config['port'], int):
            try:
                # Try to convert to int if it's a string
                config['port'] = int(config['port'])
            except (ValueError, TypeError):
                raise ValidationError("Database port must be an integer")

        if config['port'] < 1 or config['port'] > 65535:
            raise ValidationError("Database port must be between 1 and 65535")

    return True

def validate_config(config: Dict[str, Any]) -> bool:
    """
    Validate the application configuration.

    Args:
        config (dict): Configuration dictionary

    Returns:
        bool: True if the configuration is valid

    Raises:
        ValidationError: If the configuration is invalid
    """
    if not isinstance(config, dict):
        raise ValidationError("Configuration must be a dictionary")

    # Validate database configuration if present
    if 'database' in config:
        if not isinstance(config['database'], dict):
            raise ValidationError("Database configuration must be a dictionary")
        try:
            validate_db_config(config['database'])
        except ValidationError as e:
            raise ValidationError(f"Invalid database configuration: {e}")

    # Validate output configuration if present
    if 'output' in config:
        if not isinstance(config['output'], dict):
            raise ValidationError("Output configuration must be a dictionary")

        if 'directory' in config['output']:
            if not isinstance(config['output']['directory'], str):
                raise ValidationError("Output directory must be a string")

        if 'overwrite' in config['output']:
            if not isinstance(config['output']['overwrite'], bool):
                try:
                    # Try to convert to bool if it's a string
                    config['output']['overwrite'] = config['output']['overwrite'].lower() in ('true', 'yes', '1')
                except (AttributeError, ValueError):
                    raise ValidationError("Output overwrite flag must be a boolean")

    # Validate logging configuration if present
    if 'logging' in config:
        if not isinstance(config['logging'], dict):
            raise ValidationError("Logging configuration must be a dictionary")

        if 'level' in config['logging']:
            valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
            level = config['logging']['level']
            if not isinstance(level, str) or level.upper() not in valid_levels:
                raise ValidationError(f"Invalid logging level: {level}. Must be one of {valid_levels}")

        if 'file' in config['logging']:
            if not isinstance(config['logging']['file'], str):
                raise ValidationError("Logging file must be a string")

    return True