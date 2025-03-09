"""
Input Validation Module

This module provides comprehensive validation functions for all input data
used throughout the Skype Parser project. It centralizes validation logic
to ensure consistency and robustness.
"""

import json
import logging
import os
import pathlib
import re
import tarfile
from datetime import datetime
from typing import Any, BinaryIO, Dict, List, Optional, Protocol

# Set up logging
logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Exception raised for validation errors."""

    pass


def validate_path_safety(
    file_path: str,
    base_dir: Optional[str] = None,
    allow_absolute: bool = False,
    allow_symlinks: bool = False,
) -> str:
    """
    Perform strict validation on a file path to prevent security issues.

    This function checks for:
    - Path traversal attacks (e.g., "../../../etc/passwd")
    - Absolute paths (if not allowed)
    - Symbolic links (if not allowed)
    - Path normalization

    Args:
        file_path (str): Path to validate
        base_dir (str, optional): Base directory that all paths should be within
        allow_absolute (bool): Whether to allow absolute paths
        allow_symlinks (bool): Whether to allow symbolic links

    Returns:
        str: Normalized path that passed all security checks

    Raises:
        ValidationError: If the path fails any security check
    """
    if not file_path:
        raise ValidationError("File path cannot be empty")

    # Convert to Path object for safer manipulation
    path = pathlib.Path(file_path)

    # Check if path is absolute and not allowed
    if path.is_absolute() and not allow_absolute:
        raise ValidationError(f"Absolute paths are not allowed: {file_path}")

    # Normalize the path to resolve '..' and '.'
    try:
        normalized_path = path.resolve()
    except (ValueError, RuntimeError) as e:
        raise ValidationError(f"Invalid path: {file_path}. Error: {str(e)}")

    # Check for symbolic links if not allowed
    if not allow_symlinks and os.path.islink(file_path):
        raise ValidationError(f"Symbolic links are not allowed: {file_path}")

    # If base_dir is provided, ensure the path is within it
    if base_dir:
        base_path = pathlib.Path(base_dir).resolve()
        try:
            # Check if normalized_path is within base_path
            normalized_path.relative_to(base_path)
        except ValueError:
            raise ValidationError(f"Path must be within {base_dir}: {file_path}")

    # Check for common path traversal patterns
    path_str = str(normalized_path)
    if re.search(r"\.\./", file_path) or ".." in file_path.split(os.sep):
        # Double-check that the normalized path is safe
        if not base_dir:
            logger.warning(f"Path contains parent directory references: {file_path}")
        # If we have a base_dir, we've already checked that the resolved path is within it

    return str(normalized_path)


def validate_file_exists(
    file_path: str,
    base_dir: Optional[str] = None,
    allow_absolute: bool = False,
    allow_symlinks: bool = False,
) -> bool:
    """
    Validate that a file exists, is accessible, and passes path safety checks.

    Args:
        file_path (str): Path to the file to validate
        base_dir (str, optional): Base directory that all paths should be within
        allow_absolute (bool): Whether to allow absolute paths
        allow_symlinks (bool): Whether to allow symbolic links

    Returns:
        bool: True if the file exists, is accessible, and passes safety checks

    Raises:
        ValidationError: If the file does not exist, is not accessible, or fails safety checks
    """
    if not file_path:
        raise ValidationError("File path cannot be empty")

    # Perform path safety validation
    safe_path = validate_path_safety(
        file_path,
        base_dir=base_dir,
        allow_absolute=allow_absolute,
        allow_symlinks=allow_symlinks,
    )

    # Now check if the file exists and is accessible
    if not os.path.exists(safe_path):
        raise ValidationError(f"File does not exist: {safe_path}")

    if not os.path.isfile(safe_path):
        raise ValidationError(f"Path is not a file: {safe_path}")

    if not os.access(safe_path, os.R_OK):
        raise ValidationError(f"File is not readable: {safe_path}")

    return True


def validate_directory(
    directory: str,
    create_if_missing: bool = False,
    base_dir: Optional[str] = None,
    allow_absolute: bool = False,
    allow_symlinks: bool = False,
) -> bool:
    """
    Validate that a directory exists, is accessible, and passes path safety checks.

    Args:
        directory (str): Path to the directory to validate
        create_if_missing (bool): If True, create the directory if it doesn't exist
        base_dir (str, optional): Base directory that all paths should be within
        allow_absolute (bool): Whether to allow absolute paths
        allow_symlinks (bool): Whether to allow symbolic links

    Returns:
        bool: True if the directory exists, is accessible, and passes safety checks

    Raises:
        ValidationError: If the directory does not exist, is not accessible, or fails safety checks
    """
    if not directory:
        raise ValidationError("Directory path cannot be empty")

    # Perform path safety validation
    safe_path = validate_path_safety(
        directory,
        base_dir=base_dir,
        allow_absolute=allow_absolute,
        allow_symlinks=allow_symlinks,
    )

    # Check if the directory exists
    if not os.path.exists(safe_path):
        if create_if_missing:
            try:
                os.makedirs(safe_path, exist_ok=True)
                logger.info(f"Created directory: {safe_path}")
            except Exception as e:
                raise ValidationError(f"Failed to create directory {safe_path}: {e}")
        else:
            raise ValidationError(f"Directory does not exist: {safe_path}")

    # Check if it's a directory
    if not os.path.isdir(safe_path):
        raise ValidationError(f"Path is not a directory: {safe_path}")

    # Check if it's accessible
    if not os.access(safe_path, os.R_OK | os.W_OK):
        raise ValidationError(f"Directory is not accessible: {safe_path}")

    return True


def validate_file_type(
    file_path: str,
    allowed_extensions: List[str],
    base_dir: Optional[str] = None,
    allow_absolute: bool = False,
    allow_symlinks: bool = False,
) -> bool:
    """
    Validate that a file has an allowed extension and passes path safety checks.

    Args:
        file_path (str): Path to the file to validate
        allowed_extensions (list): List of allowed file extensions (e.g., ['.json', '.txt'])
        base_dir (str, optional): Base directory that all paths should be within
        allow_absolute (bool): Whether to allow absolute paths
        allow_symlinks (bool): Whether to allow symbolic links

    Returns:
        bool: True if the file has an allowed extension and passes safety checks

    Raises:
        ValidationError: If the file does not have an allowed extension or fails safety checks
    """
    if not file_path:
        raise ValidationError("File path cannot be empty")

    # Perform path safety validation
    safe_path = validate_path_safety(
        file_path,
        base_dir=base_dir,
        allow_absolute=allow_absolute,
        allow_symlinks=allow_symlinks,
    )

    # Check file extension
    _, ext = os.path.splitext(safe_path)
    if ext.lower() not in [e.lower() for e in allowed_extensions]:
        raise ValidationError(
            f"Invalid file type: {ext}. Allowed types: {', '.join(allowed_extensions)}"
        )

    return True


def validate_json_file(
    file_path: str,
    base_dir: Optional[str] = None,
    allow_absolute: bool = False,
    allow_symlinks: bool = False,
) -> Dict[str, Any]:
    """
    Validate and parse a JSON file, ensuring it passes path safety checks.

    Args:
        file_path (str): Path to the JSON file to validate
        base_dir (str, optional): Base directory that all paths should be within
        allow_absolute (bool): Whether to allow absolute paths
        allow_symlinks (bool): Whether to allow symbolic links

    Returns:
        dict: Parsed JSON data

    Raises:
        ValidationError: If the file is not a valid JSON file or fails safety checks
        json.JSONDecodeError: If the file is not valid JSON
    """
    # Validate file exists and is a JSON file
    validate_file_exists(
        file_path,
        base_dir=base_dir,
        allow_absolute=allow_absolute,
        allow_symlinks=allow_symlinks,
    )
    validate_file_type(
        file_path,
        [".json"],
        base_dir=base_dir,
        allow_absolute=allow_absolute,
        allow_symlinks=allow_symlinks,
    )

    # Parse the JSON file
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except json.JSONDecodeError as e:
        raise ValidationError(f"Invalid JSON file: {e}")
    except Exception as e:
        raise ValidationError(f"Error reading JSON file: {e}")


def validate_tar_file(
    file_path: str,
    base_dir: Optional[str] = None,
    allow_absolute: bool = False,
    allow_symlinks: bool = False,
) -> bool:
    """
    Validate that a file is a valid TAR archive and passes path safety checks.

    Args:
        file_path (str): Path to the TAR file to validate
        base_dir (str, optional): Base directory that all paths should be within
        allow_absolute (bool): Whether to allow absolute paths
        allow_symlinks (bool): Whether to allow symbolic links

    Returns:
        bool: True if the file is a valid TAR archive and passes safety checks

    Raises:
        ValidationError: If the file is not a valid TAR archive or fails safety checks
    """
    # Validate file exists and is a TAR file
    validate_file_exists(
        file_path,
        base_dir=base_dir,
        allow_absolute=allow_absolute,
        allow_symlinks=allow_symlinks,
    )
    validate_file_type(
        file_path,
        [".tar"],
        base_dir=base_dir,
        allow_absolute=allow_absolute,
        allow_symlinks=allow_symlinks,
    )

    # Check if it's a valid TAR file
    try:
        with tarfile.open(file_path) as tar:
            # Just opening the file is enough to validate it
            pass
        return True
    except tarfile.ReadError as e:
        raise ValidationError(f"Invalid TAR file: {e}")
    except Exception as e:
        raise ValidationError(f"Error reading TAR file: {e}")


def validate_tar_integrity(
    file_path: str,
    base_dir: Optional[str] = None,
    allow_absolute: bool = False,
    allow_symlinks: bool = False,
    required_files: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Validate the integrity and structure of a TAR file for Skype export data.

    This performs comprehensive validation:
    1. Checks if the TAR file is valid and can be opened
    2. Validates the presence of required files (like messages.json)
    3. Verifies basic JSON integrity of found files
    4. Returns information about the file contents

    Args:
        file_path: Path to the TAR file to validate
        base_dir: Base directory that all paths should be within
        allow_absolute: Whether to allow absolute paths
        allow_symlinks: Whether to allow symbolic links
        required_files: List of file patterns that must be present

    Returns:
        Dictionary with validation results and file info

    Raises:
        ValidationError: If validation fails
    """
    # Default required files for Skype exports
    if required_files is None:
        required_files = ["messages.json"]

    # First do basic TAR validation
    validate_tar_file(
        file_path,
        base_dir=base_dir,
        allow_absolute=allow_absolute,
        allow_symlinks=allow_symlinks,
    )

    result = {
        "is_valid": True,
        "file_path": file_path,
        "file_size": os.path.getsize(file_path),
        "json_files": [],
        "found_required_files": [],
        "missing_required_files": [],
    }

    try:
        with tarfile.open(file_path) as tar:
            # Get list of all files
            members = tar.getmembers()
            result["total_files"] = len(members)

            # Filter to find JSON files
            json_files = [m for m in members if m.name.endswith('.json')]
            result["json_files"] = [m.name for m in json_files]

            # Check for required files
            for pattern in required_files:
                found = False
                for m in json_files:
                    if pattern in m.name:
                        found = True
                        result["found_required_files"].append(m.name)
                        break
                if not found:
                    result["missing_required_files"].append(pattern)

            # Check for missing required files
            if result["missing_required_files"]:
                missing_files = ", ".join(result["missing_required_files"])
                raise ValidationError(f"Missing required files in TAR archive: {missing_files}")

            # Validate JSON integrity for key files
            for m in json_files:
                try:
                    extracted = tar.extractfile(m)
                    if extracted:
                        # Read just enough to validate it's a JSON file
                        data = extracted.read(1024)
                        try:
                            # Try to parse the beginning as JSON
                            # This is not a full validation but a basic integrity check
                            if data.strip().startswith(b'['):
                                # If it starts with array, close it for parsing
                                json.loads(data + b']' if not data.endswith(b']') else data)
                            elif data.strip().startswith(b'{'):
                                # If it starts with object, close it for parsing
                                json.loads(data + b'}' if not data.endswith(b'}') else data)
                        except json.JSONDecodeError as e:
                            raise ValidationError(f"Invalid JSON in {m.name}: {str(e)}")
                except Exception as e:
                    raise ValidationError(f"Error validating {m.name}: {str(e)}")

        return result

    except tarfile.ReadError as e:
        raise ValidationError(f"Invalid TAR archive: {str(e)}")
    except Exception as e:
        if isinstance(e, ValidationError):
            raise
        raise ValidationError(f"Error validating TAR archive: {str(e)}")


def validate_file_object(
    file_obj: BinaryIO, allowed_extensions: Optional[List[str]] = None
) -> bool:
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

    if not hasattr(file_obj, "read") or not callable(file_obj.read):
        raise ValidationError("Object does not have a 'read' method")

    if allowed_extensions and hasattr(file_obj, "name"):
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
    required_fields = ["userId", "exportDate", "conversations"]
    missing_fields = [f for f in required_fields if f not in data]
    if missing_fields:
        raise ValidationError(f"Missing required fields: {missing_fields}")

    # Validate userId
    if not isinstance(data["userId"], str) or not data["userId"].strip():
        raise ValidationError("userId must be a non-empty string")

    # Validate exportDate
    if not isinstance(data["exportDate"], str) or not data["exportDate"].strip():
        raise ValidationError("exportDate must be a non-empty string")

    # Try to parse the exportDate
    try:
        # Check if it's in ISO format
        datetime.fromisoformat(data["exportDate"].replace("Z", "+00:00"))
    except ValueError:
        logger.warning(f"exportDate is not in ISO format: {data['exportDate']}")

    # Validate conversations
    if not isinstance(data["conversations"], list):
        raise ValidationError("conversations must be a list")

    # Validate each conversation
    for i, conv in enumerate(data["conversations"]):
        if not isinstance(conv, dict):
            raise ValidationError(f"Conversation at index {i} must be a dictionary")

        if "id" not in conv:
            raise ValidationError(f"Conversation at index {i} is missing 'id' field")

        if not isinstance(conv["id"], str) or not conv["id"].strip():
            raise ValidationError(
                f"Conversation id at index {i} must be a non-empty string"
            )

        if "MessageList" not in conv:
            raise ValidationError(
                f"Conversation at index {i} is missing 'MessageList' field"
            )

        if not isinstance(conv["MessageList"], list):
            raise ValidationError(
                f"MessageList for conversation {conv['id']} must be a list"
            )

        # Validate each message (basic structure only)
        for j, msg in enumerate(conv["MessageList"]):
            if not isinstance(msg, dict):
                raise ValidationError(
                    f"Message at index {j} in conversation {conv['id']} must be a dictionary"
                )

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
    sanitized = re.sub(r'[<>:"/\\|?*]', "_", name)

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
    required_fields = ["dbname", "user"]
    missing_fields = [f for f in required_fields if f not in config]
    if missing_fields:
        raise ValidationError(
            f"Missing required database configuration fields: {missing_fields}"
        )

    # Validate field types
    if not isinstance(config["dbname"], str) or not config["dbname"].strip():
        raise ValidationError("Database name must be a non-empty string")

    if not isinstance(config["user"], str) or not config["user"].strip():
        raise ValidationError("Database user must be a non-empty string")

    # Validate optional fields if present
    if "password" in config and not isinstance(config["password"], str):
        raise ValidationError("Database password must be a string")

    if "host" in config and not isinstance(config["host"], str):
        raise ValidationError("Database host must be a string")

    if "port" in config:
        if not isinstance(config["port"], int):
            try:
                # Try to convert to int if it's a string
                config["port"] = int(config["port"])
            except (ValueError, TypeError):
                raise ValidationError("Database port must be an integer")

        if config["port"] < 1 or config["port"] > 65535:
            raise ValidationError("Database port must be between 1 and 65535")

    # Validate SSL mode if present
    if "sslmode" in config:
        if not isinstance(config["sslmode"], str):
            raise ValidationError("SSL mode must be a string")

        valid_ssl_modes = [
            "disable",
            "allow",
            "prefer",
            "require",
            "verify-ca",
            "verify-full",
        ]
        if config["sslmode"] not in valid_ssl_modes:
            raise ValidationError(
                f"Invalid SSL mode: {config['sslmode']}. Valid modes are: {', '.join(valid_ssl_modes)}"
            )

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
    if "database" in config:
        if not isinstance(config["database"], dict):
            raise ValidationError("Database configuration must be a dictionary")
        try:
            validate_db_config(config["database"])
        except ValidationError as e:
            raise ValidationError(f"Invalid database configuration: {e}")

    # Validate output configuration if present
    if "output" in config:
        if not isinstance(config["output"], dict):
            raise ValidationError("Output configuration must be a dictionary")

        if "directory" in config["output"]:
            if not isinstance(config["output"]["directory"], str):
                raise ValidationError("Output directory must be a string")

        if "overwrite" in config["output"]:
            if not isinstance(config["output"]["overwrite"], bool):
                try:
                    # Try to convert to bool if it's a string
                    config["output"]["overwrite"] = config["output"][
                        "overwrite"
                    ].lower() in ("true", "yes", "1")
                except (AttributeError, ValueError):
                    raise ValidationError("Output overwrite flag must be a boolean")

    # Validate logging configuration if present
    if "logging" in config:
        if not isinstance(config["logging"], dict):
            raise ValidationError("Logging configuration must be a dictionary")

        if "level" in config["logging"]:
            valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
            level = config["logging"]["level"]
            if not isinstance(level, str) or level.upper() not in valid_levels:
                raise ValidationError(
                    f"Invalid logging level: {level}. Must be one of {valid_levels}"
                )

        if "file" in config["logging"]:
            if not isinstance(config["logging"]["file"], str):
                raise ValidationError("Logging file must be a string")

    return True


from src.interfaces import ValidationServiceProtocol


class ValidationService(ValidationServiceProtocol):
    """
    Implementation of the ValidationServiceProtocol.

    This class wraps the module-level validation functions to provide a consistent
    interface for validation services.
    """

    def validate_file_exists(
        self,
        path: str,
        base_dir: Optional[str] = None,
        allow_absolute: bool = False,
        allow_symlinks: bool = False,
    ) -> bool:
        """
        Validate that a file exists and passes path safety checks.

        Args:
            path (str): Path to validate
            base_dir (str, optional): Base directory that all paths should be within
            allow_absolute (bool): Whether to allow absolute paths
            allow_symlinks (bool): Whether to allow symbolic links

        Returns:
            bool: True if the file exists and passes safety checks

        Raises:
            ValidationError: If the file does not exist or fails safety checks
        """
        return validate_file_exists(path, base_dir, allow_absolute, allow_symlinks)

    def validate_json_file(
        self,
        file_path: str,
        base_dir: Optional[str] = None,
        allow_absolute: bool = False,
        allow_symlinks: bool = False,
    ) -> Dict[str, Any]:
        """
        Validate and parse a JSON file, ensuring it passes path safety checks.

        Args:
            file_path (str): Path to the JSON file to validate
            base_dir (str, optional): Base directory that all paths should be within
            allow_absolute (bool): Whether to allow absolute paths
            allow_symlinks (bool): Whether to allow symbolic links

        Returns:
            dict: Parsed JSON data

        Raises:
            ValidationError: If the file does not exist, fails safety checks, or is not valid JSON
        """
        return validate_json_file(file_path, base_dir, allow_absolute, allow_symlinks)

    def validate_tar_file(
        self,
        file_path: str,
        base_dir: Optional[str] = None,
        allow_absolute: bool = False,
        allow_symlinks: bool = False,
    ) -> bool:
        """
        Validate that a file is a valid TAR archive and passes path safety checks.

        Args:
            file_path (str): Path to the TAR file to validate
            base_dir (str, optional): Base directory that all paths should be within
            allow_absolute (bool): Whether to allow absolute paths
            allow_symlinks (bool): Whether to allow symbolic links

        Returns:
            bool: True if the file is a valid TAR archive and passes safety checks

        Raises:
            ValidationError: If the file is not a valid TAR archive or fails safety checks
        """
        return validate_tar_file(file_path, base_dir, allow_absolute, allow_symlinks)

    def validate_tar_integrity(
        self,
        file_path: str,
        base_dir: Optional[str] = None,
        allow_absolute: bool = False,
        allow_symlinks: bool = False,
        required_files: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Validate the integrity and structure of a TAR file for Skype export data.

        This performs comprehensive validation:
        1. Checks if the TAR file is valid and can be opened
        2. Validates the presence of required files (like messages.json)
        3. Verifies basic JSON integrity of found files
        4. Returns information about the file contents

        Args:
            file_path: Path to the TAR file to validate
            base_dir: Base directory that all paths should be within
            allow_absolute: Whether to allow absolute paths
            allow_symlinks: Whether to allow symbolic links
            required_files: List of file patterns that must be present

        Returns:
            Dictionary with validation results and file info

        Raises:
            ValidationError: If validation fails
        """
        return validate_tar_integrity(
            file_path,
            base_dir,
            allow_absolute,
            allow_symlinks,
            required_files
        )

    def validate_user_display_name(self, name: str) -> str:
        """
        Validate and sanitize a user display name.

        Args:
            name (str): User display name to validate

        Returns:
            str: Sanitized user display name

        Raises:
            ValidationError: If the name is invalid
        """
        return validate_user_display_name(name)

    def validate_skype_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate the structure of Skype export data.

        Args:
            data (dict): Dictionary containing Skype data

        Returns:
            bool: True if valid

        Raises:
            ValidationError: If the data is invalid
        """
        try:
            # Attempt to support both older and newer Skype data formats
            if "messages" in data and not any(key in data for key in ["userId", "exportDate", "conversations"]):
                # For newer format with "messages" key, we'll apply more lenient validation
                # Just check if messages is a list or dict
                if not isinstance(data.get("messages"), (list, dict)):
                    raise ValidationError("The 'messages' field must be a list or dictionary")
                # Return True to indicate validation success for this format
                return True
            else:
                # Import the schema validation if available
                try:
                    from src.validation.schema_validation import validate_skype_data as schema_validate_skype_data
                    # Use schema validation for traditional format
                    schema_validate_skype_data(data)
                    return True
                except ImportError:
                    # Fall back to basic validation if schema validation is not available
                    return validate_skype_data(data)
        except Exception as e:
            # Wrap any schema validation errors in ValidationError for consistency
            if not isinstance(e, ValidationError):
                raise ValidationError(f"Skype data validation failed: {str(e)}")
            raise
