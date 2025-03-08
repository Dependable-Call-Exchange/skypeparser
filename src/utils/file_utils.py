"""
Utility functions for file operations.
"""

import re
import os
import json
import tarfile

def safe_filename(name):
    """
    Sanitize a string to be used as a filename.
    Removes or replaces characters that are not allowed in filenames.

    Args:
        name (str): The filename to sanitize

    Returns:
        str: A sanitized filename with invalid characters replaced by underscores
    """
    # Replace characters that are problematic in filenames
    invalid_chars = r'[<>:"/\\|?*]'
    return re.sub(invalid_chars, '_', name)

def get_file_extension(file_path):
    """
    Get the extension of a file.

    Args:
        file_path (str): Path to the file

    Returns:
        str: The file extension (lowercase, including the dot) or empty string if no extension
    """
    _, ext = os.path.splitext(file_path)
    return ext.lower()

def is_json_file(file_path):
    """
    Check if a file is a valid JSON file.

    Args:
        file_path (str): Path to the file

    Returns:
        bool: True if the file is a valid JSON file, False otherwise
    """
    if not os.path.exists(file_path):
        return False

    if not os.path.isfile(file_path):
        return False

    # Check extension
    if get_file_extension(file_path) != '.json':
        return False

    # Try to parse the file
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            json.load(f)
        return True
    except (json.JSONDecodeError, UnicodeDecodeError):
        return False
    except Exception:
        return False

def is_tar_file(file_path):
    """
    Check if a file is a valid TAR file.

    Args:
        file_path (str): Path to the file

    Returns:
        bool: True if the file is a valid TAR file, False otherwise
    """
    if not os.path.exists(file_path):
        return False

    if not os.path.isfile(file_path):
        return False

    # Check extension
    ext = get_file_extension(file_path)
    if ext not in ['.tar', '.gz', '.bz2', '.xz']:
        return False

    # Try to open the file as a TAR file
    try:
        with tarfile.open(file_path, 'r') as tar:
            return True
    except tarfile.ReadError:
        return False
    except Exception:
        return False