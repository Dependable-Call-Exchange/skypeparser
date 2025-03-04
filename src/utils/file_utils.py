"""
Utility functions for file operations.
"""

import re

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