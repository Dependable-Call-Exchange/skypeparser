"""
Utility functions for the Skype Parser project.
"""

from .file_utils import safe_filename
from .file_handler import (
    read_file,
    read_file_object,
    read_tarfile,
    read_tarfile_object,
    extract_tar_contents,
    extract_tar_object,
    list_tar_contents,
    list_tar_object
)
from .dependencies import (
    BEAUTIFULSOUP_AVAILABLE,
    BeautifulSoup,
    BS_PARSER,
    PSYCOPG2_AVAILABLE,
    psycopg2,
    check_dependency,
    require_dependency
)

__all__ = [
    # Modules
    'file_utils',
    'file_handler',
    'dependencies',

    # File utilities
    'safe_filename',
    'read_file',
    'read_file_object',
    'read_tarfile',
    'read_tarfile_object',
    'extract_tar_contents',
    'extract_tar_object',
    'list_tar_contents',
    'list_tar_object',

    # Dependencies
    'BEAUTIFULSOUP_AVAILABLE',
    'BeautifulSoup',
    'BS_PARSER',
    'PSYCOPG2_AVAILABLE',
    'psycopg2',
    'check_dependency',
    'require_dependency'
]