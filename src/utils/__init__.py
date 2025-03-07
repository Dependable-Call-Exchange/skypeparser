"""
Utility functions for the Skype Parser project.
"""

from .file_utils import safe_filename
from .file_handler import (
    read_file,
    read_file_obj,
    read_tarfile,
    read_tar_file_obj
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
from .validation import (
    ValidationError,
    validate_file_exists,
    validate_directory,
    validate_file_type,
    validate_json_file,
    validate_tar_file,
    validate_file_object,
    validate_skype_data,
    validate_user_display_name,
    validate_db_config,
    validate_config
)

__all__ = [
    # Modules
    'file_utils',
    'file_handler',
    'dependencies',
    'validation',

    # File utilities
    'safe_filename',
    'read_file',
    'read_file_obj',
    'read_tarfile',
    'read_tar_file_obj',

    # Dependencies
    'BEAUTIFULSOUP_AVAILABLE',
    'BeautifulSoup',
    'BS_PARSER',
    'PSYCOPG2_AVAILABLE',
    'psycopg2',
    'check_dependency',
    'require_dependency',

    # Validation
    'ValidationError',
    'validate_file_exists',
    'validate_directory',
    'validate_file_type',
    'validate_json_file',
    'validate_tar_file',
    'validate_file_object',
    'validate_skype_data',
    'validate_user_display_name',
    'validate_db_config',
    'validate_config'
]