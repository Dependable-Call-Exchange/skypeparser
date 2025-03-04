"""
Raw storage module for Skype data.

This module provides functionality for storing both raw and cleaned Skype data
in PostgreSQL databases.
"""

from .storage import SkypeDataStorage
from .models import (
    CREATE_RAW_TABLES_SQL,
    INSERT_RAW_DATA_SQL,
    INSERT_CLEANED_DATA_SQL,
)

__all__ = [
    'SkypeDataStorage',
    'CREATE_RAW_TABLES_SQL',
    'INSERT_RAW_DATA_SQL',
    'INSERT_CLEANED_DATA_SQL',
]