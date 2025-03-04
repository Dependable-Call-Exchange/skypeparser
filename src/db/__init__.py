"""
Skype Parser Database Module

This module provides functionality for importing Skype data into PostgreSQL.

The recommended approach is to use the SkypeETLPipeline class, which implements
a complete Extract-Transform-Load pipeline for Skype export data.
"""

import warnings

# Import the ETL pipeline
from .etl_pipeline import (
    SkypeETLPipeline,
    RAW_EXPORTS_TABLE,
    CONVERSATIONS_TABLE,
    MESSAGES_TABLE
)

# Import legacy modules with deprecation warnings
from .skype_to_postgres import (
    create_tables,
    import_skype_data,
    main as import_to_postgres
)

from .raw_storage import (
    SkypeDataStorage,
    CREATE_RAW_TABLES_SQL,
    INSERT_RAW_DATA_SQL,
    INSERT_CLEANED_DATA_SQL,
)

from .store_skype_export import (
    clean_skype_data,
    main as store_skype_export
)

# Issue deprecation warnings for legacy modules
warnings.warn(
    "The skype_to_postgres module is deprecated. "
    "Please use the SkypeETLPipeline class instead.",
    DeprecationWarning, stacklevel=2
)

warnings.warn(
    "The store_skype_export module is deprecated. "
    "Please use the SkypeETLPipeline class instead.",
    DeprecationWarning, stacklevel=2
)

__all__ = [
    # New ETL pipeline
    'SkypeETLPipeline',
    'RAW_EXPORTS_TABLE',
    'CONVERSATIONS_TABLE',
    'MESSAGES_TABLE',

    # Legacy modules (deprecated)
    'create_tables',
    'import_skype_data',
    'import_to_postgres',

    'SkypeDataStorage',
    'CREATE_RAW_TABLES_SQL',
    'INSERT_RAW_DATA_SQL',
    'INSERT_CLEANED_DATA_SQL',

    'clean_skype_data',
    'store_skype_export',
]
