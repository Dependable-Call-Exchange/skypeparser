"""
Skype Parser Database Module

This module provides functionality for importing Skype data into PostgreSQL.

The recommended approach is to use the ETLPipeline class from the modular ETL pipeline,
which provides a more maintainable and extensible architecture for ETL operations.

For backward compatibility, the SkypeETLPipeline class is still available but will be
removed in version 2.0.0.
"""

import warnings

# Import the ETL pipeline (now using the compatibility layer)
from .etl_pipeline_compat import (
    SkypeETLPipeline,
)

# Import the modular ETL pipeline components
from .etl import (
    ETLPipeline,
    ETLContext,
    Extractor,
    Transformer,
    Loader
)

# Import the database schema definitions from the modular ETL pipeline
from .etl.loader import (
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

# Issue deprecation warnings for legacy modules and classes
warnings.warn(
    "The SkypeETLPipeline class is deprecated and will be removed in version 2.0.0. "
    "Please use the ETLPipeline class from the modular ETL pipeline instead.",
    DeprecationWarning, stacklevel=2
)

warnings.warn(
    "The skype_to_postgres module is deprecated and will be removed in version 2.0.0. "
    "Please use the ETLPipeline class instead.",
    DeprecationWarning, stacklevel=2
)

warnings.warn(
    "The store_skype_export module is deprecated and will be removed in version 2.0.0. "
    "Please use the ETLPipeline class instead.",
    DeprecationWarning, stacklevel=2
)

__all__ = [
    # New modular ETL pipeline
    'ETLPipeline',
    'ETLContext',
    'Extractor',
    'Transformer',
    'Loader',

    # Compatibility layer (deprecated)
    'SkypeETLPipeline',

    # Database schema
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
