"""
Skype Parser Database Module

This module provides functionality for importing Skype data into PostgreSQL.

The recommended approach is to use the ETLPipeline class from the modular ETL pipeline,
which provides a more maintainable and extensible architecture for ETL operations.

For backward compatibility, the SkypeETLPipeline class is still available but will be
removed in version 2.0.0.

For a SOLID implementation that follows best practices, use the SOLIDSkypeETLPipeline class.
"""

import warnings

# For backward compatibility, expose any constants or functions that might be imported directly
from ..parser.core_parser import timestamp_parser

# Import the modular ETL pipeline components
from .etl import ETLContext, ETLPipeline, Extractor, Loader, Transformer

# Import the database schema definitions from the modular ETL pipeline
from .etl.loader import CONVERSATIONS_TABLE, MESSAGES_TABLE, RAW_EXPORTS_TABLE

# Import the SOLID ETL pipeline
from .etl_pipeline import SOLIDSkypeETLPipeline, create_solid_skype_etl_pipeline

# Import the ETL pipeline (now using the compatibility layer)
from .etl_pipeline_compat import SkypeETLPipeline
from .raw_storage import (
    CREATE_RAW_TABLES_SQL,
    INSERT_CLEANED_DATA_SQL,
    INSERT_RAW_DATA_SQL,
    SkypeDataStorage,
)

# Import legacy modules with deprecation warnings
from .skype_to_postgres import create_tables, import_skype_data
from .skype_to_postgres import main as import_to_postgres
from .store_skype_export import clean_skype_data
from .store_skype_export import main as store_skype_export

# Issue deprecation warnings for legacy modules and classes
warnings.warn(
    "The SkypeETLPipeline class is deprecated and will be removed in version 2.0.0. "
    "Please use the ETLPipeline class from the modular ETL pipeline instead.",
    DeprecationWarning,
    stacklevel=2,
)

warnings.warn(
    "The skype_to_postgres module is deprecated and will be removed in version 2.0.0. "
    "Please use the ETLPipeline class instead.",
    DeprecationWarning,
    stacklevel=2,
)

warnings.warn(
    "The store_skype_export module is deprecated and will be removed in version 2.0.0. "
    "Please use the ETLPipeline class instead.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    # Modular ETL pipeline
    "ETLPipeline",
    "ETLContext",
    "Extractor",
    "Transformer",
    "Loader",
    # SOLID ETL pipeline
    "SOLIDSkypeETLPipeline",
    "create_solid_skype_etl_pipeline",
    # Legacy ETL pipeline (deprecated)
    "SkypeETLPipeline",
    # Database schema definitions
    "RAW_EXPORTS_TABLE",
    "CONVERSATIONS_TABLE",
    "MESSAGES_TABLE",
    # Legacy functions (deprecated)
    "create_tables",
    "import_skype_data",
    "import_to_postgres",
    "store_skype_export",
    "clean_skype_data",
    # Raw storage
    "SkypeDataStorage",
    "CREATE_RAW_TABLES_SQL",
    "INSERT_RAW_DATA_SQL",
    "INSERT_CLEANED_DATA_SQL",
    # Utility functions
    "timestamp_parser",
]
