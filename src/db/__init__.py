"""
Skype Parser Database Module

This module provides functionality for importing Skype data into PostgreSQL.

The recommended approach is to use the ETLPipeline class from the modular ETL pipeline,
which provides a more maintainable and extensible architecture for ETL operations.

For backward compatibility, the SkypeETLPipeline class is still available but will be
removed in version 2.0.0.

For a SOLID implementation that follows best practices, use the SOLIDSkypeETLPipeline class.
"""

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

# Note: Legacy modules are deprecated and will be removed in version 2.0.0
# Use the ETLPipeline class from the modular ETL pipeline instead

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
    # Raw storage
    "SkypeDataStorage",
    "CREATE_RAW_TABLES_SQL",
    "INSERT_RAW_DATA_SQL",
    "INSERT_CLEANED_DATA_SQL",
    # Utility functions
    "timestamp_parser",
]
