#!/usr/bin/env python3
"""
ETL Pipeline for Skype Export Data (DEPRECATED)

This module is a compatibility wrapper for the old SkypeETLPipeline class.
It has been replaced by the modular ETL pipeline in src.db.etl.

DEPRECATION WARNING: This module is deprecated and will be removed in version 2.0.0.
Please use the modular ETL pipeline from src.db.etl instead.
"""

import warnings
import logging

# Import the compatibility layer
from .etl_pipeline_compat import SkypeETLPipeline

# Configure logging
logger = logging.getLogger(__name__)

# Issue a deprecation warning when this module is imported
warnings.warn(
    "The etl_pipeline module is deprecated and will be removed in version 2.0.0. "
    "Please use the modular ETL pipeline from src.db.etl instead.",
    DeprecationWarning, stacklevel=2
)

# For backward compatibility, expose any constants or functions that might be imported directly
from ..parser.core_parser import timestamp_parser

# Database schema definitions (for backward compatibility)
RAW_EXPORTS_TABLE = "skype_raw_exports"
CONVERSATIONS_TABLE = "skype_conversations"
MESSAGES_TABLE = "skype_messages"

# For backward compatibility with code that might import these classes directly
from .progress_tracker import ProgressTracker
from .etl.utils import MemoryMonitor

if __name__ == "__main__":
    # This module is not meant to be run directly
    # It should be imported and used as a library
    print("This module is not meant to be run directly.")
    print("Please import it and use the SkypeETLPipeline class instead.")
    print("See the README.md file for usage examples.")
    print("\nDEPRECATION WARNING: This module is deprecated and will be removed in version 2.0.0.")
    print("Please use the modular ETL pipeline from src.db.etl instead.")