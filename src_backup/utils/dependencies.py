#!/usr/bin/env python3
"""
Dependencies Utility Module

This module provides centralized handling of external dependencies for the Skype Parser project.
It ensures consistent behavior across all modules when dealing with optional dependencies.
"""

import logging
import sys
from typing import Tuple, Any, Optional

# Set up logging
logger = logging.getLogger(__name__)

# BeautifulSoup and HTML parsing dependencies
def get_beautifulsoup() -> Tuple[bool, Optional[Any], str]:
    """
    Get BeautifulSoup and determine the best available parser.

    Returns:
        Tuple[bool, Optional[Any], str]: A tuple containing:
            - Boolean indicating if BeautifulSoup is available
            - BeautifulSoup module (or None if not available)
            - Parser to use ('lxml', 'html.parser', or '' if not available)
    """
    try:
        from bs4 import BeautifulSoup

        # Try to import lxml, but fall back to html.parser if not available
        try:
            import lxml
            parser = 'lxml'
        except ImportError:
            parser = 'html.parser'
            logger.warning("lxml parser not found, falling back to html.parser. For best results, install lxml with: pip install lxml")

        return True, BeautifulSoup, parser
    except ImportError:
        logger.warning("\n--- WARNING ---\n"
                      "BeautifulSoup is not installed on your system. "
                      "\nIt is safer to use this script with BeautifulSoup "
                      "installed.\nYou can install BeautifulSoup using this "
                      "command:\n\n\t\t pip install beautifulsoup4\n\n")
        return False, None, ''

# Initialize BeautifulSoup once at module import time
BEAUTIFULSOUP_AVAILABLE, BeautifulSoup, BS_PARSER = get_beautifulsoup()

# Other optional dependencies can be added here in a similar pattern

# Database dependencies
def get_psycopg2() -> Tuple[bool, Optional[Any]]:
    """
    Get psycopg2 for PostgreSQL database operations.

    Returns:
        Tuple[bool, Optional[Any]]: A tuple containing:
            - Boolean indicating if psycopg2 is available
            - psycopg2 module (or None if not available)
    """
    try:
        import psycopg2
        return True, psycopg2
    except ImportError:
        logger.warning("psycopg2 is not installed. Database operations will be disabled. "
                      "Install with: pip install psycopg2-binary")
        return False, None

# Initialize database dependencies
PSYCOPG2_AVAILABLE, psycopg2 = get_psycopg2()

# Utility functions for checking dependencies
def check_dependency(dependency_name: str) -> bool:
    """
    Check if a dependency is available.

    Args:
        dependency_name (str): Name of the dependency to check

    Returns:
        bool: True if the dependency is available, False otherwise
    """
    if dependency_name.lower() == 'beautifulsoup':
        return BEAUTIFULSOUP_AVAILABLE
    elif dependency_name.lower() == 'psycopg2':
        return PSYCOPG2_AVAILABLE
    else:
        logger.warning(f"Unknown dependency: {dependency_name}")
        return False

def require_dependency(dependency_name: str) -> None:
    """
    Require a dependency to be available. Exits if not available.

    Args:
        dependency_name (str): Name of the dependency to require
    """
    if not check_dependency(dependency_name):
        logger.error(f"Required dependency {dependency_name} is not available. "
                    f"Please install it before continuing.")
        sys.exit(1)