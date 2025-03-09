"""
Test utility functions.

This module provides utility functions for testing environments.
"""

import os
import sys

def is_test_environment() -> bool:
    """
    Detect if the code is running in a test environment.

    Returns:
        bool: True if running in a test environment, False otherwise
    """
    # Check if pytest is running
    return "pytest" in sys.modules or 'PYTEST_CURRENT_TEST' in os.environ

def get_fast_test_mode() -> bool:
    """
    Check if fast test mode is enabled.

    Returns:
        bool: True if fast test mode is enabled, False otherwise
    """
    return os.environ.get("FAST_TEST_MODE", "").lower() in ("1", "true", "yes", "on")