#!/usr/bin/env python3
"""
Logging Test Script

This script runs the tests for the centralized logging system.
It has been renamed from test_logging.py to logging_test.py to avoid
namespace conflicts with the actual test module in tests/utils/test_logging.py.
"""

import argparse
import os
import sys
import unittest
from pathlib import Path

# Add the parent directory to the path so we can import the modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.utils.logging_config import configure_logging


def run_tests(verbose=False, pattern="test_*.py"):
    """
    Run the logging tests.

    Args:
        verbose: Whether to run tests in verbose mode
        pattern: Pattern to match test files
    """
    # Configure logging for tests
    configure_logging(
        app_name="skype_parser_tests",
        environment="testing",
        log_file="logging_tests.log",
        enable_json=True,
    )

    # Discover and run tests
    loader = unittest.TestLoader()

    # Get the test directory
    test_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "tests")

    # Discover tests
    if pattern == "test_logging.py":
        # Run only logging tests
        test_suite = loader.discover(
            os.path.join(test_dir, "unit"), pattern="test_structured_logging.py"
        )
    else:
        # Run all tests
        test_suite = loader.discover(test_dir, pattern=pattern)

    # Run tests
    runner = unittest.TextTestRunner(verbosity=2 if verbose else 1)
    result = runner.run(test_suite)

    # Return exit code
    return 0 if result.wasSuccessful() else 1


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Run logging tests")
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Run tests in verbose mode"
    )
    parser.add_argument(
        "-p", "--pattern", default="test_*.py", help="Pattern to match test files"
    )

    args = parser.parse_args()

    # Run tests
    sys.exit(run_tests(args.verbose, args.pattern))


if __name__ == "__main__":
    main()
