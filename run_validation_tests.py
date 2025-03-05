#!/usr/bin/env python3
"""
Script to run ETL validation tests.

This script runs both unit and integration tests for the ETL validation utilities.
"""

import os
import sys
import unittest
import argparse
import logging
from pathlib import Path

# Add the project root to the Python path
sys.path.insert(0, str(Path(__file__).resolve().parent))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("validation_tests.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def discover_and_run_tests(test_type=None, verbose=False):
    """
    Discover and run tests based on the specified type.

    Args:
        test_type: Type of tests to run ('unit', 'integration', or None for all)
        verbose: Whether to show verbose output

    Returns:
        bool: True if all tests passed, False otherwise
    """
    # Determine test directory based on test type
    if test_type == 'unit':
        test_dirs = ['tests/unit/test_etl_validation.py']
    elif test_type == 'integration':
        test_dirs = ['tests/integration/test_etl_validation_integration.py']
    else:
        test_dirs = [
            'tests/unit/test_etl_validation.py',
            'tests/integration/test_etl_validation_integration.py'
        ]

    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # Add tests to suite
    for test_dir in test_dirs:
        if os.path.exists(test_dir):
            logger.info(f"Discovering tests in {test_dir}")
            if os.path.isfile(test_dir):
                # If it's a file, load tests from that file
                discovered_tests = loader.loadTestsFromName(test_dir.replace('/', '.').replace('.py', ''))
            else:
                # If it's a directory, discover tests in that directory
                discovered_tests = loader.discover(test_dir, pattern='test_*.py')
            suite.addTests(discovered_tests)
        else:
            logger.warning(f"Test directory/file not found: {test_dir}")

    # Run tests
    verbosity = 2 if verbose else 1
    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)

    # Return True if all tests passed
    return result.wasSuccessful()

def main():
    """Main function to parse arguments and run tests."""
    parser = argparse.ArgumentParser(description='Run ETL validation tests')
    parser.add_argument('--type', choices=['unit', 'integration', 'all'], default='all',
                        help='Type of tests to run (default: all)')
    parser.add_argument('--verbose', '-v', action='store_true',
                        help='Show verbose output')

    args = parser.parse_args()

    # Map 'all' to None for discover_and_run_tests
    test_type = args.type if args.type != 'all' else None

    logger.info(f"Running {args.type} tests for ETL validation")
    success = discover_and_run_tests(test_type, args.verbose)

    if success:
        logger.info("All tests passed successfully")
        sys.exit(0)
    else:
        logger.error("Some tests failed")
        sys.exit(1)

if __name__ == "__main__":
    main()