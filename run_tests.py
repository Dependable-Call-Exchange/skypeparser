#!/usr/bin/env python3
"""
Test Runner for SkypeParser

This script runs all the tests for the SkypeParser project.
It can be used to run specific test modules or all tests.
"""

import os
import sys
import unittest
import argparse
from pathlib import Path


def discover_tests(test_dir=None, pattern=None):
    """
    Discover and load tests from the specified directory.

    Args:
        test_dir (str): Directory containing tests (default: 'tests')
        pattern (str): Pattern to match test files (default: 'test_*.py')

    Returns:
        unittest.TestSuite: Test suite containing all discovered tests
    """
    if test_dir is None:
        test_dir = 'tests'

    if pattern is None:
        pattern = 'test_*.py'

    return unittest.defaultTestLoader.discover(test_dir, pattern=pattern)


def run_specific_tests(test_names):
    """
    Run specific test modules.

    Args:
        test_names (list): List of test module names to run

    Returns:
        int: Number of test failures
    """
    test_suite = unittest.TestSuite()

    for test_name in test_names:
        # If the test name doesn't start with 'test_', add it
        if not test_name.startswith('test_'):
            test_name = f'test_{test_name}'

        # If the test name doesn't end with '.py', add it
        if not test_name.endswith('.py'):
            test_name = f'{test_name}.py'

        # Create the full module name
        module_name = f'tests.{test_name[:-3]}'

        try:
            # Import the module
            __import__(module_name)
            module = sys.modules[module_name]

            # Add all tests from the module to the suite
            tests = unittest.defaultTestLoader.loadTestsFromModule(module)
            test_suite.addTest(tests)
        except ImportError:
            print(f"Error: Could not import test module '{module_name}'")
            return 1

    # Run the tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    return len(result.failures) + len(result.errors)


def run_all_tests():
    """
    Run all tests in the 'tests' directory.

    Returns:
        int: Number of test failures
    """
    test_suite = discover_tests()
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)

    return len(result.failures) + len(result.errors)


def main():
    """Main function to parse arguments and run tests."""
    parser = argparse.ArgumentParser(description='Run tests for SkypeParser')
    parser.add_argument('--module', '-m', nargs='+', help='Specific test modules to run')
    parser.add_argument('--verbose', '-v', action='store_true', help='Increase output verbosity')

    args = parser.parse_args()

    # Set verbosity level
    if args.verbose:
        unittest.TestCase.maxDiff = None

    # Add the project root to the Python path
    project_root = Path(__file__).resolve().parent
    sys.path.insert(0, str(project_root))

    # Run tests
    if args.module:
        failures = run_specific_tests(args.module)
    else:
        failures = run_all_tests()

    # Return non-zero exit code if there were failures
    return failures


if __name__ == '__main__':
    sys.exit(main())