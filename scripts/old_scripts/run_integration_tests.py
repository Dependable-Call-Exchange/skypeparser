#!/usr/bin/env python3
"""
Integration Test Runner for SkypeParser with Supabase

This script runs integration tests for the SkypeParser project against a Supabase database.
It loads environment variables from .env file and enables database testing.
"""

import os
import sys
import subprocess
import argparse
from pathlib import Path
from dotenv import load_dotenv

def main():
    """Run integration tests with Supabase database connection."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Run integration tests with Supabase database connection.')
    parser.add_argument('--test-type', choices=['integration', 'performance', 'all'],
                        default='integration', help='Type of tests to run')
    parser.add_argument('--test-file', help='Specific test file to run')
    parser.add_argument('--verbose', '-v', action='count', default=1,
                        help='Verbosity level (use -v, -vv, etc.)')
    parser.add_argument('--log-level', default='DEBUG',
                        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
                        help='Log level for pytest')
    args = parser.parse_args()

    # Load environment variables from .env file
    load_dotenv()

    # Ensure POSTGRES_TEST_DB is set to true
    os.environ['POSTGRES_TEST_DB'] = 'true'

    # Build pytest command
    cmd = ['python', '-m', 'pytest']

    # Add verbosity (default to -v)
    cmd.extend(['-' + 'v' * args.verbose])

    # Add log level
    cmd.extend(['--log-cli-level', args.log_level])

    # Add test path based on test type
    if args.test_file:
        cmd.append(args.test_file)
    elif args.test_type == 'integration':
        cmd.append('tests/integration/')
    elif args.test_type == 'performance':
        cmd.append('tests/performance/')
    else:  # all
        cmd.extend(['tests/integration/', 'tests/performance/'])

    # Print the command being run
    print(f"Running: {' '.join(cmd)}")

    # Run the tests
    result = subprocess.run(cmd)

    # Return the exit code from pytest
    return result.returncode

if __name__ == '__main__':
    sys.exit(main())