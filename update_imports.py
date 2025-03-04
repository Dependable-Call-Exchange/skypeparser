#!/usr/bin/env python3
"""
Script to update import statements in test files after reorganization.
"""

import os
import re

def update_file(file_path):
    """Update the import statements in a file."""
    with open(file_path, 'r') as f:
        content = f.read()

    # Update sys.path.insert statements with Path
    content = re.sub(
        r'sys\.path\.insert\(0, str\(Path\(__file__\)\.resolve\(\)\.parent\.parent\)\)',
        r'sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))',
        content
    )

    # Update sys.path.insert statements with os.path.abspath
    content = re.sub(
        r'sys\.path\.insert\(0, os\.path\.abspath\(os\.path\.join\(os\.path\.dirname\(__file__\), \'\.\.\'\)\)\)',
        r'sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), \'../..\')))',
        content
    )

    # Update test_helpers imports
    content = re.sub(
        r'from tests\.test_helpers import',
        r'from tests.fixtures import',
        content
    )

    with open(file_path, 'w') as f:
        f.write(content)

    print(f"Updated {file_path}")

def main():
    """Main function to update all test files."""
    unit_test_dir = 'tests/unit'
    integration_test_dir = 'tests/integration'

    # Update unit test files
    for filename in os.listdir(unit_test_dir):
        if filename.endswith('.py'):
            file_path = os.path.join(unit_test_dir, filename)
            update_file(file_path)

    # Update integration test files
    for filename in os.listdir(integration_test_dir):
        if filename.endswith('.py'):
            file_path = os.path.join(integration_test_dir, filename)
            update_file(file_path)

    print("All files updated successfully!")

if __name__ == '__main__':
    main()