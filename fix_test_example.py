#!/usr/bin/env python3
"""
Script to fix the refactored_test_example.py file.

This script makes targeted improvements to the test file:
1. Adding proper test file creation and cleanup
2. Updating the pipeline creation with proper dependencies
3. Ensuring imports are correct
"""

import os
import re
import sys
import ast
import difflib
import argparse
import shutil
from typing import Dict, Any, Optional, List, Tuple, Set, Union

# Define the path to the test file
TEST_FILE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "tests/examples/refactored_test_example.py"
)

def validate_python_syntax(file_path: str, content: str) -> bool:
    """
    Validate that the content is valid Python syntax.

    Args:
        file_path: Path to the file being validated
        content: Python code content to validate

    Returns:
        True if the syntax is valid, False otherwise
    """
    try:
        ast.parse(content)
        return True
    except SyntaxError as e:
        print(f"ERROR: Syntax error in {file_path} after modification:")
        print(f"  Line {e.lineno}, column {e.offset}: {e.text}")
        print(f"  {e}")
        return False

def backup_file(file_path: str) -> None:
    """
    Create a backup of a file.

    Args:
        file_path: Path to the file to backup
    """
    backup_path = f"{file_path}.bak"
    print(f"Creating backup of {file_path} to {backup_path}")
    shutil.copy2(file_path, backup_path)

def show_diff(original: str, modified: str, file_path: str) -> None:
    """
    Show the diff between original and modified content.

    Args:
        original: Original content
        modified: Modified content
        file_path: Path to the file being modified
    """
    print(f"\nDiff for {file_path}:")
    diff = difflib.unified_diff(
        original.splitlines(),
        modified.splitlines(),
        fromfile=f"{file_path} (original)",
        tofile=f"{file_path} (modified)",
        lineterm=""
    )
    diff_text = "\n".join(diff)
    if diff_text:
        print(diff_text)
    else:
        print("No changes.")

def fix_imports(content: str) -> str:
    """
    Fix imports in the test file.

    Args:
        content: The file content to update

    Returns:
        The updated content
    """
    print("Fixing imports in the test file")

    # Make sure json and os are imported
    if "import json" not in content:
        content = re.sub(
            r'(import unittest\s*\n)',
            r'\1import json\n',
            content,
            flags=re.DOTALL
        )

    if "import os" not in content:
        content = re.sub(
            r'(import json\s*\n)',
            r'\1import os\n',
            content,
            flags=re.DOTALL
        )

    # Fix ETLPipeline import to use the correct path
    content = re.sub(
        r'from src\.db\.etl import ETLPipeline',
        r'from src.db.etl.pipeline_manager import ETLPipeline',
        content,
        flags=re.DOTALL
    )

    return content

def add_setup_and_teardown(content: str) -> str:
    """
    Add or update setUp and tearDown methods to create and clean up test files.

    Args:
        content: The file content to update

    Returns:
        The updated content
    """
    print("Adding or updating setUp and tearDown methods")

    # Check if the file already has setUp/tearDown
    has_setup = "def setUp" in content
    has_teardown = "def tearDown" in content
    creates_test_file = "json.dump" in content and "self.test_file_path" in content

    # If all is in place, no need to update
    if has_setup and has_teardown and creates_test_file:
        print("Test file already has proper setUp and tearDown methods")
        return content

    modified_content = content

    # Define the improved setUp method
    setup_method = """    def setUp(self):
        \"\"\"
        Set up test fixtures before each test method.
        \"\"\"
        # Setup for both original and refactored tests
        self.test_file_path = 'test.json'
        self.test_user_display_name = 'Test User'
        self.test_data = BASIC_SKYPE_DATA

        # Add a minimal db_config for tests
        self.test_db_config = {
            'host': 'localhost',
            'port': 5432,
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password'
        }

        # Create a test file
        with open(self.test_file_path, 'w') as f:
            json.dump(self.test_data, f)
    """

    # Define the tearDown method
    teardown_method = """    def tearDown(self):
        \"\"\"
        Clean up after each test method.
        \"\"\"
        # Remove the test file
        if os.path.exists(self.test_file_path):
            os.remove(self.test_file_path)
    """

    # If setUp exists but doesn't create test file, replace it
    if has_setup:
        modified_content = re.sub(
            r'(\s+)def setUp\s*\(\s*self\s*\)\s*:.*?(def|$)',
            r'\1' + setup_method + r'\n\1\2',
            modified_content,
            flags=re.DOTALL
        )
    else:
        # If no setUp, add it after the class declaration
        modified_content = re.sub(
            r'(class TestRefactoringExample.*?:.*?\n)(\s+)',
            r'\1\2' + setup_method + r'\n\2',
            modified_content,
            flags=re.DOTALL
        )

    # If tearDown doesn't exist, add it after setUp
    if not has_teardown:
        modified_content = re.sub(
            r'(def setUp.*?json\.dump.*?\n\s+})(\s+)(def|$)',
            r'\1\2' + teardown_method + r'\n\2\3',
            modified_content,
            flags=re.DOTALL
        )

    return modified_content

def update_pipeline_creation(content: str) -> str:
    """
    Update the pipeline creation in the test to include all necessary dependencies.

    Args:
        content: The file content to update

    Returns:
        The updated content
    """
    print("Updating pipeline creation in the test")

    # Check if the test already has the improved pipeline creation
    if "mock_content_extractor" in content and "mock_structured_data_extractor" in content:
        print("Test already has the improved pipeline creation")
        return content

    # Define the pattern to match the pipeline creation
    pattern = r'(# Create testable pipeline.*?\n\s+)pipeline\s*=\s*TestableETLPipeline\s*\(.*?db_connection\s*=\s*mock_db\.conn\s*\)'

    # Check if we find the pattern
    match = re.search(pattern, content, flags=re.DOTALL)
    if not match:
        print("Warning: Could not find pipeline creation pattern in the test file")
        return content

    # Define the improved pipeline creation
    pipeline_creation = """# Create mock content extractor
        mock_content_extractor = MagicMock()
        mock_content_extractor.extract_mentions.return_value = []

        # Create mock structured data extractor
        mock_structured_data_extractor = MagicMock()
        mock_structured_data_extractor.extract_structured_data.return_value = {}

        # Create mock message handler factory
        mock_message_handler_factory = MagicMock()
        mock_message_handler_factory.get_handler.return_value = MagicMock()

        # Create testable pipeline with all dependencies injected
        pipeline = TestableETLPipeline(
            # Provide a valid db_config to pass validation
            db_config=self.test_db_config,
            use_di=False,  # Explicitly set to False
            # File operations
            read_file_func=mock_env['read_file'],
            # Validation functions
            validate_file_exists_func=mock_env['validate_file_exists'],
            validate_json_file_func=mock_env['validate_json_file'],
            validate_user_display_name_func=mock_validate_user_display_name,
            # Database connection
            db_connection=mock_db.conn,
            # Additional dependencies
            content_extractor=mock_content_extractor,
            structured_data_extractor=mock_structured_data_extractor,
            message_handler_factory=mock_message_handler_factory
        )"""

    # Replace the pipeline creation
    modified_content = re.sub(pattern, r'\1' + pipeline_creation, content, flags=re.DOTALL)

    return modified_content

def fix_test_file(file_path: str, dry_run: bool = False, verbose: bool = False) -> bool:
    """
    Fix the test file with a targeted approach.

    Args:
        file_path: Path to the file to fix
        dry_run: Whether to only show changes without modifying files
        verbose: Whether to show verbose output

    Returns:
        True if changes were made or would be made, False otherwise
    """
    print(f"Fixing test file {file_path}")

    # Read the file
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()
    except FileNotFoundError:
        print(f"Error: File not found: {file_path}")
        return False
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return False

    # Save the original content
    original_content = content

    # Apply all the targeted updates
    modified_content = fix_imports(content)
    modified_content = add_setup_and_teardown(modified_content)
    modified_content = update_pipeline_creation(modified_content)

    # Check if any changes were made
    if original_content == modified_content:
        print(f"No changes needed in {file_path}")
        return False

    # Show diff
    show_diff(original_content, modified_content, file_path)

    # If it's a dry run, don't modify the file
    if dry_run:
        print(f"Dry run: Not modifying {file_path}")
        return True

    # Validate syntax
    if not validate_python_syntax(file_path, modified_content):
        print(f"Error: Modified content has syntax errors, not updating {file_path}")
        return False

    # Backup file
    backup_file(file_path)

    # Write the file
    try:
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(modified_content)
        print(f"Updated {file_path}")
        return True
    except Exception as e:
        print(f"Error writing file {file_path}: {e}")
        return False

def main():
    """Main function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Fix the refactored_test_example.py file.")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without modifying files")
    parser.add_argument("--file", default=TEST_FILE_PATH, help=f"Path to the test file (default: {TEST_FILE_PATH})")
    parser.add_argument("--verbose", action="store_true", help="Show verbose output")
    args = parser.parse_args()

    print("Starting test file fix...")

    # Fix the test file
    success = fix_test_file(args.file, args.dry_run, args.verbose)

    if success:
        if args.dry_run:
            print("Dry run completed. No files were modified.")
        else:
            print("Test file fix completed!")
    else:
        print("No changes were made to the test file.")

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())