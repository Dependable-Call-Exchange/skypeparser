#!/usr/bin/env python3
"""
Script to fix the TestableETLPipeline class.

This script makes targeted improvements to the TestableETLPipeline class:
1. Updating constructor parameters to support additional dependencies
2. Fixing the validation override using patch instead of method replacement
3. Improving the DI registration to properly handle injected dependencies
"""

import os
import re
import sys
import ast
import difflib
import argparse
import shutil
from typing import Dict, Any, Optional, List, Tuple, Set, Union

# Define the path to the TestableETLPipeline class
TESTABLE_ETL_PIPELINE_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "src/db/testable_etl_pipeline.py"
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

def update_constructor_params(content: str) -> str:
    """
    Update the constructor parameters to support additional dependencies.

    Args:
        content: The file content to update

    Returns:
        The updated content
    """
    print("Updating constructor parameters to support additional dependencies")

    # Define the pattern to match the constructor
    pattern = r'(def __init__\s*\(\s*self\s*,.*?db_connection\s*:.*?=\s*None)(.*?\).*?:)'

    # Check if the constructor already has the additional parameters
    if re.search(r'content_extractor\s*:', content):
        print("Constructor already has content_extractor parameter")
        return content

    # Define the replacement for the constructor parameters
    replacement = r'\1,\n        content_extractor: Optional[ContentExtractorProtocol] = None,\n        structured_data_extractor: Optional[StructuredDataExtractorProtocol] = None,\n        message_handler_factory: Optional[MessageHandlerFactoryProtocol] = None\2'

    # Replace the constructor parameters
    modified_content = re.sub(pattern, replacement, content, flags=re.DOTALL)

    return modified_content

def update_constructor_body(content: str) -> str:
    """
    Update the constructor body to properly initialize additional dependencies.

    Args:
        content: The file content to update

    Returns:
        The updated content
    """
    print("Updating constructor body to properly initialize additional dependencies")

    # Define the pattern to match the constructor body
    pattern = r'(# Create content extractor\s*\n\s*self\.content_extractor\s*=\s*)ContentExtractor\(\)'

    # Define the replacement for the constructor body
    replacement = r'\1content_extractor or ContentExtractor()'

    # Replace the content extractor initialization
    modified_content = re.sub(pattern, replacement, content, flags=re.DOTALL)

    # Define the pattern to match the message handler factory initialization
    pattern = r'(# Create message handler factory\s*\n\s*self\.message_handler_factory\s*=\s*)SkypeMessageHandlerFactory\(\)'

    # Define the replacement for the message handler factory initialization
    replacement = r'\1message_handler_factory or SkypeMessageHandlerFactory()'

    # Replace the message handler factory initialization
    modified_content = re.sub(pattern, replacement, modified_content, flags=re.DOTALL)

    # Define the pattern to match the structured data extractor initialization
    pattern = r'(# Create structured data extractor\s*\n\s*self\.structured_data_extractor\s*=\s*)StructuredDataExtractor\(\)'

    # Define the replacement for the structured data extractor initialization
    replacement = r'\1structured_data_extractor or StructuredDataExtractor()'

    # Replace the structured data extractor initialization
    modified_content = re.sub(pattern, replacement, modified_content, flags=re.DOTALL)

    return modified_content

def update_run_pipeline_method(content: str) -> str:
    """
    Update the run_pipeline method to use patch instead of method replacement.

    Args:
        content: The file content to update

    Returns:
        The updated content
    """
    print("Updating run_pipeline method to use patch instead of method replacement")

    # Check if the file already imports patch
    if 'from unittest.mock import patch' not in content:
        # Add the import statement
        content = re.sub(
            r'(from typing import .*?\n)',
            r'\1from unittest.mock import patch\n',
            content,
            flags=re.DOTALL
        )

    # Define the pattern to match the run_pipeline method
    pattern = r'(def run_pipeline.*?)(""".*?""").*?(return self\.pipeline\.run_pipeline)'

    # Define the replacement for the run_pipeline method
    replacement = r'\1\2\n        # Use a patch context manager to override os.path.exists and os.path.isfile\n        with patch(\'os.path.exists\', return_value=True), patch(\'os.path.isfile\', return_value=True):\n            \3'

    # Replace the run_pipeline method
    modified_content = re.sub(pattern, replacement, content, flags=re.DOTALL)

    return modified_content

def update_extract_method(content: str) -> str:
    """
    Update the extract method to also use patch.

    Args:
        content: The file content to update

    Returns:
        The updated content
    """
    print("Updating extract method to also use patch")

    # Define the pattern to match the extract method
    pattern = r'(def extract.*?)(""".*?""").*?(return self\.pipeline\.extractor\.extract)'

    # Define the replacement for the extract method
    replacement = r'\1\2\n        # Use a patch context manager to override os.path.exists\n        with patch(\'os.path.exists\', return_value=True), patch(\'os.path.isfile\', return_value=True):\n            \3'

    # Replace the extract method
    modified_content = re.sub(pattern, replacement, content, flags=re.DOTALL)

    return modified_content

def update_di_registration(content: str) -> str:
    """
    Update the DI registration to properly handle injected dependencies.

    Args:
        content: The file content to update

    Returns:
        The updated content
    """
    print("Updating DI registration to properly handle injected dependencies")

    # Define the pattern to match the DI registration
    pattern = r'(if self\.use_di:.*?# Register the context we created\s*\n\s*provider\.register_singleton\(ETLContext, self\.context\)\s*\n)(.*?# Register ETL services with our context)'

    # Define the additional registrations
    additional_registrations = """            # Register our injected dependencies if provided
            if content_extractor:
                provider.register_singleton(ContentExtractorProtocol, content_extractor)
            if structured_data_extractor:
                provider.register_singleton(StructuredDataExtractorProtocol, structured_data_extractor)
            if message_handler_factory:
                provider.register_singleton(MessageHandlerFactoryProtocol, message_handler_factory)
            if db_connection:
                provider.register_singleton(DatabaseConnectionProtocol, db_connection)

            """

    # Replace the DI registration
    modified_content = re.sub(pattern, r'\1' + additional_registrations + r'\2', content, flags=re.DOTALL)

    return modified_content

def update_transformer_init(content: str) -> str:
    """
    Update the transformer initialization in the non-DI case.

    Args:
        content: The file content to update

    Returns:
        The updated content
    """
    print("Updating transformer initialization in the non-DI case")

    # Define the pattern to match the transformer initialization
    pattern = r'(self\.pipeline\.transformer\s*=\s*Transformer\s*\(\s*context\s*=\s*self\.context\s*\))'

    # Define the replacement for the transformer initialization
    replacement = r'self.pipeline.transformer = Transformer(\n                context=self.context,\n                content_extractor=self.content_extractor,\n                message_handler_factory=self.message_handler_factory,\n                structured_data_extractor=self.structured_data_extractor\n            )'

    # Replace the transformer initialization
    modified_content = re.sub(pattern, replacement, content, flags=re.DOTALL)

    return modified_content

def fix_testable_etl_pipeline(file_path: str, dry_run: bool = False, verbose: bool = False) -> bool:
    """
    Fix the TestableETLPipeline class with a targeted approach.

    Args:
        file_path: Path to the file to fix
        dry_run: Whether to only show changes without modifying files
        verbose: Whether to show verbose output

    Returns:
        True if changes were made or would be made, False otherwise
    """
    print(f"Fixing TestableETLPipeline class in {file_path}")

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
    modified_content = update_constructor_params(content)
    modified_content = update_constructor_body(modified_content)
    modified_content = update_run_pipeline_method(modified_content)
    modified_content = update_extract_method(modified_content)
    modified_content = update_di_registration(modified_content)
    modified_content = update_transformer_init(modified_content)

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
    parser = argparse.ArgumentParser(description="Fix the TestableETLPipeline class.")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without modifying files")
    parser.add_argument("--file", default=TESTABLE_ETL_PIPELINE_PATH, help=f"Path to the TestableETLPipeline file (default: {TESTABLE_ETL_PIPELINE_PATH})")
    parser.add_argument("--verbose", action="store_true", help="Show verbose output")
    args = parser.parse_args()

    print("Starting TestableETLPipeline fix...")

    # Fix the TestableETLPipeline class
    success = fix_testable_etl_pipeline(args.file, args.dry_run, args.verbose)

    if success:
        if args.dry_run:
            print("Dry run completed. No files were modified.")
        else:
            print("TestableETLPipeline fix completed!")
    else:
        print("No changes were made to the TestableETLPipeline class.")

    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())