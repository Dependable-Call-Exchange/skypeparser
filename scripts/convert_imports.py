#!/usr/bin/env python3
"""
Script to convert relative imports to absolute imports in Python files.

This script traverses a directory tree, identifies Python files, and converts
relative imports (using dots) to absolute imports.

Examples of conversions:
- from . import module -> from src.current.package import module
- from .module import func -> from src.current.package.module import func
- from ..module import func -> from src.parent.package.module import func
- from ... import module -> from src import module (if src is 3 levels up)

Usage:
    python convert_imports.py [source_dir]

Where:
    source_dir - The directory to process (default: src)
"""

import os
import re
import sys
import ast
from pathlib import Path
import logging
from typing import Dict, List, Optional, Set, Tuple, Union

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Regex patterns for import statements and comments
IMPORT_PATTERN = re.compile(r'^(\s*from\s+\.+\S*\s+import\s+.*)$|^(\s*import\s+\.+\S*)$', re.MULTILINE)
COMMENT_PATTERN = re.compile(r'#.*$')

# Root package name
ROOT_PACKAGE = "src"


def get_absolute_module_from_relative(
    relative_import: str,
    current_file_path: Path,
    source_root: Path
) -> str:
    """
    Convert a relative import path to an absolute one based on the current file's location.

    Args:
        relative_import: The relative import string (with dots)
        current_file_path: Path to the file containing the import
        source_root: Path to the source root directory

    Returns:
        The absolute import path as a string
    """
    # Count leading dots to determine how many directories to go up
    dot_count = 0
    for char in relative_import:
        if char == '.':
            dot_count += 1
        else:
            break

    # Remove the dots from the import
    module_path = relative_import[dot_count:]

    # Get the current file's package path relative to the source root
    rel_path = os.path.relpath(current_file_path.parent, source_root)
    if rel_path == '.':
        rel_path = ''
    rel_parts = rel_path.split(os.sep) if rel_path else []

    # Go up the directory tree based on dot count
    package_parts = rel_parts
    if dot_count > len(package_parts):
        logger.warning(f"Import {relative_import} in {current_file_path} goes beyond source root")
        # Use whatever parts we can
        package_parts = []
    else:
        package_parts = package_parts[:-dot_count] if dot_count > 0 else package_parts

    # Build the absolute import path
    if module_path:
        if package_parts:
            return f"{ROOT_PACKAGE}.{'.'.join(package_parts)}.{module_path}"
        else:
            return f"{ROOT_PACKAGE}.{module_path}"
    else:
        if package_parts:
            return f"{ROOT_PACKAGE}.{'.'.join(package_parts)}"
        else:
            return ROOT_PACKAGE


def convert_file(file_path: Path, source_root: Path) -> Tuple[int, List[str], List[str]]:
    """
    Process a single file and convert its relative imports to absolute imports.

    Args:
        file_path: Path to the file to process
        source_root: Path to the source root directory

    Returns:
        Tuple of (number of imports changed, list of original imports, list of converted imports)
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Parse the file with AST to properly identify imports
        try:
            parsed = ast.parse(content)
        except SyntaxError as e:
            logger.error(f"Syntax error in {file_path}: {e}")
            return 0, [], []

        imports_changed = 0
        original_imports = []
        converted_imports = []

        # Create a new content string as we process
        new_content = content

        # Find all import statements in the AST
        for node in ast.walk(parsed):
            if isinstance(node, ast.ImportFrom) and node.level > 0:  # Level > 0 means relative import
                # Extract the original import statement from the source
                import_start = node.lineno - 1  # AST line numbers are 1-indexed
                import_text = content.splitlines()[import_start]

                # Handle multiline imports
                if '(' in import_text and ')' not in import_text:
                    # Find the closing parenthesis
                    for i in range(import_start + 1, len(content.splitlines())):
                        import_text += '\n' + content.splitlines()[i]
                        if ')' in content.splitlines()[i]:
                            break

                # Store original import
                original_imports.append(import_text)

                # Build the absolute module path
                module_prefix = '.' * node.level
                module_name = node.module or ''
                relative_import = f"{module_prefix}{module_name}"
                absolute_module = get_absolute_module_from_relative(
                    relative_import, file_path, source_root
                )

                # Create the new import statement
                names_str = ', '.join(name.name + (' as ' + name.asname if name.asname else '')
                                      for name in node.names)
                new_import = f"from {absolute_module} import {names_str}"

                # Replace the import in the content
                # Escape special regex characters in the original import
                escaped_import = re.escape(import_text)
                # Handle the case of multiline imports by using re.DOTALL
                new_content = re.sub(
                    escaped_import,
                    new_import,
                    new_content,
                    count=1,
                    flags=re.DOTALL
                )

                # Store converted import
                converted_imports.append(new_import)
                imports_changed += 1

        # Only write the file if changes were made
        if imports_changed > 0:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(new_content)

        return imports_changed, original_imports, converted_imports

    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        return 0, [], []


def convert_all_files(directory: Path, source_root: Path) -> Dict[str, Dict]:
    """
    Process all Python files in a directory tree.

    Args:
        directory: Path to the directory to process
        source_root: Path to the source root directory

    Returns:
        Dictionary with results of the conversion
    """
    results = {
        'total_files': 0,
        'files_changed': 0,
        'total_imports_changed': 0,
        'changed_files': {}
    }

    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = Path(root) / file
                # Use os.path.relpath instead of Path.relative_to to avoid errors
                rel_path = os.path.relpath(file_path, Path.cwd())

                imports_changed, original_imports, converted_imports = convert_file(
                    file_path, source_root
                )

                results['total_files'] += 1

                if imports_changed > 0:
                    results['files_changed'] += 1
                    results['total_imports_changed'] += imports_changed
                    results['changed_files'][rel_path] = {
                        'imports_changed': imports_changed,
                        'original_imports': original_imports,
                        'converted_imports': converted_imports
                    }
                    logger.info(f"Converted {imports_changed} imports in {rel_path}")

    return results


def create_backup(directory: Path) -> Path:
    """
    Create a backup of the directory before making changes.

    Args:
        directory: Path to the directory to backup

    Returns:
        Path to the backup directory
    """
    import shutil
    from datetime import datetime

    # Create a backup directory with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = directory.parent / f"{directory.name}_backup_{timestamp}"

    logger.info(f"Creating backup of {directory} to {backup_dir}")
    shutil.copytree(directory, backup_dir)

    return backup_dir


def main():
    """
    Main entry point for the script.
    """
    # Get the source directory from command line arguments or use default
    if len(sys.argv) > 1:
        source_dir = Path(sys.argv[1])
    else:
        source_dir = Path('src')

    if not source_dir.exists() or not source_dir.is_dir():
        logger.error(f"Directory {source_dir} does not exist or is not a directory")
        sys.exit(1)

    # Create a backup before making changes
    backup_dir = create_backup(source_dir)
    logger.info(f"Backup created at {backup_dir}")

    # Process all files in the directory
    logger.info(f"Converting relative imports in {source_dir}...")
    results = convert_all_files(source_dir, source_dir)

    # Print summary
    logger.info("Conversion complete!")
    logger.info(f"Total files processed: {results['total_files']}")
    logger.info(f"Files changed: {results['files_changed']}")
    logger.info(f"Total imports changed: {results['total_imports_changed']}")

    # Detailed report of changes
    if results['files_changed'] > 0:
        logger.info("\nFiles changed:")
        for file_path, details in results['changed_files'].items():
            logger.info(f"\n  {file_path} ({details['imports_changed']} imports)")
            for i in range(len(details['original_imports'])):
                logger.info(f"    {details['original_imports'][i]} -> {details['converted_imports'][i]}")


if __name__ == "__main__":
    main()