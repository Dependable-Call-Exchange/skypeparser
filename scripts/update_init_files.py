#!/usr/bin/env python3
"""
Script to update __init__.py files to properly export from canonical locations.

This script:
1. Identifies the canonical modules and their exports
2. Updates __init__.py files in domain-specific directories
3. Ensures backward compatibility by re-exporting from legacy locations

Usage:
    python update_init_files.py

The script works in conjunction with implement_reorganization.py to ensure
a consistent codebase structure.
"""

import os
import re
import ast
import sys
import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple, Optional

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
PROJECT_ROOT = Path(__file__).parent.parent
SRC_DIR = PROJECT_ROOT / "src"
MAPPING_FILE = PROJECT_ROOT / "reorganization_mapping.md"

# Regular expressions for parsing the mapping file
TABLE_PATTERN = r'\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|\s*[^|]+\s*\|'
SECTION_PATTERN = r'### (.+)'

def parse_mapping_file() -> Dict[str, Tuple[str, str]]:
    """
    Parse the reorganization mapping file to extract file mappings.

    Returns:
        Dict mapping duplicate files to (canonical location, category) tuples
    """
    if not MAPPING_FILE.exists():
        logger.error(f"Mapping file not found at {MAPPING_FILE}")
        sys.exit(1)

    mapping = {}

    with open(MAPPING_FILE, 'r') as f:
        content = f.read()

    # Find all sections
    sections = re.findall(SECTION_PATTERN, content)

    # Process each section
    for section in sections:
        section_start = content.find(f"### {section}")
        section_end = content.find("###", section_start + 1)
        if section_end == -1:
            section_end = len(content)

        section_content = content[section_start:section_end]

        # Extract file mappings from tables in this section
        if section != "Core Utilities (To be kept in core_utils)":
            file_mappings = re.findall(TABLE_PATTERN, section_content)
            for duplicate, canonical in file_mappings:
                duplicate = duplicate.strip()
                canonical = canonical.strip()
                if duplicate and canonical and duplicate != "Duplicate File":
                    mapping[duplicate] = (canonical, section)

    return mapping

def extract_exports_from_file(file_path: Path) -> List[str]:
    """
    Extract class and function definitions from a Python file.

    Args:
        file_path: Path to the Python file

    Returns:
        List of exported names (classes and functions)
    """
    try:
        with open(file_path, 'r') as f:
            content = f.read()

        # Parse the file with ast
        tree = ast.parse(content)

        # Extract class and function definitions
        exports = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ClassDef):
                exports.append(node.name)
            elif isinstance(node, ast.FunctionDef):
                # Exclude private functions
                if not node.name.startswith('_'):
                    exports.append(node.name)

        return exports
    except Exception as e:
        logger.error(f"Error extracting exports from {file_path}: {e}")
        return []

def group_files_by_directory(mapping: Dict[str, Tuple[str, str]]) -> Dict[str, List[Tuple[str, str]]]:
    """
    Group files by their canonical directory.

    Args:
        mapping: Dict mapping duplicate files to (canonical location, category) tuples

    Returns:
        Dict mapping directories to lists of (file_name, full_path) tuples
    """
    directory_mapping = {}

    for duplicate, (canonical, category) in mapping.items():
        canonical_path = Path(canonical)
        directory = str(canonical_path.parent)
        file_name = canonical_path.name

        if directory not in directory_mapping:
            directory_mapping[directory] = []

        directory_mapping[directory].append((file_name, canonical))

    return directory_mapping

def update_init_file(directory: str, files: List[Tuple[str, str]]) -> bool:
    """
    Update or create an __init__.py file in the given directory.

    Args:
        directory: Directory to update
        files: List of (file_name, full_path) tuples

    Returns:
        True if successful, False otherwise
    """
    try:
        init_path = Path(PROJECT_ROOT / directory) / "__init__.py"
        exports_by_file = {}

        # Collect exports from each file
        for file_name, full_path in files:
            module_name = Path(file_name).stem
            file_path = Path(PROJECT_ROOT / full_path)

            if file_path.exists():
                exports = extract_exports_from_file(file_path)
                if exports:
                    exports_by_file[module_name] = exports

        # Generate the new __init__.py content
        imports = []
        exports = []

        for module_name, module_exports in exports_by_file.items():
            # Add relative import
            imports.append(f"from .{module_name} import {', '.join(module_exports)}")
            # Add to __all__ list
            exports.extend(module_exports)

        # Create the __init__.py file content
        module_docstring = f'"""\n{directory.split("/")[-1]} package.\n\nThis package contains {directory.split("/")[-1]}-related modules.\n"""'
        all_exports = f"__all__ = {repr(exports)}"

        content = f"{module_docstring}\n\n{all_exports}\n\n" + "\n".join(imports) + "\n"

        # Write the __init__.py file
        os.makedirs(os.path.dirname(init_path), exist_ok=True)
        with open(init_path, 'w') as f:
            f.write(content)

        logger.info(f"Updated {init_path}")
        return True
    except Exception as e:
        logger.error(f"Error updating {directory}/__init__.py: {e}")
        return False

def update_old_init_file(directory: str, mapping: Dict[str, Tuple[str, str]]) -> bool:
    """
    Update an old __init__.py file to re-export from canonical locations.

    Args:
        directory: Directory to update
        mapping: Dict mapping duplicate files to (canonical location, category) tuples

    Returns:
        True if successful, False otherwise
    """
    try:
        init_path = Path(PROJECT_ROOT / directory) / "__init__.py"
        if not init_path.exists():
            return True  # No old __init__.py to update

        # Read the old __init__.py
        with open(init_path, 'r') as f:
            content = f.read()

        # Collect files in this directory that have been moved
        dir_path = Path(directory)
        moved_files = []
        re_exports = []

        for old_file, (new_file, category) in mapping.items():
            old_path = Path(old_file)
            if old_path.parent == dir_path:
                moved_files.append((old_path.stem, Path(new_file)))

        # Add re-exports for moved files
        for module_name, new_path in moved_files:
            new_import = f"from {new_path.parent.as_posix().replace('/', '.')} import {new_path.stem}"
            re_exports.append(f"# Re-export from canonical location\n{new_import}")

        if re_exports:
            # Add deprecation warning to the docstring
            if '"""' in content:
                end_doc = content.find('"""', content.find('"""') + 3) + 3
                warning = '\n\nWARNING: This module is being reorganized. Imports from here may be deprecated in the future.'
                content = content[:end_doc] + warning + content[end_doc:]
            else:
                warning = '"""\nWARNING: This module is being reorganized. Imports from here may be deprecated in the future.\n"""\n\n'
                content = warning + content

            # Add re-exports
            content += "\n\n# Re-exports from canonical locations\n" + "\n".join(re_exports) + "\n"

            # Write the updated __init__.py
            with open(init_path, 'w') as f:
                f.write(content)

            logger.info(f"Updated old {init_path} with re-exports")

        return True
    except Exception as e:
        logger.error(f"Error updating old {directory}/__init__.py: {e}")
        return False

def main() -> None:
    """Main function to execute the init file updates."""
    logger.info("Starting __init__.py file updates")

    # Parse mapping file
    mapping = parse_mapping_file()
    logger.info(f"Parsed {len(mapping)} file mappings from {MAPPING_FILE}")

    # Group files by directory
    directory_mapping = group_files_by_directory(mapping)
    logger.info(f"Found {len(directory_mapping)} directories to update")

    # Update init files for new directories
    for directory, files in directory_mapping.items():
        update_init_file(directory, files)

    # Update old init files (utils, db/handlers, etc.)
    old_directories = set([Path(dup).parent.as_posix() for dup in mapping.keys()])
    for directory in old_directories:
        update_old_init_file(directory, mapping)

    logger.info("__init__.py updates completed successfully")

if __name__ == "__main__":
    main()