#!/usr/bin/env python3
"""
Script to reorganize the SkypeParser project structure based on analysis results.

This script:
1. Reads the analysis results from reorganization_analysis.json
2. Creates new directories if they don't exist
3. Moves files to their new locations
4. Updates imports in all Python files
5. Creates __init__.py files with appropriate exports

Usage:
    python reorganize_project.py [analysis_file]

Where:
    analysis_file - JSON file with analysis results (default: reorganization_analysis.json)
"""

import os
import re
import sys
import json
import shutil
from pathlib import Path
import logging
import ast
from typing import Dict, List, Set, Tuple, Any, Optional

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_analysis(file_path: Path) -> Dict[str, Any]:
    """
    Load analysis results from a JSON file.

    Args:
        file_path: Path to the analysis file

    Returns:
        Dictionary with analysis results
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading analysis file {file_path}: {e}")
        sys.exit(1)


def create_directories(domains: List[str]) -> None:
    """
    Create new domain directories if they don't exist.

    Args:
        domains: List of domain names
    """
    for domain in domains:
        domain_dir = Path('src') / domain
        if not domain_dir.exists():
            logger.info(f"Creating directory {domain_dir}")
            domain_dir.mkdir(exist_ok=True)

        # Create __init__.py file if it doesn't exist
        init_file = domain_dir / '__init__.py'
        if not init_file.exists():
            logger.info(f"Creating empty __init__.py in {domain_dir}")
            init_file.touch()


def move_file(source: Path, target: Path) -> bool:
    """
    Move a file from source to target.

    Args:
        source: Source file path
        target: Target file path

    Returns:
        True if the file was moved successfully, False otherwise
    """
    try:
        # Create parent directory if it doesn't exist
        target.parent.mkdir(parents=True, exist_ok=True)

        # Check if target file already exists
        if target.exists():
            logger.warning(f"Target file {target} already exists, skipping move")
            return False

        # Copy the file
        shutil.copy2(source, target)
        logger.info(f"Moved {source} -> {target}")
        return True
    except Exception as e:
        logger.error(f"Error moving file {source} to {target}: {e}")
        return False


def update_imports_in_file(file_path: Path, old_to_new_mapping: Dict[str, str]) -> None:
    """
    Update import statements in a file to reflect new file locations.

    Args:
        file_path: Path to the file to update
        old_to_new_mapping: Mapping from old import paths to new import paths
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        updated_content = content

        # Look for import statements that match old paths
        for old_path, new_path in old_to_new_mapping.items():
            # Convert paths to import format
            old_import = old_path.replace('/', '.').replace('.py', '')
            new_import = new_path.replace('/', '.').replace('.py', '')

            # Replace in from imports
            updated_content = re.sub(
                rf'from\s+{re.escape(old_import)}\s+import',
                f'from {new_import} import',
                updated_content
            )

            # Replace in regular imports
            updated_content = re.sub(
                rf'import\s+{re.escape(old_import)}(?!\w)',
                f'import {new_import}',
                updated_content
            )

        # Only write the file if changes were made
        if updated_content != content:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(updated_content)
            logger.info(f"Updated imports in {file_path}")

    except Exception as e:
        logger.error(f"Error updating imports in {file_path}: {e}")


def generate_init_files(mapping: Dict[str, str], file_info: Dict[str, Dict]) -> None:
    """
    Generate __init__.py files for each domain with appropriate exports.

    Args:
        mapping: Mapping from old file paths to new file paths
        file_info: Dictionary with file information
    """
    # Group files by domain
    files_by_domain = {}
    for old_path, new_path in mapping.items():
        domain = Path(new_path).parent.name
        if domain not in files_by_domain:
            files_by_domain[domain] = []
        files_by_domain[domain].append((old_path, new_path))

    # Generate __init__.py for each domain
    for domain, files in files_by_domain.items():
        init_path = Path('src') / domain / '__init__.py'

        with open(init_path, 'w', encoding='utf-8') as f:
            f.write(f'"""\n{domain.title()} package for SkypeParser.\n\nThis package contains {domain}-related components.\n"""\n\n')

            # Add imports
            for old_path, new_path in files:
                module_name = Path(new_path).stem

                # Get classes/functions to export from file_info
                if old_path in file_info:
                    # Try to find classes and functions to export
                    try:
                        with open(old_path, 'r', encoding='utf-8') as src_file:
                            content = src_file.read()

                        tree = ast.parse(content)
                        exports = []

                        for node in tree.body:
                            if isinstance(node, ast.ClassDef):
                                exports.append(node.name)
                            elif isinstance(node, ast.FunctionDef) and not node.name.startswith('_'):
                                exports.append(node.name)

                        if exports:
                            exports_str = ', '.join(exports)
                            f.write(f'from .{module_name} import {exports_str}\n')
                        else:
                            f.write(f'# from .{module_name} import ...\n')
                    except Exception as e:
                        logger.error(f"Error analyzing exports from {old_path}: {e}")
                        f.write(f'# from .{module_name} import ...\n')
                else:
                    f.write(f'# from .{module_name} import ...\n')

        logger.info(f"Generated __init__.py for {domain}")


def create_import_mapping(file_mapping: Dict[str, str]) -> Dict[str, str]:
    """
    Create a mapping from old import paths to new import paths.

    Args:
        file_mapping: Mapping from old file paths to new file paths

    Returns:
        Mapping from old import paths to new import paths
    """
    import_mapping = {}

    for old_path, new_path in file_mapping.items():
        # Convert to Python import format
        old_import = old_path.replace('/', '.').replace('.py', '')
        new_import = new_path.replace('/', '.').replace('.py', '')

        import_mapping[old_import] = new_import

    return import_mapping


def reorganize_project(analysis: Dict[str, Any]) -> None:
    """
    Reorganize the project based on analysis results.

    Args:
        analysis: Analysis results dictionary
    """
    # Extract information
    domains = analysis.get('domains', [])
    mapping = analysis.get('reorganization_mapping', {})
    files_info = analysis.get('files_info', {})

    # Create new directories
    create_directories(domains)

    # Move files
    moved_files = []
    for old_path_str, new_path_str in mapping.items():
        old_path = Path(old_path_str)
        new_path = Path(new_path_str)

        if move_file(old_path, new_path):
            moved_files.append((old_path_str, new_path_str))

    # Create import mapping
    import_mapping = create_import_mapping(dict(moved_files))

    # Update imports in all Python files
    for root, _, files in os.walk('src'):
        for file in files:
            if file.endswith('.py'):
                file_path = Path(root) / file
                update_imports_in_file(file_path, import_mapping)

    # Generate __init__.py files
    generate_init_files(dict(moved_files), files_info)


def main():
    """
    Main entry point for the script.
    """
    # Get analysis file from command line arguments or use default
    if len(sys.argv) > 1:
        analysis_file = Path(sys.argv[1])
    else:
        analysis_file = Path('reorganization_analysis.json')

    if not analysis_file.exists():
        logger.error(f"Analysis file {analysis_file} does not exist")
        sys.exit(1)

    # Load analysis
    logger.info(f"Loading analysis from {analysis_file}")
    analysis = load_analysis(analysis_file)

    # Confirm with user
    file_count = len(analysis.get('reorganization_mapping', {}))
    print(f"\nAbout to reorganize {file_count} files. This will:")
    print("1. Move files to new directories")
    print("2. Update import statements")
    print("3. Generate __init__.py files\n")

    confirm = input("Do you want to continue? (y/n): ")
    if confirm.lower() != 'y':
        logger.info("Reorganization cancelled by user")
        sys.exit(0)

    # Create backup
    logger.info("Creating backup of src directory...")
    shutil.copytree('src', 'src_reorg_backup')

    # Reorganize project
    logger.info("Reorganizing project...")
    reorganize_project(analysis)

    logger.info("Reorganization complete!")
    logger.info("NOTE: You should review the changes and run tests to make sure everything works")


if __name__ == "__main__":
    main()