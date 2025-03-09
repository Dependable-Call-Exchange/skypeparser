#!/usr/bin/env python3
"""
Script to implement the codebase reorganization based on the mapping document.

This script:
1. Creates a backup of the codebase
2. Adds deprecation notices to duplicated files
3. Updates imports in the canonical files
4. Creates symbolic links for backward compatibility

Usage:
    python implement_reorganization.py

The script reads the reorganization_mapping.md file to determine which files
to process.
"""

import os
import re
import sys
import shutil
import datetime
import warnings
from pathlib import Path
from typing import Dict, List, Tuple, Set

# Set up logging
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
PROJECT_ROOT = Path(__file__).parent.parent
SRC_DIR = PROJECT_ROOT / "src"
BACKUP_DIR = PROJECT_ROOT / "backup"
MAPPING_FILE = PROJECT_ROOT / "reorganization_mapping.md"

# Regular expressions for parsing the mapping file
TABLE_PATTERN = r'\|\s*([^|]+)\s*\|\s*([^|]+)\s*\|\s*[^|]+\s*\|'
SECTION_PATTERN = r'### (.+)'

def create_backup() -> Path:
    """Create a backup of the src directory."""
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = BACKUP_DIR / f"src_backup_{timestamp}"

    logger.info(f"Creating backup at {backup_path}")
    shutil.copytree(SRC_DIR, backup_path)

    return backup_path

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
    current_section = None

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

def add_deprecation_notice(file_path: str, canonical_path: str, category: str) -> bool:
    """
    Add a deprecation notice to a file.

    Args:
        file_path: Path to the file to modify
        canonical_path: Path to the canonical file
        category: Category of the file

    Returns:
        True if successful, False otherwise
    """
    try:
        full_path = Path(PROJECT_ROOT / file_path)
        if not full_path.exists():
            logger.warning(f"File not found: {full_path}")
            return False

        with open(full_path, 'r') as f:
            content = f.read()

        # Determine the file's extension to use the correct comment syntax
        ext = full_path.suffix
        if ext == '.py':
            # For Python files, add a deprecation warning
            deprecation_notice = f'''"""
DEPRECATED: This file is deprecated and will be removed in a future version.
Please use {canonical_path} instead.
This file is maintained for backward compatibility as part of the {category} reorganization.
"""

import warnings
warnings.warn(
    f"The module {file_path} is deprecated. Use {canonical_path} instead.",
    DeprecationWarning,
    stacklevel=2
)

'''
            # Add the notice after any existing docstrings and imports
            if '"""' in content:
                # Find the end of the existing docstring
                docstring_end = content.find('"""', content.find('"""') + 3) + 3
                content = content[:docstring_end] + "\n\n" + deprecation_notice + content[docstring_end:]
            else:
                # No existing docstring, add to the top
                content = deprecation_notice + content

        with open(full_path, 'w') as f:
            f.write(content)

        logger.info(f"Added deprecation notice to {file_path}")
        return True
    except Exception as e:
        logger.error(f"Error adding deprecation notice to {file_path}: {e}")
        return False

def create_symlink(source: str, target: str) -> bool:
    """
    Create a symbolic link from source to target.

    Args:
        source: The source file (that will be symlinked)
        target: The target file (where the symlink will point)

    Returns:
        True if successful, False otherwise
    """
    try:
        source_path = Path(PROJECT_ROOT / source)
        target_path = Path(PROJECT_ROOT / target)

        # Ensure the target directory exists
        os.makedirs(target_path.parent, exist_ok=True)

        # Create the symlink
        if os.path.lexists(target_path):
            os.remove(target_path)

        # Create a relative symlink
        rel_path = os.path.relpath(source_path, target_path.parent)
        os.symlink(rel_path, target_path)

        logger.info(f"Created symlink from {target} to {source}")
        return True
    except Exception as e:
        logger.error(f"Error creating symlink from {target} to {source}: {e}")
        return False

def fix_imports(file_path: str) -> bool:
    """
    Fix imports in a file to use the new canonical paths.

    Args:
        file_path: Path to the file to modify

    Returns:
        True if successful, False otherwise
    """
    try:
        full_path = Path(PROJECT_ROOT / file_path)
        if not full_path.exists():
            logger.warning(f"File not found: {full_path}")
            return False

        with open(full_path, 'r') as f:
            content = f.read()

        # Define import patterns to replace
        # This would need to be customized based on the specific imports in your codebase
        import_replacements = [
            # DB imports
            (r'from\s+src\.core_utils\.connection\b', 'from src.db.connection'),
            (r'from\s+src\.core_utils\.connection_factory\b', 'from src.db.connection_factory'),
            (r'from\s+src\.core_utils\.connection_pool\b', 'from src.db.connection_pool'),

            # Message processing imports
            (r'from\s+src\.utils\.message_processor\b', 'from src.messages.message_processor'),
            (r'from\s+src\.utils\.message_type_handlers\b', 'from src.messages.message_type_handlers'),
            (r'from\s+src\.utils\.message_type_extractor\b', 'from src.messages.message_type_extractor'),

            # Handler imports
            (r'from\s+src\.db\.handlers\.', 'from src.data_handlers.'),
            (r'from\s+src\.utils\.file_handler\b', 'from src.data_handlers.file_handler'),
            (r'from\s+src\.utils\.attachment_handler\b', 'from src.data_handlers.attachment_handler'),

            # File-related imports
            (r'from\s+src\.utils\.file_utils\b', 'from src.files.file_utils'),
            (r'from\s+src\.utils\.tar_extractor\b', 'from src.files.tar_extractor'),
            (r'from\s+src\.parser\.file_output\b', 'from src.files.file_output'),

            # Validation imports
            (r'from\s+src\.utils\.validation\b', 'from src.validation.validation'),
            (r'from\s+src\.utils\.data_validator\b', 'from src.validation.data_validator'),
            (r'from\s+src\.utils\.configuration_validator\b', 'from src.validation.configuration_validator'),
            (r'from\s+src\.utils\.etl_validation\b', 'from src.validation.etl_validation'),
            (r'from\s+src\.utils\.schema_validation\b', 'from src.validation.schema_validation'),

            # Monitoring imports
            (r'from\s+src\.utils\.progress_tracker\b', 'from src.monitoring.progress_tracker'),
            (r'from\s+src\.db\.progress_tracker\b', 'from src.monitoring.progress_tracker'),
            (r'from\s+src\.utils\.checkpoint_manager\b', 'from src.monitoring.checkpoint_manager'),
            (r'from\s+src\.utils\.memory_monitor\b', 'from src.monitoring.memory_monitor'),
            (r'from\s+src\.utils\.phase_manager\b', 'from src.monitoring.phase_manager'),

            # Logging imports
            (r'from\s+src\.utils\.logging_config\b', 'from src.logging.logging_config'),
            (r'from\s+src\.utils\.structured_logging\b', 'from src.logging.structured_logging'),
            (r'from\s+src\.utils\.new_structured_logging\b', 'from src.logging.new_structured_logging'),
            (r'from\s+src\.utils\.logging_compat\b', 'from src.logging.logging_compat'),
            (r'from\s+src\.utils\.error_handling\b', 'from src.logging.error_handling'),
            (r'from\s+src\.utils\.error_logger\b', 'from src.logging.error_logger'),

            # Parser imports
            (r'from\s+src\.core_utils\.core_parser\b', 'from src.parser.core_parser'),
            (r'from\s+src\.core_utils\.content_extractor\b', 'from src.parser.content_extractor'),
            (r'from\s+src\.core_utils\.parser_module\b', 'from src.parser.parser_module'),
            (r'from\s+src\.core_utils\.skype_parser\b', 'from src.parser.skype_parser'),
            (r'from\s+src\.core_utils\.structured_data_extractor\b', 'from src.parser.structured_data_extractor'),

            # ETL imports
            (r'from\s+src\.core_utils\.etl_pipeline\b', 'from src.db.etl.etl_pipeline'),
            (r'from\s+src\.core_utils\.etl_pipeline_compat\b', 'from src.db.etl.etl_pipeline_compat'),
            (r'from\s+src\.core_utils\.loader\b', 'from src.db.etl.loader'),
            (r'from\s+src\.core_utils\.transformer\b', 'from src.db.etl.transformer'),
            (r'from\s+src\.core_utils\.extractor\b', 'from src.db.etl.extractor'),
            (r'from\s+src\.core_utils\.modular_pipeline\b', 'from src.db.etl.modular_pipeline'),
            (r'from\s+src\.core_utils\.pipeline_factory\b', 'from src.db.etl.pipeline_factory'),
            (r'from\s+src\.core_utils\.pipeline_manager\b', 'from src.db.etl.pipeline_manager'),
            (r'from\s+src\.core_utils\.streaming_processor\b', 'from src.db.etl.streaming_processor'),
            (r'from\s+src\.core_utils\.context\b', 'from src.db.etl.context'),
            (r'from\s+src\.core_utils\.testable_etl_pipeline\b', 'from src.db.etl.testable_etl_pipeline'),

            # API imports
            (r'from\s+src\.core_utils\.run_api\b', 'from src.api.run_api'),
            (r'from\s+src\.core_utils\.tasks\b', 'from src.api.tasks'),
            (r'from\s+src\.core_utils\.skype_api\b', 'from src.api.skype_api'),
            (r'from\s+src\.core_utils\.user_management\b', 'from src.api.user_management'),

            # Analysis imports
            (r'from\s+src\.core_utils\.queries\b', 'from src.analysis.queries'),
            (r'from\s+src\.core_utils\.reporting\b', 'from src.analysis.reporting'),
            (r'from\s+src\.core_utils\.visualization\b', 'from src.analysis.visualization'),
        ]

        for pattern, replacement in import_replacements:
            content = re.sub(pattern, replacement, content)

        with open(full_path, 'w') as f:
            f.write(content)

        logger.info(f"Fixed imports in {file_path}")
        return True
    except Exception as e:
        logger.error(f"Error fixing imports in {file_path}: {e}")
        return False

def process_files(mapping: Dict[str, Tuple[str, str]]) -> None:
    """
    Process files according to the mapping.

    Args:
        mapping: Dict mapping duplicate files to (canonical location, category) tuples
    """
    # First, fix imports in all canonical files
    canonical_files = set(canonical for canonical, _ in mapping.values())
    for canonical in canonical_files:
        fix_imports(canonical)

    # Then, add deprecation notices to duplicated files
    for duplicate, (canonical, category) in mapping.items():
        add_deprecation_notice(duplicate, canonical, category)

def main() -> None:
    """Main function to execute the reorganization."""
    logger.info("Starting codebase reorganization")

    # Create backup
    backup_path = create_backup()
    logger.info(f"Backup created at {backup_path}")

    # Parse mapping file
    mapping = parse_mapping_file()
    logger.info(f"Parsed {len(mapping)} file mappings from {MAPPING_FILE}")

    # Process files
    process_files(mapping)

    logger.info("Reorganization completed successfully")

if __name__ == "__main__":
    main()