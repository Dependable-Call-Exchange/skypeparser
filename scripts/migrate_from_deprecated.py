#!/usr/bin/env python3
"""
Migration Script for Deprecated Modules

This script helps users transition from deprecated modules to their replacements.
It scans Python files for imports of deprecated modules and suggests replacements.

Usage:
    python scripts/migrate_from_deprecated.py [--path <path>] [--apply]

Options:
    --path <path>    Path to scan for Python files (default: current directory)
    --apply          Apply suggested changes (default: only suggest changes)
"""

import os
import sys
import re
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Set

# Mapping of deprecated imports to their replacements
DEPRECATED_IMPORTS = {
    # parser_module.py replacements
    "from src.parser.parser_module import timestamp_parser": "from src.parser.core_parser import timestamp_parser",
    "from src.parser.parser_module import content_parser": "from src.parser.core_parser import content_parser",
    "from src.parser.parser_module import tag_stripper": "from src.parser.core_parser import enhanced_tag_stripper",
    "from src.parser.parser_module import pretty_quotes": "from src.parser.core_parser import pretty_quotes",
    "from src.parser.parser_module import read_file": "from src.utils.file_handler import read_file",
    "from src.parser.parser_module import read_tarfile": "from src.utils.file_handler import read_tarfile",

    # etl_pipeline.py replacements
    "from src.db.etl_pipeline import SkypeETLPipeline": "from src.db.etl import ETLPipeline",

    # skype_to_postgres.py and store_skype_export.py replacements
    "from src.db.skype_to_postgres import": "# Use src.db.etl.ETLPipeline instead of skype_to_postgres",
    "from src.db.store_skype_export import": "# Use src.db.etl.ETLPipeline instead of store_skype_export",

    # Relative imports
    "from ..parser.parser_module import": "from ..parser.core_parser import",
    "from .etl_pipeline import": "from .etl import ETLPipeline",
    "from .skype_to_postgres import": "# Use .etl.ETLPipeline instead of skype_to_postgres",
    "from .store_skype_export import": "# Use .etl.ETLPipeline instead of store_skype_export",
}

# Function name replacements
FUNCTION_REPLACEMENTS = {
    "timestamp_parser": "timestamp_parser",
    "content_parser": "content_parser",
    "tag_stripper": "enhanced_tag_stripper",
    "pretty_quotes": "pretty_quotes",
    "SkypeETLPipeline": "ETLPipeline",
}

def find_python_files(path: str) -> List[Path]:
    """
    Find all Python files in the given path.

    Args:
        path: Path to scan for Python files

    Returns:
        List of Path objects for Python files
    """
    path = Path(path)
    if path.is_file() and path.suffix == '.py':
        return [path]

    python_files = []
    for root, _, files in os.walk(path):
        for file in files:
            if file.endswith('.py'):
                python_files.append(Path(root) / file)

    return python_files

def analyze_file(file_path: Path) -> Tuple[List[Tuple[int, str, str]], Set[str]]:
    """
    Analyze a Python file for deprecated imports and function calls.

    Args:
        file_path: Path to the Python file

    Returns:
        Tuple of (list of (line number, line, replacement), set of deprecated modules used)
    """
    replacements = []
    deprecated_modules = set()

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Check for deprecated imports
    for i, line in enumerate(lines):
        line = line.strip()
        for deprecated, replacement in DEPRECATED_IMPORTS.items():
            if deprecated in line:
                replacements.append((i + 1, line, line.replace(deprecated, replacement)))

                # Extract module name from the deprecated import
                try:
                    if ' import ' in deprecated:
                        module = deprecated.split(' import')[0].split('from ')[1]
                    else:
                        module = deprecated.split('import ')[1].split(' ')[0]
                    deprecated_modules.add(module)
                except (IndexError, KeyError):
                    # Skip if we can't extract the module name
                    pass

    # Check for deprecated function calls
    for i, line in enumerate(lines):
        for deprecated_module in deprecated_modules:
            try:
                module_name = deprecated_module.split('.')[-1]
                for old_func, new_func in FUNCTION_REPLACEMENTS.items():
                    pattern = rf'{module_name}\.{old_func}\('
                    if re.search(pattern, line):
                        new_line = re.sub(pattern, f'{module_name}.{new_func}(', line)
                        replacements.append((i + 1, line.strip(), new_line.strip()))
            except (IndexError, AttributeError):
                # Skip if we can't process the module name
                pass

    return replacements, deprecated_modules

def generate_report(file_path: Path, replacements: List[Tuple[int, str, str]], deprecated_modules: Set[str]) -> str:
    """
    Generate a report for a file with suggested replacements.

    Args:
        file_path: Path to the Python file
        replacements: List of (line number, line, replacement)
        deprecated_modules: Set of deprecated modules used

    Returns:
        Report string
    """
    report = f"File: {file_path}\n"

    if not replacements:
        report += "  No deprecated imports or function calls found.\n"
        return report

    report += "  Deprecated modules used:\n"
    for module in sorted(deprecated_modules):
        report += f"    - {module}\n"

    report += "\n  Suggested replacements:\n"
    for line_num, line, replacement in sorted(replacements, key=lambda x: x[0]):
        report += f"    Line {line_num}:\n"
        report += f"      - {line}\n"
        report += f"      + {replacement}\n"

    return report

def apply_changes(file_path: Path, replacements: List[Tuple[int, str, str]]) -> bool:
    """
    Apply suggested changes to a file.

    Args:
        file_path: Path to the Python file
        replacements: List of (line number, line, replacement)

    Returns:
        True if changes were applied, False otherwise
    """
    if not replacements:
        return False

    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Apply replacements
    for line_num, line, replacement in sorted(replacements, key=lambda x: x[0], reverse=True):
        # Line numbers are 1-indexed, but list indices are 0-indexed
        lines[line_num - 1] = lines[line_num - 1].replace(line, replacement)

    # Write changes back to file
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(lines)

    return True

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Migrate from deprecated modules to their replacements')
    parser.add_argument('--path', default='.', help='Path to scan for Python files (default: current directory)')
    parser.add_argument('--apply', action='store_true', help='Apply suggested changes (default: only suggest changes)')
    args = parser.parse_args()

    # Find Python files
    python_files = find_python_files(args.path)
    print(f"Found {len(python_files)} Python files to analyze")

    # Analyze files
    total_replacements = 0
    files_with_replacements = 0

    for file_path in python_files:
        replacements, deprecated_modules = analyze_file(file_path)

        if replacements:
            files_with_replacements += 1
            total_replacements += len(replacements)

            # Generate report
            report = generate_report(file_path, replacements, deprecated_modules)
            print(report)

            # Apply changes if requested
            if args.apply:
                applied = apply_changes(file_path, replacements)
                if applied:
                    print(f"  Applied {len(replacements)} changes to {file_path}")

    # Print summary
    print(f"\nSummary:")
    print(f"  Files analyzed: {len(python_files)}")
    print(f"  Files with deprecated imports: {files_with_replacements}")
    print(f"  Total replacements suggested: {total_replacements}")

    if args.apply:
        print(f"  Changes applied: {total_replacements}")
    else:
        print(f"  Run with --apply to apply the suggested changes")

    # Provide guidance on deprecated files
    print("\nDeprecated files that should be transitioned from:")
    print("  1. src/parser/parser_module.py -> src/parser/core_parser.py")
    print("  2. src/db/etl_pipeline.py -> src/db/etl/pipeline_manager.py")
    print("  3. src/db/skype_to_postgres.py -> src/db/etl/pipeline_manager.py")
    print("  4. src/db/store_skype_export.py -> src/db/etl/pipeline_manager.py")

    print("\nRecommended next steps:")
    print("  1. Update imports using this script with --apply")
    print("  2. Update code that uses the deprecated modules")
    print("  3. Run tests to ensure everything works correctly")
    print("  4. Once all code is migrated, the deprecated files can be removed")

if __name__ == "__main__":
    main()