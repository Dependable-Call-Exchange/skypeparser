#!/usr/bin/env python3
"""
Module Migration Script - Automates the gradual migration of code from deprecated
module locations to their canonical locations.

This script:
1. Identifies modules that exist in both deprecated and canonical locations
2. Analyzes both versions to merge functionality
3. Creates forwarding modules in deprecated locations
4. Maintains backward compatibility during migration
"""

import os
import sys
import ast
import difflib
import importlib
import re
from typing import Dict, List, Set, Tuple

# Mapping of deprecated paths to canonical paths (same as in import_forger.py)
DEPRECATED_TO_CANONICAL = {
    'src.core_utils.connection': 'src.db.connection',
    'src.core_utils.connection_factory': 'src.db.connection_factory',
    'src.core_utils.connection_pool': 'src.db.connection_pool',
    'src.db.handlers.archive_handler': 'src.data_handlers.archive_handler',
    'src.db.handlers.conversation_handler': 'src.data_handlers.conversation_handler',
    'src.db.handlers.message_handler': 'src.data_handlers.message_handler',
    'src.utils.attachment_handler': 'src.data_handlers.attachment_handler',
    'src.utils.file_handler': 'src.data_handlers.file_handler',
    'src.utils.message_processor': 'src.messages.message_processor',
    'src.utils.message_type_handlers': 'src.messages.message_type_handlers',
    'src.utils.message_type_extractor': 'src.messages.message_type_extractor',
    'src.utils.validation': 'src.validation.validation',
    'src.utils.data_validator': 'src.validation.data_validator',
    'src.utils.etl_validation': 'src.validation.etl_validation',
    'src.utils.progress_tracker': 'src.monitoring.progress_tracker',
    'src.utils.checkpoint_manager': 'src.monitoring.checkpoint_manager',
    'src.utils.memory_monitor': 'src.monitoring.memory_monitor',
    'src.utils.logging_config': 'src.logging.logging_config',
    'src.utils.structured_logging': 'src.logging.structured_logging',
    'src.utils.error_handling': 'src.logging.error_handling',
    'src.utils.file_utils': 'src.files.file_utils',
    'src.utils.tar_extractor': 'src.files.tar_extractor',
    'src.core_utils.core_parser': 'src.parser.core_parser',
    'src.core_utils.content_extractor': 'src.parser.content_extractor',
    'src.core_utils.etl_pipeline': 'src.db.etl.etl_pipeline',
    'src.core_utils.loader': 'src.db.etl.loader',
    'src.core_utils.transformer': 'src.db.etl.transformer',
}

# Reverse mapping for convenience
CANONICAL_TO_DEPRECATED = {v: k for k, v in DEPRECATED_TO_CANONICAL.items()}


class ModuleAnalyzer:
    """Analyzes Python modules to extract classes, functions, and symbols."""

    def __init__(self, module_path: str):
        self.module_path = module_path
        self.content = self._read_file()
        self.ast_tree = None
        self.classes = set()
        self.functions = set()
        self.imports = set()
        self.symbols = set()
        self.errors = []

        if self.content:
            try:
                self.ast_tree = ast.parse(self.content)
                self._analyze()
            except SyntaxError as e:
                self.errors.append(f"Syntax error in {module_path}: {e}")
                self._fallback_analyze()

    def _read_file(self) -> str:
        """Read file content"""
        try:
            with open(self.module_path, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            self.errors.append(f"Error reading {self.module_path}: {e}")
            return ""

    def _analyze(self):
        """Analyze AST to extract symbols"""
        for node in ast.walk(self.ast_tree):
            if isinstance(node, ast.ClassDef):
                self.classes.add(node.name)
            elif isinstance(node, ast.FunctionDef):
                self.functions.add(node.name)
            elif isinstance(node, ast.Import):
                for name in node.names:
                    self.imports.add(name.name)
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    for name in node.names:
                        self.imports.add(f"{node.module}.{name.name}")
            elif isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        self.symbols.add(target.id)

        # All defined objects
        self.all_defined = self.classes.union(self.functions).union(self.symbols)

    def _fallback_analyze(self):
        """Fallback analysis when AST parsing fails due to syntax errors"""
        print(f"Using fallback analysis for {self.module_path} due to syntax errors")

        # Simple regex-based analysis
        # Find function definitions
        func_pattern = r'def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\('
        self.functions = set(re.findall(func_pattern, self.content))

        # Find class definitions
        class_pattern = r'class\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*[:\(]'
        self.classes = set(re.findall(class_pattern, self.content))

        # Find variable assignments (simplified)
        var_pattern = r'^([a-zA-Z_][a-zA-Z0-9_]*)\s*='
        self.symbols = set(re.findall(var_pattern, self.content, re.MULTILINE))

        # All defined objects
        self.all_defined = self.classes.union(self.functions).union(self.symbols)


def module_path_to_file_path(module_path: str) -> str:
    """Convert a module path to a file path."""
    return os.path.join(*module_path.split('.')) + '.py'


def file_path_to_module_path(file_path: str) -> str:
    """Convert a file path to a module path."""
    # Remove .py extension
    if file_path.endswith('.py'):
        file_path = file_path[:-3]
    # Convert slashes to dots
    return file_path.replace(os.sep, '.')


def get_module_pairs() -> Dict[str, str]:
    """
    Get pairs of corresponding module file paths.
    Returns a dictionary mapping deprecated paths to canonical paths.
    """
    result = {}
    for deprecated, canonical in DEPRECATED_TO_CANONICAL.items():
        deprecated_path = module_path_to_file_path(deprecated)
        canonical_path = module_path_to_file_path(canonical)

        # Check if both files exist
        if os.path.exists(deprecated_path) and os.path.exists(canonical_path):
            result[deprecated_path] = canonical_path

    return result


def compare_modules(deprecated_path: str, canonical_path: str) -> Tuple[Set[str], Set[str], float]:
    """
    Compare two modules and return:
    - Unique symbols in deprecated module
    - Unique symbols in canonical module
    - Similarity ratio (0-1)
    """
    deprecated_analyzer = ModuleAnalyzer(deprecated_path)
    canonical_analyzer = ModuleAnalyzer(canonical_path)

    # Get unique elements in each module
    unique_deprecated = deprecated_analyzer.all_defined - canonical_analyzer.all_defined
    unique_canonical = canonical_analyzer.all_defined - deprecated_analyzer.all_defined

    # Calculate similarity ratio using difflib
    similarity = difflib.SequenceMatcher(None,
                                        deprecated_analyzer.content,
                                        canonical_analyzer.content).ratio()

    return unique_deprecated, unique_canonical, similarity


def extract_function_or_class(content: str, name: str) -> str:
    """
    Extract a function or class definition from code content.
    Very basic implementation - a more robust one would use ast.
    """
    pattern = rf'(class|def)\s+{re.escape(name)}\s*[\(:].*?(?=\n(class|def)\s+|$)'
    match = re.search(pattern, content, re.DOTALL)
    if match:
        return match.group(0)
    return ""


def create_forwarding_module(deprecated_path: str, canonical_path: str, unique_symbols: Set[str]):
    """
    Create a forwarding module that:
    1. Imports from the canonical module
    2. Re-exports all symbols
    3. Includes original unique symbols if any
    4. Adds deprecation warning
    """
    module_name = os.path.basename(deprecated_path).replace('.py', '')
    canonical_module = file_path_to_module_path(canonical_path)
    deprecated_module = file_path_to_module_path(deprecated_path)

    # Read content to preserve unique symbols
    deprecated_analyzer = ModuleAnalyzer(deprecated_path)

    forwarding_content = [
        f'"""',
        f'DEPRECATED MODULE: {deprecated_module}',
        f'',
        f'This module is deprecated. Please use {canonical_module} instead.',
        f'"""',
        f'',
        f'import warnings',
        f'import sys',
        f'from {canonical_module} import *',
        f'',
        f'# Emit deprecation warning',
        f'warnings.warn(',
        f'    f"The module \'{deprecated_module}\' is deprecated. '
        f'Use \'{canonical_module}\' instead.",',
        f'    DeprecationWarning,',
        f'    stacklevel=2',
        f')',
        f'',
    ]

    # If there are unique symbols in the deprecated module, keep them
    if unique_symbols:
        forwarding_content.extend([
            f'# Preserving unique symbols from deprecated module',
            f''
        ])

        for symbol in unique_symbols:
            # Extract and add the symbol definition
            symbol_def = extract_function_or_class(deprecated_analyzer.content, symbol)
            if symbol_def:
                forwarding_content.append(symbol_def)
                forwarding_content.append('')

    # Create backup of the original file
    backup_path = deprecated_path + '.bak'
    if not os.path.exists(backup_path):
        try:
            os.rename(deprecated_path, backup_path)
            print(f"Created backup: {backup_path}")
        except Exception as e:
            print(f"Error creating backup for {deprecated_path}: {e}")
            return False

    # Write new forwarding module
    try:
        with open(deprecated_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(forwarding_content))
        print(f"Created forwarding module: {deprecated_path}")
        return True
    except Exception as e:
        print(f"Error writing forwarding module {deprecated_path}: {e}")
        # Restore from backup if writing fails
        if os.path.exists(backup_path):
            os.rename(backup_path, deprecated_path)
        return False


def migrate_unique_symbols(deprecated_path: str, canonical_path: str, unique_symbols: Set[str]):
    """
    Migrate unique symbols from deprecated module to canonical module.
    """
    if not unique_symbols:
        return True

    # Read content of both modules
    deprecated_analyzer = ModuleAnalyzer(deprecated_path)
    canonical_analyzer = ModuleAnalyzer(canonical_path)

    # Initialize new content with existing canonical content
    new_content = canonical_analyzer.content

    # Add a section for migrated code
    migrated_section = [
        '',
        '# ------------------------------------------------------',
        f'# Migrated code from {os.path.basename(deprecated_path)}',
        '# ------------------------------------------------------',
        ''
    ]

    # Extract each unique symbol and add to migrated section
    successful_migrations = []
    for symbol in unique_symbols:
        symbol_def = extract_function_or_class(deprecated_analyzer.content, symbol)
        if symbol_def:
            migrated_section.append(symbol_def)
            migrated_section.append('')
            successful_migrations.append(symbol)
        else:
            print(f"Warning: Could not extract definition for symbol '{symbol}'")

    # Only proceed if we successfully extracted at least one symbol
    if not successful_migrations:
        print("No symbols were successfully extracted for migration")
        return False

    # Add migrated section to new content
    new_content += '\n'.join(migrated_section)

    # Create backup of the canonical file
    backup_path = canonical_path + '.bak'
    if not os.path.exists(backup_path):
        try:
            os.rename(canonical_path, backup_path)
            print(f"Created backup: {backup_path}")
        except Exception as e:
            print(f"Error creating backup for {canonical_path}: {e}")
            return False

    # Write updated canonical module
    try:
        with open(canonical_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"Updated canonical module with {len(successful_migrations)} migrated symbols: {canonical_path}")
        print(f"Migrated symbols: {', '.join(successful_migrations)}")
        return True
    except Exception as e:
        print(f"Error updating canonical module {canonical_path}: {e}")
        # Restore from backup if writing fails
        if os.path.exists(backup_path):
            os.rename(backup_path, canonical_path)
        return False


def migrate_module(deprecated_path: str, canonical_path: str, dry_run: bool = False):
    """
    Migrate a module from deprecated path to canonical path:
    1. Compare modules to find unique symbols
    2. Migrate unique symbols to canonical module
    3. Create forwarding module in deprecated location
    """
    print(f"\nAnalyzing migration: {deprecated_path} → {canonical_path}")

    deprecated_analyzer = ModuleAnalyzer(deprecated_path)
    canonical_analyzer = ModuleAnalyzer(canonical_path)

    # Report any errors
    if deprecated_analyzer.errors:
        print("Errors in deprecated module:")
        for error in deprecated_analyzer.errors:
            print(f"  - {error}")

    if canonical_analyzer.errors:
        print("Errors in canonical module:")
        for error in canonical_analyzer.errors:
            print(f"  - {error}")

    # Get unique elements in each module
    unique_deprecated = deprecated_analyzer.all_defined - canonical_analyzer.all_defined
    unique_canonical = canonical_analyzer.all_defined - deprecated_analyzer.all_defined

    # Calculate similarity ratio using difflib
    similarity = difflib.SequenceMatcher(
        None,
        deprecated_analyzer.content,
        canonical_analyzer.content
    ).ratio()

    print(f"Similarity: {similarity:.2f}")
    print(f"Unique symbols in deprecated: {', '.join(unique_deprecated) if unique_deprecated else 'None'}")
    print(f"Unique symbols in canonical: {', '.join(unique_canonical) if unique_canonical else 'None'}")

    if dry_run:
        print("[DRY RUN] No changes made")
        return

    # If deprecated has unique symbols, migrate them to canonical
    if unique_deprecated:
        success = migrate_unique_symbols(deprecated_path, canonical_path, unique_deprecated)
        if not success:
            print(f"Failed to migrate symbols to {canonical_path}, aborting module migration")
            return

    # Create forwarding module in deprecated location
    success = create_forwarding_module(deprecated_path, canonical_path, set())
    if success:
        print(f"Successfully migrated module: {deprecated_path} → {canonical_path}")
    else:
        print(f"Failed to create forwarding module for {deprecated_path}")


def main():
    """Main entry point."""
    print("Module Migration Script")
    print("======================")

    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Migrate modules from deprecated to canonical locations')
    parser.add_argument('--dry-run', action='store_true', help='Analyze without making changes')
    parser.add_argument('--module', type=str, help='Specific module to migrate (e.g., src.utils.validation)')
    parser.add_argument('--list', action='store_true', help='List all module pairs without migrating')
    args = parser.parse_args()

    # Get all module pairs
    module_pairs = get_module_pairs()

    if args.list:
        print("\nModule Pairs for Migration:")
        for deprecated, canonical in module_pairs.items():
            print(f"  {deprecated} → {canonical}")
        return

    # Migrate a specific module if requested
    if args.module:
        if args.module in DEPRECATED_TO_CANONICAL:
            deprecated_path = module_path_to_file_path(args.module)
            canonical_path = module_path_to_file_path(DEPRECATED_TO_CANONICAL[args.module])
            if os.path.exists(deprecated_path) and os.path.exists(canonical_path):
                migrate_module(deprecated_path, canonical_path, args.dry_run)
            else:
                print(f"Module files not found for {args.module}")
        else:
            print(f"Unknown deprecated module: {args.module}")
        return

    # Migrate all modules
    for deprecated_path, canonical_path in module_pairs.items():
        migrate_module(deprecated_path, canonical_path, args.dry_run)

    print("\nMigration complete!")


if __name__ == "__main__":
    main()