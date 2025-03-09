#!/usr/bin/env python3
"""
Script to test the reorganized codebase.

This script:
1. Attempts to import key modules from canonical locations
2. Verifies that deprecated imports still work
3. Checks for import errors or missing dependencies
4. Reports on the health of the reorganized codebase

Usage:
    python test_reorganization.py
"""

import os
import sys
import importlib
import logging
from pathlib import Path
from typing import Dict, List, Set, Tuple

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
PROJECT_ROOT = Path(__file__).parent.parent
SRC_DIR = PROJECT_ROOT / "src"
TEST_RESULTS_FILE = PROJECT_ROOT / "reorganization_test_results.md"

# Add project root to Python path
sys.path.insert(0, str(PROJECT_ROOT))

def test_canonical_imports():
    """Test imports from canonical locations."""
    canonical_modules = [
        # Database connection modules
        "src.db.connection",
        "src.db.connection_factory",
        "src.db.connection_pool",

        # Data handlers
        "src.data_handlers.archive_handler",
        "src.data_handlers.attachment_handler",
        "src.data_handlers.conversation_handler",
        "src.data_handlers.message_handler",
        "src.data_handlers.file_handler",
        "src.data_handlers.user_handler",

        # Message processing
        "src.messages.message_processor",
        "src.messages.message_type_handlers",
        "src.messages.message_type_extractor",

        # Validation
        "src.validation.validation",
        "src.validation.data_validator",
        "src.validation.configuration_validator",
        "src.validation.etl_validation",
        "src.validation.schema_validation",

        # Monitoring
        "src.monitoring.progress_tracker",
        "src.monitoring.checkpoint_manager",
        "src.monitoring.memory_monitor",
        "src.monitoring.phase_manager",

        # Logging
        "src.logging.logging_config",
        "src.logging.structured_logging",
        "src.logging.new_structured_logging",
        "src.logging.error_handling",

        # Files
        "src.files.file_utils",
        "src.files.tar_extractor",
        "src.files.file_output",

        # Parser
        "src.parser.core_parser",
        "src.parser.content_extractor",
        "src.parser.parser_module",

        # ETL
        "src.db.etl.etl_pipeline",
        "src.db.etl.loader",
        "src.db.etl.transformer",
        "src.db.etl.extractor",

        # Core utils
        "src.core_utils.config",
        "src.core_utils.dependencies",
        "src.core_utils.di",
        "src.core_utils.interfaces",
    ]

    results = {}

    for module_name in canonical_modules:
        try:
            module = importlib.import_module(module_name)
            results[module_name] = "✅ Success"
        except Exception as e:
            results[module_name] = f"❌ Error: {str(e)}"

    return results

def test_deprecated_imports():
    """Test imports from deprecated locations."""
    deprecated_modules = [
        # Database connection modules
        "src.core_utils.connection",
        "src.core_utils.connection_factory",
        "src.core_utils.connection_pool",

        # Data handlers
        "src.db.handlers.archive_handler",
        "src.db.handlers.conversation_handler",
        "src.db.handlers.message_handler",
        "src.utils.attachment_handler",
        "src.utils.file_handler",

        # Message processing
        "src.utils.message_processor",
        "src.utils.message_type_handlers",
        "src.utils.message_type_extractor",

        # Validation
        "src.utils.validation",
        "src.utils.data_validator",
        "src.utils.etl_validation",

        # Monitoring
        "src.utils.progress_tracker",
        "src.utils.checkpoint_manager",
        "src.utils.memory_monitor",

        # Logging
        "src.utils.logging_config",
        "src.utils.structured_logging",
        "src.utils.error_handling",

        # Files
        "src.utils.file_utils",
        "src.utils.tar_extractor",

        # Parser
        "src.core_utils.core_parser",
        "src.core_utils.content_extractor",

        # ETL
        "src.core_utils.etl_pipeline",
        "src.core_utils.loader",
        "src.core_utils.transformer",
    ]

    results = {}

    for module_name in deprecated_modules:
        try:
            # Check if the deprecation warning is triggered
            with warnings_to_list() as warning_list:
                module = importlib.import_module(module_name)

            if warning_list and any("deprecated" in str(w.message).lower() for w in warning_list):
                results[module_name] = "✅ Success (Deprecation warning shown)"
            else:
                results[module_name] = "⚠️ Partial success (No deprecation warning)"
        except Exception as e:
            results[module_name] = f"❌ Error: {str(e)}"

    return results

def import_all_modules():
    """Attempt to import all Python modules in the codebase."""
    results = {}

    for root, dirs, files in os.walk(SRC_DIR):
        for file in files:
            if file.endswith(".py") and file != "__init__.py":
                module_path = os.path.join(root, file)
                rel_path = os.path.relpath(module_path, PROJECT_ROOT)
                module_name = rel_path.replace(".py", "").replace("/", ".")

                try:
                    module = importlib.import_module(module_name)
                    results[module_name] = "✅ Success"
                except Exception as e:
                    results[module_name] = f"❌ Error: {str(e)}"

    return results

def write_results(canonical_results, deprecated_results, all_modules_results):
    """Write test results to a report file."""
    with open(TEST_RESULTS_FILE, 'w') as f:
        f.write("# Codebase Reorganization Test Results\n\n")

        # Write summary
        canonical_success = sum(1 for r in canonical_results.values() if r.startswith("✅"))
        deprecated_success = sum(1 for r in deprecated_results.values() if r.startswith("✅"))
        all_success = sum(1 for r in all_modules_results.values() if r.startswith("✅"))

        f.write("## Summary\n\n")
        f.write(f"- Canonical imports: {canonical_success}/{len(canonical_results)} successful\n")
        f.write(f"- Deprecated imports: {deprecated_success}/{len(deprecated_results)} successful\n")
        f.write(f"- All modules: {all_success}/{len(all_modules_results)} successful\n\n")

        # Write canonical import results
        f.write("## Canonical Import Tests\n\n")
        f.write("| Module | Result |\n")
        f.write("|--------|--------|\n")
        for module, result in canonical_results.items():
            f.write(f"| {module} | {result} |\n")
        f.write("\n")

        # Write deprecated import results
        f.write("## Deprecated Import Tests\n\n")
        f.write("| Module | Result |\n")
        f.write("|--------|--------|\n")
        for module, result in deprecated_results.items():
            f.write(f"| {module} | {result} |\n")
        f.write("\n")

        # Write all modules results
        f.write("## All Modules Test\n\n")
        f.write("| Module | Result |\n")
        f.write("|--------|--------|\n")
        for module, result in all_modules_results.items():
            f.write(f"| {module} | {result} |\n")

class warnings_to_list:
    """Context manager to capture warnings in a list."""

    def __init__(self):
        self.warnings = []

    def __enter__(self):
        import warnings
        self._showwarning = warnings.showwarning
        warnings.showwarning = self._append_warning
        return self.warnings

    def __exit__(self, *args):
        import warnings
        warnings.showwarning = self._showwarning

    def _append_warning(self, message, category, filename, lineno, file=None, line=None):
        self.warnings.append(Warning(message, category, filename, lineno))

class Warning:
    """Simple warning class to store warning information."""

    def __init__(self, message, category, filename, lineno):
        self.message = message
        self.category = category
        self.filename = filename
        self.lineno = lineno

def main():
    """Main function to execute tests."""
    logger.info("Starting reorganization tests")

    # Test canonical imports
    logger.info("Testing canonical imports...")
    canonical_results = test_canonical_imports()

    # Test deprecated imports
    logger.info("Testing deprecated imports...")
    deprecated_results = test_deprecated_imports()

    # Test all modules
    logger.info("Testing all modules...")
    all_modules_results = import_all_modules()

    # Write results
    logger.info(f"Writing test results to {TEST_RESULTS_FILE}")
    write_results(canonical_results, deprecated_results, all_modules_results)

    # Print summary
    canonical_success = sum(1 for r in canonical_results.values() if r.startswith("✅"))
    deprecated_success = sum(1 for r in deprecated_results.values() if r.startswith("✅"))
    all_success = sum(1 for r in all_modules_results.values() if r.startswith("✅"))

    logger.info(f"Canonical imports: {canonical_success}/{len(canonical_results)} successful")
    logger.info(f"Deprecated imports: {deprecated_success}/{len(deprecated_results)} successful")
    logger.info(f"All modules: {all_success}/{len(all_modules_results)} successful")

    if canonical_success < len(canonical_results) or all_success < len(all_modules_results):
        logger.error("Some tests failed. See report for details.")
        sys.exit(1)
    else:
        logger.info("All tests passed!")

if __name__ == "__main__":
    import warnings

    # Enable deprecation warnings
    warnings.simplefilter("always", DeprecationWarning)

    main()