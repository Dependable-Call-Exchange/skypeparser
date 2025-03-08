#!/usr/bin/env python3
"""
Script to fix import errors in test files.

This script updates imports in test files to match the current API.
"""

import os
import re
from pathlib import Path

# Define the replacements to make
REPLACEMENTS = [
    # Missing generate_report in src.analysis.reporting
    (
        r"from src\.analysis\.reporting import generate_report",
        "from src.analysis.reporting import SkypeReportGenerator",
    ),
    # Missing MockFileHandler in tests.mocks
    (
        r"from tests\.mocks import .*MockFileHandler",
        "from tests.mocks import MockContentExtractor, MockMessageHandler",
    ),
    # Missing EventCallHandler in src.utils.message_type_handlers
    (
        r"from src\.utils\.message_type_handlers import .*EventCallHandler",
        "from src.utils.message_type_handlers import TextMessageHandler, MediaMessageHandler, PollMessageHandler",
    ),
    # Missing read_file_object in src.utils.file_handler
    (
        r"from src\.utils\.file_handler import .*read_file_object",
        "from src.utils.file_handler import FileHandler",
    ),
    # Missing get_file_extension in src.utils.file_utils
    (
        r"from src\.utils\.file_utils import .*get_file_extension",
        "from src.utils.file_utils import safe_filename",
    ),
    # Missing PollHandler in src.utils.message_type_handlers
    (
        r"from src\.utils\.message_type_handlers import .*PollHandler",
        "from src.utils.message_type_handlers import PollMessageHandler",
    ),
    # Missing ParserModule in src.parser.parser_module
    (
        r"from src\.parser\.parser_module import .*ParserModule",
        "from src.parser.core_parser import parse_skype_export",
    ),
    # Missing extract_tar_contents in src.utils.file_handler
    (
        r"from src\.utils\.file_handler import .*extract_tar_contents",
        "from src.utils.file_handler import FileHandler",
    ),
]


def fix_imports_in_file(file_path):
    """Fix imports in a single file."""
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    original_content = content
    for pattern, replacement in REPLACEMENTS:
        content = re.sub(pattern, replacement, content)

    if content != original_content:
        print(f"Fixing imports in {file_path}")
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)
        return True
    return False


def fix_imports_in_directory(directory):
    """Fix imports in all Python files in a directory and its subdirectories."""
    fixed_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                file_path = os.path.join(root, file)
                if fix_imports_in_file(file_path):
                    fixed_files.append(file_path)
    return fixed_files


if __name__ == "__main__":
    # Fix imports in test files
    test_dir = Path(__file__).parent / "tests"
    fixed_files = fix_imports_in_directory(test_dir)

    print(f"Fixed imports in {len(fixed_files)} files:")
    for file in fixed_files:
        print(f"  - {file}")
