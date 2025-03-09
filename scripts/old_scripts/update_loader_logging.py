#!/usr/bin/env python3
"""
Update Loader Logging Script

This script updates the Loader component to use the new centralized logging system.
"""

import os
import sys
import re
from pathlib import Path

# Add the parent directory to the path so we can import the modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def update_loader_logging():
    """Update the Loader component to use the new centralized logging system."""
    # Path to the loader file
    loader_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "src", "db", "etl", "loader.py"
    )

    # Check if the file exists
    if not os.path.exists(loader_path):
        print(f"Error: Loader file not found at {loader_path}")
        return 1

    # Read the file
    with open(loader_path, "r") as f:
        content = f.read()

    # Update imports
    content = re.sub(
        r"import logging\n",
        "import logging\nimport time\nfrom src.utils.new_structured_logging import get_logger, log_execution_time, with_context, LogContext\n",
        content
    )

    # Update logger initialization
    content = re.sub(
        r"logger = logging\.getLogger\(__name__\)",
        "logger = get_logger(__name__)",
        content
    )

    # Add context to load method
    content = re.sub(
        r"def load\(([^)]*)\):\n([^\"]*)(\"\"\"[^\"]*\"\"\")",
        r"@log_execution_time(level=logging.INFO)\n    @with_context(operation='load')\n    def load(\1):\n\2\3",
        content
    )

    # Add context to store methods
    content = re.sub(
        r"def _store_raw_export\(([^)]*)\):\n([^\"]*)(\"\"\"[^\"]*\"\"\")",
        r"@with_context(operation='store_raw_export')\n    def _store_raw_export(\1):\n\2\3",
        content
    )

    content = re.sub(
        r"def _store_conversations\(([^)]*)\):\n([^\"]*)(\"\"\"[^\"]*\"\"\")",
        r"@with_context(operation='store_conversations')\n    def _store_conversations(\1):\n\2\3",
        content
    )

    content = re.sub(
        r"def _store_messages\(([^)]*)\):\n([^\"]*)(\"\"\"[^\"]*\"\"\")",
        r"@with_context(operation='store_messages')\n    def _store_messages(\1):\n\2\3",
        content
    )

    # Add context to transaction methods
    content = re.sub(
        r"def _begin_transaction\(([^)]*)\):\n([^\"]*)(\"\"\"[^\"]*\"\"\")",
        r"@with_context(operation='begin_transaction')\n    def _begin_transaction(\1):\n\2\3",
        content
    )

    content = re.sub(
        r"def _commit_transaction\(([^)]*)\):\n([^\"]*)(\"\"\"[^\"]*\"\"\")",
        r"@with_context(operation='commit_transaction')\n    def _commit_transaction(\1):\n\2\3",
        content
    )

    content = re.sub(
        r"def _rollback_transaction\(([^)]*)\):\n([^\"]*)(\"\"\"[^\"]*\"\"\")",
        r"@with_context(operation='rollback_transaction')\n    def _rollback_transaction(\1):\n\2\3",
        content
    )

    # Update logging in load method
    content = re.sub(
        r"logger\.info\(f\"Data loaded successfully with export ID: {export_id}\"\)",
        r"logger.info(f\"Data loaded successfully with export ID: {export_id}\", extra={'export_id': export_id, 'duration_ms': (self._metrics['end_time'] - self._metrics['start_time']) * 1000})",
        content
    )

    # Update logging in error handling
    content = re.sub(
        r"logger\.error\(f\"Error loading data: {e}\"\)",
        r"logger.error(f\"Error loading data: {e}\", exc_info=True, extra={'error': str(e)})",
        content
    )

    # Write the updated content back to the file
    with open(loader_path, "w") as f:
        f.write(content)

    print(f"Updated Loader component at {loader_path} to use the new centralized logging system")
    return 0


def main():
    """Main function."""
    sys.exit(update_loader_logging())


if __name__ == "__main__":
    main()