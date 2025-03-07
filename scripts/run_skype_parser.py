#!/usr/bin/env python3
"""
Skype Parser Wrapper Script

This script serves as the main entry point for the Skype Parser application.
It ensures that all necessary patches are applied in the correct order.

Usage:
    python scripts/run_skype_parser.py -f <export_file> -u <user_display_name> [-o <output_directory>] [-v]

Example:
    python scripts/run_skype_parser.py -f 8_live_dave.leathers113_export.tar -u "David Leathers" -v
"""

import logging
import os
import sys
from typing import List

# Configure basic logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("skype_parser_wrapper")


def main(args: List[str] = None) -> int:
    """
    Main entry point for the application.

    Args:
        args: Command line arguments (defaults to sys.argv[1:])

    Returns:
        Exit code (0 for success, non-zero for errors)
    """
    # Set up sys.path
    root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    sys.path.insert(0, root_dir)

    try:
        # First import the monkey patch to ensure it's applied
        logger.info("Applying necessary patches...")
        from scripts.monkey_patch import logger as patch_logger

        # Import and run the main script
        logger.info("Initializing parser...")
        from scripts.custom_etl_script import main as run_parser

        # Run the parser with the provided arguments
        result = run_parser()

        # Check if parsing was successful
        if not result.get("success", False):
            logger.error(f"Parser failed: {result.get('error', 'Unknown error')}")
            if "details" in result:
                logger.error(f"Details: {result.get('details')}")
            return 1

        # Return success
        return 0

    except ImportError as e:
        logger.error(f"Error importing required modules: {e}")
        return 2
    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=True)
        return 3


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
