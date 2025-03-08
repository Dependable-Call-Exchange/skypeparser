#!/usr/bin/env python3
"""
Initialize Logging Script

This script initializes the centralized logging system for the application.
It can be imported and used in any entry point of the application.
"""

import os
import sys
import argparse
import json
from pathlib import Path

# Add the parent directory to the path so we can import the modules
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.utils.logging_config import configure_logging, load_logging_config


def initialize_logging(
    app_name="skype_parser",
    environment="development",
    log_level=None,
    log_file=None,
    log_dir="logs",
    config_file=None,
    enable_console=True,
    enable_file=True,
    enable_json=True
):
    """
    Initialize the centralized logging system.

    Args:
        app_name: Application name
        environment: Environment (development, testing, production)
        log_level: Logging level
        log_file: Log file path
        log_dir: Directory for log files
        config_file: Path to logging configuration file
        enable_console: Whether to enable console logging
        enable_file: Whether to enable file logging
        enable_json: Whether to use JSON formatting

    Returns:
        0 if successful, 1 if an error occurred
    """
    try:
        # Load configuration from file if provided
        config = None
        if config_file:
            if os.path.exists(config_file):
                config = load_logging_config(config_file)
            else:
                print(f"Warning: Config file not found: {config_file}")

        # Configure logging
        configure_logging(
            app_name=app_name,
            environment=environment,
            log_level=log_level,
            log_file=log_file,
            log_dir=log_dir,
            config=config,
            enable_console=enable_console,
            enable_file=enable_file,
            enable_json=enable_json
        )

        return 0
    except Exception as e:
        print(f"Error initializing logging: {e}")
        return 1


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Initialize logging system")
    parser.add_argument(
        "--app-name",
        default="skype_parser",
        help="Application name"
    )
    parser.add_argument(
        "--environment",
        default="development",
        choices=["development", "testing", "production"],
        help="Environment"
    )
    parser.add_argument(
        "--log-level",
        default=None,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level"
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Log file path"
    )
    parser.add_argument(
        "--log-dir",
        default="logs",
        help="Directory for log files"
    )
    parser.add_argument(
        "--config-file",
        default=None,
        help="Path to logging configuration file"
    )
    parser.add_argument(
        "--no-console",
        action="store_true",
        help="Disable console logging"
    )
    parser.add_argument(
        "--no-file",
        action="store_true",
        help="Disable file logging"
    )
    parser.add_argument(
        "--no-json",
        action="store_true",
        help="Disable JSON formatting"
    )

    args = parser.parse_args()

    # Initialize logging
    return initialize_logging(
        app_name=args.app_name,
        environment=args.environment,
        log_level=args.log_level,
        log_file=args.log_file,
        log_dir=args.log_dir,
        config_file=args.config_file,
        enable_console=not args.no_console,
        enable_file=not args.no_file,
        enable_json=not args.no_json
    )


if __name__ == "__main__":
    sys.exit(main())