#!/usr/bin/env python3
"""
Command-line script to run the modular ETL pipeline.

This script provides a command-line interface for the modular ETL pipeline,
supporting configuration from both configuration files and command-line options.
"""

import argparse
import json
import logging
import os
import sys
import time
from typing import Any, Dict, Optional

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.db.etl.modular_pipeline import ModularETLPipeline
from src.utils.config import load_config
from src.utils.error_handling import ErrorContext, handle_errors, report_error
from src.utils.structured_logging import get_logger, setup_logging

# Initialize logger
logger = get_logger(__name__)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Run the modular ETL pipeline")

    # File inputs
    parser.add_argument(
        "-f", "--file", help="Path to the Skype export file", required=False
    )

    # User information
    parser.add_argument("-u", "--user", help="User display name", required=False)

    # Database configuration
    parser.add_argument("-d", "--database", help="Database name", required=False)
    parser.add_argument("-H", "--host", help="Database host", default="localhost")
    parser.add_argument("-P", "--port", help="Database port", type=int, default=5432)
    parser.add_argument("-U", "--user-db", help="Database user", required=False)
    parser.add_argument("-p", "--password", help="Database password", required=False)

    # Output configuration
    parser.add_argument("-o", "--output-dir", help="Output directory", default="output")

    # Performance options
    parser.add_argument(
        "--parallel",
        help="Enable parallel processing",
        action="store_true",
        default=None,
    )
    parser.add_argument(
        "--no-parallel",
        help="Disable parallel processing",
        action="store_false",
        dest="parallel",
    )
    parser.add_argument(
        "--chunk-size", help="Size of chunks for processing", type=int, default=None
    )
    parser.add_argument(
        "--memory-limit", help="Memory limit in MB", type=int, default=None
    )

    # Logging options
    parser.add_argument(
        "--log-level",
        help="Logging level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default=None,
    )
    parser.add_argument("--log-file", help="Log file path", default=None)
    parser.add_argument(
        "--json-logs",
        help="Output logs in JSON format",
        action="store_true",
        default=None,
    )

    # Attachment options
    parser.add_argument(
        "--download-attachments",
        help="Download attachments",
        action="store_true",
        default=None,
    )
    parser.add_argument(
        "--attachments-dir", help="Directory for attachments", default=None
    )

    # Configuration file
    parser.add_argument(
        "-c", "--config", help="Path to configuration file", default=None
    )

    # Checkpoint options
    parser.add_argument(
        "--resume",
        help="Resume from last checkpoint",
        action="store_true",
        default=False,
    )
    parser.add_argument(
        "--checkpoint-file", help="Path to checkpoint file", default=None
    )

    return parser.parse_args()


def build_config(args) -> Dict[str, Any]:
    """
    Build configuration dictionary from command-line arguments and config file.

    Args:
        args: Command-line arguments

    Returns:
        Configuration dictionary
    """
    # Start with default configuration
    config = load_config()

    # Load configuration from file if specified
    if args.config:
        with open(args.config, "r") as f:
            file_config = json.load(f)
            config.update(file_config)

    # Update with command-line arguments
    # Database configuration
    if args.database:
        config["database"]["dbname"] = args.database
    if args.host:
        config["database"]["host"] = args.host
    if args.port:
        config["database"]["port"] = args.port
    if args.user_db:
        config["database"]["user"] = args.user_db
    if args.password:
        config["database"]["password"] = args.password

    # Output configuration
    if args.output_dir:
        config["output"]["directory"] = args.output_dir

    # Performance options
    if args.parallel is not None:
        config["use_parallel_processing"] = args.parallel
    if args.chunk_size:
        config["chunk_size"] = args.chunk_size
    if args.memory_limit:
        config["memory_limit_mb"] = args.memory_limit

    # Logging options
    if args.log_level:
        config["logging"]["level"] = args.log_level
    if args.log_file:
        config["logging"]["file"] = args.log_file
    if args.json_logs is not None:
        config["logging"]["json_format"] = args.json_logs

    # Attachment options
    if args.download_attachments is not None:
        if "attachments" not in config:
            config["attachments"] = {}
        config["attachments"]["download"] = args.download_attachments
    if args.attachments_dir:
        if "attachments" not in config:
            config["attachments"] = {}
        config["attachments"]["directory"] = args.attachments_dir

    return config


@handle_errors(log_level="ERROR", default_message="Error running ETL pipeline")
def run_pipeline(
    config: Dict[str, Any],
    file_path: Optional[str],
    user_display_name: Optional[str],
    resume: bool = False,
    checkpoint_file: Optional[str] = None,
) -> int:
    """
    Run the ETL pipeline with the specified configuration.

    Args:
        config: Configuration dictionary
        file_path: Path to the Skype export file
        user_display_name: User display name
        resume: Whether to resume from a checkpoint
        checkpoint_file: Path to checkpoint file

    Returns:
        Exit code
    """
    with ErrorContext(
        operation="run_pipeline", file_path=file_path, user=user_display_name
    ):
        # Configure structured logging
        setup_logging(
            level=config["logging"]["level"],
            log_file=config["logging"]["file"],
            json_format=config["logging"].get("json_format", False),
            structured=True,
        )

        logger.info("Starting ETL pipeline")

        # Create pipeline from configuration
        pipeline = ModularETLPipeline.from_config(config)

        # Run the pipeline
        start_time = time.time()
        result = pipeline.run_pipeline(
            file_path=file_path,
            user_display_name=user_display_name,
            resume_from_checkpoint=resume,
        )
        end_time = time.time()

        # Log results
        duration = end_time - start_time
        if result["status"] == "completed":
            logger.info(
                f"ETL pipeline completed successfully in {duration:.2f} seconds"
            )

            # Log metrics
            metrics = result.get("metrics", {})
            if "phase_metrics" in metrics:
                phase_metrics = metrics["phase_metrics"]
                for phase, phase_result in phase_metrics.items():
                    if isinstance(phase_result, dict):
                        logger.info(
                            f"Phase {phase}: {phase_result.get('status', 'unknown')}"
                        )

                        if "conversations_processed" in phase_result:
                            logger.info(
                                f"  Conversations processed: {phase_result['conversations_processed']}"
                            )

                        if "messages_processed" in phase_result:
                            logger.info(
                                f"  Messages processed: {phase_result['messages_processed']}"
                            )

            logger.info(f"Export ID: {result.get('export_id', 'unknown')}")
            return 0
        else:
            logger.error(f"ETL pipeline failed: {result.get('error', 'unknown error')}")
            return 1


def main():
    """Main entry point."""
    try:
        # Parse command-line arguments
        args = parse_args()

        # Build configuration
        config = build_config(args)

        # Validate required arguments
        if not args.file and not args.resume:
            print("Error: Either --file or --resume is required", file=sys.stderr)
            return 1

        # Run the pipeline
        return run_pipeline(
            config=config,
            file_path=args.file,
            user_display_name=args.user,
            resume=args.resume,
            checkpoint_file=args.checkpoint_file,
        )

    except KeyboardInterrupt:
        print("\nOperation cancelled by user", file=sys.stderr)
        return 130

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
