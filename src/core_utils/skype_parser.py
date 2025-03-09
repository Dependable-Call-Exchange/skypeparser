#!/usr/bin/env python3
"""
Skype Parser Command-line Interface

This script provides a command-line interface for parsing Skype export files
and exporting conversations in various formats. It can also store data in a
PostgreSQL database using the ETL pipeline.

Usage:
    python -m src.parser.skype_parser path/to/skype_export.tar -t -o output_dir -u "Your Name"
    python -m src.parser.skype_parser path/to/skype_export.tar -t -f json -o output_dir -u "Your Name"
    python -m src.parser.skype_parser path/to/skype_export.tar -t --store-db --db-name skype_archive --db-user postgres
"""

import argparse
import json
import logging
import os
import sys

from src.core_utils.dependencies import PSYCOPG2_AVAILABLE
from src.data_handlers.file_handler import read_file, read_tarfile
from src.core_parser import id_selector, parse_skype_data
from src.exceptions import DataExtractionError, ExportError, FileOperationError, InvalidInputError
from src.file_output import export_conversations

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Try to import the ETL pipeline
ETL_AVAILABLE = False
if PSYCOPG2_AVAILABLE:
    try:
from src.core_utils.pipeline_manager import ETLPipeline

        ETL_AVAILABLE = True
    except ImportError:
        logger.warning(
            "ETL pipeline module not available. Database operations will be disabled."
        )
else:
    logger.warning("psycopg2 is not available. Database operations will be disabled.")


class SkypeETLPipeline:
    """
    Wrapper for the ETL pipeline to process Skype export data.

    This class provides a simplified interface for using the ETL pipeline
    with Skype export data, handling database connections and configuration.
    """

    def __init__(
        self,
        db_name: str,
        db_user: str,
        db_password: str = None,
        db_host: str = "localhost",
        db_port: int = 5432,
        output_dir: str = None,
        memory_limit_mb: int = 1024,
        parallel_processing: bool = True,
    ):
        """
        Initialize the Skype ETL pipeline.

        Args:
            db_name: Database name
            db_user: Database user
            db_password: Database password
            db_host: Database host
            db_port: Database port
            output_dir: Output directory for exported files
            memory_limit_mb: Memory limit in MB
            parallel_processing: Whether to use parallel processing
        """
        if not ETL_AVAILABLE:
            raise ImportError(
                "ETL pipeline is not available. Please ensure psycopg2 is installed."
            )

        # Create database configuration
        self.db_config = {
            "dbname": db_name,
            "user": db_user,
            "password": db_password,
            "host": db_host,
            "port": db_port,
        }

        # Store other parameters
        self.output_dir = output_dir
        self.memory_limit_mb = memory_limit_mb
        self.parallel_processing = parallel_processing

        # Initialize the ETL pipeline
        self.pipeline = ETLPipeline(
            db_config=self.db_config,
            output_dir=self.output_dir,
            memory_limit_mb=self.memory_limit_mb,
            parallel_processing=self.parallel_processing,
        )

        logger.info("Initialized Skype ETL pipeline")

    def run_pipeline(
        self,
        input_file: str,
        is_tar: bool = False,
        json_index: int = None,
        output_dir: str = None,
        user_display_name: str = None,
    ) -> bool:
        """
        Run the ETL pipeline on a Skype export file.

        Args:
            input_file: Path to the input file
            is_tar: Whether the input file is a TAR archive
            json_index: Index of the JSON file to extract from TAR (if applicable)
            output_dir: Output directory for exported files
            user_display_name: User display name for the export

        Returns:
            True if the pipeline completed successfully, False otherwise
        """
        try:
            # Override output directory if provided
            if output_dir:
                self.pipeline.output_dir = output_dir

            # Run the pipeline
            result = self.pipeline.run_pipeline(
                file_path=input_file, user_display_name=user_display_name
            )

            # Check if the pipeline completed successfully
            if result and "export_id" in result:
                logger.info(
                    f"ETL pipeline completed successfully. Export ID: {result['export_id']}"
                )
                return True
            else:
                logger.error("ETL pipeline failed to complete")
                return False

        except Exception as e:
            logger.error(f"Error running ETL pipeline: {e}")
            return False


def main():
    """
    Main function to parse Skype export data and output to files.
    """
    try:
        args = get_commandline_args()

        # Validate that the input file exists
        if not os.path.exists(args.input_file):
            logger.error(f"File not found: {args.input_file}")
            sys.exit(1)

        # Create output directory if specified
        if args.output_dir:
            try:
                os.makedirs(args.output_dir, exist_ok=True)
            except Exception as e:
                logger.error(f"Error creating output directory {args.output_dir}: {e}")
                sys.exit(1)

        # Get user display name from command line or use default
        user_display_name = args.user_display_name
        if not user_display_name:
            logger.info("No user display name provided, using default: 'Me'")
            user_display_name = "Me"

        # Read the Skype export file
        try:
            main_file = (
                read_file(args.input_file)
                if not args.extract_tar
                else read_tarfile(args.input_file, args.select_json)
            )
        except (json.JSONDecodeError, KeyError, IndexError) as e:
            logger.error(f"Error reading file: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error reading file: {e}")
            sys.exit(1)

        # If database storage is requested, use the ETL pipeline
        if args.store_db and ETL_AVAILABLE:
            try:
                logger.info("Initializing ETL pipeline for database storage...")
                etl = SkypeETLPipeline(
                    db_name=args.db_name,
                    db_user=args.db_user,
                    db_password=args.db_password,
                    db_host=args.db_host,
                    db_port=args.db_port,
                )

                # Run the ETL pipeline
                result = etl.run_pipeline(
                    input_file=args.input_file,
                    is_tar=args.extract_tar,
                    json_index=args.select_json,
                    output_dir=args.output_dir,
                    user_display_name=user_display_name,
                )

                if result:
                    logger.info("ETL pipeline completed successfully.")
                    if not args.text_output and args.format not in ["json", "csv"]:
                        logger.info("\nAll done!")
                        return
                else:
                    logger.error("ETL pipeline failed.")
                    if not args.text_output and args.format not in ["json", "csv"]:
                        sys.exit(1)
            except Exception as e:
                logger.error(f"Error in ETL pipeline: {e}")
                if not args.text_output and args.format not in ["json", "csv"]:
                    sys.exit(1)
        elif args.store_db and not ETL_AVAILABLE:
            logger.error(
                "Database storage requested but ETL pipeline is not available. "
                "Please ensure psycopg2 is installed and the ETL pipeline module is accessible."
            )
            sys.exit(1)

        # Parse the Skype export data
        try:
            structured_data = parse_skype_data(main_file, user_display_name)
        except InvalidInputError as e:
            logger.error(f"Invalid input data: {e}")
            sys.exit(1)
        except DataExtractionError as e:
            logger.error(f"Error extracting data: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error parsing Skype data: {e}")
            sys.exit(1)

        # Filter conversations if requested
        if args.choose or args.select_conversations:
            filtered_conversations = {}
            all_ids = list(structured_data["conversations"].keys())

            try:
                if args.choose:
                    try:
                        selected_ids = id_selector(all_ids)
                    except InvalidInputError as e:
                        logger.error(f"Error selecting conversations: {e}")
                        sys.exit(1)
                else:
                    try:
                        selected_indices = [
                            int(idx) - 1 for idx in args.select_conversations.split(",")
                        ]
                        selected_ids = [
                            all_ids[idx]
                            for idx in selected_indices
                            if 0 <= idx < len(all_ids)
                        ]
                        if not selected_ids:
                            logger.error("No valid conversations selected.")
                            sys.exit(1)
                    except (ValueError, IndexError) as e:
                        logger.error(f"Invalid conversation selection: {e}")
                        sys.exit(1)

                # Filter conversations
                for conv_id in selected_ids:
                    if conv_id in structured_data["conversations"]:
                        filtered_conversations[conv_id] = structured_data[
                            "conversations"
                        ][conv_id]

                structured_data["conversations"] = filtered_conversations
            except Exception as e:
                logger.error(f"Error filtering conversations: {e}")
                sys.exit(1)

        # Export conversations
        try:
            export_conversations(
                structured_data,
                args.format,
                args.output_dir or "",
                args.overwrite,
                args.skip_existing,
                args.text_output,
            )
            logger.info("\nAll done!")
        except FileOperationError as e:
            logger.error(f"File operation error during export: {e}")
            sys.exit(1)
        except ExportError as e:
            logger.error(f"Error exporting conversations: {e}")
            sys.exit(1)
        except Exception as e:
            logger.error(f"Unexpected error during export: {e}")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("\nOperation cancelled by user.")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


def get_commandline_args():
    """
    Parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed command-line arguments
    """
    command = argparse.ArgumentParser(
        description="Parse Skype chat history from JSON or TAR files"
    )
    command.add_argument(
        "input_file", help="The path/name to the Skype json/tar file you want to parse"
    )
    command.add_argument(
        "-c",
        "--choose",
        action="store_true",
        help="Use this flag to choose which conversations you'd like to parse interactively",
    )
    command.add_argument(
        "-t",
        "--extract-tar",
        action="store_true",
        dest="extract_tar",
        help="Use this flag to feed in a tar file",
    )
    command.add_argument(
        "-o", "--output-dir", help="Directory to save the output files"
    )
    command.add_argument(
        "-u",
        "--user-display-name",
        help="Your display name in the logs (skips interactive prompt)",
    )
    command.add_argument(
        "-s",
        "--select-conversations",
        help='Comma-separated list of conversation indices to parse (e.g., "1,3,5")',
    )
    command.add_argument(
        "-j",
        "--select-json",
        type=int,
        help="Index of the JSON file to use if multiple are found in the tar (default: 0)",
    )
    command.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files without prompting",
    )
    command.add_argument(
        "--skip-existing",
        action="store_true",
        help="Skip existing files without prompting",
    )
    command.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    command.add_argument(
        "-f",
        "--format",
        choices=["text", "json", "csv", "all"],
        default="all",
        help="Output format (text, json, csv, or all)",
    )
    command.add_argument(
        "--text-output",
        action="store_true",
        help="Generate text output in addition to structured output",
    )

    # Database storage options
    db_group = command.add_argument_group("Database Storage Options")
    db_group.add_argument(
        "--store-db",
        action="store_true",
        help="Store data in PostgreSQL database using ETL pipeline",
    )
    db_group.add_argument("--db-name", help="PostgreSQL database name")
    db_group.add_argument("--db-user", help="PostgreSQL database user")
    db_group.add_argument("--db-password", help="PostgreSQL database password")
    db_group.add_argument(
        "--db-host",
        default="localhost",
        help="PostgreSQL database host (default: localhost)",
    )
    db_group.add_argument(
        "--db-port",
        type=int,
        default=5432,
        help="PostgreSQL database port (default: 5432)",
    )

    args = command.parse_args()

    # Set logging level based on verbose flag
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    # Validate database arguments if store_db is specified
    if args.store_db and ETL_AVAILABLE:
        if not args.db_name:
            logger.error("Database name (--db-name) is required when using --store-db")
            sys.exit(1)
        if not args.db_user:
            logger.error("Database user (--db-user) is required when using --store-db")
            sys.exit(1)

    return args


if __name__ == "__main__":
    main()
