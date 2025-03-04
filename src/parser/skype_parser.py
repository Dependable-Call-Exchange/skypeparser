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
import os
import sys
import logging
from typing import Dict, List, Any, Optional

from ..utils.file_utils import safe_filename
from ..utils.file_handler import read_file, read_tarfile
from ..utils.dependencies import PSYCOPG2_AVAILABLE
from .core_parser import parse_skype_data, id_selector
from .file_output import export_conversations
from .exceptions import (
    SkypeParserError,
    FileOperationError,
    DataExtractionError,
    ExportError,
    InvalidInputError,
    TimestampParsingError,
    ContentParsingError,
    DatabaseOperationError
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Try to import the ETL pipeline
ETL_AVAILABLE = False
if PSYCOPG2_AVAILABLE:
    try:
        from ..db.etl_pipeline import SkypeETLPipeline
        ETL_AVAILABLE = True
    except ImportError:
        logger.warning("ETL pipeline module not available. Database operations will be disabled.")
else:
    logger.warning("psycopg2 is not available. Database operations will be disabled.")


def main():
    """
    Main function to parse Skype export data and output to files.
    """
    try:
        args = get_commandline_args()

        # Validate that the input file exists
        if not os.path.exists(args.filename):
            logger.error(f"File not found: {args.filename}")
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
            main_file = read_file(args.filename) if not args.tar else read_tarfile(args.filename, args.select_json)
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
                    db_port=args.db_port
                )

                # Run the ETL pipeline
                result = etl.run_pipeline(
                    input_file=args.filename,
                    is_tar=args.tar,
                    json_index=args.select_json,
                    output_dir=args.output_dir,
                    user_display_name=user_display_name
                )

                if result:
                    logger.info("ETL pipeline completed successfully.")
                    if not args.text_output and not args.output_format in ['json', 'csv']:
                        logger.info("\nAll done!")
                        return
                else:
                    logger.error("ETL pipeline failed.")
                    if not args.text_output and not args.output_format in ['json', 'csv']:
                        sys.exit(1)
            except Exception as e:
                logger.error(f"Error in ETL pipeline: {e}")
                if not args.text_output and not args.output_format in ['json', 'csv']:
                    sys.exit(1)
        elif args.store_db and not ETL_AVAILABLE:
            logger.error("Database storage requested but ETL pipeline is not available. "
                        "Please ensure psycopg2 is installed and the ETL pipeline module is accessible.")
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
            all_ids = list(structured_data['conversations'].keys())

            try:
                if args.choose:
                    try:
                        selected_ids = id_selector(all_ids)
                    except InvalidInputError as e:
                        logger.error(f"Error selecting conversations: {e}")
                        sys.exit(1)
                else:
                    try:
                        selected_indices = [int(idx) - 1 for idx in args.select_conversations.split(',')]
                        selected_ids = [all_ids[idx] for idx in selected_indices if 0 <= idx < len(all_ids)]
                        if not selected_ids:
                            logger.error("No valid conversations selected.")
                            sys.exit(1)
                    except (ValueError, IndexError) as e:
                        logger.error(f"Invalid conversation selection: {e}")
                        sys.exit(1)

                # Filter conversations
                for conv_id in selected_ids:
                    if conv_id in structured_data['conversations']:
                        filtered_conversations[conv_id] = structured_data['conversations'][conv_id]

                structured_data['conversations'] = filtered_conversations
            except Exception as e:
                logger.error(f"Error filtering conversations: {e}")
                sys.exit(1)

        # Export conversations
        try:
            export_conversations(
                structured_data,
                args.output_format,
                args.output_dir or '',
                args.overwrite,
                args.skip_existing,
                args.text_output
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
    command = argparse.ArgumentParser(description="Parse Skype chat history from JSON or TAR files")
    command.add_argument('filename',
                         help='The path/name to the Skype json/tar file you want to parse')
    command.add_argument('-c', '--choose',
                         action='store_true',
                         help="Use this flag to choose which conversations you'd like to parse interactively")
    command.add_argument('-t', '--tar',
                         action='store_true',
                         help='Use this flag to feed in a tar file')
    command.add_argument('-o', '--output-dir',
                         help='Directory to save the output files')
    command.add_argument('-u', '--user-display-name',
                         help='Your display name in the logs (skips interactive prompt)')
    command.add_argument('-s', '--select-conversations',
                         help='Comma-separated list of conversation indices to parse (e.g., "1,3,5")')
    command.add_argument('-j', '--select-json',
                         type=int,
                         help='Index of the JSON file to use if multiple are found in the tar (default: 0)')
    command.add_argument('--overwrite',
                         action='store_true',
                         help='Overwrite existing files without prompting')
    command.add_argument('--skip-existing',
                         action='store_true',
                         help='Skip existing files without prompting')
    command.add_argument('-v', '--verbose',
                         action='store_true',
                         help='Enable verbose logging')
    command.add_argument('-f', '--output-format',
                         choices=['text', 'json', 'csv'],
                         default='text',
                         help='Output format (text, json, or csv)')
    command.add_argument('--text-output',
                         action='store_true',
                         help='Generate text output in addition to structured output')

    # Database storage options
    db_group = command.add_argument_group('Database Storage Options')
    db_group.add_argument('--store-db',
                         action='store_true',
                         help='Store data in PostgreSQL database using ETL pipeline')
    db_group.add_argument('--db-name',
                         help='PostgreSQL database name')
    db_group.add_argument('--db-user',
                         help='PostgreSQL database user')
    db_group.add_argument('--db-password',
                         help='PostgreSQL database password')
    db_group.add_argument('--db-host',
                         default='localhost',
                         help='PostgreSQL database host (default: localhost)')
    db_group.add_argument('--db-port',
                         type=int,
                         default=5432,
                         help='PostgreSQL database port (default: 5432)')

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
