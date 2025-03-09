#!/usr/bin/env python3
"""
Tar Extractor Utility

A command-line tool for extracting and listing contents of tar files.
This script demonstrates the use of the file_handler module.
"""

import argparse
import logging
import os
import sys
from src.file_handler import extract_tar_contents, list_tar_contents, read_tarfile

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Extract or list contents of a tar file'
    )

    parser.add_argument(
        'tar_file',
        help='Path to the tar file'
    )

    parser.add_argument(
        '-o', '--output-dir',
        help='Directory to extract files to'
    )

    parser.add_argument(
        '-p', '--pattern',
        help='Regex pattern to filter files'
    )

    parser.add_argument(
        '-l', '--list',
        action='store_true',
        help='List contents without extracting'
    )

    parser.add_argument(
        '-j', '--json',
        action='store_true',
        help='Extract and parse the first JSON file (or use --select-json)'
    )

    parser.add_argument(
        '-s', '--select-json',
        type=int,
        help='Index of the JSON file to extract if multiple are found'
    )

    return parser.parse_args()

def main():
    """Main entry point for the script."""
    args = parse_args()

    # Validate that the input file exists
    if not os.path.exists(args.tar_file):
        logger.error(f"File not found: {args.tar_file}")
        sys.exit(1)

    try:
        if args.list:
            # List contents of the tar file
            contents = list_tar_contents(args.tar_file, args.pattern)
            logger.info(f"Contents of {args.tar_file}:")
            for i, item in enumerate(contents, 1):
                logger.info(f"{i}: {item}")
            logger.info(f"Total: {len(contents)} items")

        elif args.json:
            # Extract and parse JSON file
            try:
                data = read_tarfile(args.tar_file, args.select_json)
                logger.info(f"Successfully extracted and parsed JSON from {args.tar_file}")
                # Print some basic info about the JSON data
                if isinstance(data, dict):
                    logger.info(f"JSON contains {len(data)} top-level keys")
                    logger.info(f"Top-level keys: {', '.join(data.keys())}")
                else:
                    logger.info(f"JSON contains {len(data)} items")
            except Exception as e:
                logger.error(f"Error extracting JSON: {e}")
                sys.exit(1)

        else:
            # Extract files from the tar
            if not args.output_dir:
                logger.error("Output directory is required for extraction")
                sys.exit(1)

            extracted = extract_tar_contents(args.tar_file, args.output_dir, args.pattern)
            logger.info(f"Extracted {len(extracted)} files to {args.output_dir}")

    except Exception as e:
        logger.error(f"Error processing tar file: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()