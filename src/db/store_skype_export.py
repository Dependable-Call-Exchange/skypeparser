#!/usr/bin/env python3
"""
Store Skype Export Data in PostgreSQL

DEPRECATED: This module is deprecated. Please use the SkypeETLPipeline class instead.
See src/db/etl_pipeline.py for the recommended approach.

This script takes a Skype export file (TAR or JSON) and stores both the raw and cleaned
versions in PostgreSQL databases.

Usage:
    python store_skype_export.py -f <skype_export_file> -u <your_display_name> -d <database_name>
                                [-H <host>] [-P <port>] [-U <username>] [-W <password>]
                                [--select-json <json_file>] [--create-tables]

Example:
    python store_skype_export.py -f 8_live_dave.leathers113_export.tar -u "David Leathers"
                                -d skype_logs -U postgres --create-tables
"""

import os
import sys
import json
import argparse
import logging
import warnings
from datetime import datetime
from typing import Dict, Optional
from pathlib import Path

from ..parser.parser_module import read_file, read_tarfile
from ..parser.skype_parser import clean_message_content
from .raw_storage import SkypeDataStorage

# Issue deprecation warning
warnings.warn(
    "The store_skype_export module is deprecated. "
    "Please use the SkypeETLPipeline class instead.",
    DeprecationWarning, stacklevel=2
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('skype_export.log')
    ]
)
logger = logging.getLogger('store-skype-export')

def load_config(config_file: Optional[str] = None) -> Dict:
    """
    Load configuration from file or environment variables.

    Args:
        config_file: Optional path to JSON config file

    Returns:
        Dict: Configuration parameters
    """
    config = {
        'host': os.getenv('POSTGRES_HOST', 'localhost'),
        'port': int(os.getenv('POSTGRES_PORT', '5432')),
        'dbname': os.getenv('POSTGRES_DB'),
        'user': os.getenv('POSTGRES_USER'),
        'password': os.getenv('POSTGRES_PASSWORD'),
    }

    if config_file and os.path.exists(config_file):
        with open(config_file) as f:
            file_config = json.load(f)
            config.update(file_config)

    return config

def validate_data(data: Dict) -> bool:
    """
    Validate the structure of Skype export data.

    Args:
        data: Dictionary containing Skype data

    Returns:
        bool: True if valid, False otherwise
    """
    required_fields = ['userId', 'exportDate', 'conversations']
    if not all(field in data for field in required_fields):
        logger.error(f"Missing required fields: {[f for f in required_fields if f not in data]}")
        return False

    if not isinstance(data['conversations'], list):
        logger.error("'conversations' field must be a list")
        return False

    for conv in data['conversations']:
        if 'id' not in conv:
            logger.error("Conversation missing 'id' field")
            return False
        if 'MessageList' not in conv or not isinstance(conv['MessageList'], list):
            logger.error(f"Conversation {conv.get('id')} has invalid 'MessageList'")
            return False

    return True

def clean_skype_data(raw_data: Dict) -> Dict:
    """
    Clean the Skype data by processing messages and removing markup.

    Args:
        raw_data: Dictionary containing the raw Skype data

    Returns:
        Dict: Cleaned version of the Skype data
    """
    if not validate_data(raw_data):
        raise ValueError("Invalid Skype export data structure")

    conversations = raw_data.get('conversations', [])
    cleaned_conversations = []
    total_messages = sum(len(conv.get('MessageList', [])) for conv in conversations)
    processed_messages = 0

    for convo in conversations:
        message_list = convo.get('MessageList', [])
        cleaned_messages = []

        for msg in message_list:
            msg_type = msg.get('messagetype', 'Unknown')
            content = msg.get('content', '')

            try:
                # Clean or replace with placeholders
                cleaned_content = clean_message_content(content, msg_type)

                # Build new message object
                new_msg = msg.copy()
                new_msg['content'] = cleaned_content
                cleaned_messages.append(new_msg)

            except Exception as e:
                logger.warning(f"Error cleaning message in conversation {convo.get('id')}: {e}")
                # Keep original content if cleaning fails
                cleaned_messages.append(msg)

            processed_messages += 1
            if processed_messages % 1000 == 0:
                logger.info(f"Processed {processed_messages}/{total_messages} messages")

        new_convo = convo.copy()
        new_convo['MessageList'] = cleaned_messages
        cleaned_conversations.append(new_convo)

    cleaned_data = raw_data.copy()
    cleaned_data['conversations'] = cleaned_conversations
    return cleaned_data

def get_commandline_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Store Skype export in PostgreSQL',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # File input options
    parser.add_argument('-f', '--filename', required=True,
                       help='Path to the Skype export file')
    parser.add_argument('-t', '--tar', action='store_true',
                       help='Input file is a tar archive')
    parser.add_argument('--select-json',
                       help='Select a specific JSON file from the tar archive')

    # Configuration options
    parser.add_argument('--config',
                       help='Path to JSON configuration file')

    # Database options
    parser.add_argument('-d', '--dbname',
                       help='PostgreSQL database name (overrides config)')
    parser.add_argument('-H', '--host',
                       help='PostgreSQL host (overrides config)')
    parser.add_argument('-P', '--port', type=int,
                       help='PostgreSQL port (overrides config)')
    parser.add_argument('-U', '--username',
                       help='PostgreSQL username (overrides config)')
    parser.add_argument('-W', '--password',
                       help='PostgreSQL password (overrides config)')

    return parser.parse_args()

def main():
    """Main function to store Skype export data."""
    try:
        args = get_commandline_args()

        # Load configuration
        config = load_config(args.config)

        # Override config with command line arguments
        if args.dbname:
            config['dbname'] = args.dbname
        if args.host:
            config['host'] = args.host
        if args.port:
            config['port'] = args.port
        if args.username:
            config['user'] = args.username
        if args.password:
            config['password'] = args.password

        # Validate configuration
        required_config = ['dbname', 'user', 'password']
        missing_config = [k for k in required_config if not config.get(k)]
        if missing_config:
            logger.error(f"Missing required configuration: {missing_config}")
            sys.exit(1)

        # Validate that the input file exists
        input_file = Path(args.filename)
        if not input_file.exists():
            logger.error(f"File not found: {input_file}")
            sys.exit(1)

        # Read the Skype export file
        try:
            raw_data = (
                read_file(str(input_file))
                if not args.tar
                else read_tarfile(str(input_file), args.select_json)
            )
        except Exception as e:
            logger.error(f"Error reading file: {e}")
            sys.exit(1)

        # Validate and clean the data
        try:
            if not validate_data(raw_data):
                logger.error("Invalid Skype export data")
                sys.exit(1)

            cleaned_data = clean_skype_data(raw_data)
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            sys.exit(1)

        # Initialize storage with retry logic
        max_retries = 3
        retry_count = 0
        storage = None

        while retry_count < max_retries:
            try:
                storage = SkypeDataStorage(config)
                break
            except Exception as e:
                retry_count += 1
                if retry_count == max_retries:
                    logger.error(f"Failed to initialize storage after {max_retries} attempts: {e}")
                    sys.exit(1)
                logger.warning(f"Attempt {retry_count} failed, retrying...")

        try:
            # Store both raw and cleaned data
            raw_id, cleaned_id = storage.store_skype_export(
                raw_data=raw_data,
                cleaned_data=cleaned_data,
                file_name=input_file.name,
                export_date=datetime.now()
            )

            logger.info("Successfully stored Skype export data:")
            logger.info(f"  Raw data ID: {raw_id}")
            logger.info(f"  Cleaned data ID: {cleaned_id}")

        except Exception as e:
            logger.error(f"Failed to store data: {e}")
            sys.exit(1)

        finally:
            if storage:
                storage.close()

    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()