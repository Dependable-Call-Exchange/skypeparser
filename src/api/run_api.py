#!/usr/bin/env python
"""
Run the Skype Parser API server.

This script provides a command-line interface for running the Skype Parser API server.
"""

import os
import sys
import argparse
import logging
from pathlib import Path

# Add the parent directory to the path to allow importing from src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.api.skype_api import SkypeParserAPI

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='Run the Skype Parser API server.')

    parser.add_argument(
        '--host',
        type=str,
        default='0.0.0.0',
        help='Host to bind to (default: 0.0.0.0)'
    )

    parser.add_argument(
        '--port',
        type=int,
        default=5000,
        help='Port to bind to (default: 5000)'
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='Run in debug mode'
    )

    parser.add_argument(
        '--upload-folder',
        type=str,
        help='Directory for temporary file uploads'
    )

    parser.add_argument(
        '--output-folder',
        type=str,
        help='Directory for ETL pipeline output'
    )

    parser.add_argument(
        '--api-key',
        type=str,
        help='API key for authentication (default: from API_KEY environment variable)'
    )

    parser.add_argument(
        '--db-host',
        type=str,
        default='localhost',
        help='Database host (default: localhost)'
    )

    parser.add_argument(
        '--db-port',
        type=int,
        default=5432,
        help='Database port (default: 5432)'
    )

    parser.add_argument(
        '--db-name',
        type=str,
        help='Database name (default: from DB_NAME environment variable)'
    )

    parser.add_argument(
        '--db-user',
        type=str,
        help='Database user (default: from DB_USER environment variable)'
    )

    parser.add_argument(
        '--db-password',
        type=str,
        help='Database password (default: from DB_PASSWORD environment variable)'
    )

    parser.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default='INFO',
        help='Logging level (default: INFO)'
    )

    return parser.parse_args()


def main():
    """Run the API server."""
    args = parse_args()

    # Set up logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Set up database configuration
    db_config = {
        'host': args.db_host,
        'port': args.db_port,
        'dbname': args.db_name or os.environ.get('DB_NAME'),
        'user': args.db_user or os.environ.get('DB_USER'),
        'password': args.db_password or os.environ.get('DB_PASSWORD')
    }

    # Check if API key is provided
    api_key = args.api_key or os.environ.get('API_KEY')
    if not api_key:
        logger.warning('No API key provided. API endpoints will be accessible without authentication.')

    # Create and run the API server
    api = SkypeParserAPI(
        upload_folder=args.upload_folder,
        output_folder=args.output_folder,
        db_config=db_config,
        api_key=api_key
    )

    logger.info(f'Starting API server with SocketIO on {args.host}:{args.port}')
    api.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == '__main__':
    main()