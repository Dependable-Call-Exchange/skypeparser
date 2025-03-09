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
from datetime import datetime

# Add the parent directory to the path to allow importing from src
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.core_utils.skype_api import SkypeParserAPI
from src.core_utils.tasks import celery_app
from src.core_utils.user_management import get_user_manager

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

    parser.add_argument(
        '--async-threshold',
        type=int,
        default=50 * 1024 * 1024,  # 50 MB
        help='File size threshold for asynchronous processing in bytes (default: 50MB)'
    )

    parser.add_argument(
        '--redis-url',
        type=str,
        default='redis://localhost:6379/0',
        help='Redis URL for Celery (default: redis://localhost:6379/0)'
    )

    parser.add_argument(
        '--worker',
        action='store_true',
        help='Run a Celery worker instead of the API server'
    )

    parser.add_argument(
        '--worker-concurrency',
        type=int,
        default=2,
        help='Number of worker processes (default: 2)'
    )

    parser.add_argument(
        '--user-file',
        type=str,
        default='users.json',
        help='Path to the user data file (default: users.json)'
    )

    parser.add_argument(
        '--secret-key',
        type=str,
        help='Secret key for session encryption (default: from SECRET_KEY environment variable or random)'
    )

    parser.add_argument(
        '--create-user',
        action='store_true',
        help='Create a new user'
    )

    parser.add_argument(
        '--username',
        type=str,
        help='Username for the new user'
    )

    parser.add_argument(
        '--password',
        type=str,
        help='Password for the new user'
    )

    parser.add_argument(
        '--email',
        type=str,
        help='Email for the new user'
    )

    parser.add_argument(
        '--display-name',
        type=str,
        help='Display name for the new user'
    )

    parser.add_argument(
        '--list-users',
        action='store_true',
        help='List all users'
    )

    return parser.parse_args()


def run_worker(args):
    """Run a Celery worker."""
    # Configure Celery
    celery_app.conf.update(
        broker_url=args.redis_url,
        result_backend=args.redis_url
    )

    # Set up database configuration for tasks
    os.environ['DB_HOST'] = args.db_host
    os.environ['DB_PORT'] = str(args.db_port)
    if args.db_name:
        os.environ['DB_NAME'] = args.db_name
    if args.db_user:
        os.environ['DB_USER'] = args.db_user
    if args.db_password:
        os.environ['DB_PASSWORD'] = args.db_password

    # Run the worker
    logger.info(f'Starting Celery worker with concurrency {args.worker_concurrency}')

    # Use sys.argv to pass arguments to Celery
    sys.argv = [
        'celery',
        '-A', 'src.api.tasks.celery_app',
        'worker',
        '--loglevel', args.log_level.lower(),
        '--concurrency', str(args.worker_concurrency)
    ]

    from celery.__main__ import main as celery_main
    celery_main()


def create_user(args):
    """Create a new user."""
    # Check if required fields are provided
    if not args.username or not args.password or not args.email or not args.display_name:
        logger.error('Missing required fields for user creation')
        print('Error: Missing required fields for user creation')
        print('Required fields: --username, --password, --email, --display-name')
        return False

    # Initialize user manager
    user_manager = get_user_manager(args.user_file)

    # Check if username already exists
    if user_manager.get_user(args.username):
        logger.error(f'Username {args.username} already exists')
        print(f'Error: Username {args.username} already exists')
        return False

    # Register user
    success = user_manager.register_user(
        username=args.username,
        password=args.password,
        email=args.email,
        display_name=args.display_name
    )

    if success:
        logger.info(f'User {args.username} created successfully')
        print(f'User {args.username} created successfully')

        # Get user
        user = user_manager.get_user(args.username)

        # Print user data
        print(f'Username: {user["username"]}')
        print(f'Email: {user["email"]}')
        print(f'Display Name: {user["display_name"]}')
        print(f'API Key: {user["api_key"]}')

        return True
    else:
        logger.error(f'Failed to create user {args.username}')
        print(f'Error: Failed to create user {args.username}')
        return False


def list_users(args):
    """List all users."""
    # Initialize user manager
    user_manager = get_user_manager(args.user_file)

    # Get all users
    users = user_manager.get_all_users()

    if not users:
        logger.info('No users found')
        print('No users found')
        return

    # Print users
    print(f'Found {len(users)} users:')
    for user in users:
        print(f'Username: {user["username"]}')
        print(f'Email: {user["email"]}')
        print(f'Display Name: {user["display_name"]}')
        print(f'Created At: {datetime.fromtimestamp(user["created_at"]).strftime("%Y-%m-%d %H:%M:%S")}')
        if user["last_login"]:
            print(f'Last Login: {datetime.fromtimestamp(user["last_login"]).strftime("%Y-%m-%d %H:%M:%S")}')
        else:
            print('Last Login: Never')
        print()


def main():
    """Run the API server or Celery worker."""
    args = parse_args()

    # Set up logging level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Set environment variables for Celery
    os.environ['CELERY_BROKER_URL'] = args.redis_url
    os.environ['CELERY_RESULT_BACKEND'] = args.redis_url

    # Handle user management commands
    if args.create_user:
        success = create_user(args)
        if not success:
            sys.exit(1)
        return

    if args.list_users:
        list_users(args)
        return

    # Run worker if requested
    if args.worker:
        run_worker(args)
        return

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
        api_key=api_key,
        async_threshold=args.async_threshold,
        user_file=args.user_file,
        secret_key=args.secret_key
    )

    logger.info(f'Starting API server with SocketIO on {args.host}:{args.port}')
    api.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == '__main__':
    main()