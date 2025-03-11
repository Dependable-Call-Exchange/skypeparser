#!/usr/bin/env python3
"""
Supabase PostgreSQL Connection Example

This script demonstrates how to connect to a Supabase PostgreSQL database
using the ETL pipeline. It loads a Skype export file and stores the data
in Supabase.

Key features demonstrated:
1. Supabase PostgreSQL connection setup
2. SSL/TLS configuration for secure connections
3. ETL pipeline integration with Supabase
4. Error handling and connection validation
5. Data loading with progress reporting

Compatibility:
- SkypeParser version: 2.0.0+
- Python version: 3.7+
- PostgreSQL version: 11.0+
- Supabase service

Prerequisites:
- Supabase project with PostgreSQL database
- Access to Supabase connection credentials
- Skype export file (.tar or .json)

Usage:
    python supabase_connection.py -f <skype_export_file> -u <your_display_name>
                                 [--config <config_file>] [--options]

Example:
    python supabase_connection.py -f export.tar -u "John Doe" --config ../config/supabase.json

Environment Variables (alternative to config file):
    SUPABASE_HOST: Database host URL
    SUPABASE_PORT: Database port (default: 5432)
    SUPABASE_DBNAME: Database name
    SUPABASE_USER: Database user
    SUPABASE_PASSWORD: Database password
    SUPABASE_SSLMODE: SSL mode (default: require)
"""

import os
import sys
import json
import argparse
import logging
import time
from pathlib import Path
from typing import Dict, Any, Optional, Tuple

# Add the parent directory to the path so we can import the modules
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Import required modules with error handling
try:
    from src.db.etl.pipeline_manager import ETLPipeline
    from src.db.etl.context import ETLContext
    from src.utils.validation import validate_file_exists, validate_directory, ValidationError
    from src.db.connection_factory import create_db_connection, test_db_connection
except ImportError as e:
    print(f"Error importing SkypeParser modules: {e}")
    print("Make sure you have properly installed SkypeParser or your PYTHONPATH is correctly set.")
    sys.exit(1)

# Check for database driver
try:
    import psycopg2
except ImportError:
    print("Error: psycopg2 is not installed.")
    print("Install it using: pip install psycopg2-binary")
    sys.exit(1)

# Set up logging
LOG_DIR = PROJECT_ROOT / "logs"
LOG_DIR.mkdir(exist_ok=True)

LOG_FILE = LOG_DIR / f"supabase_etl_{time.strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_FILE)
    ]
)
logger = logging.getLogger('supabase-connection')

def load_config(config_file: str) -> Dict[str, Any]:
    """
    Load configuration from a JSON file.

    Args:
        config_file: Path to the JSON config file

    Returns:
        Configuration parameters dictionary
    """
    config_path = Path(config_file)
    if not config_path.exists():
        logger.warning(f"Config file not found: {config_file}")
        logger.info("Using environment variables or defaults instead")
        return {}

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            logger.info(f"Loaded configuration from {config_file}")
            return config
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing config file ({config_file}): {e}")
        logger.info("Using environment variables or defaults instead")
        return {}
    except Exception as e:
        logger.error(f"Error loading config file ({config_file}): {e}")
        logger.info("Using environment variables or defaults instead")
        return {}

def get_commandline_args():
    """
    Parse command line arguments.

    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description='Connect to Supabase PostgreSQL and import Skype data',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # File input options
    file_group = parser.add_argument_group('Input Files')
    file_group.add_argument('-f', '--filename', required=True,
                       help='Path to the Skype export file (TAR or JSON)')
    file_group.add_argument('-u', '--user-display-name', required=True,
                       help='Your display name in the Skype logs')

    # Configuration options
    config_group = parser.add_argument_group('Configuration')
    config_group.add_argument('--config', default=os.environ.get('SUPABASE_CONFIG', 'config/supabase.json'),
                       help='Path to Supabase configuration file')
    config_group.add_argument('--output-dir', default=os.environ.get('OUTPUT_DIR', 'output'),
                       help='Directory for output files')

    # Database connection options (override config file)
    db_group = parser.add_argument_group('Database Connection (overrides config file)')
    db_group.add_argument('--host',
                       default=os.environ.get('SUPABASE_HOST'),
                       help='Supabase PostgreSQL host')
    db_group.add_argument('--port', type=int,
                       default=os.environ.get('SUPABASE_PORT', 5432),
                       help='Supabase PostgreSQL port')
    db_group.add_argument('--dbname',
                       default=os.environ.get('SUPABASE_DBNAME'),
                       help='Supabase PostgreSQL database name')
    db_group.add_argument('--user',
                       default=os.environ.get('SUPABASE_USER'),
                       help='Supabase PostgreSQL user')
    db_group.add_argument('--password',
                       default=os.environ.get('SUPABASE_PASSWORD'),
                       help='Supabase PostgreSQL password')
    db_group.add_argument('--sslmode',
                       default=os.environ.get('SUPABASE_SSLMODE', 'require'),
                       choices=['disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full'],
                       help='SSL mode for Supabase connection')

    # Performance options
    perf_group = parser.add_argument_group('Performance')
    perf_group.add_argument('--memory-limit', type=int,
                       default=os.environ.get('MEMORY_LIMIT', 1024),
                       help='Memory limit in MB')
    perf_group.add_argument('--parallel', action='store_true',
                       default=os.environ.get('PARALLEL_PROCESSING', '').lower() == 'true',
                       help='Enable parallel processing')
    perf_group.add_argument('--chunk-size', type=int,
                       default=os.environ.get('CHUNK_SIZE', 1000),
                       help='Chunk size for batch processing')
    perf_group.add_argument('--batch-size', type=int,
                       default=os.environ.get('BATCH_SIZE', 100),
                       help='Batch size for database operations')

    # Testing options
    test_group = parser.add_argument_group('Testing')
    test_group.add_argument('--test-connection', action='store_true',
                       help='Only test the connection and exit')
    test_group.add_argument('--skip-validation', action='store_true',
                       help='Skip database validation')
    test_group.add_argument('--dry-run', action='store_true',
                       help='Validate input and configuration without running ETL')

    return parser.parse_args()

def validate_input_file(file_path: str) -> bool:
    """
    Validate that the input file exists and is accessible.

    Args:
        file_path: Path to the input file

    Returns:
        True if the file is valid, False otherwise
    """
    try:
        validate_file_exists(file_path)
        return True
    except ValidationError as e:
        logger.error(f"Input file validation failed: {e}")
        return False

def validate_output_directory(output_dir: str) -> bool:
    """
    Validate that the output directory exists or can be created.

    Args:
        output_dir: Path to the output directory

    Returns:
        True if the directory is valid, False otherwise
    """
    try:
        validate_directory(output_dir, create_if_missing=True)
        return True
    except ValidationError as e:
        logger.error(f"Output directory validation failed: {e}")
        return False

def build_db_config(args, config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build database configuration from arguments and config file.
    Command line arguments take precedence over config file values.

    Args:
        args: Command line arguments
        config: Configuration from file

    Returns:
        Database configuration dictionary
    """
    # Start with configuration from file
    db_config = config.get('database', {})

    # Override with command line arguments if provided
    if args.host:
        db_config['host'] = args.host
    if args.port:
        db_config['port'] = args.port
    if args.dbname:
        db_config['dbname'] = args.dbname
    if args.user:
        db_config['user'] = args.user
    if args.password:
        db_config['password'] = args.password
    if args.sslmode:
        db_config['sslmode'] = args.sslmode

    # Ensure required fields have defaults
    if 'port' not in db_config:
        db_config['port'] = 5432
    if 'sslmode' not in db_config:
        db_config['sslmode'] = 'require'  # Supabase requires SSL

    return db_config

def test_supabase_connection(db_config: Dict[str, Any]) -> bool:
    """
    Test connection to Supabase PostgreSQL.

    Args:
        db_config: Database configuration dictionary

    Returns:
        True if connection successful, False otherwise
    """
    logger.info(f"Testing connection to Supabase PostgreSQL at {db_config.get('host')}:{db_config.get('port')}")

    # Validate configuration
    required_fields = ['host', 'port', 'dbname', 'user', 'password']
    missing_fields = [f for f in required_fields if f not in db_config]
    if missing_fields:
        logger.error(f"Missing required database configuration fields: {', '.join(missing_fields)}")
        return False

    # Ensure SSL mode is set for Supabase
    if db_config.get('sslmode') not in ['require', 'verify-ca', 'verify-full']:
        logger.warning(f"SSL mode '{db_config.get('sslmode')}' may not be secure enough for Supabase")
        logger.warning("Recommended to use 'require' at minimum for Supabase connections")

    # Test connection
    try:
        # Try to connect with short timeout
        connect_params = {
            'dbname': db_config['dbname'],
            'user': db_config['user'],
            'password': db_config['password'],
            'host': db_config['host'],
            'port': db_config['port'],
            'sslmode': db_config.get('sslmode', 'require'),
            'connect_timeout': 10
        }

        conn = psycopg2.connect(**connect_params)

        # Test basic query execution
        with conn.cursor() as cursor:
            # Check PostgreSQL version
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            logger.info(f"Connected to PostgreSQL: {version}")

            # Check if this is likely a Supabase instance
            # Supabase typically has certain extensions or schema patterns
            cursor.execute("""
                SELECT count(*) FROM pg_extension
                WHERE extname IN ('pg_graphql', 'pg_stat_statements', 'pgcrypto', 'pgjwt');
            """)
            ext_count = cursor.fetchone()[0]
            if ext_count >= 2:
                logger.info("Database appears to be a Supabase instance (based on extensions)")
            else:
                logger.warning("Database might not be a Supabase instance (extensions check)")

            # Check for SkypeParser schema objects
            cursor.execute("""
                SELECT count(*) FROM information_schema.tables
                WHERE table_schema = 'public'
                AND table_name IN ('skype_users', 'skype_conversations', 'skype_messages');
            """)
            table_count = cursor.fetchone()[0]
            if table_count > 0:
                logger.info(f"Found {table_count}/3 SkypeParser tables already in the database")
            else:
                logger.info("No SkypeParser tables found. Schema will be created if needed.")

        conn.close()
        logger.info("Connection test successful")
        return True

    except psycopg2.OperationalError as e:
        logger.error(f"Connection failed: {e}")

        # Provide helpful error messages based on common issues
        error_msg = str(e).lower()
        if "timeout" in error_msg:
            logger.error("Connection timeout. Possible causes:")
            logger.error("- Network connectivity issues")
            logger.error("- Firewall blocking the connection")
            logger.error("- Incorrect host or port")
            logger.error("- Your IP address may not be in the Supabase allow list")
        elif "password" in error_msg:
            logger.error("Authentication failed. Check your username and password.")
        elif "ssl" in error_msg:
            logger.error("SSL connection failed. Supabase requires SSL connections.")
            logger.error("Make sure your SSL mode is set to 'require' at minimum.")

        logger.error("Check your Supabase project settings in the dashboard:")
        logger.error("- Go to Project Settings > Database")
        logger.error("- Ensure your IP is in the 'IP Allow List'")
        logger.error("- Verify the connection parameters in 'Connection Info'")

        return False
    except Exception as e:
        logger.error(f"Unexpected error testing connection: {e}")
        return False

def main():
    """Main function to connect to Supabase and import Skype data."""
    try:
        args = get_commandline_args()

        # Load configuration
        config = load_config(args.config)

        # Build database configuration from config file and arguments
        db_config = build_db_config(args, config)

        # Output directory from args or config
        output_dir = args.output_dir or config.get('output_dir', 'output')

        # Validate input file
        if not validate_input_file(args.filename):
            logger.error("Input file validation failed. Please provide a valid file.")
            sys.exit(1)

        # Validate output directory
        if not validate_output_directory(output_dir):
            logger.error("Output directory validation failed. Please check permissions.")
            sys.exit(1)

        # Test Supabase connection
        if not args.skip_validation:
            if not test_supabase_connection(db_config):
                logger.error("Supabase connection test failed. Please check your credentials.")
                sys.exit(1)

        # If only testing connection, exit here
        if args.test_connection:
            logger.info("Connection test completed successfully.")
            sys.exit(0)

        # If dry run, exit here
        if args.dry_run:
            logger.info("Dry run completed successfully. All validation checks passed.")
            sys.exit(0)

        # Create ETL context
        context = ETLContext(
            db_config=db_config,
            output_dir=output_dir,
            memory_limit_mb=args.memory_limit,
            parallel_processing=args.parallel,
            chunk_size=args.chunk_size,
            batch_size=args.batch_size,
            task_id=f"supabase-import-{Path(args.filename).stem}"
        )

        # Create and run ETL pipeline
        pipeline = ETLPipeline(context=context)

        logger.info(f"Starting ETL pipeline with file: {args.filename}")
        logger.info(f"Connecting to Supabase PostgreSQL at: {db_config['host']}")

        # Track timing
        start_time = time.time()

        # Run the pipeline
        result = pipeline.run_pipeline(
            file_path=args.filename,
            user_display_name=args.user_display_name
        )

        # Log results
        end_time = time.time()
        duration = end_time - start_time

        if result.get('success', False):
            logger.info(f"Successfully imported data to Supabase PostgreSQL in {duration:.2f} seconds")

            # Log metrics if available
            metrics = result.get('metrics', {})
            if 'conversations_processed' in metrics:
                logger.info(f"Processed {metrics['conversations_processed']} conversations")
            if 'messages_processed' in metrics:
                logger.info(f"Processed {metrics['messages_processed']} messages")
                if duration > 0:
                    logger.info(f"Processing rate: {metrics['messages_processed'] / duration:.2f} messages/second")

            # Log export ID
            logger.info(f"Export ID: {result.get('export_id', 'unknown')}")
        else:
            logger.error(f"Failed to import data: {result.get('error', 'Unknown error')}")
            sys.exit(1)

    except KeyboardInterrupt:
        logger.info("Operation interrupted by user")
        sys.exit(130)  # Standard exit code for SIGINT
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)

if __name__ == "__main__":
    main()