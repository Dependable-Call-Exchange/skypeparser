#!/usr/bin/env python3
"""
End-to-End Test Script for SkypeParser ETL Pipeline

This script orchestrates a complete end-to-end test of the SkypeParser ETL pipeline by:
1. Testing database connectivity
2. Running the ETL pipeline with a Skype export file
3. Validating database schema and constraints
4. Verifying data was correctly inserted

Usage:
    python test_etl_end_to_end.py [options]

Example:
    python test_etl_end_to_end.py --file 8_live_dave.leathers113_export.tar --user "David Leathers"
"""

import os
import sys
import json
import logging
import argparse
import subprocess
import traceback
import importlib
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

# Configure logging
def setup_logging(log_dir: str = "logs") -> str:
    """Set up logging configuration.

    Args:
        log_dir: Directory to store log files

    Returns:
        Path to the log file
    """
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f"etl_test_{timestamp}.log")

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    logger = logging.getLogger(__name__)
    logger.info(f"Logging initialized. Log file: {log_file}")

    return log_file

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Run end-to-end test of SkypeParser ETL pipeline',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('-f', '--file',
                        help='Path to the Skype export file (TAR or JSON)',
                        default='8_live_dave.leathers113_export.tar')

    parser.add_argument('-u', '--user',
                        help='Your display name as it appears in Skype',
                        default='David Leathers')

    parser.add_argument('-c', '--config',
                        help='Path to configuration file',
                        default='config/config.json')

    parser.add_argument('-o', '--output',
                        help='Output directory for test results',
                        default='output')

    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help='Enable verbose logging')

    parser.add_argument('-d', '--debug',
                        action='store_true',
                        help='Enable debug mode')

    return parser.parse_args()

def validate_skype_export_file(file_path: str) -> bool:
    """Validate that the Skype export file exists and is accessible.

    Args:
        file_path: Path to the Skype export file

    Returns:
        True if the file exists and is valid, False otherwise
    """
    logger = logging.getLogger(__name__)

    if not os.path.exists(file_path):
        logger.error(f"Skype export file not found: {file_path}")
        return False

    if not os.path.isfile(file_path):
        logger.error(f"Specified path is not a file: {file_path}")
        return False

    file_extension = os.path.splitext(file_path)[1].lower()
    if file_extension not in ['.tar', '.json']:
        logger.warning(f"File does not have a standard Skype export extension (.tar or .json): {file_path}")

    file_size = os.path.getsize(file_path)
    logger.info(f"Skype export file validated: {file_path} ({file_size} bytes)")

    return True

def load_config(config_path: str) -> Dict[str, Any]:
    """Load configuration from file.

    Args:
        config_path: Path to the configuration file

    Returns:
        Loaded configuration
    """
    logger = logging.getLogger(__name__)

    try:
        with open(config_path, 'r') as f:
            config = json.load(f)

        logger.info(f"Configuration loaded from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        raise

def test_database_connection(config: Dict[str, Any]) -> bool:
    """Test database connectivity using the provided configuration.

    Args:
        config: Database configuration

    Returns:
        True if connection successful, False otherwise
    """
    logger = logging.getLogger(__name__)
    logger.info("Testing database connection...")

    try:
        # Try to import database connection module
        from src.utils.config import get_db_config
        from src.db.connection import DatabaseConnection

        # Get database configuration if not provided
        if not config.get("database"):
            db_config = get_db_config()
        else:
            db_config = config["database"]

        # Create database connection with timeout
        logger.info(f"Connecting to database at {db_config.get('host', 'unknown')}...")
        conn = DatabaseConnection(db_config, connect_timeout=10)

        # Test connection by running a simple query
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()

        # Close connection
        cursor.close()
        conn.close()

        if result and result[0] == 1:
            logger.info("Database connection successful!")
            return True
        else:
            logger.error("Database connection test failed: unexpected result")
            return False
    except ImportError as e:
        logger.error(f"Error importing database modules: {e}")
        return False
    except Exception as e:
        logger.error(f"Error testing database connection: {e}")
        return False

def run_etl_pipeline(args: argparse.Namespace) -> Tuple[bool, str]:
    """Run the ETL pipeline using run_supabase_etl.py.

    Args:
        args: Command line arguments

    Returns:
        Tuple of (success status, log output)
    """
    logger = logging.getLogger(__name__)
    logger.info(f"Running ETL pipeline with file: {args.file}")

    try:
        # Build command to run run_supabase_etl.py
        cmd = [
            sys.executable,
            "scripts/run_supabase_etl.py",
            "-f", args.file,
            "-u", args.user,
            "-c", args.config,
            "-o", args.output
        ]

        if args.verbose:
            cmd.append("-v")

        if args.debug:
            cmd.append("--debug")

        # Run the ETL pipeline
        logger.info(f"Executing command: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)

        if result.returncode == 0:
            logger.info("ETL pipeline completed successfully!")
            return True, result.stdout
        else:
            logger.error(f"ETL pipeline failed: {result.stderr}")
            return False, result.stderr
    except Exception as e:
        logger.error(f"Error running ETL pipeline: {e}")
        logger.debug(traceback.format_exc())
        return False, str(e)

def validate_database_schema() -> bool:
    """Validate database schema using existing validation scripts.

    Returns:
        True if validation passed, False otherwise
    """
    logger = logging.getLogger(__name__)
    logger.info("Validating database schema...")

    validations = [
        {
            "name": "Archives Schema",
            "script": "scripts/check_archives_schema.py"
        },
        {
            "name": "Archives Constraints",
            "script": "scripts/check_archives_constraints.py"
        },
        {
            "name": "Messages Schema",
            "script": "scripts/check_messages_schema.py"
        }
    ]

    all_passed = True

    for validation in validations:
        logger.info(f"Running {validation['name']} validation...")

        try:
            cmd = [sys.executable, validation["script"]]
            result = subprocess.run(cmd, capture_output=True, text=True)

            if result.returncode == 0:
                logger.info(f"{validation['name']} validation passed!")
                logger.debug(result.stdout)
            else:
                logger.error(f"{validation['name']} validation failed: {result.stderr}")
                all_passed = False
        except Exception as e:
            logger.error(f"Error running {validation['name']} validation: {e}")
            all_passed = False

    return all_passed

def verify_data_insertion() -> bool:
    """Verify that data was inserted correctly by running queries.

    Returns:
        True if verification passed, False otherwise
    """
    logger = logging.getLogger(__name__)
    logger.info("Verifying data insertion...")

    sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

    try:
        from src.utils.config import get_db_config
        from src.db.connection import DatabaseConnection

        # Get database configuration
        db_config = get_db_config()

        # Create database connection
        conn = DatabaseConnection(db_config)
        cursor = conn.cursor()

        # Check archives table
        cursor.execute("SELECT COUNT(*) FROM archives")
        archives_count = cursor.fetchone()[0]
        logger.info(f"Archives count: {archives_count}")

        # Check conversations table
        cursor.execute("SELECT COUNT(*) FROM conversations")
        conversations_count = cursor.fetchone()[0]
        logger.info(f"Conversations count: {conversations_count}")

        # Check messages table
        cursor.execute("SELECT COUNT(*) FROM messages")
        messages_count = cursor.fetchone()[0]
        logger.info(f"Messages count: {messages_count}")

        # Check if the latest archive has a file path ending with .tar
        cursor.execute("SELECT file_path FROM archives ORDER BY created_at DESC LIMIT 1")
        file_path = cursor.fetchone()[0] if cursor.rowcount > 0 else None

        if file_path and file_path.endswith('.tar'):
            logger.info(f"Latest archive file path correctly ends with .tar: {file_path}")
        else:
            logger.warning(f"Latest archive file path does not end with .tar: {file_path}")

        # Close connection
        cursor.close()
        conn.close()

        return (archives_count > 0 and conversations_count > 0 and messages_count > 0)
    except Exception as e:
        logger.error(f"Error verifying data insertion: {e}")
        logger.debug(traceback.format_exc())
        return False

def generate_report(output_dir: str, etl_success: bool, schema_valid: bool, data_verified: bool, log_file: str) -> str:
    """Generate a comprehensive report of the end-to-end test.

    Args:
        output_dir: Directory to store the report
        etl_success: Whether the ETL pipeline completed successfully
        schema_valid: Whether the database schema validation passed
        data_verified: Whether data insertion verification passed
        log_file: Path to the log file

    Returns:
        Path to the generated report
    """
    logger = logging.getLogger(__name__)

    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = os.path.join(output_dir, f"etl_test_report_{timestamp}.md")

    # Calculate overall status
    overall_status = "✅ PASSED" if all([etl_success, schema_valid, data_verified]) else "❌ FAILED"

    with open(report_file, 'w') as f:
        f.write(f"# SkypeParser ETL Pipeline End-to-End Test Report\n\n")
        f.write(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"**Overall Status:** {overall_status}\n\n")

        f.write("## Test Results\n\n")
        f.write(f"1. **ETL Pipeline Execution:** {'✅ Passed' if etl_success else '❌ Failed'}\n")
        f.write(f"2. **Database Schema Validation:** {'✅ Passed' if schema_valid else '❌ Failed'}\n")
        f.write(f"3. **Data Insertion Verification:** {'✅ Passed' if data_verified else '❌ Failed'}\n\n")

        f.write("## Log File\n\n")
        f.write(f"The complete log file is available at: `{log_file}`\n\n")

        f.write("## Recommendations\n\n")
        if not etl_success:
            f.write("- Review the ETL pipeline execution logs for errors\n")
        if not schema_valid:
            f.write("- Verify that the database schema is correctly defined\n")
        if not data_verified:
            f.write("- Investigate data insertion issues\n")
        if all([etl_success, schema_valid, data_verified]):
            f.write("- The ETL pipeline is functioning correctly, no action required\n")

    logger.info(f"Report generated: {report_file}")

    return report_file

def main():
    """Main function to run the end-to-end test."""
    # Parse arguments
    args = parse_arguments()

    # Set up logging
    log_file = setup_logging()
    logger = logging.getLogger(__name__)

    # Set log level
    log_level = logging.DEBUG if args.debug else (logging.INFO if args.verbose else logging.WARNING)
    logger.setLevel(log_level)

    logger.info("Starting SkypeParser ETL Pipeline End-to-End Test")

    try:
        # Validate Skype export file
        if not validate_skype_export_file(args.file):
            logger.error("Skype export file validation failed")
            return 1

        # Load configuration
        config = load_config(args.config)

        # Test database connection
        if not test_database_connection(config):
            logger.error("Database connection test failed")
            return 1

        # Run ETL pipeline
        etl_success, etl_output = run_etl_pipeline(args)

        # Validate database schema
        schema_valid = validate_database_schema() if etl_success else False

        # Verify data insertion
        data_verified = verify_data_insertion() if etl_success and schema_valid else False

        # Generate report
        report_file = generate_report(
            args.output,
            etl_success,
            schema_valid,
            data_verified,
            log_file
        )

        # Print final status
        if all([etl_success, schema_valid, data_verified]):
            logger.info("✅ End-to-End Test PASSED! The ETL pipeline is functioning correctly.")
            logger.info(f"Report: {report_file}")
            return 0
        else:
            logger.error("❌ End-to-End Test FAILED! See report for details.")
            logger.info(f"Report: {report_file}")
            return 1

    except Exception as e:
        logger.error(f"Error in end-to-end test: {e}")
        logger.debug(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main())