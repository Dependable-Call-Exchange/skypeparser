#!/usr/bin/env python3
"""
Test PostgreSQL Connection

This script tests the connection to a PostgreSQL database.
It's useful for verifying that your PostgreSQL setup is working correctly
before attempting to import Skype data.

Usage:
    python test_postgres_connection.py -d <database_name> [-H <host>] [-P <port>] [-U <username>] [-W <password>]

Example:
    python test_postgres_connection.py -d skype_logs -U postgres
"""

import sys
import argparse
import logging
import psycopg2

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('postgres-connection-test')

def get_commandline_args():
    """
    Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed command line arguments
    """
    parser = argparse.ArgumentParser(description='Test PostgreSQL connection')

    # Database options
    parser.add_argument('-d', '--dbname', required=True, help='PostgreSQL database name')
    parser.add_argument('-H', '--host', default='localhost', help='PostgreSQL host (default: localhost)')
    parser.add_argument('-P', '--port', type=int, default=5432, help='PostgreSQL port (default: 5432)')
    parser.add_argument('-U', '--username', help='PostgreSQL username')
    parser.add_argument('-W', '--password', help='PostgreSQL password')

    return parser.parse_args()

def test_connection(dbname, host, port, username, password):
    """
    Test connection to PostgreSQL database.

    Args:
        dbname (str): Database name
        host (str): Database host
        port (int): Database port
        username (str): Database username
        password (str): Database password

    Returns:
        bool: True if connection successful, False otherwise
    """
    try:
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=dbname,
            user=username,
            password=password,
            host=host,
            port=port
        )

        # Get PostgreSQL version
        with conn.cursor() as cur:
            cur.execute('SELECT version();')
            version = cur.fetchone()[0]
            logger.info(f"Connected to PostgreSQL: {version}")

            # Get list of tables
            cur.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                ORDER BY table_name;
            """)
            tables = cur.fetchall()

            if tables:
                logger.info("Existing tables in database:")
                for table in tables:
                    logger.info(f"  - {table[0]}")
            else:
                logger.info("No tables found in database.")

        # Close connection
        conn.close()
        logger.info("Connection test successful")
        return True

    except Exception as e:
        logger.error(f"Error connecting to PostgreSQL: {e}")
        return False

def main():
    """
    Main function to test PostgreSQL connection.
    """
    try:
        args = get_commandline_args()

        # Test connection
        if not test_connection(
            args.dbname,
            args.host,
            args.port,
            args.username,
            args.password
        ):
            sys.exit(1)

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()