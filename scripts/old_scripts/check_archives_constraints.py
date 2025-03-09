#!/usr/bin/env python3
"""
Check Archives Constraints Script

This script connects to the Supabase PostgreSQL database and checks all constraints
on the 'archives' table, focusing on the file_path column constraint.

Usage:
    python check_archives_constraints.py
"""

import os
import sys
import json
import logging
import psycopg2

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

def load_config():
    """Load database configuration from config file."""
    config_path = os.path.join("config", "config.json")
    try:
        with open(config_path, "r") as f:
            config = json.load(f)
        return config.get("database", {})
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        sys.exit(1)

def check_archives_constraints():
    """Check the constraints of the archives table."""
    db_config = load_config()

    # Extract connection parameters
    host = db_config.get("host")
    port = db_config.get("port")
    dbname = db_config.get("dbname")
    user = db_config.get("user")
    password = db_config.get("password")
    sslmode = db_config.get("sslmode", "require")

    logger.info(f"Attempting to connect to {dbname} on {host}:{port} as {user}")

    try:
        # Connect to the database
        conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password,
            sslmode=sslmode
        )

        # Create a cursor
        cursor = conn.cursor()

        # Check all constraints on the archives table
        cursor.execute("""
            SELECT tc.constraint_name, tc.constraint_type, cc.check_clause
            FROM information_schema.table_constraints tc
            LEFT JOIN information_schema.check_constraints cc
            ON tc.constraint_name = cc.constraint_name
            WHERE tc.table_name = 'archives'
            AND tc.table_schema = 'public';
        """)

        constraints = cursor.fetchall()

        logger.info("Constraints on the 'archives' table:")
        for constraint in constraints:
            constraint_name, constraint_type, check_clause = constraint
            logger.info(f"  {constraint_name} - {constraint_type} - {check_clause}")

        # Check for specific constraint on file_path column
        cursor.execute("""
            SELECT pg_get_constraintdef(con.oid) as constraint_def
            FROM pg_constraint con
            INNER JOIN pg_class rel ON rel.oid = con.conrelid
            WHERE rel.relname = 'archives'
            AND con.contype = 'c'
            AND con.conname = 'check_valid_file_path';
        """)

        file_path_constraint = cursor.fetchone()
        if file_path_constraint:
            logger.info(f"File path constraint: {file_path_constraint[0]}")
        else:
            logger.info("No specific file_path constraint found with name 'check_valid_file_path'")

        # Get all column constraints
        cursor.execute("""
            SELECT column_name,
                   pg_get_constraintdef(con.oid) as constraint_def
            FROM pg_constraint con
            INNER JOIN pg_class rel ON rel.oid = con.conrelid
            INNER JOIN pg_attribute att ON att.attrelid = con.conrelid AND att.attnum = ANY(con.conkey)
            INNER JOIN information_schema.columns isc ON isc.table_schema = 'public'
                AND isc.table_name = rel.relname
                AND isc.column_name = att.attname
            WHERE rel.relname = 'archives'
            AND con.contype = 'c';
        """)

        column_constraints = cursor.fetchall()
        logger.info("Column constraints:")
        for column_constraint in column_constraints:
            column_name, constraint_def = column_constraint
            logger.info(f"  {column_name} - {constraint_def}")

        # Close the connection
        conn.close()
        logger.info("Connection closed successfully.")

    except Exception as e:
        logger.error(f"Error checking archives constraints: {e}")
        sys.exit(1)

def main():
    """Main function to run the script."""
    logger.info("Starting script to check archives table constraints")
    check_archives_constraints()
    logger.info("Successfully checked archives table constraints")

if __name__ == "__main__":
    main()