"""
ETL Validation Utilities

This module provides specialized validation functions for the ETL pipeline,
focusing on data structure validation, configuration validation, and
database schema validation.
"""

import logging
import os
import json
import re
import psycopg2
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime

logger = logging.getLogger(__name__)

class ETLValidationError(Exception):
    """Exception raised for ETL validation errors."""
    pass

def validate_supabase_config(config: Dict[str, Any]) -> bool:
    """
    Validate Supabase configuration.

    Args:
        config: Supabase configuration dictionary

    Returns:
        bool: True if the configuration is valid

    Raises:
        ETLValidationError: If the configuration is invalid
    """
    if not isinstance(config, dict):
        raise ETLValidationError("Supabase configuration must be a dictionary")

    # Check required fields for Supabase
    required_fields = ['host', 'port', 'dbname', 'user', 'password', 'sslmode']
    missing_fields = [f for f in required_fields if f not in config]
    if missing_fields:
        raise ETLValidationError(f"Missing required Supabase configuration fields: {missing_fields}")

    # Validate host format for Supabase
    host = config.get('host', '')
    if not host:
        raise ETLValidationError("Supabase host cannot be empty")

    # Check for Supabase host patterns
    supabase_patterns = [
        r'db\.[a-z0-9-]+\.supabase\.co',  # Direct connection
        r'aws-0-[a-z0-9-]+\.pooler\.supabase\.com'  # Pooler connection
    ]

    if not any(re.match(pattern, host) for pattern in supabase_patterns):
        logger.warning(f"Host '{host}' does not match typical Supabase host patterns")

    # Validate port
    port = config.get('port')
    if not isinstance(port, int):
        try:
            port = int(port)
        except (ValueError, TypeError):
            raise ETLValidationError("Port must be an integer")

    # Check for Supabase port patterns
    if port not in [5432, 6543]:
        logger.warning(f"Port {port} is not a typical Supabase port (5432 for session mode, 6543 for transaction mode)")

    # Validate SSL mode for Supabase
    sslmode = config.get('sslmode', '')
    if sslmode != 'require':
        logger.warning(f"SSL mode '{sslmode}' is not recommended for Supabase. 'require' is the recommended setting.")

    return True

def validate_database_schema(conn: psycopg2.extensions.connection) -> Tuple[bool, List[str]]:
    """
    Validate that the database has the required schema for the ETL pipeline.

    Args:
        conn: Database connection

    Returns:
        Tuple[bool, List[str]]: (is_valid, missing_tables)

    Raises:
        ETLValidationError: If the database connection is invalid
    """
    if not conn:
        raise ETLValidationError("Database connection is required")

    required_tables = ['skype_raw_exports', 'skype_conversations', 'skype_messages']
    missing_tables = []

    try:
        with conn.cursor() as cursor:
            # Check if tables exist
            for table in required_tables:
                cursor.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = %s
                    )
                """, (table,))
                exists = cursor.fetchone()[0]
                if not exists:
                    missing_tables.append(table)

        if missing_tables:
            logger.warning(f"Missing required tables: {missing_tables}")
            return False, missing_tables

        return True, []

    except Exception as e:
        raise ETLValidationError(f"Error validating database schema: {e}")

def validate_checkpoint_data(checkpoint_data: Dict[str, Any]) -> bool:
    """
    Validate checkpoint data structure.

    Args:
        checkpoint_data: Checkpoint data to validate

    Returns:
        bool: True if the checkpoint data is valid

    Raises:
        ETLValidationError: If the checkpoint data is invalid
    """
    if not isinstance(checkpoint_data, dict):
        raise ETLValidationError("Checkpoint data must be a dictionary")

    # Check required top-level fields
    required_fields = ['checkpoint_version', 'serialized_at', 'context']
    missing_fields = [f for f in required_fields if f not in checkpoint_data]
    if missing_fields:
        raise ETLValidationError(f"Missing required checkpoint fields: {missing_fields}")

    # Check context data
    context = checkpoint_data.get('context', {})
    if not isinstance(context, dict):
        raise ETLValidationError("Checkpoint context must be a dictionary")

    # Check required context fields
    required_context_fields = ['db_config', 'task_id']
    missing_context_fields = [f for f in required_context_fields if f not in context]
    if missing_context_fields:
        raise ETLValidationError(f"Missing required checkpoint context fields: {missing_context_fields}")

    # Validate available checkpoints
    available_checkpoints = checkpoint_data.get('available_checkpoints', [])
    if not isinstance(available_checkpoints, list):
        raise ETLValidationError("Available checkpoints must be a list")

    # Validate data files if present
    data_files = checkpoint_data.get('data_files', {})
    if not isinstance(data_files, dict):
        raise ETLValidationError("Data files must be a dictionary")

    # Check if data files exist
    for attr, file_path in data_files.items():
        if not os.path.exists(file_path):
            logger.warning(f"Data file for {attr} does not exist: {file_path}")

    return True

def validate_transformed_data_structure(transformed_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Validate and sanitize transformed data structure.

    Args:
        transformed_data: Transformed data to validate

    Returns:
        Dict[str, Any]: Sanitized transformed data

    Raises:
        ETLValidationError: If the transformed data is invalid
    """
    if not isinstance(transformed_data, dict):
        raise ETLValidationError("Transformed data must be a dictionary")

    # Check required top-level fields
    required_fields = ['metadata', 'conversations']
    missing_fields = [f for f in required_fields if f not in transformed_data]
    if missing_fields:
        raise ETLValidationError(f"Missing required transformed data fields: {missing_fields}")

    # Validate metadata
    metadata = transformed_data.get('metadata', {})
    if not isinstance(metadata, dict):
        raise ETLValidationError("Metadata must be a dictionary")

    # Validate conversations
    conversations = transformed_data.get('conversations', {})
    if not isinstance(conversations, dict):
        raise ETLValidationError("Conversations must be a dictionary")

    # Create a sanitized copy
    sanitized_data = {
        'metadata': metadata.copy(),
        'conversations': {}
    }

    # Sanitize conversation data
    for conv_id, conv in conversations.items():
        if not isinstance(conv, dict):
            logger.warning(f"Skipping invalid conversation '{conv_id}': not a dictionary")
            continue

        # Sanitize conversation ID
        sanitized_conv_id = re.sub(r'[<>:"/\\|?*]', '_', conv_id)
        if sanitized_conv_id != conv_id:
            logger.warning(f"Sanitized conversation ID from '{conv_id}' to '{sanitized_conv_id}'")

        # Create sanitized conversation
        sanitized_data['conversations'][sanitized_conv_id] = {
            'display_name': conv.get('display_name', ''),
            'messages': []
        }

        # Copy other fields
        for key, value in conv.items():
            if key != 'messages':
                sanitized_data['conversations'][sanitized_conv_id][key] = value

        # Sanitize messages
        if 'messages' in conv and isinstance(conv['messages'], list):
            for msg in conv['messages']:
                if not isinstance(msg, dict):
                    logger.warning(f"Skipping invalid message in conversation '{conv_id}': not a dictionary")
                    continue

                # Create sanitized message
                sanitized_msg = msg.copy()

                # Sanitize content fields
                for content_field in ['raw_content', 'cleaned_content']:
                    if content_field in sanitized_msg and sanitized_msg[content_field] is not None:
                        # Ensure content is a string
                        if not isinstance(sanitized_msg[content_field], str):
                            sanitized_msg[content_field] = str(sanitized_msg[content_field])

                sanitized_data['conversations'][sanitized_conv_id]['messages'].append(sanitized_msg)

    return sanitized_data

def validate_connection_string(connection_string: str) -> Dict[str, Any]:
    """
    Parse and validate a PostgreSQL connection string.

    Args:
        connection_string: PostgreSQL connection string

    Returns:
        Dict[str, Any]: Parsed connection parameters

    Raises:
        ETLValidationError: If the connection string is invalid
    """
    if not connection_string:
        raise ETLValidationError("Connection string cannot be empty")

    # Try to parse the connection string
    try:
        # Handle both URI format and keyword=value format
        if connection_string.startswith('postgresql://') or connection_string.startswith('postgres://'):
            # URI format
            params = {}

            # Extract user, password, host, port, and dbname from URI
            # Format: postgresql://username:password@hostname:port/dbname
            uri_pattern = r'(?:postgresql|postgres)://([^:@]+)(?::([^@]+))?@([^:/]+)(?::(\d+))?/([^?]+)'
            match = re.match(uri_pattern, connection_string)

            if match:
                user, password, host, port, dbname = match.groups()
                params['user'] = user
                if password:
                    params['password'] = password
                params['host'] = host
                if port:
                    params['port'] = int(port)
                params['dbname'] = dbname

                # Extract additional parameters from query string
                query_string = connection_string.split('?', 1)[1] if '?' in connection_string else ''
                if query_string:
                    for param in query_string.split('&'):
                        if '=' in param:
                            key, value = param.split('=', 1)
                            params[key] = value
            else:
                raise ETLValidationError("Invalid PostgreSQL URI format")
        else:
            # Keyword=value format
            params = {}
            for param in connection_string.split():
                if '=' in param:
                    key, value = param.split('=', 1)
                    # Handle quoted values
                    if value.startswith("'") and value.endswith("'"):
                        value = value[1:-1]
                    params[key] = value
                else:
                    logger.warning(f"Ignoring invalid parameter format: {param}")

        # Validate required parameters
        required_params = ['host', 'dbname', 'user']
        missing_params = [p for p in required_params if p not in params]
        if missing_params:
            raise ETLValidationError(f"Missing required connection parameters: {missing_params}")

        # Convert port to integer if present
        if 'port' in params and not isinstance(params['port'], int):
            try:
                params['port'] = int(params['port'])
            except ValueError:
                raise ETLValidationError(f"Invalid port value: {params['port']}")

        return params

    except Exception as e:
        if not isinstance(e, ETLValidationError):
            raise ETLValidationError(f"Error parsing connection string: {e}")
        raise