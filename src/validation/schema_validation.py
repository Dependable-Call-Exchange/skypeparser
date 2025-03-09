#!/usr/bin/env python3
"""
Schema Validation Module

This module provides utilities for validating structured data against schemas
using JSON Schema. It includes functions for validating configuration, input data,
and other structured data with clear error messages.
"""

import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple, Union

import jsonschema
from jsonschema import Draft7Validator, ValidationError, validators

from src.core_utils.exceptions import InvalidInputError, SkypeParserError

# Set up logging
logger = logging.getLogger(__name__)

# Default schema directory
SCHEMA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "schemas")


class SchemaValidationError(SkypeParserError):
    """
    Exception raised for schema validation errors.

    This exception provides detailed information about schema validation failures,
    including specific fields that failed validation and the expected format.
    """

    def __init__(self, message: str, errors: Optional[List[Dict[str, Any]]] = None):
        """
        Initialize the exception.

        Args:
            message: Error message
            errors: List of validation errors
        """
        self.errors = errors or []
        super().__init__(message)


def extend_with_default(validator_class):
    """
    Extend the JSON Schema validator to fill in default values.

    This function creates a new validator class that fills in default values
    from the schema when validating data.

    Args:
        validator_class: The validator class to extend

    Returns:
        A new validator class with default value support
    """
    validate_properties = validator_class.VALIDATORS["properties"]

    def set_defaults(validator, properties, instance, schema):
        for property_name, subschema in properties.items():
            if "default" in subschema and instance.get(property_name) is None:
                instance[property_name] = subschema["default"]

        for error in validate_properties(validator, properties, instance, schema):
            yield error

    return validators.extend(validator_class, {"properties": set_defaults})


# Create a validator that fills in default values
DefaultValidatingDraft7Validator = extend_with_default(Draft7Validator)


def load_schema(schema_name: str, schema_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Load a JSON Schema from file.

    Args:
        schema_name: Name of the schema file (with or without .json extension)
        schema_dir: Directory containing schema files

    Returns:
        The loaded schema

    Raises:
        FileNotFoundError: If the schema file doesn't exist
        json.JSONDecodeError: If the schema file contains invalid JSON
    """
    # Ensure schema name has .json extension
    if not schema_name.endswith(".json"):
        schema_name = f"{schema_name}.json"

    # Use default schema directory if not specified
    if not schema_dir:
        schema_dir = SCHEMA_DIR

    # Create the schema directory if it doesn't exist
    if not os.path.exists(schema_dir):
        os.makedirs(schema_dir, exist_ok=True)

    # Load the schema
    schema_path = os.path.join(schema_dir, schema_name)
    try:
        with open(schema_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Schema file not found: {schema_path}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in schema file {schema_path}: {e}")
        raise


def format_validation_error(error: ValidationError) -> Dict[str, Any]:
    """
    Format a validation error into a user-friendly format.

    Args:
        error: ValidationError instance

    Returns:
        Formatted error message
    """
    path = ".".join(str(p) for p in error.path) if error.path else "root"

    # Extract type expectations from schema if available
    type_info = ""
    if error.schema is not None and "type" in error.schema:
        expected_type = error.schema["type"]
        if isinstance(expected_type, list):
            type_info = f" (expected one of: {', '.join(expected_type)})"
        else:
            type_info = f" (expected: {expected_type})"

    # Format validation message
    message = error.message
    # Remove instance formatting
    message = re.sub(r"instance\['[^']*'\]", "field", message)
    message = re.sub(r"instance", "input", message)

    # Build final error
    return {
        "path": path,
        "message": message + type_info,
        "schema_path": ".".join(str(p) for p in error.schema_path)
        if error.schema_path
        else "",
        "validator": error.validator,
        "validator_value": error.validator_value,
    }


def validate_with_schema(
    data: Dict[str, Any],
    schema: Dict[str, Any],
    fill_defaults: bool = True,
    schema_name: str = "unknown",
) -> Tuple[bool, List[Dict[str, Any]]]:
    """
    Validate data against a schema.

    Args:
        data: Data to validate
        schema: JSON Schema
        fill_defaults: Whether to fill in default values
        schema_name: Name of the schema (for error messages)

    Returns:
        Tuple of (is_valid, error_list)
    """
    # Choose the validator class based on whether to fill in defaults
    validator_class = (
        DefaultValidatingDraft7Validator if fill_defaults else Draft7Validator
    )

    # Create a validator instance
    validator = validator_class(schema)

    # Collect all validation errors
    errors = []
    for error in validator.iter_errors(data):
        errors.append(format_validation_error(error))

    # Return validation result
    is_valid = len(errors) == 0

    # Log validation results
    if is_valid:
        logger.debug(f"Validation passed for {schema_name}")
    else:
        logger.warning(f"Validation failed for {schema_name}: {len(errors)} errors")
        for error in errors:
            logger.warning(f"  - {error['path']}: {error['message']}")

    return is_valid, errors


def validate_data(
    data: Dict[str, Any],
    schema_name: str,
    fill_defaults: bool = True,
    schema_dir: Optional[str] = None,
    raise_exception: bool = True,
) -> Dict[str, Any]:
    """
    Validate data against a named schema.

    Args:
        data: Data to validate
        schema_name: Name of the schema file
        fill_defaults: Whether to fill in default values
        schema_dir: Directory containing schema files
        raise_exception: Whether to raise an exception if validation fails

    Returns:
        The validated data (with defaults filled in if requested)

    Raises:
        SchemaValidationError: If validation fails and raise_exception is True
    """
    # Load the schema
    schema = load_schema(schema_name, schema_dir)

    # Create a copy of the data to avoid modifying the original
    data_copy = json.loads(json.dumps(data))

    # Validate the data
    is_valid, errors = validate_with_schema(
        data_copy, schema, fill_defaults, schema_name
    )

    # Raise an exception if validation failed and requested
    if not is_valid and raise_exception:
        # Create a detailed error message
        error_paths = ", ".join(error["path"] for error in errors[:3])
        if len(errors) > 3:
            error_paths += f", and {len(errors) - 3} more"

        raise SchemaValidationError(
            f"Validation failed for {schema_name}: Problems with {error_paths}",
            errors=errors,
        )

    # Return the validated data (with defaults filled in if requested)
    return data_copy


def validate_config(
    config: Dict[str, Any],
    config_type: str = "app_config",
    fill_defaults: bool = True,
    schema_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Validate a configuration dictionary.

    Args:
        config: Configuration to validate
        config_type: Type of configuration (corresponding to schema file name)
        fill_defaults: Whether to fill in default values
        schema_dir: Directory containing schema files

    Returns:
        The validated configuration (with defaults filled in if requested)

    Raises:
        SchemaValidationError: If validation fails
    """
    try:
        return validate_data(
            data=config,
            schema_name=f"{config_type}_schema",
            fill_defaults=fill_defaults,
            schema_dir=schema_dir,
            raise_exception=True,
        )
    except SchemaValidationError as e:
        logger.error(f"Configuration validation failed: {e}")
        raise


def validate_skype_data(
    data: Dict[str, Any], fill_defaults: bool = False, schema_dir: Optional[str] = None
) -> Dict[str, Any]:
    """
    Validate Skype export data.

    Args:
        data: Skype export data to validate
        fill_defaults: Whether to fill in default values
        schema_dir: Directory containing schema files

    Returns:
        The validated data (with defaults filled in if requested)

    Raises:
        InvalidInputError: If validation fails
    """
    try:
        return validate_data(
            data=data,
            schema_name="skype_export_schema",
            fill_defaults=fill_defaults,
            schema_dir=schema_dir,
            raise_exception=True,
        )
    except SchemaValidationError as e:
        # Convert to InvalidInputError for consistency with existing code
        raise InvalidInputError(f"Invalid Skype export data: {str(e)}")


def create_schema_directory(schema_dir: Optional[str] = None) -> str:
    """
    Create the schema directory if it doesn't exist.

    Args:
        schema_dir: Directory to create

    Returns:
        Path to the created directory
    """
    # Use default schema directory if not specified
    if not schema_dir:
        schema_dir = SCHEMA_DIR

    # Create the directory if it doesn't exist
    if not os.path.exists(schema_dir):
        os.makedirs(schema_dir, exist_ok=True)
        logger.info(f"Created schema directory: {schema_dir}")

    return schema_dir


def save_schema(
    schema: Dict[str, Any],
    schema_name: str,
    schema_dir: Optional[str] = None,
    overwrite: bool = False,
) -> str:
    """
    Save a schema to a file.

    Args:
        schema: Schema to save
        schema_name: Name of the schema file
        schema_dir: Directory to save to
        overwrite: Whether to overwrite an existing file

    Returns:
        Path to the saved schema file

    Raises:
        FileExistsError: If the schema file already exists and overwrite is False
    """
    # Ensure schema name has .json extension
    if not schema_name.endswith(".json"):
        schema_name = f"{schema_name}.json"

    # Create the schema directory
    schema_dir = create_schema_directory(schema_dir)

    # Save the schema
    schema_path = os.path.join(schema_dir, schema_name)
    if os.path.exists(schema_path) and not overwrite:
        logger.warning(f"Schema file already exists: {schema_path}")
        raise FileExistsError(f"Schema file already exists: {schema_path}")

    with open(schema_path, "w", encoding="utf-8") as f:
        json.dump(schema, f, indent=2)

    logger.info(f"Saved schema to {schema_path}")
    return schema_path


def create_base_app_config_schema() -> Dict[str, Any]:
    """
    Create a basic schema for application configuration.

    Returns:
        JSON Schema for application configuration
    """
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Application Configuration",
        "description": "Schema for validating application configuration",
        "type": "object",
        "required": ["database", "output", "logging"],
        "properties": {
            "database": {
                "type": "object",
                "required": ["host", "port", "dbname", "user"],
                "properties": {
                    "host": {"type": "string", "default": "localhost"},
                    "port": {"type": "integer", "default": 5432},
                    "dbname": {"type": "string", "default": "skype_archive"},
                    "user": {"type": "string", "default": "postgres"},
                    "password": {"type": "string", "default": ""},
                },
            },
            "output": {
                "type": "object",
                "required": ["directory"],
                "properties": {
                    "directory": {"type": "string", "default": "output"},
                    "overwrite": {"type": "boolean", "default": False},
                },
            },
            "logging": {
                "type": "object",
                "properties": {
                    "level": {
                        "type": "string",
                        "enum": ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        "default": "INFO",
                    },
                    "file": {"type": ["string", "null"], "default": None},
                    "json_format": {"type": "boolean", "default": False},
                    "structured": {"type": "boolean", "default": True},
                    "rotation": {
                        "type": "string",
                        "enum": ["size", "time", None],
                        "default": "size",
                    },
                    "max_bytes": {"type": "integer", "default": 10485760},
                    "backup_count": {"type": "integer", "default": 5},
                },
            },
            "message_types": {
                "type": "object",
                "additionalProperties": {"type": "string"},
            },
            "default_message_format": {"type": "string"},
            "chunk_size": {"type": "integer", "default": 1000},
            "db_batch_size": {"type": "integer", "default": 100},
            "use_parallel_processing": {"type": "boolean", "default": False},
            "max_workers": {"type": ["integer", "null"], "default": None},
            "memory_limit_mb": {"type": "integer", "default": 1024},
        },
        "additionalProperties": True,
    }


def create_skype_export_schema() -> Dict[str, Any]:
    """
    Create a schema for Skype export data.

    Returns:
        JSON Schema for Skype export data
    """
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Skype Export Data",
        "description": "Schema for validating Skype export data",
        "type": "object",
        "required": ["userId", "exportDate", "conversations"],
        "properties": {
            "userId": {"type": "string"},
            "exportDate": {"type": "string", "format": "date-time"},
            "conversations": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["id", "threadName", "messages"],
                    "properties": {
                        "id": {"type": "string"},
                        "threadName": {"type": "string"},
                        "threadType": {"type": "string"},
                        "messages": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "required": ["id", "messageType", "timestamp"],
                                "properties": {
                                    "id": {"type": "string"},
                                    "conversationId": {"type": "string"},
                                    "messageType": {"type": "string"},
                                    "timestamp": {
                                        "type": "string",
                                        "format": "date-time",
                                    },
                                    "content": {"type": ["string", "null"]},
                                    "fromDisplayName": {"type": "string"},
                                    "fromId": {"type": "string"},
                                    "properties": {"type": "object"},
                                    "attachments": {
                                        "type": "array",
                                        "items": {
                                            "type": "object",
                                            "properties": {
                                                "id": {"type": "string"},
                                                "name": {"type": "string"},
                                                "type": {"type": "string"},
                                                "url": {"type": "string"},
                                            },
                                        },
                                    },
                                },
                            },
                        },
                    },
                },
            },
        },
    }


def initialize_schemas(
    schema_dir: Optional[str] = None, overwrite: bool = False
) -> None:
    """
    Initialize the schema directory with default schemas.

    Args:
        schema_dir: Directory to save schemas to
        overwrite: Whether to overwrite existing schemas
    """
    # Create schema directory
    schema_dir = create_schema_directory(schema_dir)

    # Create and save app config schema
    app_config_schema = create_base_app_config_schema()
    try:
        save_schema(app_config_schema, "app_config_schema", schema_dir, overwrite)
    except FileExistsError:
        logger.info("App config schema already exists, skipping")

    # Create and save Skype export schema
    skype_export_schema = create_skype_export_schema()
    try:
        save_schema(skype_export_schema, "skype_export_schema", schema_dir, overwrite)
    except FileExistsError:
        logger.info("Skype export schema already exists, skipping")


if __name__ == "__main__":
    # Set up basic logging
    logging.basicConfig(level=logging.INFO)

    # Initialize schemas
    initialize_schemas(overwrite=True)

    logger.info("Schema initialization complete")
