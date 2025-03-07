#!/usr/bin/env python3
"""
Serialization Utils

This module provides utilities for serializing complex data types,
particularly for JSON serialization of datetime objects and other
non-standard types.
"""

import json
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

logger = logging.getLogger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    """
    Custom JSON encoder that can handle datetime objects.

    Converts datetime objects to ISO 8601 format strings during JSON serialization.
    """

    def default(self, obj: Any) -> Any:
        """
        Override the default method to handle datetime objects.

        Args:
            obj: Object to serialize

        Returns:
            JSON serializable representation of the object
        """
        if isinstance(obj, datetime):
            return obj.isoformat()
        # Let the base class handle anything else or raise TypeError
        return super().default(obj)


def to_serializable(obj: Any) -> Any:
    """
    Convert an object to a JSON serializable representation.

    Handles:
    - datetime objects (converts to ISO format)
    - complex nested structures (dicts, lists)
    - basic types (returns as is)

    Args:
        obj: Object to convert

    Returns:
        JSON serializable representation of the object
    """
    if isinstance(obj, datetime):
        return obj.isoformat()
    elif isinstance(obj, dict):
        return {k: to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [to_serializable(item) for item in obj]
    # Add more type handlers as needed
    return obj


def serialize_to_json(
    data: Any,
    file_path: Optional[str] = None,
    indent: int = 2,
    ensure_ascii: bool = False,
) -> Optional[str]:
    """
    Serialize data to JSON, handling complex types like datetimes.

    Args:
        data: Data to serialize
        file_path: Optional path to save the JSON data. If None, returns the JSON string
        indent: Number of spaces for indentation in the output
        ensure_ascii: If True, ensure output contains only ASCII characters

    Returns:
        JSON string if file_path is None, otherwise None (data is written to file)
    """
    try:
        # Use either the custom encoder or preprocess the data
        if file_path:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(
                    data,
                    f,
                    indent=indent,
                    ensure_ascii=ensure_ascii,
                    cls=DateTimeEncoder,
                )
            logger.debug(f"Serialized data to {file_path}")
            return None
        else:
            return json.dumps(
                data, indent=indent, ensure_ascii=ensure_ascii, cls=DateTimeEncoder
            )
    except (TypeError, ValueError, OSError) as e:
        logger.error(f"Error serializing data: {str(e)}")
        # Fall back to using our manual conversion
        preprocessed_data = to_serializable(data)

        if file_path:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(
                    preprocessed_data, f, indent=indent, ensure_ascii=ensure_ascii
                )
            logger.debug(f"Serialized preprocessed data to {file_path}")
            return None
        else:
            return json.dumps(
                preprocessed_data, indent=indent, ensure_ascii=ensure_ascii
            )


def deserialize_from_json(
    json_str: Optional[str] = None, file_path: Optional[str] = None
) -> Any:
    """
    Deserialize data from JSON.

    Args:
        json_str: JSON string to parse. Ignored if file_path is provided
        file_path: Path to JSON file to read

    Returns:
        Deserialized data
    """
    try:
        if file_path:
            with open(file_path, "r", encoding="utf-8") as f:
                return json.load(f)
        elif json_str:
            return json.loads(json_str)
        else:
            raise ValueError("Either json_str or file_path must be provided")
    except (json.JSONDecodeError, OSError) as e:
        logger.error(f"Error deserializing data: {str(e)}")
        raise
