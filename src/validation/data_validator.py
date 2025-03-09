"""
Data validator for validating input data.

This module provides the DataValidator class that handles
validation of input data for the ETL pipeline.
"""

import logging
from typing import Dict, Any, List, Optional, Callable

from src.logging.new_structured_logging import get_logger

logger = get_logger(__name__)


class DataValidator:
    """Handles validation of input data."""

    def __init__(self, strict_mode: bool = False):
        """Initialize the data validator.

        Args:
            strict_mode: Whether to use strict validation
        """
        self.strict_mode = strict_mode
        self.validation_errors = []

    def validate(self, data: Dict[str, Any], schema: Optional[Dict[str, Any]] = None) -> bool:
        """Validate data against a schema.

        Args:
            data: Data to validate
            schema: Schema to validate against

        Returns:
            Whether the data is valid
        """
        self.validation_errors = []

        # Basic validation
        if not isinstance(data, dict):
            self._add_error("Data must be a dictionary")
            return False

        # If no schema is provided, use default validation
        if not schema:
            return self._validate_default(data)

        # Validate against schema
        return self._validate_schema(data, schema)

    def _validate_default(self, data: Dict[str, Any]) -> bool:
        """Validate data using default validation rules.

        Args:
            data: Data to validate

        Returns:
            Whether the data is valid
        """
        # Check for required keys - support both formats
        if "messages" not in data and "conversations" not in data:
            self._add_error("Data must contain either 'messages' or 'conversations' key")
            return False

        # If we have conversations, validate them
        if "conversations" in data:
            return self._validate_conversations(data.get("conversations", []))

        # If we have messages, validate them
        if "messages" in data:
            return self._validate_messages(data.get("messages", []))

        return True

    def _validate_conversations(self, conversations: Any) -> bool:
        """Validate conversations.

        Args:
            conversations: Conversations to validate

        Returns:
            Whether the conversations are valid
        """
        # Check if conversations is a list
        if not isinstance(conversations, list):
            self._add_error("Conversations must be a list")
            return False

        # In non-strict mode, empty conversations list is valid
        if not conversations and not self.strict_mode:
            return True

        # In strict mode, at least one conversation is required
        if not conversations and self.strict_mode:
            self._add_error("At least one conversation is required")
            return False

        # Check if at least one conversation has a MessageList
        has_messages = False
        for conv in conversations:
            if isinstance(conv, dict) and "MessageList" in conv:
                has_messages = True
                break

        # In strict mode, at least one conversation must have a MessageList
        if not has_messages and self.strict_mode:
            self._add_error("At least one conversation must have a MessageList")
            return False

        return True

    def _validate_messages(self, messages: Any) -> bool:
        """Validate messages.

        Args:
            messages: Messages to validate

        Returns:
            Whether the messages are valid
        """
        # Check if messages is a list
        if not isinstance(messages, list):
            self._add_error("Messages must be a list")
            return False

        # In non-strict mode, empty messages list is valid
        if not messages and not self.strict_mode:
            return True

        # In strict mode, at least one message is required
        if not messages and self.strict_mode:
            self._add_error("At least one message is required")
            return False

        return True

    def _validate_schema(self, data: Dict[str, Any], schema: Dict[str, Any]) -> bool:
        """Validate data against a schema.

        Args:
            data: Data to validate
            schema: Schema to validate against

        Returns:
            Whether the data is valid
        """
        # Check required fields
        for field, field_schema in schema.items():
            if field_schema.get("required", False) and field not in data:
                self._add_error(f"Required field '{field}' is missing")
                if self.strict_mode:
                    return False

        # Validate fields
        for field, value in data.items():
            if field in schema:
                field_schema = schema[field]
                field_type = field_schema.get("type")

                # Validate type
                if field_type and not self._validate_type(value, field_type):
                    self._add_error(f"Field '{field}' has invalid type, expected {field_type}")
                    if self.strict_mode:
                        return False

                # Validate enum
                if "enum" in field_schema and value not in field_schema["enum"]:
                    self._add_error(f"Field '{field}' has invalid value, expected one of {field_schema['enum']}")
                    if self.strict_mode:
                        return False

                # Validate custom validator
                if "validator" in field_schema and callable(field_schema["validator"]):
                    if not field_schema["validator"](value):
                        self._add_error(f"Field '{field}' failed custom validation")
                        if self.strict_mode:
                            return False

        return len(self.validation_errors) == 0

    def _validate_type(self, value: Any, expected_type: str) -> bool:
        """Validate the type of a value.

        Args:
            value: Value to validate
            expected_type: Expected type

        Returns:
            Whether the value has the expected type
        """
        if expected_type == "string":
            return isinstance(value, str)
        elif expected_type == "number":
            return isinstance(value, (int, float))
        elif expected_type == "integer":
            return isinstance(value, int)
        elif expected_type == "boolean":
            return isinstance(value, bool)
        elif expected_type == "array":
            return isinstance(value, list)
        elif expected_type == "object":
            return isinstance(value, dict)
        elif expected_type == "null":
            return value is None
        else:
            return True

    def _add_error(self, error: str) -> None:
        """Add a validation error.

        Args:
            error: Error message
        """
        logger.debug(f"Validation error: {error}")
        self.validation_errors.append(error)

    def get_errors(self) -> List[str]:
        """Get validation errors.

        Returns:
            List of validation errors
        """
        return self.validation_errors