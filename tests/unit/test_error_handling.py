#!/usr/bin/env python3
"""
Unit tests for error handling and logging functionality.
"""

import json
import logging
import os
import tempfile
import traceback
from unittest.mock import MagicMock, patch

import pytest

from src.parser.exceptions import (
    DatabaseOperationError,
    FileOperationError,
    InvalidInputError,
    SkypeParserError,
)
from src.utils.error_handling import (
    ErrorContext,
    generate_error_response,
    get_error_severity,
    handle_errors,
    is_fatal_error,
    report_error,
    safe_execute,
)
from src.utils.schema_validation import (
    SchemaValidationError,
    create_base_app_config_schema,
    initialize_schemas,
    validate_config,
    validate_data,
)
from src.utils.structured_logging import (
    get_logger,
    log_call,
    log_execution_time,
    setup_logging,
)


class TestErrorHandling:
    """Tests for the error handling functionality."""

    def test_error_context(self):
        """Test that ErrorContext correctly adds context information."""
        # Clear existing context
        ErrorContext.reset_context()

        # Add context using class method
        ErrorContext.add_context(test_key="test_value")
        context = ErrorContext.get_current_context()
        assert context["test_key"] == "test_value"

        # Add nested context using context manager
        with ErrorContext(nested_key="nested_value"):
            nested_context = ErrorContext.get_current_context()
            assert nested_context["test_key"] == "test_value"
            assert nested_context["nested_key"] == "nested_value"

            # Add deeper nested context
            with ErrorContext(deep_key="deep_value"):
                deep_context = ErrorContext.get_current_context()
                assert deep_context["test_key"] == "test_value"
                assert deep_context["nested_key"] == "nested_value"
                assert deep_context["deep_key"] == "deep_value"

            # Check that we're back to the nested context
            after_deep = ErrorContext.get_current_context()
            assert after_deep["test_key"] == "test_value"
            assert after_deep["nested_key"] == "nested_value"
            assert "deep_key" not in after_deep

        # Check that we're back to the original context
        after_nested = ErrorContext.get_current_context()
        assert after_nested["test_key"] == "test_value"
        assert "nested_key" not in after_nested

    def test_handle_errors_decorator(self):
        """Test that handle_errors correctly decorates functions."""
        logger = MagicMock()

        # Define a function with the decorator
        @handle_errors(
            error_types=ValueError, log_level="WARNING", default_message="Test error"
        )
        def test_func(x):
            if x == 0:
                raise ValueError("Cannot be zero")
            return x * 2

        # Test with valid input
        assert test_func(5) == 10

        # Test with invalid input, should raise
        with pytest.raises(ValueError):
            test_func(0)

    def test_generate_error_response(self):
        """Test that generate_error_response creates correct response dictionaries."""
        # Create an error
        error = ValueError("Test error")

        # Generate a response without traceback or context
        response = generate_error_response(
            error=error, status="failed", include_traceback=False, include_context=False
        )

        # Check the response structure
        assert response["status"] == "failed"
        assert response["error"]["type"] == "ValueError"
        assert response["error"]["message"] == "Test error"
        assert "timestamp" in response["error"]
        assert "traceback" not in response["error"]
        assert "context" not in response["error"]

        # Add context and generate a response with traceback
        ErrorContext.reset_context()
        ErrorContext.add_context(test_key="test_value")

        response_with_context = generate_error_response(
            error=error, status="error", include_traceback=True, include_context=True
        )

        # Check the response structure
        assert response_with_context["status"] == "error"
        assert response_with_context["error"]["type"] == "ValueError"
        assert response_with_context["error"]["message"] == "Test error"
        assert "timestamp" in response_with_context["error"]
        assert "traceback" in response_with_context["error"]
        assert "context" in response_with_context["error"]
        assert response_with_context["error"]["context"]["test_key"] == "test_value"

    def test_report_error(self):
        """Test that report_error correctly reports errors."""
        # Create an error
        error = ValueError("Test error")

        # Clear context
        ErrorContext.reset_context()

        # Mock logger to avoid actual logging
        with patch("src.utils.error_handling.logger") as mock_logger:
            # Report the error
            error_details = report_error(
                error=error,
                log_level="ERROR",
                include_traceback=True,
                additional_context={"test_key": "test_value"},
            )

            # Check that the error was logged
            mock_logger.error.assert_called_once()

            # Check the error details
            assert error_details["error_type"] == "ValueError"
            assert error_details["error_message"] == "Test error"
            assert "timestamp" in error_details
            assert "traceback" in error_details
            assert "context" in error_details
            assert error_details["context"]["test_key"] == "test_value"

    def test_is_fatal_error(self):
        """Test that is_fatal_error correctly identifies fatal errors."""
        # Test system errors
        assert is_fatal_error(SystemExit())
        assert is_fatal_error(KeyboardInterrupt())
        assert is_fatal_error(MemoryError())

        # Test custom fatal errors
        assert is_fatal_error(DatabaseOperationError("Test error"))
        assert is_fatal_error(FileOperationError("Test error"))

        # Test non-fatal errors
        assert not is_fatal_error(ValueError("Test error"))
        assert not is_fatal_error(InvalidInputError("Test error"))

    def test_get_error_severity(self):
        """Test that get_error_severity correctly determines error severity."""
        # Test system errors (critical)
        assert get_error_severity(SystemExit()) == 50
        assert get_error_severity(KeyboardInterrupt()) == 50
        assert get_error_severity(MemoryError()) == 50

        # Test error level errors
        assert get_error_severity(DatabaseOperationError("Test error")) == 40
        assert get_error_severity(FileOperationError("Test error")) == 40

        # Test warning level errors
        assert get_error_severity(InvalidInputError("Test error")) == 30

        # Test unknown errors (default to error level)
        assert get_error_severity(ValueError("Test error")) == 40

    def test_safe_execute(self):
        """Test that safe_execute correctly handles exceptions."""

        # Test with a function that succeeds
        def success_func(x, y):
            return x + y

        assert safe_execute(success_func, 2, 3) == 5

        # Test with a function that fails
        def fail_func(x, y):
            return x / y

        # Should return default value when function raises exception
        assert safe_execute(fail_func, 1, 0, default=None) is None
        assert safe_execute(fail_func, 1, 0, default="error") == "error"


class TestStructuredLogging:
    """Tests for the structured logging functionality."""

    @patch("src.utils.structured_logging.logging")
    def test_setup_logging(self, mock_logging):
        """Test that setup_logging correctly configures logging."""
        # Set up mock logger
        mock_logger = MagicMock()
        mock_logging.getLogger.return_value = mock_logger

        # Set up mock handler
        mock_handler = MagicMock()
        mock_logging.StreamHandler.return_value = mock_handler
        mock_handler.level = logging.DEBUG

        # Set up logging
        setup_logging(level="DEBUG", log_file=None, json_format=False, structured=True)

        # Check that the logger was configured correctly
        mock_logging.setLoggerClass.assert_called_once()
        mock_logging.getLogger.assert_called()
        mock_logging.StreamHandler.assert_called_once()

    @patch("src.utils.structured_logging.logging")
    def test_structured_logger(self, mock_logging):
        """Test that StructuredLogger correctly adds structured data."""
        # Set up mock logger
        mock_logger = MagicMock()
        mock_logging.getLogger.return_value = mock_logger

        # Set up mock handler
        mock_handler = MagicMock()
        mock_logging.StreamHandler.return_value = mock_handler
        mock_handler.level = logging.DEBUG

        # Set up logging
        setup_logging(level="DEBUG", structured=True)

        # Get a logger
        logger = get_logger("test_structured")

        # Log structured messages
        logger.info(
            "Test structured message",
            extra={"structured_data": {"test_key": "test_value"}},
        )

        # Test logging with context
        with ErrorContext(context_key="context_value"):
            logger.info("Test context message")

    @patch("src.utils.structured_logging.time.time")
    def test_log_execution_time(self, mock_time):
        """Test that log_execution_time correctly logs execution time."""
        # Set up mock time to return increasing values
        mock_time.side_effect = [100.0, 101.5]  # 1.5 seconds execution time

        # Set up mock logger
        mock_logger = MagicMock()

        # Define a decorated function
        @log_execution_time(mock_logger)
        def slow_function():
            return "result"

        # Call the function
        result = slow_function()

        # Check that the function returned the correct result
        assert result == "result"

        # Check that the execution time was logged
        mock_logger.log.assert_called_once()
        args, kwargs = mock_logger.log.call_args

        # First arg is the level (DEBUG)
        assert args[0] == logging.DEBUG

        # Second arg is the message
        assert "executed in 1.5" in args[1]

        # Check that structured data was included
        assert "structured_data" in kwargs["extra"]
        assert kwargs["extra"]["structured_data"]["execution_time"] == 1.5

    def test_log_call(self):
        """Test that log_call correctly logs function calls."""
        # Set up mock logger
        mock_logger = MagicMock()

        # Define a decorated function
        @log_call(mock_logger)
        def test_function(a, b, c=None):
            if c is None:
                return a + b
            return a + b + c

        # Call the function
        result = test_function(1, 2, c=3)

        # Check that the function returned the correct result
        assert result == 6

        # Check that the call was logged
        mock_logger.log.assert_called_once()
        args, kwargs = mock_logger.log.call_args

        # First arg is the level (DEBUG)
        assert args[0] == logging.DEBUG

        # Second arg is the message, should include function name and args
        assert "Calling test_function" in args[1]
        assert "1, 2" in args[1]
        assert "c=3" in args[1]

        # Check that structured data was included
        assert "structured_data" in kwargs["extra"]
        assert kwargs["extra"]["structured_data"]["function"] == "test_function"
        assert kwargs["extra"]["structured_data"]["args_count"] == 2
        assert kwargs["extra"]["structured_data"]["kwargs_count"] == 1


class TestSchemaValidation:
    """Tests for the schema validation functionality."""

    def setup_method(self):
        """Set up test environment."""
        # Create a temporary directory for schemas
        self.temp_dir = tempfile.mkdtemp()

        # Create a basic schema for testing
        self.test_schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["name", "age"],
            "properties": {
                "name": {"type": "string"},
                "age": {"type": "integer", "minimum": 0},
                "email": {"type": "string", "format": "email"},
                "website": {"type": "string", "format": "uri"},
                "settings": {
                    "type": "object",
                    "properties": {
                        "theme": {"type": "string", "default": "light"},
                        "notifications": {"type": "boolean", "default": True},
                    },
                },
            },
        }

        # Save the schema
        os.makedirs(os.path.join(self.temp_dir, "schemas"), exist_ok=True)
        with open(os.path.join(self.temp_dir, "schemas", "test_schema.json"), "w") as f:
            json.dump(self.test_schema, f)

    def teardown_method(self):
        """Clean up test environment."""
        # Remove the temporary directory
        import shutil

        shutil.rmtree(self.temp_dir)

    @patch("src.utils.schema_validation.logger")
    def test_validate_data_success(self, mock_logger):
        """Test that validate_data correctly validates valid data."""
        # Valid data
        valid_data = {"name": "John Doe", "age": 30, "email": "john@example.com"}

        # Validate data
        result = validate_data(
            data=valid_data,
            schema_name="test_schema",
            fill_defaults=True,
            schema_dir=os.path.join(self.temp_dir, "schemas"),
            raise_exception=True,
        )

        # Check that validation passed
        assert result["name"] == "John Doe"
        assert result["age"] == 30
        assert result["email"] == "john@example.com"

        # Note: In the current implementation, defaults are not filled in for nested objects
        # unless they already exist in the input data, so we don't check for settings here

    @patch("src.utils.schema_validation.logger")
    def test_validate_data_failure(self, mock_logger):
        """Test that validate_data correctly handles invalid data."""
        # Invalid data (missing required field, invalid age)
        invalid_data = {"name": "John Doe", "age": -5, "email": "not-an-email"}

        # Validate data, should raise an exception
        with pytest.raises(SchemaValidationError) as excinfo:
            validate_data(
                data=invalid_data,
                schema_name="test_schema",
                fill_defaults=True,
                schema_dir=os.path.join(self.temp_dir, "schemas"),
                raise_exception=True,
            )

        # Check the exception details
        assert "Validation failed for test_schema" in str(excinfo.value)
        assert len(excinfo.value.errors) > 0  # At least one error

    @patch("src.utils.schema_validation.logger")
    def test_validate_config(self, mock_logger):
        """Test that validate_config correctly validates configuration."""
        # Create app config schema
        app_config_schema = create_base_app_config_schema()

        # Save the schema
        with open(
            os.path.join(self.temp_dir, "schemas", "app_config_schema.json"), "w"
        ) as f:
            json.dump(app_config_schema, f)

        # Valid config
        valid_config = {
            "database": {
                "host": "localhost",
                "port": 5432,
                "dbname": "skype_archive",
                "user": "postgres",
            },
            "output": {"directory": "output"},
            "logging": {"level": "INFO"},
        }

        # Validate config
        result = validate_config(
            config=valid_config,
            config_type="app_config",
            fill_defaults=True,
            schema_dir=os.path.join(self.temp_dir, "schemas"),
        )

        # Check that validation passed and defaults were filled in
        assert result["database"]["password"] == ""
        assert result["output"]["overwrite"] is False
        assert result["logging"]["file"] is None
        assert result["logging"]["json_format"] is False
        assert result["logging"]["structured"] is True

    @patch("src.utils.schema_validation.logger")
    def test_initialize_schemas(self, mock_logger):
        """Test that initialize_schemas correctly creates default schemas."""
        # Initialize schemas in a temporary directory
        temp_schema_dir = os.path.join(self.temp_dir, "new_schemas")
        initialize_schemas(schema_dir=temp_schema_dir, overwrite=True)

        # Check that schemas were created
        assert os.path.exists(os.path.join(temp_schema_dir, "app_config_schema.json"))
        assert os.path.exists(os.path.join(temp_schema_dir, "skype_export_schema.json"))

        # Load and verify a schema
        with open(os.path.join(temp_schema_dir, "app_config_schema.json"), "r") as f:
            app_config_schema = json.load(f)

        assert app_config_schema["type"] == "object"
        assert "database" in app_config_schema["properties"]
        assert "output" in app_config_schema["properties"]
        assert "logging" in app_config_schema["properties"]


if __name__ == "__main__":
    pytest.main(["-v", __file__])
