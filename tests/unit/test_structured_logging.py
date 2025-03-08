"""
Unit tests for the structured logging system.
"""

import json
import logging
import os
import sys
import unittest
from io import StringIO

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.utils.new_structured_logging import (
    JsonFormatter,
    LogContext,
    clear_context,
    get_context_data,
    get_logger,
    handle_errors,
    log_call,
    log_execution_time,
    set_context,
    with_context,
)
from tests.utils.test_logging import LogCapture, capture_logs


class TestStructuredLogging(unittest.TestCase):
    """Test cases for the structured logging system."""

    def setUp(self):
        """Set up test fixtures."""
        # Clear context before each test
        clear_context()

    def test_get_logger(self):
        """Test getting a logger."""
        logger = get_logger("test_logger")
        self.assertEqual(logger.name, "test_logger")

    def test_context_management(self):
        """Test context management."""
        # Set context
        set_context("key1", "value1")
        set_context("key2", "value2")

        # Get context data
        context_data = get_context_data()
        self.assertEqual(context_data["key1"], "value1")
        self.assertEqual(context_data["key2"], "value2")

        # Clear context
        clear_context()
        self.assertEqual(get_context_data(), {})

    def test_log_context_manager(self):
        """Test LogContext context manager."""
        with LogContext(key1="value1", key2="value2"):
            context_data = get_context_data()
            self.assertEqual(context_data["key1"], "value1")
            self.assertEqual(context_data["key2"], "value2")

        # Context should be cleared after exiting
        self.assertEqual(get_context_data(), {})

    def test_nested_log_context(self):
        """Test nested LogContext context managers."""
        with LogContext(key1="value1"):
            self.assertEqual(get_context_data()["key1"], "value1")

            with LogContext(key2="value2"):
                context_data = get_context_data()
                self.assertEqual(context_data["key1"], "value1")
                self.assertEqual(context_data["key2"], "value2")

            # Inner context should be cleared
            self.assertEqual(get_context_data()["key1"], "value1")
            self.assertNotIn("key2", get_context_data())

        # Outer context should be cleared
        self.assertEqual(get_context_data(), {})

    def test_json_formatter(self):
        """Test JSON formatter."""
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        # Format the record
        formatted = formatter.format(record)
        log_data = json.loads(formatted)

        # Check basic fields
        self.assertEqual(log_data["logger"], "test_logger")
        self.assertEqual(log_data["level"], "INFO")
        self.assertEqual(log_data["message"], "Test message")
        self.assertEqual(log_data["module"], "test")
        self.assertEqual(log_data["function"], "")
        self.assertEqual(log_data["line"], 1)

    def test_json_formatter_with_context(self):
        """Test JSON formatter with context."""
        formatter = JsonFormatter()
        record = logging.LogRecord(
            name="test_logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=1,
            msg="Test message",
            args=(),
            exc_info=None,
        )

        # Set context
        set_context("key1", "value1")
        set_context("key2", "value2")

        # Format the record
        formatted = formatter.format(record)
        log_data = json.loads(formatted)

        # Check context fields
        self.assertEqual(log_data["context"]["key1"], "value1")
        self.assertEqual(log_data["context"]["key2"], "value2")

    def test_json_formatter_with_exception(self):
        """Test JSON formatter with exception."""
        formatter = JsonFormatter()
        try:
            raise ValueError("Test error")
        except ValueError:
            record = logging.LogRecord(
                name="test_logger",
                level=logging.ERROR,
                pathname="test.py",
                lineno=1,
                msg="Test error message",
                args=(),
                exc_info=sys.exc_info(),
            )

        # Format the record
        formatted = formatter.format(record)
        log_data = json.loads(formatted)

        # Check exception fields
        self.assertEqual(log_data["exception"]["type"], "ValueError")
        self.assertEqual(log_data["exception"]["message"], "Test error")
        # Check that traceback is a list of strings
        self.assertTrue(isinstance(log_data["exception"]["traceback"], list))
        # Check that at least one line contains "Traceback"
        self.assertTrue(
            any("Traceback" in line for line in log_data["exception"]["traceback"])
        )

    @capture_logs
    def test_log_capture(self, logs):
        """Test log capture utility."""
        logger = get_logger("test_logger")
        logger.info("Test info message")
        logger.error("Test error message")

        # Check captured logs
        log_messages = logs.get_log_messages()
        self.assertIn("Test info message", log_messages)
        self.assertIn("Test error message", log_messages)

        # Check logs by level
        info_logs = logs.get_logs_with_level("INFO")
        error_logs = logs.get_logs_with_level("ERROR")
        self.assertEqual(len(info_logs), 1)
        self.assertEqual(len(error_logs), 1)
        self.assertEqual(info_logs[0]["message"], "Test info message")
        self.assertEqual(error_logs[0]["message"], "Test error message")

    def test_with_context_decorator(self):
        """Test with_context decorator."""

        @with_context(key1="value1", key2="value2")
        def test_function():
            return get_context_data()

        # Call the function
        context_data = test_function()
        self.assertEqual(context_data["key1"], "value1")
        self.assertEqual(context_data["key2"], "value2")

        # Context should be cleared after function returns
        self.assertEqual(get_context_data(), {})

    def test_with_context_decorator_with_callable(self):
        """Test with_context decorator with callable values."""

        def get_value(arg):
            return f"value-{arg}"

        @with_context(key1=lambda arg: f"value-{arg}")
        def test_function(arg):
            return get_context_data()

        # Call the function
        context_data = test_function("test")
        self.assertEqual(context_data["key1"], "value-test")

    @capture_logs
    def test_log_execution_time(self, logs):
        """Test log_execution_time decorator."""
        logger = get_logger("test_logger")

        @log_execution_time(logger)
        def test_function():
            return "result"

        # Call the function
        result = test_function()
        self.assertEqual(result, "result")

        # Check that execution time was logged
        self.assertTrue(logs.assert_log_contains("completed in", "INFO"))
        self.assertTrue(any("duration_ms" in log for log in logs.get_logs()))

    @capture_logs
    def test_log_call(self, logs):
        """Test log_call decorator."""
        logger = get_logger("test_logger")

        @log_call(logger)
        def test_function(arg1, arg2, kwarg1="default"):
            return "result"

        # Call the function
        result = test_function("value1", "value2", kwarg1="custom")
        self.assertEqual(result, "result")

        # Check that the call was logged
        self.assertTrue(logs.assert_log_contains("test_function", "DEBUG"))
        self.assertTrue(logs.assert_log_contains("value1", "DEBUG"))
        self.assertTrue(logs.assert_log_contains("value2", "DEBUG"))
        self.assertTrue(logs.assert_log_contains("custom", "DEBUG"))

    @capture_logs
    def test_handle_errors(self, logs):
        """Test handle_errors decorator."""
        logger = get_logger("test_logger")

        @handle_errors(logger, log_level="ERROR", default_message="Function failed")
        def test_function():
            raise ValueError("Test error")

        # Call the function (should raise exception)
        with self.assertRaises(ValueError):
            test_function()

        # Check that error was logged
        self.assertTrue(logs.assert_log_contains("Function failed", "ERROR"))
        self.assertTrue(logs.assert_log_contains("ValueError", "ERROR"))
        self.assertTrue(logs.assert_log_contains("Test error", "ERROR"))

    @capture_logs
    def test_handle_errors_no_reraise(self, logs):
        """Test handle_errors decorator with reraise=False."""
        logger = get_logger("test_logger")

        @handle_errors(
            logger,
            log_level="ERROR",
            default_message="Function failed",
            reraise=False,
            default_return="default",
        )
        def test_function():
            raise ValueError("Test error")

        # Call the function (should not raise exception)
        result = test_function()
        self.assertEqual(result, "default")

        # Check that error was logged
        self.assertTrue(logs.assert_log_contains("Function failed", "ERROR"))
        self.assertTrue(logs.assert_log_contains("ValueError", "ERROR"))
        self.assertTrue(logs.assert_log_contains("Test error", "ERROR"))


if __name__ == "__main__":
    unittest.main()
