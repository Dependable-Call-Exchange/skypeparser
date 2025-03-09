"""
Unit tests for the structured logging system using pytest.
"""

import json
import logging
import os
import sys
import pytest
from io import StringIO
from functools import wraps

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
from tests.utils.test_logging import LogCapture


@pytest.fixture(autouse=True)
def setup_teardown():
    """Set up test fixtures."""
    # Clear context before each test
    clear_context()
    yield
    # Ensure context is cleared after each test as well
    clear_context()


@pytest.fixture
def logs():
    """Create a log capture fixture for tests."""
    with LogCapture() as logs_capture:
        yield logs_capture


def test_get_logger():
    """Test getting a logger."""
    logger = get_logger("test_logger")
    assert logger.name == "test_logger"


def test_context_management():
    """Test context management."""
    # Set context
    set_context("key1", "value1")
    set_context("key2", "value2")

    # Get context data
    context_data = get_context_data()
    assert context_data["key1"] == "value1"
    assert context_data["key2"] == "value2"

    # Clear context
    clear_context()
    assert get_context_data() == {}


def test_log_context_manager():
    """Test LogContext context manager."""
    with LogContext(key1="value1", key2="value2"):
        context_data = get_context_data()
        assert context_data["key1"] == "value1"
        assert context_data["key2"] == "value2"

    # Context should be cleared after exiting
    assert get_context_data() == {}


def test_nested_log_context():
    """Test nested LogContext context managers."""
    with LogContext(key1="value1"):
        assert get_context_data()["key1"] == "value1"

        with LogContext(key2="value2"):
            context_data = get_context_data()
            assert context_data["key1"] == "value1"
            assert context_data["key2"] == "value2"

        # Inner context should be cleared
        assert get_context_data()["key1"] == "value1"
        assert "key2" not in get_context_data()

    # Outer context should be cleared
    assert get_context_data() == {}


def test_json_formatter():
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
    assert log_data["logger"] == "test_logger"
    assert log_data["level"] == "INFO"
    assert log_data["message"] == "Test message"
    assert log_data["module"] == "test"
    assert log_data["function"] == ""
    assert log_data["line"] == 1


def test_json_formatter_with_context():
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
    assert log_data["context"]["key1"] == "value1"
    assert log_data["context"]["key2"] == "value2"


def test_json_formatter_with_exception():
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
    assert log_data["exception"]["type"] == "ValueError"
    assert log_data["exception"]["message"] == "Test error"
    # Check that traceback is a list of strings
    assert isinstance(log_data["exception"]["traceback"], list)
    # Check that at least one line contains "Traceback"
    assert any("Traceback" in line for line in log_data["exception"]["traceback"])


def test_log_capture(logs):
    """Test log capture utility."""
    logger = get_logger("test_logger")
    logger.info("Test info message")
    logger.error("Test error message")

    # Check captured logs
    log_messages = logs.get_log_messages()
    assert "Test info message" in log_messages
    assert "Test error message" in log_messages

    # Check logs by level
    info_logs = logs.get_logs_with_level("INFO")
    error_logs = logs.get_logs_with_level("ERROR")
    assert len(info_logs) == 1
    assert len(error_logs) == 1
    assert info_logs[0]["message"] == "Test info message"
    assert error_logs[0]["message"] == "Test error message"


def test_with_context_decorator():
    """Test with_context decorator."""

    @with_context(key1="value1", key2="value2")
    def test_function():
        return get_context_data()

    # Call the function
    context_data = test_function()
    assert context_data["key1"] == "value1"
    assert context_data["key2"] == "value2"

    # Context should be cleared after function returns
    assert get_context_data() == {}


def test_with_context_decorator_with_callable():
    """Test with_context decorator with callable values."""

    def get_value(arg):
        return f"value-{arg}"

    @with_context(key1=lambda arg: f"value-{arg}")
    def test_function(arg):
        return get_context_data()

    # Call the function
    context_data = test_function("test")
    assert context_data["key1"] == "value-test"


@pytest.fixture
def logger():
    """Provide a logger for tests."""
    return get_logger("test_logger")


def test_log_execution_time(logger, logs):
    """Test log_execution_time decorator."""

    @log_execution_time(logger)
    def test_function():
        return "result"

    # Call the function
    result = test_function()
    assert result == "result"

    # Check that execution time was logged
    assert logs.assert_log_contains("completed in", "INFO")
    assert any("duration_ms" in log for log in logs.get_logs())


def test_log_call(logger, logs):
    """Test log_call decorator."""

    @log_call(logger)
    def test_function(arg1, arg2, kwarg1="default"):
        return "result"

    # Call the function
    result = test_function("value1", "value2", kwarg1="custom")
    assert result == "result"

    # Check that the call was logged
    assert logs.assert_log_contains("test_function", "DEBUG")
    assert logs.assert_log_contains("value1", "DEBUG")
    assert logs.assert_log_contains("value2", "DEBUG")
    assert logs.assert_log_contains("custom", "DEBUG")


def test_handle_errors(logger, logs):
    """Test handle_errors decorator."""

    @handle_errors(logger, log_level="ERROR", default_message="Function failed")
    def test_function():
        raise ValueError("Test error")

    # Call the function (should raise exception)
    with pytest.raises(ValueError, match="Test error"):
        test_function()

    # Check that error was logged
    assert logs.assert_log_contains("Function failed", "ERROR")
    assert logs.assert_log_contains("ValueError", "ERROR")
    assert logs.assert_log_contains("Test error", "ERROR")


def test_handle_errors_no_reraise(logger, logs):
    """Test handle_errors decorator with reraise=False."""

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
    assert result == "default"

    # Check that error was logged
    assert logs.assert_log_contains("Function failed", "ERROR")
    assert logs.assert_log_contains("ValueError", "ERROR")
    assert logs.assert_log_contains("Test error", "ERROR")
