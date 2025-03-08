"""
Logging Test Utilities

This module provides utilities for testing logging functionality.
"""

import json
import logging
from io import StringIO
from typing import List, Dict, Any, Optional

from src.utils.new_structured_logging import JsonFormatter


class LogCapture:
    """Capture logs for testing."""

    def __init__(self, level: int = logging.DEBUG, json_format: bool = True):
        """
        Initialize the log capture.

        Args:
            level: Minimum log level to capture
            json_format: Whether to use JSON formatting
        """
        self.log_output = StringIO()
        self.handler = logging.StreamHandler(self.log_output)

        if json_format:
            self.handler.setFormatter(JsonFormatter())
        else:
            self.handler.setFormatter(logging.Formatter('%(levelname)s:%(name)s:%(message)s'))

        self.handler.setLevel(level)
        self.old_level = None

    def __enter__(self):
        """
        Start capturing logs.

        Returns:
            Self for context manager
        """
        root_logger = logging.getLogger()
        self.old_level = root_logger.level
        root_logger.setLevel(logging.DEBUG)
        root_logger.addHandler(self.handler)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Stop capturing logs.
        """
        root_logger = logging.getLogger()
        root_logger.setLevel(self.old_level)
        root_logger.removeHandler(self.handler)

    def get_logs(self) -> List[Dict[str, Any]]:
        """
        Get captured logs as a list of dictionaries.

        Returns:
            List of log records as dictionaries
        """
        logs = []
        for line in self.log_output.getvalue().splitlines():
            if line.strip():
                try:
                    logs.append(json.loads(line))
                except json.JSONDecodeError:
                    # Handle non-JSON formatted logs
                    parts = line.split(':', 2)
                    if len(parts) == 3:
                        level, name, message = parts
                        logs.append({
                            'level': level,
                            'logger': name,
                            'message': message
                        })
        return logs

    def get_log_messages(self) -> List[str]:
        """
        Get captured log messages as a list of strings.

        Returns:
            List of log messages
        """
        return [log.get('message', '') for log in self.get_logs()]

    def get_logs_for_logger(self, logger_name: str) -> List[Dict[str, Any]]:
        """
        Get logs for a specific logger.

        Args:
            logger_name: Logger name

        Returns:
            List of log records for the specified logger
        """
        return [log for log in self.get_logs() if log.get('logger') == logger_name]

    def get_logs_with_level(self, level: str) -> List[Dict[str, Any]]:
        """
        Get logs with a specific level.

        Args:
            level: Log level (e.g., 'INFO', 'ERROR')

        Returns:
            List of log records with the specified level
        """
        return [log for log in self.get_logs() if log.get('level') == level]

    def assert_log_contains(self, message_substring: str, level: Optional[str] = None) -> bool:
        """
        Assert that the logs contain a message with the specified substring.

        Args:
            message_substring: Substring to search for in log messages
            level: Optional log level to filter by

        Returns:
            True if the logs contain the substring, False otherwise
        """
        logs = self.get_logs()
        if level:
            logs = [log for log in logs if log.get('level') == level]

        # Check message field
        if any(message_substring in log.get('message', '') for log in logs):
            return True

        # Check exception fields
        for log in logs:
            if 'exception' in log:
                exception = log['exception']
                if message_substring in exception.get('type', ''):
                    return True
                if message_substring in exception.get('message', ''):
                    return True
                if isinstance(exception.get('traceback'), list):
                    if any(message_substring in line for line in exception['traceback']):
                        return True
                elif isinstance(exception.get('traceback'), str):
                    if message_substring in exception['traceback']:
                        return True

        # Check context fields
        for log in logs:
            if 'context' in log:
                context = log['context']
                if message_substring in str(context):
                    return True

        # Check extra fields
        for log in logs:
            for key, value in log.items():
                if key not in ['message', 'level', 'logger', 'timestamp', 'module', 'function', 'line', 'exception', 'context']:
                    if message_substring in str(value):
                        return True

        return False

    def assert_log_matches(self, predicate, message: Optional[str] = None) -> bool:
        """
        Assert that at least one log matches the predicate function.

        Args:
            predicate: Function that takes a log record and returns a boolean
            message: Optional message to display if the assertion fails

        Returns:
            True if at least one log matches the predicate, False otherwise
        """
        logs = self.get_logs()
        result = any(predicate(log) for log in logs)

        if not result and message:
            print(f"Assertion failed: {message}")
            print(f"Logs: {logs}")

        return result


def capture_logs(func):
    """
    Decorator to capture logs during a test function.

    Args:
        func: Test function to decorate

    Returns:
        Decorated function
    """
    def wrapper(*args, **kwargs):
        with LogCapture() as logs:
            # Add logs to kwargs
            kwargs['logs'] = logs
            return func(*args, **kwargs)
    return wrapper