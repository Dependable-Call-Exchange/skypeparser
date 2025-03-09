"""
Logging package for SkypeParser.

This package contains logging-related components.
"""

from .structured_logging import StructuredLogRecord, StructuredLogger, JSONFormatter, StructuredFormatter, setup_logging, get_logger, log_execution_time, log_call
from .logging_config import configure_logging, configure_library_loggers, load_logging_config, get_log_level_name, set_log_level, get_all_loggers, list_loggers, add_system_logging_context, configure_module_logging, configure_modules_logging, create_external_handler, get_module_loggers, set_modules_log_level, get_logger_hierarchy
from .logging_compat import get_logger, with_context, log_execution_time, log_call, handle_errors, StructuredLoggerAdapter, get_structured_logger, patch_logging
from .error_logger import ErrorStrategy, FatalErrorStrategy, WarningErrorStrategy, ErrorLogger
from .new_structured_logging import JsonFormatter, initialize_logging, get_logger, set_context, get_context_data, get_context_value, clear_context, update_context, LogContext, with_context, log_execution_time, log_call, handle_errors, get_structured_logger, log_metrics, get_system_metrics, log_system_metrics, measure_execution_time, create_request_context, with_request_context, log_database_query
from .error_handling import ErrorContext, handle_errors, generate_error_response, report_error, is_fatal_error, get_error_severity, safe_execute
