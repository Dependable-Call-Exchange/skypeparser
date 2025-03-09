"""
Logging Configuration Module

This module provides centralized configuration for the application's logging system.
It includes functions for setting up logging with different configurations for
development, testing, and production environments.
"""

import os
import logging
import json
import socket
import platform
from typing import Dict, Any, Optional, List, Union, Set

from src.utils.new_structured_logging import (
    initialize_logging,
    get_logger,
    set_context,
    update_context,
    get_system_metrics
)

# Default log levels for different environments
DEFAULT_LOG_LEVELS = {
    "development": "DEBUG",
    "testing": "INFO",
    "production": "WARNING"
}

# Default log files for different environments
DEFAULT_LOG_FILES = {
    "development": "skype_parser_dev.log",
    "testing": "skype_parser_test.log",
    "production": "skype_parser.log"
}

# Noisy libraries to set higher log levels for
NOISY_LIBRARIES = {
    "urllib3": logging.WARNING,
    "psycopg2": logging.WARNING,
    "matplotlib": logging.WARNING,
    "PIL": logging.WARNING,
    "requests": logging.WARNING,
    "boto3": logging.WARNING,
    "botocore": logging.WARNING,
    "paramiko": logging.WARNING,
    "asyncio": logging.WARNING,
    "sqlalchemy": logging.WARNING,
    "werkzeug": logging.WARNING,
    "chardet": logging.WARNING,
    "parso": logging.WARNING,
    "jedi": logging.WARNING,
}

# Default log directories for different environments
DEFAULT_LOG_DIRS = {
    "development": "logs/dev",
    "testing": "logs/test",
    "production": "logs"
}


def configure_logging(
    app_name: str = "skype_parser",
    environment: str = "development",
    log_level: Optional[str] = None,
    log_file: Optional[str] = None,
    log_dir: Optional[str] = None,
    config: Optional[Dict[str, Any]] = None,
    enable_console: bool = True,
    enable_file: bool = True,
    enable_json: bool = True,
    max_file_size_mb: int = 10,
    backup_count: int = 5,
    add_system_context: bool = True,
    configure_libraries: bool = True,
    external_handlers: Optional[List[logging.Handler]] = None,
) -> None:
    """Configure logging for the application.

    Args:
        app_name: Application name
        environment: Environment (development, testing, production)
        log_level: Logging level (overrides environment default)
        log_file: Log file path (overrides environment default)
        log_dir: Directory for log files
        config: Configuration dictionary
        enable_console: Whether to enable console logging
        enable_file: Whether to enable file logging
        enable_json: Whether to use JSON formatting
        max_file_size_mb: Maximum log file size in MB
        backup_count: Number of backup log files to keep
        add_system_context: Whether to add system information to the logging context
        configure_libraries: Whether to configure third-party libraries
        external_handlers: Additional logging handlers to add
    """
    # Get logger for this module
    logger = get_logger(__name__)

    # Merge with provided config
    if config:
        # Extract logging-related config
        if "logging" in config:
            logging_config = config["logging"]
            if "level" in logging_config and not log_level:
                log_level = logging_config["level"]
            if "file" in logging_config and not log_file:
                log_file = logging_config["file"]
            if "dir" in logging_config and not log_dir:
                log_dir = logging_config["dir"]
            if "enable_console" in logging_config:
                enable_console = logging_config["enable_console"]
            if "enable_file" in logging_config:
                enable_file = logging_config["enable_file"]
            if "enable_json" in logging_config:
                enable_json = logging_config["enable_json"]
            if "max_file_size_mb" in logging_config:
                max_file_size_mb = logging_config["max_file_size_mb"]
            if "backup_count" in logging_config:
                backup_count = logging_config["backup_count"]
            if "add_system_context" in logging_config:
                add_system_context = logging_config["add_system_context"]
            if "configure_libraries" in logging_config:
                configure_libraries = logging_config["configure_libraries"]

    # Get default log level for environment
    if not log_level:
        log_level = DEFAULT_LOG_LEVELS.get(environment, "INFO")

    # Get default log file for environment
    if not log_file:
        log_file = DEFAULT_LOG_FILES.get(environment, f"{app_name}.log")

    # Get default log directory for environment
    if not log_dir and enable_file:
        log_dir = DEFAULT_LOG_DIRS.get(environment, "logs")

    # Prepend log directory if specified
    if log_dir and enable_file:
        # Create log directory if it doesn't exist
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, log_file)

    # Initialize logging
    initialize_logging(
        log_level=log_level,
        log_file=log_file if enable_file else None,
        enable_console=enable_console,
        enable_json=enable_json,
        max_file_size_mb=max_file_size_mb,
        backup_count=backup_count
    )

    # Add external handlers if provided
    if external_handlers:
        root_logger = logging.getLogger()
        for handler in external_handlers:
            root_logger.addHandler(handler)

    # Configure library loggers
    if configure_libraries:
        configure_library_loggers()

    # Add system context
    if add_system_context:
        add_system_logging_context(app_name, environment)

    # Log configuration
    logger.info(
        f"Logging configured for {app_name} in {environment} environment",
        extra={
            "app_name": app_name,
            "environment": environment,
            "log_level": log_level,
            "log_file": log_file if enable_file else None,
            "enable_console": enable_console,
            "enable_json": enable_json
        }
    )


def configure_library_loggers() -> None:
    """Configure logging for third-party libraries."""
    for library, level in NOISY_LIBRARIES.items():
        logging.getLogger(library).setLevel(level)


def load_logging_config(config_file: str) -> Dict[str, Any]:
    """Load logging configuration from a JSON file.

    Args:
        config_file: Path to the configuration file

    Returns:
        Configuration dictionary
    """
    try:
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config
    except (json.JSONDecodeError, FileNotFoundError) as e:
        logger = get_logger(__name__)
        logger.warning(f"Failed to load logging config from {config_file}: {e}")
        return {}


def get_log_level_name(level: Union[int, str]) -> str:
    """Get the name of a log level.

    Args:
        level: Log level (int or string)

    Returns:
        Log level name
    """
    if isinstance(level, str):
        return level.upper()
    return logging.getLevelName(level)


def set_log_level(logger_name: str, level: Union[int, str]) -> None:
    """Set the log level for a logger.

    Args:
        logger_name: Logger name
        level: Log level (int or string)
    """
    logger = logging.getLogger(logger_name)
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(level)


def get_all_loggers() -> Dict[str, logging.Logger]:
    """Get all loggers in the application.

    Returns:
        Dictionary of logger names to logger objects
    """
    return {name: logging.getLogger(name) for name in logging.root.manager.loggerDict}


def list_loggers() -> List[Dict[str, Any]]:
    """List all loggers and their levels.

    Returns:
        List of dictionaries with logger information
    """
    loggers = []
    for name, logger in get_all_loggers().items():
        loggers.append({
            "name": name,
            "level": logging.getLevelName(logger.level),
            "effective_level": logging.getLevelName(logger.getEffectiveLevel()),
            "handlers": len(logger.handlers),
            "propagate": logger.propagate
        })
    return loggers


def add_system_logging_context(app_name: str, environment: str) -> None:
    """Add system information to the logging context.

    Args:
        app_name: Application name
        environment: Environment name
    """
    # Get system information
    system_info = {
        "app_name": app_name,
        "environment": environment,
        "hostname": socket.gethostname(),
        "os": platform.system(),
        "os_version": platform.release(),
        "python_version": platform.python_version(),
    }

    # Add system metrics
    system_metrics = get_system_metrics()
    system_info.update({
        "memory_rss_mb": system_metrics["memory_rss_mb"],
        "cpu_count": os.cpu_count() or 0,
    })

    # Update context
    update_context(**system_info)


def configure_module_logging(module_name: str, level: Union[int, str]) -> None:
    """Configure logging for a specific module.

    Args:
        module_name: Module name
        level: Logging level
    """
    logger = logging.getLogger(module_name)
    if isinstance(level, str):
        level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(level)


def configure_modules_logging(module_levels: Dict[str, Union[int, str]]) -> None:
    """Configure logging for multiple modules.

    Args:
        module_levels: Dictionary mapping module names to log levels
    """
    for module_name, level in module_levels.items():
        configure_module_logging(module_name, level)


def create_external_handler(
    handler_type: str,
    **kwargs
) -> Optional[logging.Handler]:
    """Create an external logging handler.

    Args:
        handler_type: Type of handler to create (syslog, http, etc.)
        **kwargs: Additional arguments for the handler

    Returns:
        Logging handler or None if the handler type is not supported
    """
    if handler_type == "syslog":
        from logging.handlers import SysLogHandler
        facility = kwargs.get("facility", SysLogHandler.LOG_USER)
        address = kwargs.get("address", "/dev/log")
        return SysLogHandler(address=address, facility=facility)

    elif handler_type == "http":
        from logging.handlers import HTTPHandler
        host = kwargs.get("host", "localhost")
        url = kwargs.get("url", "/log")
        method = kwargs.get("method", "POST")
        return HTTPHandler(host, url, method=method)

    elif handler_type == "smtp":
        from logging.handlers import SMTPHandler
        mailhost = kwargs.get("mailhost", "localhost")
        fromaddr = kwargs.get("fromaddr", "skype_parser@localhost")
        toaddrs = kwargs.get("toaddrs", ["admin@localhost"])
        subject = kwargs.get("subject", "SkypeParser Log")
        credentials = kwargs.get("credentials", None)
        return SMTPHandler(mailhost, fromaddr, toaddrs, subject, credentials=credentials)

    elif handler_type == "file":
        from logging.handlers import RotatingFileHandler
        filename = kwargs.get("filename", "external.log")
        maxBytes = kwargs.get("maxBytes", 10 * 1024 * 1024)  # 10 MB
        backupCount = kwargs.get("backupCount", 5)
        return RotatingFileHandler(filename, maxBytes=maxBytes, backupCount=backupCount)

    else:
        logger = get_logger(__name__)
        logger.warning(f"Unsupported external handler type: {handler_type}")
        return None


def get_module_loggers(prefix: str) -> List[logging.Logger]:
    """Get all loggers for modules with the given prefix.

    Args:
        prefix: Module name prefix

    Returns:
        List of loggers
    """
    return [
        logger for name, logger in get_all_loggers().items()
        if name.startswith(prefix)
    ]


def set_modules_log_level(prefix: str, level: Union[int, str]) -> None:
    """Set the log level for all modules with the given prefix.

    Args:
        prefix: Module name prefix
        level: Log level
    """
    for logger in get_module_loggers(prefix):
        if isinstance(level, str):
            level = getattr(logging, level.upper(), logging.INFO)
        logger.setLevel(level)


def get_logger_hierarchy(logger_name: str) -> Set[str]:
    """Get the hierarchy of loggers for a given logger name.

    Args:
        logger_name: Logger name

    Returns:
        Set of logger names in the hierarchy
    """
    hierarchy = set()
    parts = logger_name.split('.')

    for i in range(1, len(parts) + 1):
        hierarchy.add('.'.join(parts[:i]))

    return hierarchy