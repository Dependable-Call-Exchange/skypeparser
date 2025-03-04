"""
Configuration Management Module

This module provides utilities for loading and managing configuration settings
from environment variables, configuration files, and command-line arguments.
"""

import os
import json
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    'database': {
        'host': 'localhost',
        'port': 5432,
        'dbname': 'skype_archive',
        'user': 'postgres',
        'password': '',
    },
    'output': {
        'directory': 'output',
        'overwrite': False,
    },
    'logging': {
        'level': 'INFO',
        'file': None,
    }
}

def load_config(config_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Load configuration from environment variables and optionally from a JSON file.
    Environment variables take precedence over file configuration.

    Args:
        config_file (str, optional): Path to a JSON configuration file

    Returns:
        Dict[str, Any]: Configuration dictionary
    """
    # Start with default configuration
    config = DEFAULT_CONFIG.copy()

    # Load from file if provided
    if config_file and os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                file_config = json.load(f)
                # Merge file config with defaults (deep merge)
                _deep_update(config, file_config)
            logger.info(f"Loaded configuration from {config_file}")
        except Exception as e:
            logger.warning(f"Error loading configuration from {config_file}: {e}")

    # Override with environment variables
    # Database settings
    if os.getenv('POSTGRES_HOST'):
        config['database']['host'] = os.getenv('POSTGRES_HOST')
    if os.getenv('POSTGRES_PORT'):
        config['database']['port'] = int(os.getenv('POSTGRES_PORT'))
    if os.getenv('POSTGRES_DB'):
        config['database']['dbname'] = os.getenv('POSTGRES_DB')
    if os.getenv('POSTGRES_USER'):
        config['database']['user'] = os.getenv('POSTGRES_USER')
    if os.getenv('POSTGRES_PASSWORD'):
        config['database']['password'] = os.getenv('POSTGRES_PASSWORD')

    # Output settings
    if os.getenv('OUTPUT_DIR'):
        config['output']['directory'] = os.getenv('OUTPUT_DIR')
    if os.getenv('OUTPUT_OVERWRITE'):
        config['output']['overwrite'] = os.getenv('OUTPUT_OVERWRITE').lower() in ('true', 'yes', '1')

    # Logging settings
    if os.getenv('LOG_LEVEL'):
        config['logging']['level'] = os.getenv('LOG_LEVEL')
    if os.getenv('LOG_FILE'):
        config['logging']['file'] = os.getenv('LOG_FILE')

    return config

def get_db_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract database configuration from the main configuration.

    Args:
        config (Dict[str, Any]): Main configuration dictionary

    Returns:
        Dict[str, Any]: Database configuration dictionary
    """
    return {
        'host': config['database']['host'],
        'port': config['database']['port'],
        'dbname': config['database']['dbname'],
        'user': config['database']['user'],
        'password': config['database']['password'],
    }

def setup_logging(config: Dict[str, Any]) -> None:
    """
    Set up logging based on configuration.

    Args:
        config (Dict[str, Any]): Configuration dictionary
    """
    log_level = getattr(logging, config['logging']['level'].upper(), logging.INFO)
    log_file = config['logging']['file']

    handlers = [logging.StreamHandler()]
    if log_file:
        handlers.append(logging.FileHandler(log_file))

    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=handlers
    )

def _deep_update(target: Dict, source: Dict) -> None:
    """
    Deep update a nested dictionary with another dictionary.

    Args:
        target (Dict): Target dictionary to update
        source (Dict): Source dictionary with new values
    """
    for key, value in source.items():
        if key in target and isinstance(target[key], dict) and isinstance(value, dict):
            _deep_update(target[key], value)
        else:
            target[key] = value