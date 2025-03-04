#!/usr/bin/env python3
"""
Tests for the configuration module.
"""

import os
import json
import tempfile
import unittest
import copy
from pathlib import Path
from unittest.mock import patch

# Add the parent directory to the path so we can import from src
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.utils.config import (
    load_config,
    get_db_config,
    get_message_type_description,
    setup_logging,
    _deep_update,
    DEFAULT_CONFIG
)


class TestConfig(unittest.TestCase):
    """Test the configuration module."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a temporary directory
        self.temp_dir = tempfile.mkdtemp()

        # Save the original DEFAULT_CONFIG
        self.original_default_config = copy.deepcopy(DEFAULT_CONFIG)

        # Create a sample config file
        self.config_data = {
            "database": {
                "host": "test-host",
                "port": 5555,
                "dbname": "test-db",
                "user": "test-user",
                "password": "test-password"
            },
            "output": {
                "directory": "test-output",
                "overwrite": True
            },
            "logging": {
                "level": "DEBUG",
                "file": "test.log"
            }
        }
        self.config_file = os.path.join(self.temp_dir, "config.json")
        with open(self.config_file, "w") as f:
            json.dump(self.config_data, f)

        # Create a sample message types file
        self.message_types_data = {
            "message_types": {
                "Test/Type1": "Test description 1",
                "Test/Type2": "Test description 2"
            },
            "default_message_format": "Test default format: {message_type}"
        }
        self.message_types_file = os.path.join(self.temp_dir, "message_types.json")
        with open(self.message_types_file, "w") as f:
            json.dump(self.message_types_data, f)

    def tearDown(self):
        """Tear down test fixtures."""
        # Remove temporary directory and files
        for root, dirs, files in os.walk(self.temp_dir, topdown=False):
            for name in files:
                os.remove(os.path.join(root, name))
            for name in dirs:
                os.rmdir(os.path.join(root, name))
        os.rmdir(self.temp_dir)

        # Restore the original DEFAULT_CONFIG
        for key in DEFAULT_CONFIG:
            DEFAULT_CONFIG[key] = self.original_default_config[key]

    def test_load_config_default(self):
        """Test loading default configuration."""
        # Clear any environment variables that might affect the test
        with patch.dict(os.environ, {
            'POSTGRES_HOST': '',
            'POSTGRES_PORT': '',
            'POSTGRES_DB': '',
            'POSTGRES_USER': '',
            'POSTGRES_PASSWORD': '',
            'OUTPUT_DIR': '',
            'OUTPUT_OVERWRITE': '',
            'LOG_LEVEL': '',
            'LOG_FILE': ''
        }, clear=True):
            # Explicitly pass None for config files to ensure they're not loaded
            config = load_config(config_file=None, message_types_file=None)
            self.assertEqual(config['database']['host'], 'localhost')
            self.assertEqual(config['output']['directory'], 'output')
            self.assertEqual(config['logging']['level'], 'INFO')

    def test_load_config_from_file(self):
        """Test loading configuration from a file."""
        config = load_config(config_file=self.config_file)
        self.assertEqual(config['database']['host'], 'test-host')
        self.assertEqual(config['database']['port'], 5555)
        self.assertEqual(config['output']['directory'], 'test-output')
        self.assertEqual(config['output']['overwrite'], True)
        self.assertEqual(config['logging']['level'], 'DEBUG')
        self.assertEqual(config['logging']['file'], 'test.log')

    def test_load_config_message_types(self):
        """Test loading message types configuration."""
        config = load_config(message_types_file=self.message_types_file)
        self.assertEqual(config['message_types']['Test/Type1'], 'Test description 1')
        self.assertEqual(config['message_types']['Test/Type2'], 'Test description 2')
        self.assertEqual(config['default_message_format'], 'Test default format: {message_type}')

    def test_load_config_both_files(self):
        """Test loading configuration from both files."""
        config = load_config(config_file=self.config_file, message_types_file=self.message_types_file)
        self.assertEqual(config['database']['host'], 'test-host')
        self.assertEqual(config['message_types']['Test/Type1'], 'Test description 1')

    @patch.dict(os.environ, {
        'POSTGRES_HOST': 'env-host',
        'POSTGRES_PORT': '6666',
        'POSTGRES_DB': 'env-db',
        'POSTGRES_USER': 'env-user',
        'POSTGRES_PASSWORD': 'env-password',
        'OUTPUT_DIR': 'env-output',
        'OUTPUT_OVERWRITE': 'true',
        'LOG_LEVEL': 'ERROR',
        'LOG_FILE': 'env.log'
    })
    def test_load_config_from_env(self):
        """Test loading configuration from environment variables."""
        config = load_config()
        self.assertEqual(config['database']['host'], 'env-host')
        self.assertEqual(config['database']['port'], 6666)
        self.assertEqual(config['database']['dbname'], 'env-db')
        self.assertEqual(config['database']['user'], 'env-user')
        self.assertEqual(config['database']['password'], 'env-password')
        self.assertEqual(config['output']['directory'], 'env-output')
        self.assertEqual(config['output']['overwrite'], True)
        self.assertEqual(config['logging']['level'], 'ERROR')
        self.assertEqual(config['logging']['file'], 'env.log')

    def test_get_db_config(self):
        """Test getting database configuration."""
        config = load_config(config_file=self.config_file)
        db_config = get_db_config(config)
        self.assertEqual(db_config['host'], 'test-host')
        self.assertEqual(db_config['port'], 5555)
        self.assertEqual(db_config['dbname'], 'test-db')
        self.assertEqual(db_config['user'], 'test-user')
        self.assertEqual(db_config['password'], 'test-password')

    def test_get_message_type_description_known(self):
        """Test getting a known message type description."""
        config = load_config(message_types_file=self.message_types_file)
        description = get_message_type_description(config, 'Test/Type1')
        self.assertEqual(description, 'Test description 1')

    def test_get_message_type_description_unknown(self):
        """Test getting an unknown message type description."""
        config = load_config(message_types_file=self.message_types_file)
        description = get_message_type_description(config, 'Unknown/Type')
        self.assertEqual(description, 'Test default format: Unknown/Type')

    def test_get_message_type_description_empty(self):
        """Test getting an empty message type description."""
        config = load_config(message_types_file=self.message_types_file)
        description = get_message_type_description(config, '')
        self.assertEqual(description, 'Unknown message type')

    def test_deep_update(self):
        """Test deep updating a dictionary."""
        target = {
            'a': 1,
            'b': {
                'c': 2,
                'd': 3
            }
        }
        source = {
            'b': {
                'c': 4,
                'e': 5
            },
            'f': 6
        }
        _deep_update(target, source)
        self.assertEqual(target['a'], 1)
        self.assertEqual(target['b']['c'], 4)
        self.assertEqual(target['b']['d'], 3)
        self.assertEqual(target['b']['e'], 5)
        self.assertEqual(target['f'], 6)

    def test_default_config_not_modified(self):
        """Test that the DEFAULT_CONFIG is not modified by load_config."""
        # Store the original values
        original_host = self.original_default_config['database']['host']

        # Load a custom configuration
        with patch.dict(os.environ, {}, clear=True):
            config = load_config(config_file=self.config_file)

        # Verify that DEFAULT_CONFIG hasn't changed
        self.assertEqual(DEFAULT_CONFIG['database']['host'], original_host)
        self.assertNotEqual(config['database']['host'], original_host)


if __name__ == "__main__":
    unittest.main()