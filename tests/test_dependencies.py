#!/usr/bin/env python3
"""
Tests for the dependencies module.

This module contains tests for the functionality in src.utils.dependencies.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.utils.dependencies import (
    check_dependency,
    check_all_dependencies,
    install_dependency,
    DependencyError
)


class TestDependencies(unittest.TestCase):
    """Test cases for the dependencies module."""

    @patch('importlib.import_module')
    def test_check_dependency_success(self, mock_import):
        """Test check_dependency function with a successful import."""
        # Configure the mock to return successfully
        mock_import.return_value = MagicMock()

        # Check a dependency that should succeed
        result = check_dependency('os')

        # Verify that importlib.import_module was called with 'os'
        mock_import.assert_called_once_with('os')

        # Verify that the function returned True
        self.assertTrue(result)

    @patch('importlib.import_module')
    def test_check_dependency_failure(self, mock_import):
        """Test check_dependency function with a failed import."""
        # Configure the mock to raise an ImportError
        mock_import.side_effect = ImportError("No module named 'nonexistent'")

        # Check a dependency that should fail
        result = check_dependency('nonexistent')

        # Verify that importlib.import_module was called with 'nonexistent'
        mock_import.assert_called_once_with('nonexistent')

        # Verify that the function returned False
        self.assertFalse(result)

    @patch('src.utils.dependencies.check_dependency')
    def test_check_all_dependencies_success(self, mock_check):
        """Test check_all_dependencies function with all dependencies available."""
        # Configure the mock to return True for all dependencies
        mock_check.return_value = True

        # Check dependencies
        dependencies = ['os', 'sys', 'json']
        result = check_all_dependencies(dependencies)

        # Verify that check_dependency was called for each dependency
        self.assertEqual(mock_check.call_count, len(dependencies))

        # Verify that the function returned True
        self.assertTrue(result)

    @patch('src.utils.dependencies.check_dependency')
    def test_check_all_dependencies_failure(self, mock_check):
        """Test check_all_dependencies function with some dependencies missing."""
        # Configure the mock to return False for 'nonexistent'
        mock_check.side_effect = lambda dep: dep != 'nonexistent'

        # Check dependencies including one that should fail
        dependencies = ['os', 'sys', 'nonexistent']
        result = check_all_dependencies(dependencies)

        # Verify that check_dependency was called for each dependency
        self.assertEqual(mock_check.call_count, len(dependencies))

        # Verify that the function returned False
        self.assertFalse(result)

    @patch('subprocess.check_call')
    def test_install_dependency_success(self, mock_check_call):
        """Test install_dependency function with a successful installation."""
        # Configure the mock to return successfully
        mock_check_call.return_value = 0

        # Install a dependency
        install_dependency('test_package')

        # Verify that subprocess.check_call was called with the correct command
        mock_check_call.assert_called_once()
        args = mock_check_call.call_args[0][0]
        self.assertIn('pip', args[0])
        self.assertEqual(args[1], 'install')
        self.assertEqual(args[2], 'test_package')

    @patch('subprocess.check_call')
    def test_install_dependency_failure(self, mock_check_call):
        """Test install_dependency function with a failed installation."""
        # Configure the mock to raise an exception
        mock_check_call.side_effect = Exception("Installation failed")

        # Attempt to install a dependency and expect a DependencyError
        with self.assertRaises(DependencyError):
            install_dependency('test_package')

        # Verify that subprocess.check_call was called with the correct command
        mock_check_call.assert_called_once()
        args = mock_check_call.call_args[0][0]
        self.assertIn('pip', args[0])
        self.assertEqual(args[1], 'install')
        self.assertEqual(args[2], 'test_package')


if __name__ == '__main__':
    unittest.main()