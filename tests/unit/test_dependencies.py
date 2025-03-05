#!/usr/bin/env python3
"""
Tests for the dependencies module.

This module contains tests for the functionality in src.utils.dependencies.
"""

import sys
import unittest
from pathlib import Path
from unittest.mock import patch, MagicMock

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.utils.dependencies import (
    check_dependency,
    require_dependency,
    get_beautifulsoup,
    get_psycopg2,
    BEAUTIFULSOUP_AVAILABLE,
    PSYCOPG2_AVAILABLE
)


class TestDependencies(unittest.TestCase):
    """Test cases for the dependencies module."""

    @patch('importlib.import_module')
    def test_check_dependency_success(self, mock_import):
        """Test check_dependency function with a successful import."""
        # Configure the mock to return successfully
        mock_import.return_value = MagicMock()

        # Test with a known dependency
        result = check_dependency('beautifulsoup')
        self.assertEqual(result, BEAUTIFULSOUP_AVAILABLE)

        # Test with another known dependency
        result = check_dependency('psycopg2')
        self.assertEqual(result, PSYCOPG2_AVAILABLE)

    def test_check_dependency_unknown(self):
        """Test check_dependency function with an unknown dependency."""
        # Test with an unknown dependency
        with self.assertLogs(level='WARNING') as cm:
            result = check_dependency('unknown_dependency')
            self.assertFalse(result)
            self.assertIn('Unknown dependency', cm.output[0])

    @patch('sys.exit')
    def test_require_dependency_available(self, mock_exit):
        """Test require_dependency function with an available dependency."""
        # Mock check_dependency to return True
        with patch('src.utils.dependencies.check_dependency', return_value=True):
            require_dependency('beautifulsoup')
            mock_exit.assert_not_called()

    @patch('sys.exit')
    def test_require_dependency_unavailable(self, mock_exit):
        """Test require_dependency function with an unavailable dependency."""
        # Mock check_dependency to return False
        with patch('src.utils.dependencies.check_dependency', return_value=False):
            require_dependency('beautifulsoup')
            mock_exit.assert_called_once_with(1)

    def test_get_beautifulsoup(self):
        """Test get_beautifulsoup function."""
        available, bs, parser = get_beautifulsoup()
        self.assertIsInstance(available, bool)
        if available:
            self.assertIsNotNone(bs)
            self.assertIn(parser, ['lxml', 'html.parser', ''])
        else:
            self.assertIsNone(bs)
            self.assertEqual(parser, '')

    def test_get_psycopg2(self):
        """Test get_psycopg2 function."""
        available, psycopg2 = get_psycopg2()
        self.assertIsInstance(available, bool)
        if available:
            self.assertIsNotNone(psycopg2)
        else:
            self.assertIsNone(psycopg2)


if __name__ == '__main__':
    unittest.main()