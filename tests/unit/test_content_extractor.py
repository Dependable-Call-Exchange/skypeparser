#!/usr/bin/env python3
"""
Tests for the content_extractor module.

This module contains test cases for the content extraction functions in src.parser.content_extractor.
"""

import unittest
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.parser.content_extractor import (
    ContentExtractor,
    extract_content_data,
    format_content_with_markup,
    format_content_with_regex
)


class TestContentExtractor(unittest.TestCase):
    """Test cases for content extractor functions."""

    def setUp(self):
        """Set up test fixtures."""
        # Sample message content with various HTML elements
        self.sample_content_with_mentions = '<at id="user123">John Doe</at> Hello there!'
        self.sample_content_with_links = 'Check out <a href="https://example.com">this link</a>'
        self.sample_content_with_formatting = '<b>Bold</b> and <i>italic</i> and <u>underlined</u> text'
        self.sample_content_with_quotes = '<quote author="Jane Smith">Original message</quote>And my response'
        self.sample_content_with_mixed = '''
            <at id="user456">Jane Smith</at> mentioned in
            <quote author="John Doe">Have you seen <a href="https://example.com">this</a>?</quote>
            Yes, I have! It's <b>amazing</b>!
        '''
        self.sample_content_with_plain_url = 'Check out https://example.org and let me know what you think'
        self.sample_content_with_line_breaks = 'First line<br>Second line<br />Third line'

    def test_extract_mentions(self):
        """Test extraction of mentions from content."""
        mentions = ContentExtractor.extract_mentions(self.sample_content_with_mentions)
        self.assertEqual(len(mentions), 1)
        self.assertEqual(mentions[0]['id'], 'user123')
        self.assertEqual(mentions[0]['name'], 'John Doe')

        # Test with mixed content
        mentions = ContentExtractor.extract_mentions(self.sample_content_with_mixed)
        self.assertEqual(len(mentions), 1)
        self.assertEqual(mentions[0]['id'], 'user456')
        self.assertEqual(mentions[0]['name'], 'Jane Smith')

    def test_extract_links(self):
        """Test extraction of links from content."""
        links = ContentExtractor.extract_links(self.sample_content_with_links)
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0]['url'], 'https://example.com')
        self.assertEqual(links[0]['text'], 'this link')

        # Test with plain URLs
        links = ContentExtractor.extract_links(self.sample_content_with_plain_url)
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0]['url'], 'https://example.org')
        self.assertEqual(links[0]['text'], 'https://example.org')

        # Test with mixed content
        links = ContentExtractor.extract_links(self.sample_content_with_mixed)
        self.assertEqual(len(links), 1)
        self.assertEqual(links[0]['url'], 'https://example.com')
        self.assertEqual(links[0]['text'], 'this')

    def test_extract_quotes(self):
        """Test extraction of quotes from content."""
        quotes = ContentExtractor.extract_quotes(self.sample_content_with_quotes)
        self.assertEqual(len(quotes), 1)
        self.assertEqual(quotes[0]['author'], 'Jane Smith')
        self.assertEqual(quotes[0]['text'], 'Original message')

        # Test with mixed content
        quotes = ContentExtractor.extract_quotes(self.sample_content_with_mixed)
        self.assertEqual(len(quotes), 1)
        self.assertEqual(quotes[0]['author'], 'John Doe')
        self.assertTrue('Have you seen' in quotes[0]['text'])

    def test_extract_formatting(self):
        """Test extraction of formatted text from content."""
        formatting = ContentExtractor.extract_formatting(self.sample_content_with_formatting)
        self.assertIn('bold', formatting)
        self.assertIn('italic', formatting)
        self.assertIn('underline', formatting)
        self.assertEqual(formatting['bold'][0], 'Bold')
        self.assertEqual(formatting['italic'][0], 'italic')
        self.assertEqual(formatting['underline'][0], 'underlined')

    def test_extract_all(self):
        """Test extraction of all structured data from content."""
        structured_data = ContentExtractor.extract_all(self.sample_content_with_mixed)
        self.assertIn('mentions', structured_data)
        self.assertIn('links', structured_data)
        self.assertIn('quotes', structured_data)

        # Test with empty content
        structured_data = ContentExtractor.extract_all("")
        self.assertEqual(structured_data, {})

    def test_format_content_with_markup(self):
        """Test formatting of content with markup."""
        # Test mentions formatting
        formatted = format_content_with_markup(self.sample_content_with_mentions)
        self.assertIn('@John Doe', formatted)

        # Test links formatting
        formatted = format_content_with_markup(self.sample_content_with_links)
        self.assertIn('this link (https://example.com)', formatted)

        # Test formatting tags
        formatted = format_content_with_markup(self.sample_content_with_formatting)
        self.assertIn('*Bold*', formatted)
        self.assertIn('_italic_', formatted)
        self.assertIn('_underlined_', formatted)

        # Test quotes formatting
        formatted = format_content_with_markup(self.sample_content_with_quotes)
        self.assertIn('> Jane Smith wrote:', formatted)
        self.assertIn('> Original message', formatted)

        # Test line breaks
        formatted = format_content_with_markup(self.sample_content_with_line_breaks)
        self.assertIn('First line\nSecond line\nThird line', formatted)

        # Test with empty content
        formatted = format_content_with_markup("")
        self.assertEqual(formatted, "")

    def test_format_content_with_regex(self):
        """Test formatting of content with regex."""
        # Test with content that might cause BeautifulSoup to fail
        malformed_content = '<at id="user123">John Doe</at> <unclosed tag> <a href="https://example.com">link'
        formatted = format_content_with_regex(malformed_content)
        self.assertIn('@John Doe', formatted)
        self.assertNotIn('<unclosed tag>', formatted)

        # Test with empty content
        formatted = format_content_with_regex("")
        self.assertEqual(formatted, "")

    def test_extract_content_data_function(self):
        """Test the extract_content_data function."""
        data = extract_content_data(self.sample_content_with_mixed)
        self.assertIsInstance(data, dict)
        self.assertIn('mentions', data)
        self.assertIn('links', data)
        self.assertIn('quotes', data)


if __name__ == '__main__':
    unittest.main()