# Pytest version of test_content_extractor.py
#
# This file was migrated from unittest.TestCase style to pytest style.
# Manual adjustments may still be needed for:
# - Complex assertions
# - Mock implementations
# - Test parameterization
# - Dependency injection

import pytest
"""
DEPRECATED: This file has been migrated to test_content_extractor_pytest.py
Please use the newer version which follows current testing standards.
This file will be removed once migration verification is complete.

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

    def test_extract_content(self):
        """Test the extract_content method."""
        # Create a ContentExtractor instance
        extractor = ContentExtractor()

        # Test with a message containing HTML content
        message = {'content': self.sample_content_with_mentions}
        content = extractor.extract_content(message)
        self.assertIsInstance(content, str)
        self.assertIn('@John Doe', content)
        self.assertNotIn('<at', content)

        # Test with a message containing mixed content
        message = {'content': self.sample_content_with_mixed}
        content = extractor.extract_content(message)
        self.assertIsInstance(content, str)
        self.assertIn('@Jane Smith', content)
        self.assertNotIn('<at', content)
        self.assertNotIn('<quote', content)
        self.assertNotIn('<a href', content)
        self.assertNotIn('<b>', content)

        # Test with a message containing no content
        message = {'content': ''}
        content = extractor.extract_content(message)
        self.assertEqual(content, '')

        # Test with a message missing the content field
        message = {}
        content = extractor.extract_content(message)
        self.assertEqual(content, '')

    def test_extract_html_content(self):
        """Test the extract_html_content method."""
        # Create a ContentExtractor instance
        extractor = ContentExtractor()

        # Test with a message containing HTML content
        message = {'content': self.sample_content_with_mentions}
        html_content = extractor.extract_html_content(message)
        self.assertIsInstance(html_content, str)
        self.assertEqual(html_content, self.sample_content_with_mentions)

        # Test with a message containing mixed content
        message = {'content': self.sample_content_with_mixed}
        html_content = extractor.extract_html_content(message)
        self.assertIsInstance(html_content, str)
        self.assertEqual(html_content, self.sample_content_with_mixed)

        # Test with a message containing no content
        message = {'content': ''}
        html_content = extractor.extract_html_content(message)
        self.assertEqual(html_content, '')

        # Test with a message missing the content field
        message = {}
        html_content = extractor.extract_html_content(message)
        self.assertEqual(html_content, '')


if __name__ == '__main__':
    unittest.main()


@pytest.fixture(scope="function")
def contentextractor_setup():
    """Set up test fixtures."""
    # Sample message content with various HTML elements
    sample_content_with_mentions = '<at id="user123">John Doe</at> Hello there!'
    sample_content_with_links = 'Check out <a href="https://example.com">this link</a>'
    sample_content_with_formatting = '<b>Bold</b> and <i>italic</i> and <u>underlined</u> text'
    sample_content_with_quotes = '<quote author="Jane Smith">Original message</quote>And my response'
    sample_content_with_mixed = '''
            <at id="user456">Jane Smith</at> mentioned in
            <quote author="John Doe">Have you seen <a href="https://example.com">this</a>?</quote>
            Yes, I have! It's <b>amazing</b>!
        '''
    sample_content_with_plain_url = 'Check out https://example.org and let me know what you think'
    sample_content_with_line_breaks = 'First line<br>Second line<br />Third line'
    yield

def test_extract_mentions(testcontentextractor_fixture):
    """Test extraction of mentions from content."""
    mentions = ContentExtractor.extract_mentions(sample_content_with_mentions)
    assert len(mentions) == 1
    assert mentions[0]['id'] == 'user123'
    assert mentions[0]['name'] == 'John Doe'

    # Test with mixed content
    mentions = ContentExtractor.extract_mentions(sample_content_with_mixed)
    assert len(mentions) == 1
    assert mentions[0]['id'] == 'user456'
    assert mentions[0]['name'] == 'Jane Smith'

def test_extract_links(testcontentextractor_fixture):
    """Test extraction of links from content."""
    links = ContentExtractor.extract_links(sample_content_with_links)
    assert len(links) == 1
    assert links[0]['url'] == 'https://example.com'
    assert links[0]['text'] == 'this link'

    # Test with plain URLs
    links = ContentExtractor.extract_links(sample_content_with_plain_url)
    assert len(links) == 1
    assert links[0]['url'] == 'https://example.org'
    assert links[0]['text'] == 'https://example.org'

    # Test with mixed content
    links = ContentExtractor.extract_links(sample_content_with_mixed)
    assert len(links) == 1
    assert links[0]['url'] == 'https://example.com'
    assert links[0]['text'] == 'this'

def test_extract_quotes(testcontentextractor_fixture):
    """Test extraction of quotes from content."""
    quotes = ContentExtractor.extract_quotes(sample_content_with_quotes)
    assert len(quotes) == 1
    assert quotes[0]['author'] == 'Jane Smith'
    assert quotes[0]['text'] == 'Original message'

    # Test with mixed content
    quotes = ContentExtractor.extract_quotes(sample_content_with_mixed)
    assert len(quotes) == 1
    assert quotes[0]['author'] == 'John Doe'
    assert 'Have you seen' in quotes[0]['text']

def test_extract_formatting(testcontentextractor_fixture):
    """Test extraction of formatted text from content."""
    formatting = ContentExtractor.extract_formatting(sample_content_with_formatting)
    assert 'bold' in formatting
    assert 'italic' in formatting
    assert 'underline' in formatting
    assert formatting['bold'][0] == 'Bold'
    assert formatting['italic'][0] == 'italic'
    assert formatting['underline'][0] == 'underlined'

def test_extract_all(testcontentextractor_fixture):
    """Test extraction of all structured data from content."""
    structured_data = ContentExtractor.extract_all(sample_content_with_mixed)
    assert 'mentions' in structured_data
    assert 'links' in structured_data
    assert 'quotes' in structured_data

    # Test with empty content
    structured_data = ContentExtractor.extract_all("")
    assert structured_data == {}

def test_format_content_with_markup(testcontentextractor_fixture):
    """Test formatting of content with markup."""
    # Test mentions formatting
    formatted = format_content_with_markup(sample_content_with_mentions)
    assert '@John Doe' in formatted

    # Test links formatting
    formatted = format_content_with_markup(sample_content_with_links)
    assert 'this link (https://example.com)' in formatted

    # Test formatting tags
    formatted = format_content_with_markup(sample_content_with_formatting)
    assert '*Bold*' in formatted
    assert '_italic_' in formatted
    assert '_underlined_' in formatted

    # Test quotes formatting
    formatted = format_content_with_markup(sample_content_with_quotes)
    assert '> Jane Smith wrote:' in formatted
    assert '> Original message' in formatted

    # Test line breaks
    formatted = format_content_with_markup(sample_content_with_line_breaks)
    assert 'First line\nSecond line\nThird line' in formatted

    # Test with empty content
    formatted = format_content_with_markup("")
    assert formatted == ""

def test_format_content_with_regex(testcontentextractor_fixture):
    """Test formatting of content with regex."""
    # Test with content that might cause BeautifulSoup to fail
    malformed_content = '<at id="user123">John Doe</at> <unclosed tag> <a href="https://example.com">link'
    formatted = format_content_with_regex(malformed_content)
    assert '@John Doe' in formatted
    assert '<unclosed tag>' not in formatted

    # Test with empty content
    formatted = format_content_with_regex("")
    assert formatted == ""

def test_extract_content_data_function(testcontentextractor_fixture):
    """Test the extract_content_data function."""
    data = extract_content_data(sample_content_with_mixed)
    assert isinstance(data, dict)
    assert 'mentions' in data
    assert 'links' in data
    assert 'quotes' in data

def test_extract_content(testcontentextractor_fixture):
    """Test the extract_content method."""
    # Create a ContentExtractor instance
    extractor = ContentExtractor()

    # Test with a message containing HTML content
    message = {'content': sample_content_with_mentions}
    content = extractor.extract_content(message)
    assert isinstance(content, str)
    assert '@John Doe' in content
    assert '<at' not in content

    # Test with a message containing mixed content
    message = {'content': sample_content_with_mixed}
    content = extractor.extract_content(message)
    assert isinstance(content, str)
    assert '@Jane Smith' in content
    assert '<at' not in content
    assert '<quote' not in content
    assert '<a href' not in content
    assert '<b>' not in content

    # Test with a message containing no content
    message = {'content': ''}
    content = extractor.extract_content(message)
    assert content == ''

    # Test with a message missing the content field
    message = {}
    content = extractor.extract_content(message)
    assert content == ''

def test_extract_html_content(testcontentextractor_fixture):
    """Test the extract_html_content method."""
    # Create a ContentExtractor instance
    extractor = ContentExtractor()

    # Test with a message containing HTML content
    message = {'content': sample_content_with_mentions}
    html_content = extractor.extract_html_content(message)
    assert isinstance(html_content, str)
    assert html_content == self.sample_content_with_mentions

    # Test with a message containing mixed content
    message = {'content': sample_content_with_mixed}
    html_content = extractor.extract_html_content(message)
    assert isinstance(html_content, str)
    assert html_content == self.sample_content_with_mixed

    # Test with a message containing no content
    message = {'content': ''}
    html_content = extractor.extract_html_content(message)
    assert html_content == ''

    # Test with a message missing the content field
    message = {}
    html_content = extractor.extract_html_content(message)
    assert html_content == ''