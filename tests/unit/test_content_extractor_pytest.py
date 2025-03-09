#!/usr/bin/env python3
"""
Pytest version of test_content_extractor.py.

This module contains test cases for the content extraction functions in src.parser.content_extractor,
migrated from unittest.TestCase style to pytest style with dependency injection.
"""

import sys
from pathlib import Path
from typing import Dict, List, Any

import pytest

# Add the src directory to the path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from src.parser.content_extractor import (
    ContentExtractor,
    extract_content_data,
    format_content_with_markup,
    format_content_with_regex
)

# Import fixtures and mocks
from tests.fixtures.mocks import MockContentExtractor


@pytest.fixture
def sample_content_with_mentions():
    """Sample message content with mentions."""
    return '<at id="user123">John Doe</at> Hello there!'


@pytest.fixture
def sample_content_with_links():
    """Sample message content with links."""
    return 'Check out <a href="https://example.com">this link</a>'


@pytest.fixture
def sample_content_with_formatting():
    """Sample message content with formatting."""
    return '<b>Bold</b> and <i>italic</i> and <u>underlined</u> text'


@pytest.fixture
def sample_content_with_quotes():
    """Sample message content with quotes."""
    return '<quote author="Jane Smith">Original message</quote>And my response'


@pytest.fixture
def sample_content_with_mixed():
    """Sample message content with mixed elements."""
    return '''
        <at id="user456">Jane Smith</at> mentioned in
        <quote author="John Doe">Have you seen <a href="https://example.com">this</a>?</quote>
        Yes, I have! It's <b>amazing</b>!
    '''


@pytest.fixture
def sample_content_with_plain_url():
    """Sample message content with plain URL."""
    return 'Check out https://example.org and let me know what you think'


@pytest.fixture
def sample_content_with_line_breaks():
    """Sample message content with line breaks."""
    return 'First line<br>Second line<br />Third line'


@pytest.fixture
def sample_message():
    """Sample message dictionary."""
    return {
        "id": "msg123",
        "content": '<at id="user123">John Doe</at> Hello there!',
        "type": "RichText/HTML"
    }


@pytest.fixture
def sample_message_with_mixed():
    """Sample message dictionary with mixed content."""
    return {
        "id": "msg456",
        "content": '''
            <at id="user456">Jane Smith</at> mentioned in
            <quote author="John Doe">Have you seen <a href="https://example.com">this</a>?</quote>
            Yes, I have! It's <b>amazing</b>!
        ''',
        "type": "RichText/HTML"
    }


@pytest.fixture
def content_extractor():
    """Create a ContentExtractor instance."""
    return ContentExtractor()


@pytest.fixture
def mock_content_extractor():
    """Create a MockContentExtractor instance."""
    return MockContentExtractor(
        extract_all_return={
            "mentions": [{"id": "user123", "name": "John Doe"}],
            "links": [{"url": "https://example.com", "text": "this link"}],
            "quotes": [{"author": "Jane Smith", "text": "Original message"}],
            "formatting": {"bold": ["amazing"]},
            "cleaned_content": "Clean content"
        },
        clean_content_return="Clean content",
        extract_mentions_return=[{"id": "user123", "name": "John Doe"}],
        extract_links_return=[{"url": "https://example.com", "text": "this link"}]
    )


def test_extract_mentions(content_extractor, sample_content_with_mentions, sample_content_with_mixed):
    """Test extraction of mentions from content."""
    # Test with simple content
    mentions = content_extractor.extract_mentions(sample_content_with_mentions)
    assert len(mentions) == 1
    assert mentions[0]['id'] == 'user123'
    assert mentions[0]['name'] == 'John Doe'

    # Test with mixed content
    mentions = content_extractor.extract_mentions(sample_content_with_mixed)
    assert len(mentions) == 1
    assert mentions[0]['id'] == 'user456'
    assert mentions[0]['name'] == 'Jane Smith'


def test_extract_links(content_extractor, sample_content_with_links, sample_content_with_mixed, sample_content_with_plain_url):
    """Test extraction of links from content."""
    # Test with simple content
    links = content_extractor.extract_links(sample_content_with_links)
    assert len(links) == 1
    assert links[0]['url'] == 'https://example.com'
    assert links[0]['text'] == 'this link'

    # Test with mixed content
    links = content_extractor.extract_links(sample_content_with_mixed)
    assert len(links) == 1
    assert links[0]['url'] == 'https://example.com'
    assert links[0]['text'] == 'this'

    # Test with plain URLs
    links = content_extractor.extract_links(sample_content_with_plain_url)
    assert len(links) == 1
    assert links[0]['url'] == 'https://example.org'
    assert links[0]['text'] == 'https://example.org'


def test_extract_quotes(content_extractor, sample_content_with_quotes, sample_content_with_mixed):
    """Test extraction of quotes from content."""
    # Test with simple content
    quotes = content_extractor.extract_quotes(sample_content_with_quotes)
    assert len(quotes) == 1
    assert quotes[0]['author'] == 'Jane Smith'
    assert 'text' in quotes[0]  # The key is 'text', not 'content'
    assert quotes[0]['text'] == 'Original message'

    # Test with mixed content
    quotes = content_extractor.extract_quotes(sample_content_with_mixed)
    assert len(quotes) == 1
    assert quotes[0]['author'] == 'John Doe'
    assert 'Have you seen' in quotes[0]['text']


def test_extract_formatting(content_extractor, sample_content_with_formatting):
    """Test extraction of formatting from content."""
    formatting = content_extractor.extract_formatting(sample_content_with_formatting)
    assert 'bold' in formatting
    assert 'italic' in formatting
    assert 'underline' in formatting


def test_extract_all(content_extractor, sample_content_with_mixed):
    """Test extraction of all content elements."""
    result = content_extractor.extract_all(sample_content_with_mixed)

    assert 'mentions' in result
    assert len(result['mentions']) == 1
    assert result['mentions'][0]['id'] == 'user456'

    assert 'links' in result
    assert len(result['links']) == 1
    assert result['links'][0]['url'] == 'https://example.com'

    assert 'quotes' in result
    assert len(result['quotes']) == 1
    assert result['quotes'][0]['author'] == 'John Doe'


def test_format_content_with_markup_function(sample_content_with_mentions, sample_content_with_links, sample_content_with_formatting):
    """Test the standalone format_content_with_markup function."""
    # Test with mentions
    formatted = format_content_with_markup(sample_content_with_mentions)
    assert 'John Doe' in formatted
    assert '<at id=' not in formatted

    # Test with links
    formatted = format_content_with_markup(sample_content_with_links)
    assert 'Check out this link' in formatted
    assert 'https://example.com' in formatted
    assert '<a href=' not in formatted

    # Test with formatting
    formatted = format_content_with_markup(sample_content_with_formatting)
    assert 'Bold' in formatted
    assert 'italic' in formatted
    assert 'underlined' in formatted
    assert '<b>' not in formatted
    assert '<i>' not in formatted
    assert '<u>' not in formatted


def test_format_content_with_regex(sample_content_with_mentions, sample_content_with_links):
    """Test formatting content with regex."""
    # Test with mentions
    formatted = format_content_with_regex(sample_content_with_mentions)
    assert 'John Doe' in formatted
    assert '<at id=' not in formatted

    # Test with links
    formatted = format_content_with_regex(sample_content_with_links)
    assert 'Check out this link' in formatted
    assert '<a href=' not in formatted


def test_extract_content_data_function(sample_content_with_mixed):
    """Test the extract_content_data function."""
    result = extract_content_data(sample_content_with_mixed)

    assert 'mentions' in result
    assert 'links' in result
    assert 'quotes' in result
    assert 'formatting' in result
    # The function doesn't return 'cleaned_content'


def test_extract_content(content_extractor, sample_message, sample_message_with_mixed):
    """Test the extract_content method."""
    # Test with default parameters
    result = content_extractor.extract_content(sample_message)
    assert 'John Doe' in result
    assert 'Hello there!' in result

    # Test with mixed content
    result = content_extractor.extract_content(sample_message_with_mixed)
    assert 'Jane Smith' in result
    assert 'mentioned in' in result
    assert 'Have you seen' in result
    assert 'this' in result
    assert 'Yes, I have!' in result
    assert 'amazing' in result


def test_extract_html_content(content_extractor, sample_message, sample_message_with_mixed):
    """Test the extract_html_content method."""
    # Test with simple content
    result = content_extractor.extract_html_content(sample_message)
    assert '<at id="user123">John Doe</at>' in result
    assert 'Hello there!' in result

    # Test with mixed content
    result = content_extractor.extract_html_content(sample_message_with_mixed)
    assert '<at id="user456">Jane Smith</at>' in result
    assert '<quote author="John Doe">' in result
    assert '<a href="https://example.com">this</a>' in result
    assert '<b>amazing</b>' in result


def test_dependency_injection(mock_content_extractor):
    """Test using dependency injection with MockContentExtractor."""
    # Test extract_all method
    result = mock_content_extractor.extract_all("Test content")
    assert "mentions" in result
    assert "links" in result
    assert "quotes" in result
    assert "formatting" in result
    assert "cleaned_content" in result
    assert len(mock_content_extractor.extract_all_calls) == 1

    # Test clean_content method
    result = mock_content_extractor.clean_content("Test content")
    assert result == "Clean content"
    assert len(mock_content_extractor.clean_content_calls) == 1

    # Test extract_mentions method
    result = mock_content_extractor.extract_mentions("Test content")
    assert len(result) == 1
    assert result[0]["id"] == "user123"
    assert result[0]["name"] == "John Doe"
    assert len(mock_content_extractor.extract_mentions_calls) == 1

    # Test extract_links method
    result = mock_content_extractor.extract_links("Test content")
    assert len(result) == 1
    assert result[0]["url"] == "https://example.com"
    assert result[0]["text"] == "this link"
    assert len(mock_content_extractor.extract_links_calls) == 1

