#!/usr/bin/env python3
"""
Content Extractor Module

This module provides specialized functions for extracting structured data from
message content, including mentions, links, formatting, and other elements.
"""

import re
import html
import logging
from typing import Dict, Any, List, Optional, Tuple

# Import centralized dependency handling
from ..utils.dependencies import (
    BEAUTIFULSOUP_AVAILABLE as BEAUTIFULSOUP,
    BS_PARSER
)

if BEAUTIFULSOUP:
    from bs4 import BeautifulSoup

# Set up logging
logger = logging.getLogger(__name__)


class ContentExtractor:
    """
    Extracts structured data from message content.
    """

    @staticmethod
    def extract_mentions(content: str) -> List[Dict[str, str]]:
        """
        Extract @mentions from message content.

        Args:
            content (str): Message content

        Returns:
            list: List of mention dictionaries with 'id' and 'name' keys
        """
        mentions = []

        if BEAUTIFULSOUP:
            try:
                soup = BeautifulSoup(content, BS_PARSER)
                for mention in soup.find_all('at'):
                    mention_id = mention.get('id', '')
                    mention_name = mention.get_text(strip=True)
                    mentions.append({"id": mention_id, "name": mention_name})
            except Exception as e:
                logger.warning(f"Error extracting mentions with BeautifulSoup: {e}")
                # Fall back to regex
                mention_matches = re.findall(r'<at id=["\'](.*?)["\']>(.*?)</at>', content)
                for mention_id, mention_name in mention_matches:
                    mentions.append({"id": mention_id, "name": mention_name})
        else:
            # Use regex directly
            mention_matches = re.findall(r'<at id=["\'](.*?)["\']>(.*?)</at>', content)
            for mention_id, mention_name in mention_matches:
                mentions.append({"id": mention_id, "name": mention_name})

        return mentions

    @staticmethod
    def extract_links(content: str) -> List[Dict[str, str]]:
        """
        Extract links from message content.

        Args:
            content (str): Message content

        Returns:
            list: List of link dictionaries with 'url' and 'text' keys
        """
        links = []

        if BEAUTIFULSOUP:
            try:
                soup = BeautifulSoup(content, BS_PARSER)
                for link in soup.find_all('a'):
                    href = link.get('href', '')
                    link_text = link.get_text(strip=True)
                    links.append({"url": href, "text": link_text})
            except Exception as e:
                logger.warning(f"Error extracting links with BeautifulSoup: {e}")
                # Fall back to regex
                link_matches = re.findall(r'<a href=["\'](.*?)["\'].*?>(.*?)</a>', content)
                for href, link_text in link_matches:
                    links.append({"url": href, "text": link_text})
        else:
            # Use regex directly
            link_matches = re.findall(r'<a href=["\'](.*?)["\'].*?>(.*?)</a>', content)
            for href, link_text in link_matches:
                links.append({"url": href, "text": link_text})

        # Also extract plain URLs
        url_matches = re.findall(r'https?://[^\s<>"\']+', content)
        for url in url_matches:
            # Check if this URL is already in the links list
            if not any(link['url'] == url for link in links):
                links.append({"url": url, "text": url})

        return links

    @staticmethod
    def extract_quotes(content: str) -> List[Dict[str, str]]:
        """
        Extract quotes from message content.

        Args:
            content (str): Message content

        Returns:
            list: List of quote dictionaries with 'author' and 'text' keys
        """
        quotes = []

        if BEAUTIFULSOUP:
            try:
                soup = BeautifulSoup(content, BS_PARSER)
                for quote in soup.find_all('quote'):
                    author = quote.get('author', '')
                    quote_text = quote.get_text(strip=True)
                    quotes.append({"author": author, "text": quote_text})
            except Exception as e:
                logger.warning(f"Error extracting quotes with BeautifulSoup: {e}")
                # Fall back to regex
                quote_matches = re.findall(r'<quote author=["\'](.*?)["\'].*?>(.*?)</quote>', content, re.DOTALL)
                for author, quote_text in quote_matches:
                    quotes.append({"author": author, "text": quote_text.strip()})

                # Also match quotes without authors
                quote_matches = re.findall(r'<quote>(.*?)</quote>', content, re.DOTALL)
                for quote_text in quote_matches:
                    quotes.append({"author": "", "text": quote_text.strip()})
        else:
            # Use regex directly
            quote_matches = re.findall(r'<quote author=["\'](.*?)["\'].*?>(.*?)</quote>', content, re.DOTALL)
            for author, quote_text in quote_matches:
                quotes.append({"author": author, "text": quote_text.strip()})

            # Also match quotes without authors
            quote_matches = re.findall(r'<quote>(.*?)</quote>', content, re.DOTALL)
            for quote_text in quote_matches:
                quotes.append({"author": "", "text": quote_text.strip()})

        return quotes

    @staticmethod
    def extract_formatting(content: str) -> Dict[str, List[str]]:
        """
        Extract formatted text from message content.

        Args:
            content (str): Message content

        Returns:
            dict: Dictionary with keys 'bold', 'italic', 'underline', 'strikethrough', 'code'
                  and values as lists of formatted text
        """
        formatting = {
            'bold': [],
            'italic': [],
            'underline': [],
            'strikethrough': [],
            'code': []
        }

        if BEAUTIFULSOUP:
            try:
                soup = BeautifulSoup(content, BS_PARSER)

                # Extract bold text
                for bold in soup.find_all(['b', 'strong']):
                    formatting['bold'].append(bold.get_text(strip=True))

                # Extract italic text
                for italic in soup.find_all(['i', 'em']):
                    formatting['italic'].append(italic.get_text(strip=True))

                # Extract underlined text
                for underline in soup.find_all('u'):
                    formatting['underline'].append(underline.get_text(strip=True))

                # Extract strikethrough text
                for strike in soup.find_all(['s', 'strike', 'del']):
                    formatting['strikethrough'].append(strike.get_text(strip=True))

                # Extract code
                for code in soup.find_all(['code', 'pre']):
                    formatting['code'].append(code.get_text(strip=True))
            except Exception as e:
                logger.warning(f"Error extracting formatting with BeautifulSoup: {e}")
                # Fall back to regex
                formatting['bold'] = re.findall(r'<(?:b|strong)[^>]*>(.*?)</(?:b|strong)>', content)
                formatting['italic'] = re.findall(r'<(?:i|em)[^>]*>(.*?)</(?:i|em)>', content)
                formatting['underline'] = re.findall(r'<u[^>]*>(.*?)</u>', content)
                formatting['strikethrough'] = re.findall(r'<(?:s|strike|del)[^>]*>(.*?)</(?:s|strike|del)>', content)
                formatting['code'] = re.findall(r'<(?:code|pre)[^>]*>(.*?)</(?:code|pre)>', content)
        else:
            # Use regex directly
            formatting['bold'] = re.findall(r'<(?:b|strong)[^>]*>(.*?)</(?:b|strong)>', content)
            formatting['italic'] = re.findall(r'<(?:i|em)[^>]*>(.*?)</(?:i|em)>', content)
            formatting['underline'] = re.findall(r'<u[^>]*>(.*?)</u>', content)
            formatting['strikethrough'] = re.findall(r'<(?:s|strike|del)[^>]*>(.*?)</(?:s|strike|del)>', content)
            formatting['code'] = re.findall(r'<(?:code|pre)[^>]*>(.*?)</(?:code|pre)>', content)

        # Remove empty lists
        return {k: v for k, v in formatting.items() if v}

    @staticmethod
    def extract_all(content: str) -> Dict[str, Any]:
        """
        Extract all structured data from message content.

        Args:
            content (str): Message content

        Returns:
            dict: Dictionary with all extracted structured data
        """
        structured_data = {}

        # Extract mentions
        mentions = ContentExtractor.extract_mentions(content)
        if mentions:
            structured_data['mentions'] = mentions

        # Extract links
        links = ContentExtractor.extract_links(content)
        if links:
            structured_data['links'] = links

        # Extract quotes
        quotes = ContentExtractor.extract_quotes(content)
        if quotes:
            structured_data['quotes'] = quotes

        # Extract formatting
        formatting = ContentExtractor.extract_formatting(content)
        if formatting:
            structured_data['formatting'] = formatting

        return structured_data


def extract_content_data(content: str) -> Dict[str, Any]:
    """
    Extract structured data from message content.

    Args:
        content (str): Message content

    Returns:
        dict: Dictionary with all extracted structured data
    """
    return ContentExtractor.extract_all(content)


def format_content_with_markup(content: str) -> str:
    """
    Format message content with markup for better readability.

    Args:
        content (str): Message content

    Returns:
        str: Formatted message content
    """
    if not content:
        return ""

    if BEAUTIFULSOUP:
        try:
            # Use the detected parser (lxml or html.parser)
            soup = BeautifulSoup(content, BS_PARSER)

            # Process @mentions - format as "@Username"
            for mention in soup.find_all('at'):
                mention_name = mention.get_text(strip=True)
                mention.replace_with(f"@{mention_name}")

            # Process links - format as "link_text (url)"
            for link in soup.find_all('a'):
                href = link.get('href', '')
                link_text = link.get_text(strip=True)
                if href and link_text and href != link_text:
                    link.replace_with(f"{link_text} ({href})")
                elif href:
                    link.replace_with(href)

            # Process formatting tags
            for bold in soup.find_all(['b', 'strong']):
                bold.replace_with(f"*{bold.get_text(strip=True)}*")

            for italic in soup.find_all(['i', 'em']):
                italic.replace_with(f"_{italic.get_text(strip=True)}_")

            for underline in soup.find_all('u'):
                underline.replace_with(f"_{underline.get_text(strip=True)}_")

            for strike in soup.find_all(['s', 'strike', 'del']):
                strike.replace_with(f"~{strike.get_text(strip=True)}~")

            for code in soup.find_all(['code', 'pre']):
                code.replace_with(f"`{code.get_text(strip=True)}`")

            # Process quotes and replies
            for quote in soup.find_all('quote'):
                author = quote.get('author', '')
                quote_text = quote.get_text(strip=True)
                if author:
                    quote.replace_with(f"\n> {author} wrote:\n> {quote_text}\n")
                else:
                    quote.replace_with(f"\n> {quote_text}\n")

            # Process line breaks
            for br in soup.find_all('br'):
                br.replace_with('\n')

            # Get the processed text
            text = soup.get_text()

            # Clean up extra whitespace but preserve line breaks
            text = re.sub(r'[ \t]+', ' ', text)  # Replace multiple spaces/tabs with a single space
            text = re.sub(r' *\n *', '\n', text)  # Clean up spaces around newlines
            text = re.sub(r'\n{3,}', '\n\n', text)  # Replace 3+ consecutive newlines with 2
            text = text.strip()  # Remove leading/trailing whitespace

            return text
        except Exception as e:
            logger.warning(f"Error formatting content with BeautifulSoup: {e}")
            # Fall back to regex
            return format_content_with_regex(content)
    else:
        return format_content_with_regex(content)


def format_content_with_regex(content: str) -> str:
    """
    Format message content with markup using regex.

    Args:
        content (str): Message content

    Returns:
        str: Formatted message content
    """
    if not content:
        return ""

    try:
        # Process @mentions - <at id="user123">User Name</at> -> @User Name
        content = re.sub(r'<at[^>]*>(.*?)</at>', r'@\1', content)

        # Process links - <a href="url">link text</a> -> link text (url)
        def process_link(match):
            href = re.search(r'href=["\'](.*?)["\']', match.group(0))
            link_text = re.search(r'>(.*?)</a>', match.group(0))

            if href and link_text:
                href_val = href.group(1)
                text_val = link_text.group(1)

                if href_val != text_val:
                    return f"{text_val} ({href_val})"
                else:
                    return href_val
            elif href:
                return href.group(1)
            elif link_text:
                return link_text.group(1)
            else:
                return ""

        content = re.sub(r'<a[^>]*>.*?</a>', process_link, content)

        # Process formatting
        content = re.sub(r'<(?:b|strong)[^>]*>(.*?)</(?:b|strong)>', r'*\1*', content)
        content = re.sub(r'<(?:i|em)[^>]*>(.*?)</(?:i|em)>', r'_\1_', content)
        content = re.sub(r'<u[^>]*>(.*?)</u>', r'_\1_', content)
        content = re.sub(r'<(?:s|strike|del)[^>]*>(.*?)</(?:s|strike|del)>', r'~\1~', content)
        content = re.sub(r'<(?:code|pre)[^>]*>(.*?)</(?:code|pre)>', r'`\1`', content)

        # Process quotes
        def process_quote(match):
            author = re.search(r'author=["\'](.*?)["\']', match.group(0))
            quote_text = re.search(r'>(.*?)</quote>', match.group(0), re.DOTALL)

            if author and quote_text:
                return f"\n> {author.group(1)} wrote:\n> {quote_text.group(1).strip()}\n"
            elif quote_text:
                return f"\n> {quote_text.group(1).strip()}\n"
            else:
                return ""

        content = re.sub(r'<quote[^>]*>.*?</quote>', process_quote, content, flags=re.DOTALL)

        # Process line breaks
        content = re.sub(r'<br[^>]*>', '\n', content)

        # Remove remaining HTML tags
        content = re.sub(r'<[^>]+>', '', content)

        # Decode all HTML entities
        content = html.unescape(content)

        # Clean up extra whitespace but preserve line breaks
        content = re.sub(r'[ \t]+', ' ', content)  # Replace multiple spaces/tabs with a single space
        content = re.sub(r' *\n *', '\n', content)  # Clean up spaces around newlines
        content = re.sub(r'\n{3,}', '\n\n', content)  # Replace 3+ consecutive newlines with 2
        content = content.strip()  # Remove leading/trailing whitespace

        return content
    except Exception as e:
        logger.warning(f"Error formatting content with regex: {e}")
        # Fall back to simple tag stripping
        return re.sub(r'<[^>]+>', '', content)