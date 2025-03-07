#!/usr/bin/env python3
"""
Content Extractor Module

This module provides specialized functions for extracting structured data from
message content, including mentions, links, formatting, and other elements.

It includes both class-based and standalone functions for content extraction
and formatting, with careful handling of BeautifulSoup warnings and URL-like content.
"""

import html
import logging
import re
import warnings
from typing import Any, Dict, List, Optional, Tuple

# Import centralized dependency handling
from ..utils.dependencies import BEAUTIFULSOUP_AVAILABLE as BEAUTIFULSOUP
from ..utils.dependencies import BS_PARSER

if BEAUTIFULSOUP:
    from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning

    # Filter out warnings about content resembling URLs rather than HTML
    warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

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
                for mention in soup.find_all("at"):
                    mention_id = mention.get("id", "")
                    mention_name = mention.get_text(strip=True)
                    mentions.append({"id": mention_id, "name": mention_name})
            except Exception as e:
                logger.error(f"Error extracting mentions: {e}")

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
                for link in soup.find_all("a"):
                    href = link.get("href", "")
                    link_text = link.get_text(strip=True)
                    links.append({"url": href, "text": link_text})
            except Exception as e:
                logger.warning(f"Error extracting links with BeautifulSoup: {e}")
                # Fall back to regex
                link_matches = re.findall(
                    r'<a href=["\'](.*?)["\'].*?>(.*?)</a>', content
                )
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
            if not any(link["url"] == url for link in links):
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
                for quote in soup.find_all("quote"):
                    author = quote.get("author", "")
                    quote_text = quote.get_text(strip=True)
                    quotes.append({"author": author, "text": quote_text})
            except Exception as e:
                logger.warning(f"Error extracting quotes with BeautifulSoup: {e}")
                # Fall back to regex
                quote_matches = re.findall(
                    r'<quote author=["\'](.*?)["\'].*?>(.*?)</quote>',
                    content,
                    re.DOTALL,
                )
                for author, quote_text in quote_matches:
                    quotes.append({"author": author, "text": quote_text.strip()})

                # Also match quotes without authors
                quote_matches = re.findall(r"<quote>(.*?)</quote>", content, re.DOTALL)
                for quote_text in quote_matches:
                    quotes.append({"author": "", "text": quote_text.strip()})
        else:
            # Use regex directly
            quote_matches = re.findall(
                r'<quote author=["\'](.*?)["\'].*?>(.*?)</quote>', content, re.DOTALL
            )
            for author, quote_text in quote_matches:
                quotes.append({"author": author, "text": quote_text.strip()})

            # Also match quotes without authors
            quote_matches = re.findall(r"<quote>(.*?)</quote>", content, re.DOTALL)
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
            "bold": [],
            "italic": [],
            "underline": [],
            "strikethrough": [],
            "code": [],
        }

        if BEAUTIFULSOUP:
            try:
                soup = BeautifulSoup(content, BS_PARSER)

                # Extract bold text
                for bold in soup.find_all(["b", "strong"]):
                    formatting["bold"].append(bold.get_text(strip=True))

                # Extract italic text
                for italic in soup.find_all(["i", "em"]):
                    formatting["italic"].append(italic.get_text(strip=True))

                # Extract underlined text
                for underline in soup.find_all("u"):
                    formatting["underline"].append(underline.get_text(strip=True))

                # Extract strikethrough text
                for strike in soup.find_all(["s", "strike", "del"]):
                    formatting["strikethrough"].append(strike.get_text(strip=True))

                # Extract code
                for code in soup.find_all(["code", "pre"]):
                    formatting["code"].append(code.get_text(strip=True))
            except Exception as e:
                logger.warning(f"Error extracting formatting with BeautifulSoup: {e}")
                # Fall back to regex
                formatting["bold"] = re.findall(
                    r"<(?:b|strong)[^>]*>(.*?)</(?:b|strong)>", content
                )
                formatting["italic"] = re.findall(
                    r"<(?:i|em)[^>]*>(.*?)</(?:i|em)>", content
                )
                formatting["underline"] = re.findall(r"<u[^>]*>(.*?)</u>", content)
                formatting["strikethrough"] = re.findall(
                    r"<(?:s|strike|del)[^>]*>(.*?)</(?:s|strike|del)>", content
                )
                formatting["code"] = re.findall(
                    r"<(?:code|pre)[^>]*>(.*?)</(?:code|pre)>", content
                )
        else:
            # Use regex directly
            formatting["bold"] = re.findall(
                r"<(?:b|strong)[^>]*>(.*?)</(?:b|strong)>", content
            )
            formatting["italic"] = re.findall(
                r"<(?:i|em)[^>]*>(.*?)</(?:i|em)>", content
            )
            formatting["underline"] = re.findall(r"<u[^>]*>(.*?)</u>", content)
            formatting["strikethrough"] = re.findall(
                r"<(?:s|strike|del)[^>]*>(.*?)</(?:s|strike|del)>", content
            )
            formatting["code"] = re.findall(
                r"<(?:code|pre)[^>]*>(.*?)</(?:code|pre)>", content
            )

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
            structured_data["mentions"] = mentions

        # Extract links
        links = ContentExtractor.extract_links(content)
        if links:
            structured_data["links"] = links

        # Extract quotes
        quotes = ContentExtractor.extract_quotes(content)
        if quotes:
            structured_data["quotes"] = quotes

        # Extract formatting
        formatting = ContentExtractor.extract_formatting(content)
        if formatting:
            structured_data["formatting"] = formatting

        return structured_data

    def extract_cleaned_content(self, content_html: str) -> str:
        """
        Extract cleaned content from HTML content.

        Args:
            content_html: The HTML content

        Returns:
            Cleaned text content
        """
        if not content_html:
            return ""

        # Remove HTML tags if BeautifulSoup is available
        if BEAUTIFULSOUP:
            try:
                soup = BeautifulSoup(content_html, BS_PARSER)
                # Replace <at> tags with their text content
                for mention in soup.find_all("at"):
                    mention_text = mention.get_text(strip=True)
                    mention.replace_with(f"@{mention_text}")

                # Get text content
                text = soup.get_text(strip=True)
                # Unescape HTML entities
                text = html.unescape(text)
                return text
            except Exception as e:
                logger.error(f"Error cleaning content with BeautifulSoup: {e}")

        # Fallback: basic HTML tag removal with regex
        try:
            # Remove HTML tags
            text = re.sub(r"<[^>]+>", " ", content_html)
            # Unescape HTML entities
            text = html.unescape(text)
            # Normalize whitespace
            text = re.sub(r"\s+", " ", text).strip()
            return text
        except Exception as e:
            logger.error(f"Error cleaning content with regex: {e}")
            return content_html

    def extract_content(self, message: Dict[str, Any]) -> str:
        """
        Extract cleaned content from a message.

        Args:
            message: The message data

        Returns:
            Cleaned content as a string
        """
        # Get the content from the message
        content_html = message.get("content", "")

        # Use the existing method to clean the content
        return self.extract_cleaned_content(content_html)

    def extract_html_content(self, message: Dict[str, Any]) -> str:
        """
        Extract HTML content from a message.

        Args:
            message: The message data

        Returns:
            HTML content as a string
        """
        # Simply return the raw HTML content
        return message.get("content", "")

    def format_content_with_markup(
        self, content: str, structured_data: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Format message content with markup based on structured data.

        This method enhances message content by adding proper formatting for elements
        like links, mentions, and quotes. It uses the standalone format_content_with_markup
        function but provides better compatibility with the object-oriented approach.

        For plain URLs, it skips the BeautifulSoup processing to avoid unnecessary warnings
        and improve performance.

        Args:
            content (str): Original message content
            structured_data (dict, optional): Pre-extracted structured data

        Returns:
            str: Formatted message content

        Notes:
            - This method is part of the class to maintain compatibility with the
              method-based approach, but delegates to the standalone function.
            - It provides a fallback to regex-based formatting if BeautifulSoup is not available.
        """
        try:
            # For plain URLs, skip BeautifulSoup processing
            if (
                content.strip().startswith(("http://", "https://"))
                and " " not in content.strip()
                and "<" not in content
            ):
                logger.debug(
                    "Content appears to be a plain URL, skipping BeautifulSoup parsing"
                )
                return content.strip()

            return format_content_with_markup(content)
        except Exception as e:
            logger.warning(f"Error formatting content with markup: {e}")
            # Return original content if formatting fails
            return content


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
    Format message content with markup for better display.

    This standalone function enhances message content by formatting elements
    like links, mentions, and quotes. It serves as a utility function that
    can be used without instantiating the ContentExtractor class.

    For large exports or performance-critical scenarios, special optimizations
    are included to handle URL-like content efficiently.

    Args:
        content (str): Original message content

    Returns:
        str: Formatted message content

    Notes:
        - Uses BeautifulSoup for HTML parsing when available
        - Falls back to regex-based formatting when BeautifulSoup is not available
        - Special handling for plain URL content to avoid unnecessary processing
        - Handles potential parsing errors gracefully
    """
    if not content:
        return ""

    # For plain URLs, avoid BeautifulSoup processing
    if (
        content.strip().startswith(("http://", "https://"))
        and " " not in content.strip()
        and "<" not in content
    ):
        logger.debug(
            "Content appears to be a plain URL, skipping BeautifulSoup parsing"
        )
        return content.strip()

    try:
        if BEAUTIFULSOUP:
            return _format_with_beautifulsoup(content)
        else:
            logger.debug("BeautifulSoup not available, using regex-based formatting")
            return format_content_with_regex(content)
    except Exception as e:
        logger.warning(f"Error formatting content: {e}")
        # Fall back to regex-based formatting if BeautifulSoup fails
        try:
            return format_content_with_regex(content)
        except Exception as regex_error:
            logger.error(f"Regex formatting also failed: {regex_error}")
            return content


def _format_with_beautifulsoup(content: str) -> str:
    """
    Format content using BeautifulSoup.

    Private helper function to format content using BeautifulSoup.
    Extracted to improve code organization and error handling.

    Args:
        content (str): Original message content

    Returns:
        str: Formatted message content

    Raises:
        Exception: If BeautifulSoup parsing fails
    """
    with warnings.catch_warnings():
        # Suppress BeautifulSoup warnings about markup resembling URLs
        warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning)

        soup = BeautifulSoup(content, BS_PARSER)

        # Process links
        for link in soup.find_all("a"):
            href = link.get("href", "")
            if href:
                link_text = link.get_text()
                if link_text != href:
                    link.replace_with(f"{link_text} ({href})")
                else:
                    link.replace_with(href)

        # Process formatting elements
        for tag_name, replacement in [
            ("b", "**{}**"),
            ("strong", "**{}**"),
            ("i", "_{}_"),
            ("em", "_{}_"),
            ("u", "_{}_"),
            ("s", "~{}~"),
            ("strike", "~{}~"),
            ("del", "~{}~"),
        ]:
            for tag in soup.find_all(tag_name):
                text = tag.get_text()
                tag.replace_with(replacement.format(text))

        return soup.get_text()


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
        content = re.sub(r"<at[^>]*>(.*?)</at>", r"@\1", content)

        # Process links - <a href="url">link text</a> -> link text (url)
        def process_link(match):
            href = re.search(r'href=["\'](.*?)["\']', match.group(0))
            link_text = re.search(r">(.*?)</a>", match.group(0))

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

        content = re.sub(r"<a[^>]*>.*?</a>", process_link, content)

        # Process formatting
        content = re.sub(r"<(?:b|strong)[^>]*>(.*?)</(?:b|strong)>", r"*\1*", content)
        content = re.sub(r"<(?:i|em)[^>]*>(.*?)</(?:i|em)>", r"_\1_", content)
        content = re.sub(r"<u[^>]*>(.*?)</u>", r"_\1_", content)
        content = re.sub(
            r"<(?:s|strike|del)[^>]*>(.*?)</(?:s|strike|del)>", r"~\1~", content
        )
        content = re.sub(r"<(?:code|pre)[^>]*>(.*?)</(?:code|pre)>", r"`\1`", content)

        # Process quotes
        def process_quote(match):
            author = re.search(r'author=["\'](.*?)["\']', match.group(0))
            quote_text = re.search(r">(.*?)</quote>", match.group(0), re.DOTALL)

            if author and quote_text:
                return (
                    f"\n> {author.group(1)} wrote:\n> {quote_text.group(1).strip()}\n"
                )
            elif quote_text:
                return f"\n> {quote_text.group(1).strip()}\n"
            else:
                return ""

        content = re.sub(
            r"<quote[^>]*>.*?</quote>", process_quote, content, flags=re.DOTALL
        )

        # Process line breaks
        content = re.sub(r"<br[^>]*>", "\n", content)

        # Remove remaining HTML tags
        content = re.sub(r"<[^>]+>", "", content)

        # Decode all HTML entities
        content = html.unescape(content)

        # Clean up extra whitespace but preserve line breaks
        content = re.sub(
            r"[ \t]+", " ", content
        )  # Replace multiple spaces/tabs with a single space
        content = re.sub(r" *\n *", "\n", content)  # Clean up spaces around newlines
        content = re.sub(
            r"\n{3,}", "\n\n", content
        )  # Replace 3+ consecutive newlines with 2
        content = content.strip()  # Remove leading/trailing whitespace

        return content
    except Exception as e:
        logger.warning(f"Error formatting content with regex: {e}")
        # Fall back to simple tag stripping
        return re.sub(r"<[^>]+>", "", content)
