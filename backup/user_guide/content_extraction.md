# Content Extraction Module

This document describes the Content Extraction module, which provides specialized functions for extracting structured data from Skype message content.

## Overview

The Content Extraction module (`src/parser/content_extractor.py`) is responsible for:

1. Extracting structured data from message content (mentions, links, quotes, formatting)
2. Formatting message content for better readability
3. Providing fallback mechanisms when BeautifulSoup is not available

This module is a key component of the Enhanced Content Parsing feature, which improves the handling of complex HTML, mentions, links, and other Skype message elements.

## Components

### ContentExtractor Class

The `ContentExtractor` class provides static methods for extracting different types of structured data from message content:

- `extract_mentions`: Extracts @mentions from message content
- `extract_links`: Extracts links from message content
- `extract_quotes`: Extracts quotes from message content
- `extract_formatting`: Extracts formatted text from message content
- `extract_all`: Extracts all structured data from message content

### Helper Functions

The module also provides several helper functions:

- `extract_content_data`: Extracts all structured data from message content (wrapper for `ContentExtractor.extract_all`)
- `format_content_with_markup`: Formats message content with markup for better readability
- `format_content_with_regex`: Formats message content with markup using regex (fallback when BeautifulSoup is not available)

## Usage

### Extracting Structured Data

To extract structured data from message content:

```python
from src.parser.content_extractor import extract_content_data

# Extract structured data from message content
message_content = '<at id="user123">John Doe</at> Check out <a href="https://example.com">this link</a>'
structured_data = extract_content_data(message_content)

# Access extracted data
if 'mentions' in structured_data:
    for mention in structured_data['mentions']:
        print(f"Mention: {mention['name']} (ID: {mention['id']})")

if 'links' in structured_data:
    for link in structured_data['links']:
        print(f"Link: {link['text']} (URL: {link['url']})")
```

### Formatting Message Content

To format message content for better readability:

```python
from src.parser.content_extractor import format_content_with_markup

# Format message content
message_content = '<at id="user123">John Doe</at> Check out <a href="https://example.com">this link</a>'
formatted_content = format_content_with_markup(message_content)

# Output: "@John Doe Check out this link (https://example.com)"
print(formatted_content)
```

## Structured Data Format

The structured data extracted from message content is returned as a dictionary with the following structure:

```json
{
  "mentions": [
    {
      "id": "user123",
      "name": "John Doe"
    }
  ],
  "links": [
    {
      "url": "https://example.com",
      "text": "this link"
    }
  ],
  "quotes": [
    {
      "author": "Jane Smith",
      "text": "Original message"
    }
  ],
  "formatting": {
    "bold": ["Bold text"],
    "italic": ["italic text"],
    "underline": ["underlined text"],
    "strikethrough": ["strikethrough text"],
    "code": ["code block"]
  }
}
```

## Supported HTML Elements

The Content Extraction module supports the following HTML elements:

| Element | Description | Example |
|---------|-------------|---------|
| `<at>` | Mentions | `<at id="user123">John Doe</at>` |
| `<a>` | Links | `<a href="https://example.com">link text</a>` |
| `<b>`, `<strong>` | Bold text | `<b>Bold text</b>` |
| `<i>`, `<em>` | Italic text | `<i>Italic text</i>` |
| `<u>` | Underlined text | `<u>Underlined text</u>` |
| `<s>`, `<strike>`, `<del>` | Strikethrough text | `<s>Strikethrough text</s>` |
| `<code>`, `<pre>` | Code blocks | `<code>Code block</code>` |
| `<quote>` | Quotes | `<quote author="Jane">Original message</quote>` |
| `<br>` | Line breaks | `Line 1<br>Line 2` |

## Fallback Mechanisms

The Content Extraction module uses BeautifulSoup for parsing HTML content when available. If BeautifulSoup is not available, or if parsing with BeautifulSoup fails, the module falls back to using regex patterns for extraction and formatting.

This ensures that the module can still function even when BeautifulSoup is not installed, although with potentially reduced accuracy.

## Integration with Core Parser

The Content Extraction module is integrated with the Core Parser module (`src/parser/core_parser.py`) through the following functions:

- `content_parser`: Uses `format_content_with_markup` to format message content
- `_process_message_content`: Uses `extract_content_data` to extract structured data from message content

## Performance Considerations

The Content Extraction module is designed to be efficient, but parsing complex HTML content can be resource-intensive. To optimize performance:

1. The module uses BeautifulSoup with the lxml parser when available, which is faster than the default html.parser
2. Fallback regex patterns are optimized for speed
3. The module only extracts the data that is present in the content, avoiding unnecessary processing

## Error Handling

The Content Extraction module includes robust error handling to ensure that it can process a wide variety of message content without failing:

1. If BeautifulSoup parsing fails, the module falls back to regex
2. If regex parsing fails, the module returns a simplified version of the content
3. All errors are logged for debugging purposes

## Future Improvements

Potential future improvements to the Content Extraction module include:

1. Support for additional Skype message elements
2. Improved handling of nested HTML elements
3. Better performance through caching or parallel processing
4. Integration with machine learning for more accurate content extraction