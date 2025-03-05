# Message Type Handling

## Overview

Skype exports contain various message types that need to be properly handled during the ETL process. This document explains how message types are processed in the Skype Parser.

## Message Type Configuration

Message types are configured in the `config/message_types.json` file. This file contains a mapping of message types to their human-readable descriptions. The configuration file has the following structure:

```json
{
  "message_types": {
    "RichText": "***Text message***",
    "RichText/UriObject": "***Sent a photo or file***",
    "Poll": "***Created a poll***",
    ...
  },
  "default_message_format": "***Sent a {message_type}***",
  "extractable_types": {
    "Poll": true,
    "Event/Call": true,
    "RichText/Location": true,
    "RichText/Contacts": true,
    "RichText/Media_AudioMsg": true,
    "RichText/Media_Video": true,
    "RichText/Media_GenericFile": true,
    "RichText/UriObject": true,
    "RichText/ScheduledCallInvite": true
  }
}
```

The `default_message_format` is used for message types that are not explicitly defined in the `message_types` object.

## Message Type Distribution

Based on analysis of sample data, the most common message types are:

1. **RichText** (86.66%): Standard text messages
2. **RichText/UriObject** (5.67%): Messages with photos or files
3. **ThreadActivity/AddMember** (3.84%): System messages about adding members
4. **ThreadActivity/DeleteMember** (1.14%): System messages about removing members
5. **RichText/Media_GenericFile** (0.45%): Messages with file attachments

For a complete list of message types and their distribution, see the `config/message_types_report.txt` file.

## Message Type Processing

Message types are processed by the `type_parser` function in `src/parser/core_parser.py`. This function maps message types to their human-readable descriptions using the configuration file.

```python
def type_parser(msg_type: str) -> str:
    """
    Map message types to their human-readable descriptions.

    Args:
        msg_type (str): Skype message type

    Returns:
        str: Human-readable description

    Raises:
        InvalidInputError: If the message type is invalid or empty
    """
    if not msg_type:
        error_msg = "Empty message type provided"
        logger.warning(error_msg)
        raise InvalidInputError(error_msg)

    # Use the configuration utility to get the message type description
    description = get_message_type_description(config, msg_type)

    # Log unknown message types to help identify gaps in our configuration
    if description == config.get('default_message_format', '***Sent a {message_type}***').format(message_type=msg_type):
        logger.info(f"Encountered unconfigured message type: {msg_type}")

    return description
```

## Handling Unknown Message Types

When an unknown message type is encountered, the `type_parser` function:

1. Uses the `default_message_format` to generate a description
2. Logs the unknown message type to help identify gaps in the configuration

This approach ensures that all message types are handled gracefully, even if they are not explicitly defined in the configuration file.

## Updating Message Type Configuration

To update the message type configuration:

1. Run the `scripts/extract_message_types.py` script on a representative sample of Skype exports:

   ```bash
   python scripts/extract_message_types.py --input path/to/messages.json --output config/message_types.json --report
   ```

2. Review the generated report in `config/message_types_report.txt`
3. Edit the `config/message_types.json` file to update or add message type descriptions

## Message Type Categories

Message types in Skype exports can be categorized as follows:

### User Messages

- **RichText**: Standard text messages
- **Text**: Plain text messages
- **RichText/UriObject**: Messages with photos or files
- **RichText/Media_***: Various media messages (audio, video, files, etc.)
- **Poll**: Poll messages
- **RichText/Location**: Location sharing
- **RichText/Contacts**: Contact sharing

### System Messages

- **ThreadActivity/***: System messages about conversation changes
- **Event/***: System messages about events (calls, etc.)
- **Guest/NameUpdate**: System messages about guest name changes
- **Translation**: Message translations
- **TranslationSettings**: Translation setting changes

## Best Practices

1. **Regular Updates**: Regularly update the message type configuration as new message types are encountered
2. **Descriptive Messages**: Use clear, descriptive messages for each message type
3. **Consistent Formatting**: Use consistent formatting for message type descriptions
4. **Logging**: Log unknown message types to identify gaps in the configuration
5. **Testing**: Add unit tests for each message type to ensure proper handling

## Enhanced Message Type Processing

In addition to basic message type handling, the Skype Parser now supports enhanced processing for specific message types. This allows for extracting structured data from messages based on their type.

### Extractable Message Types

The following message types support enhanced data extraction:

1. **Poll**: Extracts poll question and options
2. **Event/Call**: Extracts call duration and participants
3. **RichText/Location**: Extracts location coordinates and address
4. **RichText/Contacts**: Extracts contact information (name, phone, email)
5. **RichText/Media_AudioMsg**: Extracts audio file metadata (filename, size, duration, etc.)
6. **RichText/Media_Video**: Extracts video file metadata (filename, size, dimensions, duration, etc.)
7. **RichText/Media_GenericFile**: Extracts file metadata (filename, size, type, etc.)
8. **RichText/UriObject**: Extracts file and URL metadata
9. **RichText/ScheduledCallInvite**: Extracts scheduled call details (title, time, duration)
10. **RichText/Media_Album**: Extracts album metadata (item count, individual image details)
11. **RichText/Media_Card**: Extracts media card details (title, description, URL, thumbnail)
12. **PopCard**: Extracts pop card information (title, content, action, type)
13. **Translation**: Extracts translation details (text, languages, original text)
14. **ThreadActivity/***: Extracts thread activity details (members, values, initiators)

### Configuration

Extractable message types are configured in the `config/message_types.json` file under the `extractable_types` key:

```json
{
  "message_types": {
    // Message type descriptions
  },
  "default_message_format": "***Sent a {message_type}***",
  "extractable_types": {
    "Poll": true,
    "Event/Call": true,
    "RichText/Location": true,
    // Other extractable types
  }
}
```

### Implementation

Enhanced message type handling is implemented using specialized handlers in the `src/utils/message_type_handlers.py` module. Each handler is responsible for extracting structured data from a specific message type.

The `_process_message_content` function in `src/parser/core_parser.py` has been updated to use these handlers:

```python
def _process_message_content(msg_type: str, msg_content_raw: str, full_message: Dict[str, Any] = None) -> Dict[str, str]:
    # Basic content processing
    # ...

    # Extract structured data for specific message types
    if full_message and get_handler_for_message_type(msg_type):
        structured_data = extract_structured_data(full_message)
        if structured_data:
            result.update(structured_data)

    return result
```

### Extracted Data

The structured data extracted from messages is included in the message data structure. For example, a poll message might include:

```json
{
  "timestamp": "2023-01-01T12:00:00Z",
  "from_name": "John Doe",
  "content": "***Created a poll***",
  "poll_question": "What's your favorite color?",
  "poll_options": ["Red", "Green", "Blue"]
}
```

### Adding New Handlers

To add support for a new message type:

1. Create a new handler class in `src/utils/message_type_handlers.py`
2. Add the message type to the `MESSAGE_TYPE_HANDLERS` dictionary
3. Update the `extractable_types` section in `config/message_types.json`

Example handler implementation:

```python
class MyNewTypeHandler(MessageTypeHandler):
    @staticmethod
    def extract_data(message: Dict[str, Any]) -> Dict[str, Any]:
        content = message.get('content', '')
        result = {
            'my_field': '',
            'my_other_field': []
        }

        try:
            # Parse the content and extract data
            # ...

            logger.info(f"Extracted data: {result}")
        except Exception as e:
            logger.error(f"Error extracting data: {e}")

        return result
```

### Enhanced Media Metadata Extraction

The MediaHandler has been enhanced to extract more detailed metadata from media messages:

```python
# Example of enhanced media metadata extraction
{
    "timestamp": "2023-01-01T12:00:00Z",
    "from_name": "John Doe",
    "content": "***Sent a video***",
    "media_filename": "vacation.mp4",
    "media_filesize": "10485760",
    "media_filesize_formatted": "10.0 MB",
    "media_filetype": "video/mp4",
    "media_url": "https://example.com/videos/vacation.mp4",
    "media_thumbnail_url": "https://example.com/thumbnails/vacation.jpg",
    "media_width": "1920",
    "media_height": "1080",
    "media_duration": "00:02:30",
    "media_description": "Our summer vacation"
}
```

For album messages, additional information about each item in the album is extracted:

```python
# Example of album metadata extraction
{
    "timestamp": "2023-01-01T12:00:00Z",
    "from_name": "John Doe",
    "content": "***Sent an album of images***",
    "media_album_count": 3,
    "media_album_items": [
        {
            "url": "https://example.com/images/photo1.jpg",
            "thumbnail": "https://example.com/thumbnails/photo1.jpg",
            "width": "1200",
            "height": "800"
        },
        {
            "url": "https://example.com/images/photo2.jpg",
            "thumbnail": "https://example.com/thumbnails/photo2.jpg",
            "width": "1200",
            "height": "800"
        },
        {
            "url": "https://example.com/images/photo3.jpg",
            "thumbnail": "https://example.com/thumbnails/photo3.jpg",
            "width": "1200",
            "height": "800"
        }
    ]
}
```

### Thread Activity Handling

Thread activity messages (system messages about conversation changes) are now handled by the ThreadActivityHandler:

```python
# Example of thread activity data extraction
{
    "timestamp": "2023-01-01T12:00:00Z",
    "from_name": "System",
    "content": "***Added a member to the conversation***",
    "activity_type": "AddMember",
    "activity_members": [
        {
            "id": "user123",
            "name": "Jane Smith"
        }
    ],
    "activity_initiator": "John Doe"
}
```

For topic updates and other value-based activities:

```python
# Example of topic update data extraction
{
    "timestamp": "2023-01-01T12:00:00Z",
    "from_name": "System",
    "content": "***Updated the conversation topic***",
    "activity_type": "TopicUpdate",
    "activity_value": "Project Discussion",
    "activity_initiator": "John Doe"
}
```