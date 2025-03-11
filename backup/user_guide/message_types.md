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

### Export Formats with Structured Data

The structured data extracted from different message types is included in all export formats:

#### JSON Export

In JSON exports, the structured data is included directly in the message object under the `structuredData` key:

```json
{
  "timestamp": "2023-01-01T12:00:00Z",
  "from_name": "John Doe",
  "content": "***Sent a video***",
  "structuredData": {
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
}
```

#### CSV Export

In CSV exports, common structured data fields are included as additional columns:

- Media fields: `media_filename`, `media_filesize`, `media_filetype`, `media_url`
- Poll fields: `poll_question`, `poll_options`
- Location fields: `location_latitude`, `location_longitude`, `location_address`

#### Text Export

In text exports, structured data is included as additional lines after the message content:

```
[12:00:00] John Doe: ***Sent a video***
    File: vacation.mp4
    Size: 10.0 MB
    URL: https://example.com/videos/vacation.mp4
```

For polls:

```
[12:00:00] John Doe: ***Created a poll***
    Poll Question: What's your favorite color?
    Poll Options:
      - Red
      - Green
      - Blue
```

### Database Schema for Structured Data

The database schema has been updated to store structured data in a JSONB column:

```sql
CREATE TABLE IF NOT EXISTS skype_messages (
    id SERIAL PRIMARY KEY,
    message_id VARCHAR(255) UNIQUE,
    conversation_id VARCHAR(255) REFERENCES skype_conversations(conversation_id),
    timestamp TIMESTAMP,
    from_id VARCHAR(255),
    from_name VARCHAR(255),
    content TEXT,
    content_raw TEXT,
    message_type VARCHAR(50),
    is_edited BOOLEAN DEFAULT FALSE,
    structured_data JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

The `structured_data` column stores the extracted structured data in JSON format, allowing for flexible querying and indexing of the structured data fields.

For specialized storage, the clean storage module also includes dedicated tables for different types of structured data:

- `clean_skype_message_media`: Stores media-specific data
- `clean_skype_message_polls`: Stores poll questions and options
- `clean_skype_message_locations`: Stores location data

This dual approach provides both flexibility (through the JSONB column) and structured querying capabilities (through the specialized tables).

## Enhanced Attachment Handling

The latest version includes enhanced attachment handling capabilities for media and file attachments in Skype messages.

### Attachment Handler

The new `AttachmentHandler` class provides functionality for handling attachments in Skype messages, including:

- Downloading attachments to local storage
- Organizing attachments by content type (images, videos, audio, documents, etc.)
- Generating thumbnails for image attachments
- Extracting metadata from image files
- Enriching attachment data with local paths and additional information

### Using the Attachment Handler

To process attachments in your application:

```python
from src.utils.attachment_handler import AttachmentHandler

# Initialize attachment handler with a storage directory
handler = AttachmentHandler(storage_dir="path/to/attachments")

# Process a message with attachments
message = {
    "id": "msg123",
    "content": "Sent you a file",
    "attachments": [
        {
            "type": "file",
            "name": "document.pdf",
            "url": "https://example.com/document.pdf",
            "content_type": "application/pdf",
            "size": 1024000
        }
    ]
}

# Process all attachments in the message
processed_message = handler.process_message_attachments(message)

# Access the enriched attachment data
attachment = processed_message["attachments"][0]
local_path = attachment.get("local_path")
print(f"Downloaded to: {local_path}")
```

### Enriched Attachment Data

The attachment handler enriches attachment data with additional information:

```json
{
  "type": "file",
  "name": "document.pdf",
  "url": "https://example.com/document.pdf",
  "content_type": "application/pdf",
  "size": 1024000,
  "local_path": "/path/to/attachments/documents/document.pdf"
}
```

For image attachments, additional metadata and a thumbnail path are included:

```json
{
  "type": "image",
  "name": "photo.jpg",
  "url": "https://example.com/photo.jpg",
  "content_type": "image/jpeg",
  "size": 512000,
  "local_path": "/path/to/attachments/images/photo.jpg",
  "thumbnail_path": "/path/to/attachments/thumbnails/thumb_photo.jpg",
  "metadata": {
    "format": "JPEG",
    "mode": "RGB",
    "width": 1920,
    "height": 1080,
    "exif": {
      "tag_271": "Camera Manufacturer",
      "tag_272": "Camera Model"
    }
  }
}
```

### Storage Organization

Attachments are automatically organized in subdirectories based on their content type:

- `images/`: Image files (JPEG, PNG, GIF, etc.)
- `videos/`: Video files (MP4, MOV, etc.)
- `audio/`: Audio files (MP3, WAV, etc.)
- `documents/`: Document files (PDF, DOC, etc.)
- `other/`: Other file types
- `thumbnails/`: Thumbnails for image files

## Advanced Message Type Handling

### Poll Messages

Enhanced support for poll messages now includes detailed extraction of:

- Poll title
- Poll options with vote counts
- Selected options (by the current user)
- Total vote count
- Poll status (open/closed)
- Vote visibility (public/private)
- Poll creator

Example of extracted poll data:

```json
{
  "poll_title": "What's your favorite programming language?",
  "poll_options": [
    {
      "text": "Python",
      "vote_count": 5,
      "is_selected": false
    },
    {
      "text": "JavaScript",
      "vote_count": 3,
      "is_selected": true
    },
    {
      "text": "Java",
      "vote_count": 2,
      "is_selected": false
    }
  ],
  "poll_metadata": {
    "status": "open",
    "vote_visibility": "public",
    "creator": "John Doe",
    "total_votes": 10,
    "created_at": "2023-06-15T14:30:00Z"
  }
}
```

### Scheduled Call Messages

Enhanced support for scheduled call messages now includes detailed extraction of:

- Call title
- Start time and date
- End time or duration
- Organizer
- Participants
- Description
- Meeting link (Teams, Zoom, Google Meet, etc.)
- Call ID or meeting ID

Example of extracted scheduled call data:

```json
{
  "scheduled_call": {
    "title": "Weekly Team Meeting",
    "start_time": "2023-06-20T10:00:00",
    "duration_minutes": 60,
    "organizer": "Meeting Organizer",
    "participants": ["John Doe", "Jane Smith"],
    "description": "Discuss project progress and next steps",
    "meeting_link": "https://teams.microsoft.com/l/meetup-join/...",
    "call_id": "meeting_id"
  }
}
```

### Adding Support for More Message Types

To add support for additional message types, follow these steps:

1. Identify the structure of the new message type
2. Create a new handler class in `src.utils.message_type_handlers.py`
3. Register the new handler in `SkypeMessageHandlerFactory`
4. Update the `message_types.json` configuration if needed
5. Write tests for the new handler

Example of a custom message type handler:

```python
class MyNewTypeHandler(MessageTypeHandler):
    def can_handle(self, message_type: str) -> bool:
        return message_type == 'CustomType'

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        data = super().extract_structured_data(message)

        # Extract your custom data
        data['custom_field'] = 'value'

        return data
```

Then register your handler in the factory:

```python
class SkypeMessageHandlerFactory(MessageHandlerFactoryProtocol):
    def __init__(self):
        self.handlers = [
            # ... existing handlers
            MyNewTypeHandler(),
            # Keep UnknownMessageHandler last
            UnknownMessageHandler()
        ]
```