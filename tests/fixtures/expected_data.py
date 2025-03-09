"""
Centralized test expectations for the SkypeParser test suite.

This module provides common expected output patterns for tests,
reducing duplication and making it easier to update expectations
when the implementation changes.
"""

from datetime import datetime
from typing import Any, Dict, List, Union, Optional, Tuple, TypedDict


# Type definitions for enhanced IDE support
class TransformedMessage(TypedDict):
    """Type definition for an expected transformed message."""
    id: str
    timestamp: str
    sender_id: str
    sender_display_name: str
    content: str
    message_type: str
    is_edited: bool
    is_deleted: bool


class TransformedParticipant(TypedDict):
    """Type definition for a participant in a conversation."""
    id: str
    display_name: str
    is_self: bool


class TransformedConversation(TypedDict):
    """Type definition for an expected transformed conversation."""
    id: str
    display_name: str
    participants: List[TransformedParticipant]
    messages: List[TransformedMessage]


class ApiResponseSuccess(TypedDict):
    """Type definition for a successful API response."""
    status: str
    message: str


class ApiResponseError(TypedDict):
    """Type definition for an error API response."""
    status: str
    message: str


# Expected ETL transformation results
BASIC_TRANSFORMED_MESSAGE: TransformedMessage = {
    "id": "msg1",
    "timestamp": "2023-01-01T12:00:00Z",
    "sender_id": "user1",
    "sender_display_name": "User 1",
    "content": "Hello world",
    "message_type": "RichText",
    "is_edited": False,
    "is_deleted": False
}

BASIC_TRANSFORMED_CONVERSATION: TransformedConversation = {
    "id": "conv1",
    "display_name": "Test Conversation",
    "participants": [
        {
            "id": "user1",
            "display_name": "User 1",
            "is_self": True
        }
    ],
    "messages": [BASIC_TRANSFORMED_MESSAGE]
}

BASIC_TRANSFORMED_DATA: Dict[str, Any] = {
    "user": {
        "id": "test-user-id",
        "display_name": "Test User"
    },
    "conversations": {
        "conv1": BASIC_TRANSFORMED_CONVERSATION
    },
    "messages": {
        "conv1": [BASIC_TRANSFORMED_MESSAGE]
    },
    "metadata": {
        "conversation_count": 1,
        "message_count": 1
    }
}

# Expected message type descriptions
MESSAGE_TYPE_DESCRIPTIONS: Dict[str, str] = {
    "RichText": "***Text message***",
    "RichText/UriObject": "***Sent a photo or file***",
    "RichText/HTML": "***Sent HTML content***",
    "RichText/Link": "***Shared a link***",
    "Poll": "***Created a poll***",
    "ThreadActivity/AddMember": "***Added a member to the conversation***",
    "ThreadActivity/DeleteMember": "***Removed a member from the conversation***",
    "ThreadActivity/TopicUpdate": "***Updated the conversation topic***",
    "ThreadActivity/PictureUpdate": "***Updated the conversation picture***",
    "Event/Call": "***Started a call***",
    "Event/CallEnded": "***Ended a call***",
    "RichText/Media_Video": "***Sent a video***",
    "RichText/Media_AudioMsg": "***Sent a voice message***",
    "RichText/Media_GenericFile": "***Sent a file***",
    "RichText/Media_Card": "***Shared a card***",
    "SystemMessage": "***System message***"
}

# Expected error messages
ERROR_MESSAGES: Dict[str, str] = {
    "file_not_found": "File not found: {file_path}",
    "invalid_input": "Invalid input: {input}",
    "database_connection": "Error connecting to database: {error}",
    "invalid_message_type": "Invalid message type: {message_type}",
    "missing_required_field": "Missing required field: {field}",
    "validation_error": "Validation error: {error}",
    "extraction_error": "Error during extraction: {error}",
    "transformation_error": "Error during transformation: {error}",
    "loading_error": "Error during loading: {error}"
}

# Expected database queries
EXPECTED_DB_QUERIES: Dict[str, str] = {
    "insert_export": "INSERT INTO skype_raw_exports",
    "insert_conversation": "INSERT INTO skype_conversations",
    "insert_message": "INSERT INTO skype_messages",
    "insert_participant": "INSERT INTO skype_participants",
    "insert_attachment": "INSERT INTO skype_attachments",
    "select_export": "SELECT * FROM skype_raw_exports",
    "select_conversation": "SELECT * FROM skype_conversations",
    "select_message": "SELECT * FROM skype_messages"
}

# Expected API responses
API_RESPONSES: Dict[str, Union[ApiResponseSuccess, ApiResponseError]] = {
    "success": {
        "status": "success",
        "message": "Operation completed successfully"
    },
    "error": {
        "status": "error",
        "message": "An error occurred: {error}"
    },
    "not_found": {
        "status": "error",
        "message": "Resource not found: {resource}"
    },
    "invalid_request": {
        "status": "error",
        "message": "Invalid request: {reason}"
    }
}

# Expected results for message type handlers
EXPECTED_MEDIA_MESSAGE_DATA: Dict[str, str] = {
    "media_filename": "video.mp4",
    "media_filesize": "1024000",
    "media_filetype": "video/mp4",
    "media_url": "https://example.com/video.mp4"
}

EXPECTED_SCHEDULED_CALL_DATA: Dict[str, str] = {
    "scheduled_call_title": "Team Meeting",
    "scheduled_call_time": "2023-01-01T12:00:00Z",
    "scheduled_call_duration": "01:00:00"
}

EXPECTED_POLL_DATA: Dict[str, Union[str, List[str]]] = {
    "poll_question": "What is your favorite color?",
    "poll_options": ["Red", "Green", "Blue"]
}

# Expected schema validation constants
EXPECTED_SCHEMA_ERRORS: Dict[str, str] = {
    "missing_field": "Missing required field: {field}",
    "invalid_type": "Invalid type for field {field}: expected {expected}, got {actual}",
    "invalid_format": "Invalid format for field {field}: {reason}"
}

# Helper function to customize expected message handler data
def get_expected_handler_data(handler_type: str, **kwargs: Any) -> Dict[str, Any]:
    """
    Get expected data for a specific message handler with optional customization.

    Args:
        handler_type: The type of handler ('media', 'poll', 'scheduled_call', etc.)
        **kwargs: Custom values to override in the default expected data

    Returns:
        The expected data for the handler
    """
    base_data: Dict[str, Any] = {
        "media": EXPECTED_MEDIA_MESSAGE_DATA,
        "scheduled_call": EXPECTED_SCHEDULED_CALL_DATA,
        "poll": EXPECTED_POLL_DATA
    }.get(handler_type, {})

    # Create a copy to avoid modifying the original
    result = base_data.copy()

    # Update with custom values
    result.update(kwargs)

    return result

# Functions for generating dynamic expectations

def get_expected_transformed_message(
    msg_id: str = "msg1",
    timestamp: str = "2023-01-01T12:00:00Z",
    sender_id: str = "user1",
    sender_name: str = "User 1",
    content: str = "Hello world",
    message_type: str = "RichText",
    is_edited: bool = False,
    is_deleted: bool = False
) -> TransformedMessage:
    """
    Generate an expected transformed message with customizable fields.

    Args:
        msg_id: Message ID
        timestamp: Message timestamp
        sender_id: Sender ID
        sender_name: Sender display name
        content: Message content
        message_type: Message type
        is_edited: Whether the message was edited
        is_deleted: Whether the message was deleted

    Returns:
        Expected transformed message
    """
    return {
        "id": msg_id,
        "timestamp": timestamp,
        "sender_id": sender_id,
        "sender_display_name": sender_name,
        "content": content,
        "message_type": message_type,
        "is_edited": is_edited,
        "is_deleted": is_deleted
    }


def get_expected_transformed_conversation(
    conv_id: str = "conv1",
    display_name: str = "Test Conversation",
    participants: Optional[List[Dict[str, Any]]] = None,
    messages: Optional[List[Dict[str, Any]]] = None
) -> TransformedConversation:
    """
    Generate an expected transformed conversation with customizable fields.

    Args:
        conv_id: Conversation ID
        display_name: Conversation display name
        participants: List of participants
        messages: List of messages

    Returns:
        Expected transformed conversation
    """
    if participants is None:
        participants = [
            {
                "id": "user1",
                "display_name": "User 1",
                "is_self": True
            }
        ]

    if messages is None:
        messages = [BASIC_TRANSFORMED_MESSAGE]

    return {
        "id": conv_id,
        "display_name": display_name,
        "participants": participants,
        "messages": messages
    }


def get_expected_error_message(error_type: str, **kwargs: Any) -> str:
    """
    Get an expected error message with placeholder values filled in.

    Args:
        error_type: Type of error message
        **kwargs: Values to fill in placeholders

    Returns:
        Formatted error message
    """
    if error_type not in ERROR_MESSAGES:
        return f"Unknown error: {error_type}"

    return ERROR_MESSAGES[error_type].format(**kwargs)


def get_expected_api_response(response_type: str, **kwargs: Any) -> Dict[str, Any]:
    """
    Get an expected API response with placeholder values filled in.

    Args:
        response_type: Type of response ('success', 'error', 'not_found', 'invalid_request')
        **kwargs: Values to fill in placeholders

    Returns:
        Expected API response
    """
    if response_type not in API_RESPONSES:
        return {"status": "unknown", "message": f"Unknown response type: {response_type}"}

    response = dict(API_RESPONSES[response_type])

    # Format any placeholders in the message
    if "{" in response["message"]:
        response["message"] = response["message"].format(**kwargs)

    return response