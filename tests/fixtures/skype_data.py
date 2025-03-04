"""
Test fixtures for Skype data used in ETL pipeline tests.

This module contains predefined data structures that match the Skype export format
for use in tests. Centralizing these fixtures makes tests more maintainable and
reduces duplication across test files.
"""

# Basic sample data structure that matches Skype export format
BASIC_SKYPE_DATA = {
    "userId": "test-user-id",
    "exportDate": "2023-01-01T12:00:00Z",
    "conversations": [
        {
            "id": "conversation1",
            "displayName": "Test Conversation",
            "MessageList": [
                {
                    "id": "message1",
                    "content": "Hello world",
                    "from": "user1",
                    "originalarrivaltime": "2023-01-01T12:00:00Z",
                    "messagetype": "RichText"
                }
            ]
        }
    ]
}

# More complex sample with various message types and edge cases
COMPLEX_SKYPE_DATA = {
    "userId": "test-user-id",  # Changed from test_user to match expected format
    "exportDate": "2023-01-01T12:00:00Z",
    "conversations": [
        {
            "id": "conversation1",
            "displayName": "Test Conversation 1",
            "MessageList": [
                {
                    "id": "msg1",
                    "originalarrivaltime": "2023-01-01T12:30:00Z",
                    "from": "user1",
                    "content": "Hello, world!",
                    "messagetype": "RichText"
                },
                {
                    "id": "msg2",
                    "originalarrivaltime": "2023-01-01T12:35:00Z",
                    "from": "user2",
                    "content": "Hi there!",
                    "messagetype": "RichText"
                },
                {
                    "id": "msg3",
                    "originalarrivaltime": "2023-01-01T12:40:00Z",
                    "from": "user1",
                    "content": "<b>Bold text</b> and <i>italic text</i>",
                    "messagetype": "RichText/HTML"
                }
            ]
        },
        {
            "id": "conversation2",
            "displayName": "Test Conversation 2",
            "MessageList": [
                {
                    "id": "msg4",
                    "originalarrivaltime": "2023-01-01T13:00:00Z",
                    "from": "user3",
                    "content": "Message with emoji 😊",
                    "messagetype": "RichText"
                },
                {
                    "id": "msg5",
                    "originalarrivaltime": "2023-01-01T13:05:00Z",
                    "from": "user1",
                    "content": "https://example.com",
                    "messagetype": "RichText/Link"
                }
            ]
        },
        # This conversation has a None displayName and should be skipped during processing
        {
            "id": "conversation3",
            "displayName": None,
            "MessageList": [
                {
                    "id": "msg6",
                    "originalarrivaltime": "2023-01-01T14:00:00Z",
                    "from": "user4",
                    "content": "This message is in a conversation with no display name",
                    "messagetype": "RichText"
                }
            ]
        }
    ]
}

# Sample with invalid data to test error handling
INVALID_SKYPE_DATA = {
    "userId": "test-user-id",
    "exportDate": "invalid_date",  # Invalid date format
    "conversations": [
        {
            "id": "conversation1",
            "displayName": "Test Conversation",
            "MessageList": [
                {
                    "id": "message1",
                    "content": "Hello world",
                    "from": "user1",
                    "originalarrivaltime": "invalid_timestamp",  # Invalid timestamp
                    "messagetype": "RichText"
                },
                {
                    "id": "message2",
                    # Missing required fields
                    "messagetype": "RichText"
                }
            ]
        },
        {
            # Missing id
            "displayName": "Test Conversation 2",
            "MessageList": []
        }
    ]
}

# Minimal valid data for simple tests
MINIMAL_SKYPE_DATA = {
    "userId": "test-user-id",
    "exportDate": "2023-01-01T12:00:00Z",
    "conversations": []
}

# Data specifically for testing conversation skipping behavior
CONVERSATION_SKIP_TEST_DATA = {
    "userId": "test-user-id",
    "exportDate": "2023-01-01T12:00:00Z",
    "conversations": [
        {
            "id": "conversation1",
            "displayName": None,  # This should cause skipping
            "MessageList": [
                {
                    "id": "message1",
                    "content": "This message is in a conversation with no display name",
                    "from": "user1",
                    "originalarrivaltime": "2023-01-01T12:00:00Z",
                    "messagetype": "RichText"
                }
            ]
        },
        {
            "id": "conversation2",
            "displayName": "",  # Empty string (should be processed)
            "MessageList": [
                {
                    "id": "message2",
                    "content": "This message is in a conversation with empty display name",
                    "from": "user1",
                    "originalarrivaltime": "2023-01-01T12:00:00Z",
                    "messagetype": "RichText"
                }
            ]
        }
    ]
}