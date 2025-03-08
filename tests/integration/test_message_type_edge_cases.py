#!/usr/bin/env python3
"""
Edge case and message type variety tests for the SkypeParser project.

This test suite focuses on testing edge cases and various message types,
ensuring the parser can handle all possible Skype export formats.
"""

import json
import os
import sys
import tempfile
import unittest
from typing import Any, Dict
from unittest.mock import patch

import pytest

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.db.etl import ETLPipeline
from src.utils.config import get_db_config
from src.utils.message_type_handlers import (
    CallMessageHandler,
    MediaMessageHandler,
    PollMessageHandler,
    SkypeMessageHandlerFactory,
    TextMessageHandler,
)
from tests.fixtures import (
    SkypeConversationFactory,
    SkypeDataFactory,
    SkypeMessageFactory,
    is_db_available,
    test_db_connection,
)


# Mock classes for missing handlers
class RichTextHTMLHandler:
    def can_handle(self, message_type: str) -> bool:
        return message_type.lower() == "richtext/html"

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": message.get("id", ""),
            "timestamp": message.get("originalarrivaltime", ""),
            "sender_id": message.get("from", ""),
            "sender_name": message.get("displayName", ""),
            "content": message.get("content", ""),
            "message_type": "richtext/html",
            "html_content": message.get("content", ""),
        }


class RichTextLinkHandler:
    def can_handle(self, message_type: str) -> bool:
        return message_type.lower() == "richtext/link"

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": message.get("id", ""),
            "timestamp": message.get("originalarrivaltime", ""),
            "sender_id": message.get("from", ""),
            "sender_name": message.get("displayName", ""),
            "content": message.get("content", ""),
            "message_type": "richtext/link",
            "link": message.get("properties", {}).get("url", ""),
        }


class SystemMessageHandler:
    def can_handle(self, message_type: str) -> bool:
        return message_type.lower() == "systemmessage"

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": message.get("id", ""),
            "timestamp": message.get("originalarrivaltime", ""),
            "sender_id": message.get("from", ""),
            "sender_name": message.get("displayName", ""),
            "content": message.get("content", ""),
            "message_type": "systemmessage",
            "system_event": message.get("properties", {}).get("eventType", ""),
        }


def create_test_file_with_message_types(file_path, message_types):
    """Create a test file with various message types."""
    conversation = SkypeConversationFactory.build(
        id="edge_case_conversation",
        displayName="Edge Case Test Conversation",
        MessageList=message_types,
    )

    data = SkypeDataFactory.build(
        userId="edge_case_user",
        exportDate="2023-01-01T12:00:00Z",
        conversations=[conversation],
    )

    with open(file_path, "w") as f:
        json.dump(data, f)

    return file_path


@pytest.mark.integration
@pytest.mark.edge_cases
class TestMessageTypeEdgeCases(unittest.TestCase):
    """Tests for edge cases and various message types."""

    def setUp(self):
        """Set up the test environment."""
        # Skip integration tests unless database is available
        if not is_db_available():
            self.skipTest("Integration tests disabled. Database not available.")

        # Set up temporary directories
        self.temp_dir = tempfile.mkdtemp()
        self.test_dir = os.path.join(self.temp_dir, "test_output")
        os.makedirs(self.test_dir, exist_ok=True)

        # Get database configuration
        self.db_config = get_test_db_config()

        # Create ETL context
        self.context = ETLContext(db_config=self.db_config, output_dir=self.test_dir)

        # Create ETL pipeline with use_di=True to use the registered services
        self.pipeline = ETLPipeline(
            db_config=self.db_config,
            output_dir=self.test_dir,
            context=self.context,
            use_di=True,
        )

    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary directory
        import shutil

        shutil.rmtree(self.temp_dir)

    def test_all_message_types(self):
        """Test handling of all documented message types."""
        # Create messages for all known types
        messages = [
            SkypeMessageFactory.build(text=True, id="msg_text"),
            SkypeMessageFactory.build(html=True, id="msg_html"),
            SkypeMessageFactory.build(link=True, id="msg_link"),
            SkypeMessageFactory.build(call=True, id="msg_call"),
            SkypeMessageFactory.build(system=True, id="msg_system"),
            SkypeMessageFactory.build(poll=True, id="msg_poll"),
            SkypeMessageFactory.build(scheduled_call=True, id="msg_scheduled_call"),
            SkypeMessageFactory.build(media_image=True, id="msg_media_image"),
            SkypeMessageFactory.build(media_video=True, id="msg_media_video"),
            SkypeMessageFactory.build(media_audio=True, id="msg_media_audio"),
            SkypeMessageFactory.build(file_transfer=True, id="msg_file_transfer"),
            SkypeMessageFactory.build(contact_info=True, id="msg_contact_info"),
            SkypeMessageFactory.build(location=True, id="msg_location"),
            SkypeMessageFactory.build(edited=True, id="msg_edited"),
            SkypeMessageFactory.build(deleted=True, id="msg_deleted"),
        ]

        # Create test file
        test_file = os.path.join(self.temp_dir, "all_types.json")
        create_test_file_with_message_types(test_file, messages)

        # Run pipeline
        result = self.pipeline.run_pipeline(
            file_path=test_file, user_display_name="All Message Types Test"
        )

        # Verify pipeline success
        self.assertTrue(
            result["success"],
            f"Pipeline failed: {result.get('error', 'Unknown error')}",
        )

        # Verify messages in database
        with test_db_connection(self.db_config) as conn:
            with conn.cursor() as cursor:
                # Get all messages
                cursor.execute(
                    """
                    SELECT message_id, type, content
                    FROM skype_messages
                    WHERE conversation_id IN (
                        SELECT conversation_id FROM skype_conversations
                        WHERE export_id = %s
                    )
                    ORDER BY message_id
                """,
                    (result["export_id"],),
                )

                db_messages = cursor.fetchall()

                # Verify count
                self.assertEqual(
                    len(db_messages),
                    len(messages),
                    f"Expected {len(messages)} messages, got {len(db_messages)}",
                )

                # Verify each message type is present
                message_types = [msg[1] for msg in db_messages]
                self.assertIn("text", message_types, "Text message type not found")
                self.assertIn("html", message_types, "HTML message type not found")
                self.assertIn("link", message_types, "Link message type not found")
                self.assertIn("call", message_types, "Call message type not found")
                self.assertIn("system", message_types, "System message type not found")
                self.assertIn("poll", message_types, "Poll message type not found")

    def test_empty_and_minimal_content(self):
        """Test handling of empty and minimal content."""
        # Create messages with empty/minimal content
        messages = [
            # Empty content
            SkypeMessageFactory.build(id="empty_text", content="", messagetype="Text"),
            # Single character
            SkypeMessageFactory.build(
                id="single_char", content=".", messagetype="Text"
            ),
            # Whitespace only
            SkypeMessageFactory.build(
                id="whitespace", content="   ", messagetype="Text"
            ),
            # Empty HTML
            SkypeMessageFactory.build(
                id="empty_html", content="", messagetype="RichText/HTML"
            ),
            # Minimal HTML
            SkypeMessageFactory.build(
                id="minimal_html", content="<div></div>", messagetype="RichText/HTML"
            ),
        ]

        # Create test file
        test_file = os.path.join(self.temp_dir, "empty_content.json")
        create_test_file_with_message_types(test_file, messages)

        # Run pipeline
        result = self.pipeline.run_pipeline(
            file_path=test_file, user_display_name="Empty Content Test"
        )

        # Verify pipeline success
        self.assertTrue(
            result["success"],
            f"Pipeline failed: {result.get('error', 'Unknown error')}",
        )

        # Verify all messages were processed
        with test_db_connection(self.db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM skype_messages
                    WHERE conversation_id IN (
                        SELECT conversation_id FROM skype_conversations
                        WHERE export_id = %s
                    )
                """,
                    (result["export_id"],),
                )

                message_count = cursor.fetchone()[0]
                self.assertEqual(
                    message_count,
                    len(messages),
                    f"Expected {len(messages)} messages, got {message_count}",
                )

    def test_malformed_and_invalid_content(self):
        """Test handling of malformed and invalid content."""
        # Create messages with malformed content
        messages = [
            # Malformed HTML
            SkypeMessageFactory.build(
                id="malformed_html",
                content="<div>Unclosed tag",
                messagetype="RichText/HTML",
            ),
            # Invalid HTML
            SkypeMessageFactory.build(
                id="invalid_html",
                content="<invalid>This is not valid HTML</invalid>",
                messagetype="RichText/HTML",
            ),
            # Truncated JSON
            SkypeMessageFactory.build(
                id="truncated_json",
                content='{"truncated":',
                messagetype="RichText/JSON",
            ),
            # Very long content
            SkypeMessageFactory.build(
                id="long_content",
                content="A" * 10000,  # 10,000 characters
                messagetype="Text",
            ),
            # Content with control characters
            SkypeMessageFactory.build(
                id="control_chars",
                content="Text with control chars: \x00\x01\x02\x03\x04",
                messagetype="Text",
            ),
            # Content with unusual Unicode
            SkypeMessageFactory.build(
                id="unicode_chars",
                content="Unicode: 😊🌍🚀🔥🎉 and more: 🦄👾🤖👽👻",
                messagetype="Text",
            ),
        ]

        # Create test file
        test_file = os.path.join(self.temp_dir, "malformed_content.json")
        create_test_file_with_message_types(test_file, messages)

        # Run pipeline
        result = self.pipeline.run_pipeline(
            file_path=test_file, user_display_name="Malformed Content Test"
        )

        # Verify pipeline success - should handle errors gracefully
        self.assertTrue(
            result["success"],
            f"Pipeline failed: {result.get('error', 'Unknown error')}",
        )

        # Verify all messages were processed (even if some content was cleaned/sanitized)
        with test_db_connection(self.db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM skype_messages
                    WHERE conversation_id IN (
                        SELECT conversation_id FROM skype_conversations
                        WHERE export_id = %s
                    )
                """,
                    (result["export_id"],),
                )

                message_count = cursor.fetchone()[0]
                self.assertEqual(
                    message_count,
                    len(messages),
                    f"Expected {len(messages)} messages, got {message_count}",
                )

    def test_nested_conversations_and_edge_structures(self):
        """Test handling of nested conversations and edge case structures."""
        # Create complex data structure with nested and unusual patterns
        data = SkypeDataFactory.build(
            userId="nested_test_user",
            exportDate="2023-01-01T12:00:00Z",
            conversations=[
                # Normal conversation
                SkypeConversationFactory.build(
                    id="normal_conv",
                    displayName="Normal Conversation",
                    MessageList=[SkypeMessageFactory.build(text=True, id="normal_msg")],
                ),
                # Empty conversation (no messages)
                SkypeConversationFactory.build(
                    id="empty_conv", displayName="Empty Conversation", MessageList=[]
                ),
                # Conversation with missing display name
                SkypeConversationFactory.build(
                    id="missing_name_conv",
                    displayName=None,
                    MessageList=[
                        SkypeMessageFactory.build(text=True, id="missing_name_msg")
                    ],
                ),
                # Conversation with duplicate messages (same ID)
                SkypeConversationFactory.build(
                    id="duplicate_msgs_conv",
                    displayName="Duplicate Messages",
                    MessageList=[
                        SkypeMessageFactory.build(text=True, id="duplicate_id"),
                        SkypeMessageFactory.build(html=True, id="duplicate_id"),
                    ],
                ),
            ],
        )

        # Add extra fields that aren't part of the standard schema
        data["extra_field"] = "This is not in the schema"
        data["conversations"][0]["extra_conv_field"] = "Extra conversation field"
        data["conversations"][0]["MessageList"][0][
            "extra_msg_field"
        ] = "Extra message field"

        # Create test file
        test_file = os.path.join(self.temp_dir, "nested_structure.json")
        with open(test_file, "w") as f:
            json.dump(data, f)

        # Run pipeline
        result = self.pipeline.run_pipeline(
            file_path=test_file, user_display_name="Nested Structure Test"
        )

        # Verify pipeline success - should handle unusual structures
        self.assertTrue(
            result["success"],
            f"Pipeline failed: {result.get('error', 'Unknown error')}",
        )

        # Verify processed conversations in database
        with test_db_connection(self.db_config) as conn:
            with conn.cursor() as cursor:
                # Get conversations
                cursor.execute(
                    """
                    SELECT conversation_id, display_name
                    FROM skype_conversations
                    WHERE export_id = %s
                """,
                    (result["export_id"],),
                )

                conversations = cursor.fetchall()

                # Should have at least the normal conversation
                self.assertGreaterEqual(
                    len(conversations),
                    1,
                    "At least one conversation should be processed",
                )

                # Get message count
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM skype_messages
                    WHERE conversation_id IN (
                        SELECT conversation_id FROM skype_conversations
                        WHERE export_id = %s
                    )
                """,
                    (result["export_id"],),
                )

                message_count = cursor.fetchone()[0]

                # Should have at least the normal message
                self.assertGreaterEqual(
                    message_count, 1, "At least one message should be processed"
                )

    def test_unknown_message_types(self):
        """Test handling of unknown message types."""
        # Create messages with unknown types
        messages = [
            # Unknown message type
            SkypeMessageFactory.build(
                id="unknown_type",
                content="This has an unknown type",
                messagetype="UnknownType",
            ),
            # Missing message type
            SkypeMessageFactory.build(
                id="missing_type", content="This has no message type", messagetype=None
            ),
            # Empty message type
            SkypeMessageFactory.build(
                id="empty_type",
                content="This has an empty message type",
                messagetype="",
            ),
            # Numeric message type
            SkypeMessageFactory.build(
                id="numeric_type",
                content="This has a numeric message type",
                messagetype="123",
            ),
        ]

        # Create test file
        test_file = os.path.join(self.temp_dir, "unknown_types.json")
        create_test_file_with_message_types(test_file, messages)

        # Run pipeline
        result = self.pipeline.run_pipeline(
            file_path=test_file, user_display_name="Unknown Types Test"
        )

        # Verify pipeline success - should handle unknown types
        self.assertTrue(
            result["success"],
            f"Pipeline failed: {result.get('error', 'Unknown error')}",
        )

        # Verify all messages were processed
        with test_db_connection(self.db_config) as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM skype_messages
                    WHERE conversation_id IN (
                        SELECT conversation_id FROM skype_conversations
                        WHERE export_id = %s
                    )
                """,
                    (result["export_id"],),
                )

                message_count = cursor.fetchone()[0]

                # All messages should be processed, using default handler if needed
                self.assertEqual(
                    message_count,
                    len(messages),
                    f"Expected {len(messages)} messages, got {message_count}",
                )

    def test_corrupted_json_file(self):
        """Test handling of corrupted JSON files."""
        # Create a file with invalid JSON
        corrupt_file = os.path.join(self.temp_dir, "corrupt.json")
        with open(corrupt_file, "w") as f:
            f.write('{"this": "is not valid JSON')

        # Run pipeline
        with self.assertRaises(Exception) as context:
            self.pipeline.run_pipeline(
                file_path=corrupt_file, user_display_name="Corrupted File Test"
            )

        # Verify appropriate error was raised
        self.assertIn(
            "JSON", str(context.exception), "Error should mention JSON parsing issue"
        )

    def test_partially_corrupted_json_file(self):
        """Test handling of partially corrupted JSON files."""
        # Create valid base data
        data = SkypeDataFactory.build(
            userId="partial_corrupt_user",
            exportDate="2023-01-01T12:00:00Z",
            conversations=[
                SkypeConversationFactory.build(
                    id="partial_corrupt_conv",
                    displayName="Partially Corrupted",
                    MessageList=[
                        SkypeMessageFactory.build(text=True, id="valid_msg1"),
                        SkypeMessageFactory.build(text=True, id="valid_msg2"),
                    ],
                )
            ],
        )

        # Write valid JSON first
        partial_file = os.path.join(self.temp_dir, "partial_corrupt.json")
        with open(partial_file, "w") as f:
            json.dump(data, f)

        # Append invalid JSON to make file partially corrupted
        with open(partial_file, "a") as f:
            f.write('\n{"this": "will break parsing"')

        # Run pipeline - this should fail but in a controlled way
        with self.assertRaises(Exception) as context:
            self.pipeline.run_pipeline(
                file_path=partial_file,
                user_display_name="Partially Corrupted File Test",
            )

        # Verify appropriate error
        self.assertIn(
            "JSON", str(context.exception), "Error should mention JSON parsing issue"
        )


# Helper function to get test database configuration
def get_test_db_config():
    """Get database configuration for testing."""
    from tests.fixtures import get_test_db_config

    return get_test_db_config()


if __name__ == "__main__":
    unittest.main()
