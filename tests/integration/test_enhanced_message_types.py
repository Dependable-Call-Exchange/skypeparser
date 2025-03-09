#!/usr/bin/env python3
"""
Integration tests for enhanced message type support and attachment handling.

This test suite focuses on testing the enhanced message type extraction and
attachment handling functionality in the complete ETL pipeline.
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
import uuid
from typing import Any, Dict, List
from unittest.mock import MagicMock, patch

import pytest

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.db.etl import ETLContext, ETLPipeline
from src.utils.attachment_handler import AttachmentHandler
from src.utils.di import get_service_provider
from src.utils.interfaces import (
    DatabaseConnectionProtocol,
    ExtractorProtocol,
    FileHandlerProtocol,
    LoaderProtocol,
    TransformerProtocol,
)
from src.utils.message_type_handlers import (
    MediaMessageHandler,
    PollMessageHandler,
    ScheduledCallHandler,
    TextMessageHandler,
)
# Import consolidated mocks from the mocks directory
from tests.fixtures.mocks import MockStructuredDataExtractor, MockContentExtractor, MockFileHandler, MockMessageHandler

# Test data
SAMPLE_POLL_MESSAGE = {
    "id": "poll123",
    "messagetype": "Poll",
    "originalarrivaltime": "2023-06-15T14:30:00Z",
    "from": "user123",
    "displayName": "Test User",
    "content": """
    <div class="pollContainer">
        <div class="pollTitle">What's your favorite programming language?</div>
        <div class="pollOption">
            <div class="pollOptionText">Python</div>
            <div class="pollOptionVoteCount">5 votes</div>
        </div>
        <div class="pollOption selected">
            <div class="pollOptionText">JavaScript</div>
            <div class="pollOptionVoteCount">3 votes</div>
        </div>
        <div class="pollOption">
            <div class="pollOptionText">Java</div>
            <div class="pollOptionVoteCount">2 votes</div>
        </div>
        <div class="pollTotalVotes">10 total votes</div>
        <div class="pollStatus">Poll is still open</div>
        <div class="pollVisibility">Votes are public</div>
        <div class="pollCreator">Created by: John Doe</div>
    </div>
    """,
}

SAMPLE_SCHEDULED_CALL_MESSAGE = {
    "id": "call123",
    "messagetype": "RichText/ScheduledCallInvite",
    "originalarrivaltime": "2023-06-16T10:00:00Z",
    "from": "user456",
    "displayName": "Meeting Organizer",
    "content": """
    <div>
        <div class="callTitle">Weekly Team Meeting</div>
        <div class="callStartTime">June 20, 2023 10:00 AM</div>
        <div class="callDuration">1 hour</div>
        <div class="callOrganizer">Meeting Organizer</div>
        <div class="callDescription">Discuss project progress and next steps</div>
        <a href="https://teams.microsoft.com/l/meetup-join/19:meeting_id@thread.v2/0?context=%7b%22Tid%22%3a%22id%22%7d">Join Meeting</a>
        <div class="callParticipant">John Doe</div>
        <div class="callParticipant">Jane Smith</div>
    </div>
    """,
}

SAMPLE_MEDIA_MESSAGE = {
    "id": "media123",
    "messagetype": "RichText/Media_GenericFile",
    "originalarrivaltime": "2023-06-17T15:45:00Z",
    "from": "user789",
    "displayName": "File Sender",
    "content": "Sent a file",
    "properties": {
        "attachments": [
            {
                "type": "file",
                "name": "document.pdf",
                "url": "https://example.com/document.pdf",
                "contentType": "application/pdf",
                "size": 1024000,
            }
        ]
    },
}

SAMPLE_MESSAGES = {
    "poll": SAMPLE_POLL_MESSAGE,
    "scheduled_call": SAMPLE_SCHEDULED_CALL_MESSAGE,
    "media": SAMPLE_MEDIA_MESSAGE,
}


class MockETLComponents:
    """Mock ETL components for testing."""

    def __init__(self, test_dir: str, messages: Dict[str, Any]):
        """Initialize mock components.

        Args:
            test_dir: Directory for test output
            messages: Sample messages for testing
        """
        self.test_dir = test_dir
        self.messages = messages
        self.file_handler = None
        self.extractor = None
        self.transformer = None
        self.loader = None
        self.db_connection = None

        # Create directories
        self.output_dir = os.path.join(test_dir, "output")
        os.makedirs(self.output_dir, exist_ok=True)

        # Create attachments directory
        self.attachments_dir = os.path.join(test_dir, "attachments")
        os.makedirs(self.attachments_dir, exist_ok=True)

        # Initialize attachment handler
        self.attachment_handler = AttachmentHandler(storage_dir=self.attachments_dir)

        # Set up mock data
        self._setup_mock_data()

    def _setup_mock_data(self):
        """Set up mock data for testing."""
        # Create a sample messages.json file
        self.messages_file = os.path.join(self.test_dir, "messages.json")
        with open(self.messages_file, "w") as f:
            json.dump({"messages": list(self.messages.values())}, f)

        # Create mock components
        self._create_mock_components()

    def _create_mock_components(self):
        """Create mock ETL components."""
        # Mock file handler
        self.file_handler = MockFileHandler()
        self.file_handler.read_file_return = {
            "conversations": [
                {
                    "id": "conv123",
                    "displayName": "Test Conversation",
                    "messages": list(self.messages.values()),
                }
            ]
        }

        # Mock DB connection
        self.db_connection = MagicMock(spec=DatabaseConnectionProtocol)

        # Mock extractor
        self.extractor = MagicMock(spec=ExtractorProtocol)
        self.extractor.extract.return_value = self.file_handler.read_file_return

        # Mock transformer with real message type handlers
        self.transformer = MagicMock(spec=TransformerProtocol)

        def transform_mock(raw_data, user_display_name=None):
            """Mock transformer that uses real message handlers."""
            transformed = {"conversations": {}, "messages": []}

            for conv in raw_data.get("conversations", []):
                conv_id = conv.get("id", f"conv_{uuid.uuid4()}")
                transformed_conv = {
                    "id": conv_id,
                    "displayName": conv.get("displayName"),
                    "message_count": len(conv.get("messages", [])),
                    "messages": [],
                }
                transformed["conversations"][conv_id] = transformed_conv

                for msg in conv.get("messages", []):
                    # Use real handlers to transform messages
                    handler = self._get_handler_for_type(msg.get("messagetype", ""))
                    if handler:
                        transformed_msg = handler.extract_structured_data(msg)
                        transformed_msg["conversation_id"] = conv_id

                        # Process attachments if present
                        if "properties" in msg and "attachments" in msg["properties"]:
                            attachments = []
                            for attachment in msg["properties"]["attachments"]:
                                attachments.append(
                                    {
                                        "type": attachment.get("type", "unknown"),
                                        "name": attachment.get("name", ""),
                                        "url": attachment.get("url", ""),
                                        "content_type": attachment.get(
                                            "contentType", ""
                                        ),
                                        "size": attachment.get("size", 0),
                                    }
                                )
                            transformed_msg["attachments"] = attachments

                        transformed["messages"].append(transformed_msg)
                        transformed_conv["messages"].append(transformed_msg)

            return transformed

        self.transformer.transform.side_effect = transform_mock

        # Mock loader
        self.loader = MagicMock(spec=LoaderProtocol)
        self.loader.load.return_value = 1  # Export ID

    def _get_handler_for_type(self, message_type: str):
        """Get the appropriate handler for a message type."""
        handlers = [
            PollMessageHandler(),
            ScheduledCallHandler(),
            MediaMessageHandler(),
            TextMessageHandler(),
        ]

        for handler in handlers:
            if handler.can_handle(message_type):
                return handler

        # Fallback for specific message types that might not be handled by the standard handlers
        if "media" in message_type.lower() or "richtext/media" in message_type.lower():
            return MediaMessageHandler()

        return None

    def register_di_components(self):
        """Register components with dependency injection."""
        # Get the global service provider
        service_provider = get_service_provider()

        # Clear existing registrations by reinitializing the dictionaries
        service_provider._singletons = {}
        service_provider._transients = {}
        service_provider._factories = {}

        # Register components
        service_provider.register_singleton(FileHandlerProtocol, self.file_handler)
        service_provider.register_singleton(
            DatabaseConnectionProtocol, self.db_connection
        )
        service_provider.register_singleton(ExtractorProtocol, self.extractor)
        service_provider.register_singleton(TransformerProtocol, self.transformer)
        service_provider.register_singleton(LoaderProtocol, self.loader)


class TestEnhancedMessageTypes(unittest.TestCase):
    """Test cases for enhanced message type support."""

    def setUp(self):
        """Set up test environment."""
        # Create temporary directory
        self.temp_dir = tempfile.mkdtemp()

        # Set up mock components
        self.mock_components = MockETLComponents(self.temp_dir, SAMPLE_MESSAGES)
        self.mock_components.register_di_components()

        # Database config
        self.db_config = {
            "host": "localhost",
            "port": 5432,
            "database": "test_db",
            "user": "test_user",
            "password": "test_password",
        }

    def tearDown(self):
        """Clean up after tests."""
        shutil.rmtree(self.temp_dir)

        # Clear service provider registrations
        service_provider = get_service_provider()
        service_provider._singletons = {}
        service_provider._transients = {}
        service_provider._factories = {}

    def test_poll_message_extraction(self):
        """Test extraction of poll message data."""
        # Create a handler and extract data
        handler = PollMessageHandler()
        data = handler.extract_structured_data(SAMPLE_POLL_MESSAGE)

        # Verify extracted data
        self.assertIn("poll_title", data)
        self.assertEqual(
            data["poll_title"], "What's your favorite programming language?"
        )

        self.assertIn("poll_options", data)
        self.assertEqual(len(data["poll_options"]), 3)

        # Check first option
        self.assertEqual(data["poll_options"][0]["text"], "Python")
        self.assertEqual(data["poll_options"][0]["vote_count"], 5)
        self.assertFalse(data["poll_options"][0]["is_selected"])

        # Check second option (selected)
        self.assertEqual(data["poll_options"][1]["text"], "JavaScript")
        self.assertEqual(data["poll_options"][1]["vote_count"], 3)
        self.assertTrue(data["poll_options"][1]["is_selected"])

        # Check metadata
        self.assertIn("poll_metadata", data)
        self.assertEqual(data["poll_metadata"].get("status"), "open")
        self.assertEqual(data["poll_metadata"].get("vote_visibility"), "public")
        self.assertEqual(data["poll_metadata"].get("creator"), "Created by: John Doe")
        self.assertEqual(data["poll_metadata"].get("total_votes"), 10)

    def test_scheduled_call_extraction(self):
        """Test extraction of scheduled call data."""
        # Create a handler and extract data
        handler = ScheduledCallHandler()
        data = handler.extract_structured_data(SAMPLE_SCHEDULED_CALL_MESSAGE)

        # Verify extracted data
        self.assertIn("scheduled_call", data)
        call_data = data["scheduled_call"]

        self.assertEqual(call_data["title"], "Weekly Team Meeting")
        self.assertEqual(call_data["start_time"], "June 20, 2023 10:00 AM")
        self.assertEqual(call_data["duration_minutes"], 60)  # 1 hour
        self.assertEqual(call_data["organizer"], "Meeting Organizer")
        self.assertEqual(
            call_data["description"], "Discuss project progress and next steps"
        )

        # Check meeting link
        self.assertTrue(
            call_data["meeting_link"].startswith("https://teams.microsoft.com/")
        )

        # Check participants
        self.assertEqual(len(call_data["participants"]), 2)
        self.assertIn("John Doe", call_data["participants"])
        self.assertIn("Jane Smith", call_data["participants"])

    def test_media_message_extraction(self):
        """Test extraction of media message data."""
        # Create a handler and extract data
        handler = MediaMessageHandler()
        data = handler.extract_structured_data(SAMPLE_MEDIA_MESSAGE)

        # Verify extracted data
        self.assertIn("attachments", data)
        self.assertEqual(len(data["attachments"]), 1)

        attachment = data["attachments"][0]
        self.assertEqual(attachment["type"], "file")
        self.assertEqual(attachment["name"], "document.pdf")
        self.assertEqual(attachment["url"], "https://example.com/document.pdf")
        self.assertEqual(attachment["content_type"], "application/pdf")
        self.assertEqual(attachment["size"], 1024000)

    @patch.object(AttachmentHandler, "download_attachment")
    def test_attachment_processing(self, mock_download):
        """Test processing of attachments."""
        # Mock download to return a local path
        local_path = os.path.join(self.temp_dir, "attachments", "document.pdf")
        mock_download.return_value = local_path

        # Create a sample PDF file
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, "wb") as f:
            f.write(b"test PDF content")

        # Initialize attachment handler
        handler = AttachmentHandler(
            storage_dir=os.path.join(self.temp_dir, "attachments")
        )

        # Process a message with attachment
        message = SAMPLE_MEDIA_MESSAGE.copy()
        message["attachments"] = [
            {
                "type": "file",
                "name": "document.pdf",
                "url": "https://example.com/document.pdf",
                "content_type": "application/pdf",
                "size": 1024000,
            }
        ]

        processed = handler.process_message_attachments(message)

        # Verify processed message
        self.assertIn("attachments", processed)
        self.assertEqual(len(processed["attachments"]), 1)

        # Verify attachment data was enriched
        attachment = processed["attachments"][0]
        self.assertEqual(attachment["name"], "document.pdf")
        self.assertEqual(attachment["content_type"], "application/pdf")

        # Check that local path was added
        self.assertIn("local_path", attachment)
        self.assertEqual(attachment["local_path"], local_path)

    def test_etl_pipeline_with_enhanced_types(self):
        """Test the ETL pipeline with enhanced message type extraction."""
        # Create ETL context and pipeline
        context = ETLContext(
            db_config=self.db_config, output_dir=os.path.join(self.temp_dir, "output")
        )

        # Set the file path to process
        context.file_path = self.mock_components.messages_file

        # Create pipeline with use_di=False to allow injecting our mocks
        pipeline = ETLPipeline(
            db_config=self.db_config,
            context=context,
            use_di=False,  # Don't use DI to get services
        )

        # Manually set the mocked components
        pipeline.extractor = self.mock_components.extractor
        pipeline.transformer = self.mock_components.transformer
        pipeline.loader = self.mock_components.loader
        pipeline.db_connection = self.mock_components.db_connection

        # Run pipeline
        result = pipeline.run()

        # Verify pipeline ran successfully
        self.assertTrue(result)

        # Verify extractor was called
        self.mock_components.extractor.extract.assert_called_once()

        # Verify transformer was called
        self.mock_components.transformer.transform.assert_called_once()

        # Verify loader was called
        self.mock_components.loader.load.assert_called_once()

        # Get the transformed data that was passed to the loader
        call_args = self.mock_components.loader.load.call_args
        transformed_data = call_args[0][1]  # Second argument to load()

        # Verify enhanced message types were processed
        messages = transformed_data.get("messages", [])
        self.assertEqual(len(messages), 3)

        # Find poll message
        poll_messages = [msg for msg in messages if msg.get("message_type") == "Poll"]
        self.assertEqual(len(poll_messages), 1)
        poll_message = poll_messages[0]
        self.assertIn("poll_title", poll_message)
        self.assertIn("poll_options", poll_message)

        # Find scheduled call message
        call_messages = [
            msg
            for msg in messages
            if msg.get("message_type") == "RichText/ScheduledCallInvite"
        ]
        self.assertEqual(len(call_messages), 1)
        call_message = call_messages[0]
        self.assertIn("scheduled_call", call_message)

        # Find media message
        media_messages = [
            msg
            for msg in messages
            if msg.get("message_type") == "RichText/Media_GenericFile"
        ]
        self.assertEqual(len(media_messages), 1)
        media_message = media_messages[0]
        self.assertIn("attachments", media_message)


if __name__ == "__main__":
    unittest.main()
