#!/usr/bin/env python3
"""
Example of using factory patterns for testing.

This module demonstrates how to use the factory patterns
to create test data and mock objects in a flexible and maintainable way.
"""

import os
import unittest
from unittest.mock import patch

import pytest

from src.db.etl_pipeline import SOLIDSkypeETLPipeline
from tests.fixtures import (  # Factory classes; Other fixtures
    MockBuilderFactory,
    MockDatabase,
    MockServiceFactory,
    SkypeConversationFactory,
    SkypeDataFactory,
    SkypeMessageFactory,
)


class TestFactoryPatterns(unittest.TestCase):
    """
    Tests demonstrating how to use the factory patterns.
    """

    def setUp(self):
        """
        Set up test fixtures before each test method.
        """
        # Setup test data
        self.test_file_path = "test.json"
        self.test_user_display_name = "Test User"

        # Add a minimal db_config for tests
        self.test_db_config = {
            "host": "localhost",
            "port": 5432,
            "dbname": "test_db",
            "user": "test_user",
            "password": "test_password",
        }

    def test_skype_data_factory(self):
        """
        Test using the SkypeDataFactory to create test data.
        """
        # Create basic Skype data
        data = SkypeDataFactory.build()

        # Verify
        self.assertIsNotNone(data)
        self.assertIn("userId", data)
        self.assertIn("exportDate", data)
        self.assertIn("conversations", data)

        # Create Skype data with specific fields
        custom_data = SkypeDataFactory.build(
            userId="custom-user-id", exportDate="2023-01-01T00:00:00Z"
        )

        # Verify
        self.assertEqual(custom_data["userId"], "custom-user-id")
        self.assertEqual(custom_data["exportDate"], "2023-01-01T00:00:00Z")

        # Create Skype data with a specific number of conversations
        large_data = SkypeDataFactory.build(with_conversation_count=5)

        # Verify
        self.assertEqual(len(large_data.conversations), 5)

    def test_conversation_factory(self):
        """
        Test using the SkypeConversationFactory to create test data.
        """
        # Create a conversation with default messages
        conversation = SkypeConversationFactory.build()

        # Verify
        self.assertIsNotNone(conversation)
        self.assertIn("id", conversation)
        self.assertIn("displayName", conversation)
        self.assertIn("MessageList", conversation)

        # Create a conversation with specific fields
        custom_conversation = SkypeConversationFactory.build(
            id="custom-conv-id", displayName="Custom Conversation"
        )

        # Verify
        self.assertEqual(custom_conversation["id"], "custom-conv-id")
        self.assertEqual(custom_conversation["displayName"], "Custom Conversation")

        # Create a conversation with a specific number of messages
        conversation_with_messages = SkypeConversationFactory.build(
            with_message_count=5
        )

        # Verify
        self.assertEqual(len(conversation_with_messages["MessageList"]), 5)

        # Create a conversation with custom messages
        conversation_with_custom_messages = SkypeConversationFactory.build(
            with_messages=[
                {"id": "msg1", "content": "Hello"},
                {"id": "msg2", "content": "World"},
            ]
        )

        # Verify
        self.assertEqual(len(conversation_with_custom_messages["MessageList"]), 2)
        self.assertEqual(
            conversation_with_custom_messages["MessageList"][0]["id"], "msg1"
        )
        self.assertEqual(
            conversation_with_custom_messages["MessageList"][0]["content"], "Hello"
        )

    def test_message_factory(self):
        """
        Test using the SkypeMessageFactory to create test data.
        """
        # Create a basic message
        message = SkypeMessageFactory.build()

        # Verify
        self.assertIsNotNone(message)
        self.assertIn("id", message)
        self.assertIn("content", message)
        self.assertIn("from_id", message)

        # Create a message with specific fields
        custom_message = SkypeMessageFactory.build(
            id="custom-msg-id", content="Custom content", from_id="custom-user"
        )

        # Verify
        self.assertEqual(custom_message["id"], "custom-msg-id")
        self.assertEqual(custom_message["content"], "Custom content")
        self.assertEqual(custom_message["from_id"], "custom-user")

        # Create a message with a trait
        html_message = SkypeMessageFactory.build(html=True)

        # Verify
        self.assertEqual(html_message["messagetype"], "RichText/HTML")
        self.assertEqual(
            html_message["content"], "<b>Bold text</b> and <i>italic text</i>"
        )

    @patch("os.path.exists")
    def test_mock_service_factory(self, mock_exists):
        """
        Test using the MockServiceFactory to create mock services.
        """
        # Setup
        mock_exists.return_value = True

        # Create mock services
        content_extractor = MockServiceFactory.create_content_extractor(
            extract_content_return="Custom content",
            extract_html_content_return="<p>Custom HTML content</p>",
            extract_cleaned_content_return="Custom cleaned content",
        )

        validation_service = MockServiceFactory.create_validation_service(
            file_exists_return=True,
            user_display_name_return=self.test_user_display_name,
        )

        file_handler = MockServiceFactory.create_file_handler(
            read_file_return=SkypeDataFactory.build()
        )

        structured_data_extractor = MockServiceFactory.create_structured_data_extractor(
            extract_structured_data_return={
                "type": "custom",
                "mentions": ["@user1"],
                "links": ["https://example.com"],
                "formatted": True,
            }
        )

        message_handler_factory = MockServiceFactory.create_message_handler_factory(
            can_handle_return=True,
            extract_data_return={"type": "custom", "content": "Custom content"},
        )

        # Create mock database
        mock_db = MockDatabase()

        # Create pipeline with all dependencies injected
        pipeline = SOLIDSkypeETLPipeline(
            db_config=self.test_db_config,
            file_handler=file_handler,
            validation_service=validation_service,
            db_connection=mock_db.conn,
            content_extractor=content_extractor,
            structured_data_extractor=structured_data_extractor,
            message_handler_factory=message_handler_factory,
        )

        # Run the pipeline
        result = pipeline.run_pipeline(
            file_path=self.test_file_path, user_display_name=self.test_user_display_name
        )

        # Verify
        self.assertIn("extraction", result)
        self.assertIn("transformation", result)
        self.assertIn("loading", result)
        self.assertTrue(result["extraction"]["success"])
        self.assertTrue(result["transformation"]["success"])
        self.assertTrue(result["loading"]["success"])

        # Verify function calls
        validation_service.validate_file_exists_mock.assert_called_with(
            self.test_file_path
        )
        validation_service.validate_user_display_name_mock.assert_called_with(
            self.test_user_display_name
        )
        file_handler.read_file.assert_called_with(self.test_file_path)

        # Verify custom return values
        self.assertEqual(
            content_extractor.extract_content.return_value, "Custom content"
        )
        self.assertEqual(
            content_extractor.extract_html_content.return_value,
            "<p>Custom HTML content</p>",
        )
        self.assertEqual(
            content_extractor.extract_cleaned_content.return_value,
            "Custom cleaned content",
        )

    @patch("os.path.exists")
    def test_mock_builder_factory(self, mock_exists):
        """
        Test using the MockBuilderFactory to create mock services.
        """
        # Setup
        mock_exists.return_value = True

        # Create mock services using builder pattern
        content_extractor = (
            MockBuilderFactory.content_extractor()
            .with_content("Builder content")
            .with_html_content("<p>Builder HTML content</p>")
            .with_cleaned_content("Builder cleaned content")
            .build()
        )

        validation_service = (
            MockBuilderFactory.validation_service()
            .with_file_exists(True)
            .with_user_display_name(self.test_user_display_name)
            .build()
        )

        # Create other mocks
        file_handler = MockServiceFactory.create_file_handler()
        structured_data_extractor = (
            MockServiceFactory.create_structured_data_extractor()
        )
        message_handler_factory = MockServiceFactory.create_message_handler_factory()

        # Create mock database
        mock_db = MockDatabase()

        # Create pipeline with all dependencies injected
        pipeline = SOLIDSkypeETLPipeline(
            db_config=self.test_db_config,
            file_handler=file_handler,
            validation_service=validation_service,
            db_connection=mock_db.conn,
            content_extractor=content_extractor,
            structured_data_extractor=structured_data_extractor,
            message_handler_factory=message_handler_factory,
        )

        # Run the pipeline
        result = pipeline.run_pipeline(
            file_path=self.test_file_path, user_display_name=self.test_user_display_name
        )

        # Verify
        self.assertIn("extraction", result)
        self.assertIn("transformation", result)
        self.assertIn("loading", result)
        self.assertTrue(result["extraction"]["success"])
        self.assertTrue(result["transformation"]["success"])
        self.assertTrue(result["loading"]["success"])

        # Verify function calls
        validation_service.validate_file_exists_mock.assert_called_with(
            self.test_file_path
        )
        validation_service.validate_user_display_name_mock.assert_called_with(
            self.test_user_display_name
        )

        # Verify custom return values
        self.assertEqual(
            content_extractor.extract_content.return_value, "Builder content"
        )
        self.assertEqual(
            content_extractor.extract_html_content.return_value,
            "<p>Builder HTML content</p>",
        )
        self.assertEqual(
            content_extractor.extract_cleaned_content.return_value,
            "Builder cleaned content",
        )

    @patch("os.path.exists")
    def test_error_scenario_with_builder(self, mock_exists):
        """
        Test creating an error scenario using the builder pattern.
        """
        # Setup
        mock_exists.return_value = True

        # Create a validation service that raises an error
        validation_service = (
            MockBuilderFactory.validation_service()
            .with_error("validate_file_exists", ValueError("File not found"))
            .build()
        )

        # Create other mocks
        file_handler = MockServiceFactory.create_file_handler()
        content_extractor = MockServiceFactory.create_content_extractor()
        structured_data_extractor = (
            MockServiceFactory.create_structured_data_extractor()
        )
        message_handler_factory = MockServiceFactory.create_message_handler_factory()

        # Create mock database
        mock_db = MockDatabase()

        # Create pipeline with all dependencies injected
        pipeline = SOLIDSkypeETLPipeline(
            db_config=self.test_db_config,
            file_handler=file_handler,
            validation_service=validation_service,
            db_connection=mock_db.conn,
            content_extractor=content_extractor,
            structured_data_extractor=structured_data_extractor,
            message_handler_factory=message_handler_factory,
        )

        # Run the pipeline and expect an error
        with self.assertRaises(ValueError) as context:
            pipeline.run_pipeline(
                file_path=self.test_file_path,
                user_display_name=self.test_user_display_name,
            )

        # Verify error message
        self.assertIn("File not found", str(context.exception))


if __name__ == "__main__":
    pytest.main(["-xvs", __file__])
