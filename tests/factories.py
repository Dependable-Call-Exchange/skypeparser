"""
Test Factories for Skype Parser

This module provides factory classes for generating test data for the Skype Parser.
It uses factory_boy and faker to create realistic and varied test data while
maintaining consistency and reducing duplication in tests.
"""

import random
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List, Optional
from unittest.mock import MagicMock

import factory
from factory.faker import Faker
from faker import Faker as RealFaker

# Set a fixed seed for reproducible tests
faker = RealFaker()
faker.seed_instance(12345)
factory.Faker._DEFAULT_LOCALE = "en_US"

# Import implementations for proper mocking
from src.parser.content_extractor import ContentExtractor
from src.utils.file_handler import FileHandler

# Import interfaces for mock services
from src.utils.interfaces import (
    ContentExtractorProtocol,
    FileHandlerProtocol,
    MessageHandlerFactoryProtocol,
    StructuredDataExtractorProtocol,
    ValidationServiceProtocol,
)
from src.utils.message_type_handlers import SkypeMessageHandlerFactory
from src.utils.structured_data_extractor import StructuredDataExtractor
from src.utils.validation import ValidationService


class SkypeMessageFactory(factory.Factory):
    """Factory for generating Skype message data."""

    class Meta:
        model = dict

    id = factory.Sequence(lambda n: f"message{n}")
    originalarrivaltime = factory.LazyFunction(lambda: datetime.now().isoformat())
    from_id = factory.Sequence(lambda n: f"user{n % 3 + 1}")
    from_name = factory.Sequence(lambda n: f"User {n % 3 + 1}")
    content = factory.Faker("text", max_nb_chars=100)
    messagetype = "RichText"
    edittime = None

    class Params:
        """Parameters for creating different types of messages."""

        edited = factory.Trait(
            edittime=factory.LazyFunction(
                lambda: (datetime.now() + timedelta(minutes=5)).isoformat()
            )
        )
        html = factory.Trait(
            messagetype="RichText/HTML",
            content="<b>Bold text</b> and <i>italic text</i>",
        )
        link = factory.Trait(messagetype="RichText/Link", content="https://example.com")
        call = factory.Trait(messagetype="Event/Call", content="Call started")
        system = factory.Trait(
            messagetype="SystemMessage", content="User joined the conversation"
        )


class SkypeConversationFactory(factory.Factory):
    """Factory for generating Skype conversation data."""

    class Meta:
        model = dict

    id = factory.Sequence(lambda n: f"conversation{n}")
    displayName = factory.Sequence(lambda n: f"Test Conversation {n}")

    @factory.lazy_attribute
    def MessageList(self):
        """Generate a list of messages for this conversation."""
        return [SkypeMessageFactory() for _ in range(3)]

    @factory.post_generation
    def with_messages(self, create, extracted, **kwargs):
        """
        Allow specifying custom messages for the conversation.

        Usage:
            SkypeConversationFactory(with_messages=[
                {'id': 'msg1', 'content': 'Hello'},
                {'id': 'msg2', 'content': 'World'}
            ])
        """
        if extracted:
            self["MessageList"] = [
                SkypeMessageFactory(**msg_kwargs) for msg_kwargs in extracted
            ]

    @factory.post_generation
    def with_message_count(self, create, extracted, **kwargs):
        """
        Generate a specific number of messages.

        Usage:
            SkypeConversationFactory(with_message_count=10)
        """
        if extracted and isinstance(extracted, int):
            self["MessageList"] = [SkypeMessageFactory() for _ in range(extracted)]


class SkypeDataFactory(factory.Factory):
    """Factory for generating complete Skype export data."""

    class Meta:
        model = dict

    userId = factory.Sequence(lambda n: f"test-user-{n}")
    exportDate = factory.LazyFunction(lambda: datetime.now().isoformat())

    @factory.lazy_attribute
    def conversations(self):
        """Generate a list of conversations for this export."""
        return [SkypeConversationFactory() for _ in range(2)]

    @factory.post_generation
    def with_conversation_count(self, create, extracted, **kwargs):
        """
        Generate a specific number of conversations.

        Usage:
            SkypeDataFactory(with_conversation_count=5)
        """
        if extracted and isinstance(extracted, int):
            self.conversations = [SkypeConversationFactory() for _ in range(extracted)]


# Common test data patterns as factory fixtures
BASIC_SKYPE_DATA = SkypeDataFactory.build(
    userId="test-user-id",
    exportDate="2023-01-01T12:00:00Z",
    conversations=[
        SkypeConversationFactory.build(
            id="conversation1",
            displayName="Test Conversation",
            MessageList=[
                SkypeMessageFactory.build(
                    id="message1",
                    originalarrivaltime="2023-01-01T12:00:00Z",
                    from_id="user1",
                    from_name="User 1",
                    content="Hello world",
                    messagetype="RichText",
                )
            ],
        )
    ],
)

COMPLEX_SKYPE_DATA = SkypeDataFactory.build(
    userId="test-user-id",
    exportDate="2023-01-01T12:00:00Z",
    conversations=[
        SkypeConversationFactory.build(
            id="conversation1",
            displayName="Test Conversation 1",
            MessageList=[
                SkypeMessageFactory.build(
                    id="msg1",
                    originalarrivaltime="2023-01-01T12:30:00Z",
                    from_id="user1",
                    from_name="User 1",
                    content="Hello, world!",
                ),
                SkypeMessageFactory.build(
                    id="msg2",
                    originalarrivaltime="2023-01-01T12:35:00Z",
                    from_id="user2",
                    from_name="User 2",
                    content="Hi there!",
                ),
                SkypeMessageFactory.build(
                    id="msg3",
                    originalarrivaltime="2023-01-01T12:40:00Z",
                    from_id="user1",
                    from_name="User 1",
                    content="<b>Bold text</b> and <i>italic text</i>",
                    messagetype="RichText/HTML",
                ),
            ],
        ),
        SkypeConversationFactory.build(
            id="conversation2",
            displayName="Test Conversation 2",
            MessageList=[
                SkypeMessageFactory.build(
                    id="msg4",
                    originalarrivaltime="2023-01-01T13:00:00Z",
                    from_id="user3",
                    from_name="User 3",
                    content="Message with emoji 😊",
                ),
                SkypeMessageFactory.build(
                    id="msg5",
                    originalarrivaltime="2023-01-01T13:05:00Z",
                    from_id="user1",
                    from_name="User 1",
                    content="https://example.com",
                    messagetype="RichText/Link",
                ),
            ],
        ),
        # Conversation with None displayName (should be skipped during processing)
        SkypeConversationFactory.build(
            id="conversation3",
            displayName=None,
            MessageList=[
                SkypeMessageFactory.build(
                    id="msg6",
                    originalarrivaltime="2023-01-01T14:00:00Z",
                    from_id="user4",
                    from_name="User 4",
                    content="This message is in a conversation with no display name",
                )
            ],
        ),
    ],
)

INVALID_SKYPE_DATA = SkypeDataFactory.build(
    userId=None,  # Missing user ID
    exportDate="invalid-date",  # Invalid date format
    conversations=[],  # Empty conversations list
)


# Factory for database records
class DatabaseRecordFactory(factory.Factory):
    """Factory for generating database records."""

    class Meta:
        model = dict

    id = factory.Sequence(lambda n: n)
    created_at = factory.LazyFunction(lambda: datetime.now().isoformat())

    class Params:
        """Parameters for creating different types of database records."""

        conversation = factory.Trait(
            conversation_id=factory.Sequence(lambda n: f"conv{n}"),
            display_name=factory.Faker("sentence", nb_words=3),
            export_id=factory.SelfAttribute("id"),
        )

        message = factory.Trait(
            message_id=factory.Sequence(lambda n: f"msg{n}"),
            conversation_id=factory.Sequence(lambda n: n % 5),  # Link to a conversation
            content=factory.Faker("paragraph"),
            timestamp=factory.LazyFunction(lambda: datetime.now().isoformat()),
            message_type="RichText",
        )


# Mock Service Factories


class MockServiceFactory:
    """Factory for creating mock service objects."""

    @staticmethod
    def create_content_extractor(
        extract_content_return: str = "Test content",
        extract_html_content_return: str = "<p>Test content</p>",
        extract_cleaned_content_return: str = "Test content",
    ) -> MagicMock:
        """
        Create a mock ContentExtractor.

        Args:
            extract_content_return: Return value for extract_content method
            extract_html_content_return: Return value for extract_html_content method
            extract_cleaned_content_return: Return value for extract_cleaned_content method

        Returns:
            MagicMock: A configured mock ContentExtractor
        """
        mock = MagicMock(spec=ContentExtractor)
        mock.extract_content.return_value = extract_content_return
        mock.extract_html_content.return_value = extract_html_content_return
        mock.extract_cleaned_content.return_value = extract_cleaned_content_return
        return mock

    @staticmethod
    def create_structured_data_extractor(
        extract_structured_data_return: Dict[str, Any] = None
    ) -> MagicMock:
        """
        Create a mock StructuredDataExtractor.

        Args:
            extract_structured_data_return: Return value for extract_structured_data method

        Returns:
            MagicMock: A configured mock StructuredDataExtractor
        """
        if extract_structured_data_return is None:
            extract_structured_data_return = {
                "type": "text",
                "mentions": [],
                "links": [],
                "formatted": False,
            }

        mock = MagicMock(spec=StructuredDataExtractor)
        mock.extract_structured_data.return_value = extract_structured_data_return
        return mock

    @staticmethod
    def create_message_handler_factory(
        can_handle_return: bool = True, extract_data_return: Dict[str, Any] = None
    ) -> MagicMock:
        """
        Create a mock MessageHandlerFactory.

        Args:
            can_handle_return: Return value for can_handle method
            extract_data_return: Return value for extract_data method

        Returns:
            MagicMock: A configured mock MessageHandlerFactory
        """
        if extract_data_return is None:
            extract_data_return = {"type": "text", "content": "Test content"}

        handler = MagicMock()
        handler.can_handle.return_value = can_handle_return
        handler.extract_data.return_value = extract_data_return

        factory = MagicMock(spec=SkypeMessageHandlerFactory)
        factory.get_handler.return_value = handler

        return factory

    @staticmethod
    def create_validation_service(
        file_exists_return: bool = True,
        file_object_return: bool = True,
        json_file_return: Dict[str, Any] = None,
        user_display_name_return: str = "Test User",
    ) -> MagicMock:
        """
        Create a mock ValidationService.

        Args:
            file_exists_return: Return value for validate_file_exists method
            file_object_return: Return value for validate_file_object method
            json_file_return: Return value for validate_json_file method
            user_display_name_return: Return value for validate_user_display_name method

        Returns:
            MagicMock: A configured mock ValidationService
        """
        if json_file_return is None:
            json_file_return = SkypeDataFactory.build()

        mock = MagicMock(spec=ValidationService)
        mock.validate_file_exists.return_value = file_exists_return
        mock.validate_file_object.return_value = file_object_return
        mock.validate_json_file.return_value = json_file_return
        mock.validate_user_display_name.return_value = user_display_name_return

        # Add mock methods for testing
        mock.validate_file_exists_mock = MagicMock(return_value=file_exists_return)
        mock.validate_file_object_mock = MagicMock(return_value=file_object_return)
        mock.validate_user_display_name_mock = MagicMock(
            return_value=user_display_name_return
        )

        return mock

    @staticmethod
    def create_file_handler(
        read_file_return: Dict[str, Any] = None,
        read_file_object_return: Dict[str, Any] = None,
    ) -> MagicMock:
        """
        Create a mock FileHandler.

        Args:
            read_file_return: Return value for read_file method
            read_file_object_return: Return value for read_file_object method

        Returns:
            MagicMock: A configured mock FileHandler
        """
        if read_file_return is None:
            read_file_return = SkypeDataFactory.build()

        if read_file_object_return is None:
            read_file_object_return = SkypeDataFactory.build()

        mock = MagicMock(spec=FileHandler)
        mock.read_file.return_value = read_file_return
        mock.read_file_object.return_value = read_file_object_return

        return mock


class MockBuilderFactory:
    """Factory for creating complex mock objects using a builder pattern."""

    class ContentExtractorBuilder:
        """Builder for ContentExtractor mocks."""

        def __init__(self):
            self.mock = MagicMock(spec=ContentExtractor)
            self.mock.extract_content.return_value = "Test content"
            self.mock.extract_html_content.return_value = "<p>Test content</p>"
            self.mock.extract_cleaned_content.return_value = "Test content"

        def with_content(
            self, content: str
        ) -> "MockBuilderFactory.ContentExtractorBuilder":
            """Set the return value for extract_content."""
            self.mock.extract_content.return_value = content
            return self

        def with_html_content(
            self, html_content: str
        ) -> "MockBuilderFactory.ContentExtractorBuilder":
            """Set the return value for extract_html_content."""
            self.mock.extract_html_content.return_value = html_content
            return self

        def with_cleaned_content(
            self, cleaned_content: str
        ) -> "MockBuilderFactory.ContentExtractorBuilder":
            """Set the return value for extract_cleaned_content."""
            self.mock.extract_cleaned_content.return_value = cleaned_content
            return self

        def with_error(
            self, method: str, error: Exception
        ) -> "MockBuilderFactory.ContentExtractorBuilder":
            """Set a side effect (error) for a method."""
            getattr(self.mock, method).side_effect = error
            return self

        def build(self) -> MagicMock:
            """Build and return the configured mock."""
            return self.mock

    class ValidationServiceBuilder:
        """Builder for ValidationService mocks."""

        def __init__(self):
            self.mock = MagicMock(spec=ValidationService)
            self.mock.validate_file_exists.return_value = True
            self.mock.validate_file_object.return_value = True
            self.mock.validate_json_file.return_value = SkypeDataFactory.build()
            self.mock.validate_user_display_name.return_value = "Test User"

            # Add mock methods for testing
            self.mock.validate_file_exists_mock = MagicMock(return_value=True)
            self.mock.validate_file_object_mock = MagicMock(return_value=True)
            self.mock.validate_user_display_name_mock = MagicMock(
                return_value="Test User"
            )

        def with_file_exists(
            self, exists: bool
        ) -> "MockBuilderFactory.ValidationServiceBuilder":
            """Set the return value for validate_file_exists."""
            self.mock.validate_file_exists.return_value = exists
            self.mock.validate_file_exists_mock.return_value = exists
            return self

        def with_file_object(
            self, valid: bool
        ) -> "MockBuilderFactory.ValidationServiceBuilder":
            """Set the return value for validate_file_object."""
            self.mock.validate_file_object.return_value = valid
            self.mock.validate_file_object_mock.return_value = valid
            return self

        def with_json_file(
            self, data: Dict[str, Any]
        ) -> "MockBuilderFactory.ValidationServiceBuilder":
            """Set the return value for validate_json_file."""
            self.mock.validate_json_file.return_value = data
            return self

        def with_user_display_name(
            self, name: str
        ) -> "MockBuilderFactory.ValidationServiceBuilder":
            """Set the return value for validate_user_display_name."""
            self.mock.validate_user_display_name.return_value = name
            self.mock.validate_user_display_name_mock.return_value = name
            return self

        def with_error(
            self, method: str, error: Exception
        ) -> "MockBuilderFactory.ValidationServiceBuilder":
            """Set a side effect (error) for a method."""
            getattr(self.mock, method).side_effect = error
            if method == "validate_file_exists":
                self.mock.validate_file_exists_mock.side_effect = error
            elif method == "validate_file_object":
                self.mock.validate_file_object_mock.side_effect = error
            elif method == "validate_user_display_name":
                self.mock.validate_user_display_name_mock.side_effect = error
            return self

        def build(self) -> MagicMock:
            """Build and return the configured mock."""
            return self.mock

    @staticmethod
    def content_extractor() -> ContentExtractorBuilder:
        """Create a ContentExtractorBuilder."""
        return MockBuilderFactory.ContentExtractorBuilder()

    @staticmethod
    def validation_service() -> ValidationServiceBuilder:
        """Create a ValidationServiceBuilder."""
        return MockBuilderFactory.ValidationServiceBuilder()
