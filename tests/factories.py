"""
Test Factories for Skype Parser

This module provides factory classes for generating test data for the Skype Parser.
It uses factory_boy and faker to create realistic and varied test data while
maintaining consistency and reducing duplication in tests.
"""

import factory
from factory.faker import Faker
from datetime import datetime, timedelta
import random
from faker import Faker as RealFaker

# Set a fixed seed for reproducible tests
faker = RealFaker()
faker.seed_instance(12345)
factory.Faker._DEFAULT_LOCALE = 'en_US'

class SkypeMessageFactory(factory.Factory):
    """Factory for generating Skype message data."""

    class Meta:
        model = dict

    id = factory.Sequence(lambda n: f'message{n}')
    originalarrivaltime = factory.LazyFunction(lambda: datetime.now().isoformat())
    from_id = factory.Sequence(lambda n: f'user{n % 3 + 1}')
    from_name = factory.Sequence(lambda n: f'User {n % 3 + 1}')
    content = factory.Faker('text', max_nb_chars=100)
    messagetype = 'RichText'
    edittime = None

    class Params:
        """Parameters for creating different types of messages."""
        edited = factory.Trait(
            edittime=factory.LazyFunction(
                lambda: (datetime.now() + timedelta(minutes=5)).isoformat()
            )
        )
        html = factory.Trait(
            messagetype='RichText/HTML',
            content='<b>Bold text</b> and <i>italic text</i>'
        )
        link = factory.Trait(
            messagetype='RichText/Link',
            content='https://example.com'
        )
        call = factory.Trait(
            messagetype='Event/Call',
            content='Call started'
        )
        system = factory.Trait(
            messagetype='SystemMessage',
            content='User joined the conversation'
        )

class SkypeConversationFactory(factory.Factory):
    """Factory for generating Skype conversation data."""

    class Meta:
        model = dict

    id = factory.Sequence(lambda n: f'conversation{n}')
    displayName = factory.Sequence(lambda n: f'Test Conversation {n}')

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
            self['MessageList'] = [
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
            self['MessageList'] = [SkypeMessageFactory() for _ in range(extracted)]

class SkypeDataFactory(factory.Factory):
    """Factory for generating complete Skype export data."""

    class Meta:
        model = dict

    userId = factory.Sequence(lambda n: f'test-user-{n}')
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
    userId='test-user-id',
    exportDate='2023-01-01T12:00:00Z',
    conversations=[
        SkypeConversationFactory.build(
            id='conversation1',
            displayName='Test Conversation',
            MessageList=[
                SkypeMessageFactory.build(
                    id='message1',
                    originalarrivaltime='2023-01-01T12:00:00Z',
                    from_id='user1',
                    from_name='User 1',
                    content='Hello world',
                    messagetype='RichText'
                )
            ]
        )
    ]
)

COMPLEX_SKYPE_DATA = SkypeDataFactory.build(
    userId='test-user-id',
    exportDate='2023-01-01T12:00:00Z',
    conversations=[
        SkypeConversationFactory.build(
            id='conversation1',
            displayName='Test Conversation 1',
            MessageList=[
                SkypeMessageFactory.build(
                    id='msg1',
                    originalarrivaltime='2023-01-01T12:30:00Z',
                    from_id='user1',
                    from_name='User 1',
                    content='Hello, world!'
                ),
                SkypeMessageFactory.build(
                    id='msg2',
                    originalarrivaltime='2023-01-01T12:35:00Z',
                    from_id='user2',
                    from_name='User 2',
                    content='Hi there!'
                ),
                SkypeMessageFactory.build(
                    id='msg3',
                    originalarrivaltime='2023-01-01T12:40:00Z',
                    from_id='user1',
                    from_name='User 1',
                    content='<b>Bold text</b> and <i>italic text</i>',
                    messagetype='RichText/HTML'
                )
            ]
        ),
        SkypeConversationFactory.build(
            id='conversation2',
            displayName='Test Conversation 2',
            MessageList=[
                SkypeMessageFactory.build(
                    id='msg4',
                    originalarrivaltime='2023-01-01T13:00:00Z',
                    from_id='user3',
                    from_name='User 3',
                    content='Message with emoji 😊'
                ),
                SkypeMessageFactory.build(
                    id='msg5',
                    originalarrivaltime='2023-01-01T13:05:00Z',
                    from_id='user1',
                    from_name='User 1',
                    content='https://example.com',
                    messagetype='RichText/Link'
                )
            ]
        ),
        # Conversation with None displayName (should be skipped during processing)
        SkypeConversationFactory.build(
            id='conversation3',
            displayName=None,
            MessageList=[
                SkypeMessageFactory.build(
                    id='msg6',
                    originalarrivaltime='2023-01-01T14:00:00Z',
                    from_id='user4',
                    from_name='User 4',
                    content='This message is in a conversation with no display name'
                )
            ]
        )
    ]
)

INVALID_SKYPE_DATA = SkypeDataFactory.build(
    userId=None,  # Missing user ID
    exportDate='invalid-date',  # Invalid date format
    conversations=[]  # Empty conversations list
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
            conversation_id=factory.Sequence(lambda n: f'conv{n}'),
            display_name=factory.Faker('sentence', nb_words=3),
            export_id=factory.SelfAttribute('id')
        )

        message = factory.Trait(
            message_id=factory.Sequence(lambda n: f'msg{n}'),
            conversation_id=factory.Sequence(lambda n: n % 5),  # Link to a conversation
            content=factory.Faker('paragraph'),
            timestamp=factory.LazyFunction(lambda: datetime.now().isoformat()),
            message_type='RichText'
        )