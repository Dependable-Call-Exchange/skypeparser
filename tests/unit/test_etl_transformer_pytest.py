#!/usr/bin/env python3
"""
Pytest-based tests for the ETL Transformer class.

These tests validate the transformation logic of the ETL pipeline using pytest
and the factory-based fixtures.
"""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl.transformer import Transformer
from tests.mocks import (
    MockContentExtractor,
    mock_message_handler_factory,
    mock_structured_data_extractor
)


@pytest.fixture
def mock_content_extractor():
    """Fixture for a mock content extractor."""
    return MockContentExtractor()


@pytest.fixture
def transformer(mock_content_extractor):
    """Fixture for a transformer instance with default settings and mocked dependencies."""
    return Transformer(
        parallel_processing=False,
        chunk_size=2,
        content_extractor=mock_content_extractor,
        message_handler_factory=mock_message_handler_factory,
        structured_data_extractor=mock_structured_data_extractor
    )


@pytest.fixture
def parallel_transformer(mock_content_extractor):
    """Fixture for a transformer instance with parallel processing enabled and mocked dependencies."""
    return Transformer(
        parallel_processing=True,
        max_workers=2,
        content_extractor=mock_content_extractor,
        message_handler_factory=mock_message_handler_factory,
        structured_data_extractor=mock_structured_data_extractor
    )


def test_transform_basic(transformer, basic_skype_data):
    """Test basic transformation functionality."""
    # Transform the raw data
    transformed_data = transformer.transform(basic_skype_data, 'Test User')

    # Verify the structure of the transformed data
    assert 'metadata' in transformed_data
    assert 'conversations' in transformed_data

    # Verify metadata
    metadata = transformed_data['metadata']
    assert metadata['user_display_name'] == 'Test User'
    assert metadata['total_conversations'] == 1
    assert metadata['total_messages'] == 1

    # Verify conversations
    conversations = transformed_data['conversations']
    assert 'conversation1' in conversations

    # Verify conversation details
    conv1 = conversations['conversation1']
    assert conv1['display_name'] == 'Test Conversation'
    assert len(conv1['messages']) == 1

    # Verify message details
    msg1 = conv1['messages'][0]
    assert msg1['id'] == 'message1'
    assert msg1['content'] == 'Hello world'
    assert msg1['from_id'] == 'user1'
    assert msg1['message_type'] == 'RichText'
    assert not msg1['is_edited']
    assert msg1['cleaned_content'] == 'Cleaned content'  # From mock


def test_transform_with_parallel_processing(parallel_transformer, complex_skype_data):
    """Test transformation with parallel processing enabled."""
    # Transform the raw data
    transformed_data = parallel_transformer.transform(complex_skype_data, 'Test User')

    # Verify the structure of the transformed data
    assert 'metadata' in transformed_data
    assert 'conversations' in transformed_data

    # Verify metadata
    metadata = transformed_data['metadata']
    assert metadata['user_display_name'] == 'Test User'
    assert metadata['total_conversations'] == 2
    assert metadata['total_messages'] == 5


def test_transform_with_invalid_data(transformer, invalid_skype_data):
    """Test transformation with invalid data."""
    # Transform the invalid data
    transformed_data = transformer.transform(invalid_skype_data, 'Test User')

    # Verify the structure of the transformed data
    assert 'metadata' in transformed_data
    assert 'conversations' in transformed_data

    # Verify metadata
    metadata = transformed_data['metadata']
    assert metadata['user_display_name'] == 'Test User'
    assert metadata['total_conversations'] == 0
    assert metadata['total_messages'] == 0


def test_transform_with_missing_display_name(transformer, skype_data_with_missing_display_name):
    """Test transformation with missing display name."""
    # Transform the data
    transformed_data = transformer.transform(skype_data_with_missing_display_name, 'Test User')

    # Verify that the conversation with missing display name was skipped
    conversations = transformed_data['conversations']
    assert 'conv1' not in conversations
    assert 'conv2' in conversations

    # Verify metadata
    metadata = transformed_data['metadata']
    assert metadata['total_conversations'] == 1
    assert metadata['total_messages'] == 1


def test_transform_with_complex_data(transformer, complex_skype_data):
    """Test transformation with complex data."""
    # Transform the data
    transformed_data = transformer.transform(complex_skype_data, 'Test User')

    # Verify the structure of the transformed data
    assert 'metadata' in transformed_data
    assert 'conversations' in transformed_data

    # Verify metadata
    metadata = transformed_data['metadata']
    assert metadata['user_display_name'] == 'Test User'
    assert metadata['total_conversations'] == 2
    assert metadata['total_messages'] == 5

    # Verify conversations
    conversations = transformed_data['conversations']
    assert 'conversation1' in conversations
    assert 'conversation2' in conversations

    # Verify message types
    conv1 = conversations['conversation1']
    assert len(conv1['messages']) == 3
    assert conv1['messages'][2]['message_type'] == 'RichText/HTML'

    conv2 = conversations['conversation2']
    assert len(conv2['messages']) == 2
    assert conv2['messages'][1]['message_type'] == 'RichText/Link'


def test_transform_with_various_message_types(transformer, skype_data_with_various_message_types):
    """Test transformation with various message types."""
    # Transform the data
    transformed_data = transformer.transform(skype_data_with_various_message_types, 'Test User')

    # Verify that all message types were processed correctly
    conversations = transformed_data['conversations']
    conv1 = conversations['conv1']
    messages = conv1['messages']

    assert len(messages) == 5
    assert messages[0]['message_type'] == 'RichText'
    assert messages[1]['message_type'] == 'RichText/HTML'
    assert messages[2]['message_type'] == 'RichText/Link'
    assert messages[3]['message_type'] == 'Event/Call'
    assert messages[4]['message_type'] == 'SystemMessage'


def test_transform_with_edited_messages(transformer, skype_data_with_edited_messages):
    """Test transformation with edited messages."""
    # Transform the data
    transformed_data = transformer.transform(skype_data_with_edited_messages, 'Test User')

    # Verify that edited messages are marked correctly
    conversations = transformed_data['conversations']
    conv1 = conversations['conv1']
    messages = conv1['messages']

    assert len(messages) == 2
    assert not messages[0]['is_edited']
    assert messages[1]['is_edited']


@pytest.mark.parametrize('message_type', [
    'RichText',
    'RichText/HTML',
    'RichText/Link',
    'Event/Call',
    'SystemMessage'
])
def test_transform_message_types(transformer, message_type):
    """Test transformation with different message types using parametrization."""
    from tests.factories import SkypeDataFactory, SkypeConversationFactory, SkypeMessageFactory

    # Create data with the specified message type
    data = SkypeDataFactory.build(
        conversations=[
            SkypeConversationFactory.build(
                MessageList=[
                    SkypeMessageFactory.build(
                        messagetype=message_type
                    )
                ]
            )
        ]
    )

    # Transform the data
    transformed_data = transformer.transform(data, 'Test User')

    # Verify the message type was preserved
    conversations = transformed_data['conversations']
    conv = list(conversations.values())[0]
    messages = conv['messages']

    assert len(messages) == 1
    assert messages[0]['message_type'] == message_type
    assert messages[0]['cleaned_content'] == 'Cleaned content'  # From mock