#!/usr/bin/env python3
"""
Message Type Handlers

This module provides specialized handlers for different Skype message types,
extracting structured data from messages based on their type.
"""

import json
import re
import logging
from typing import Dict, Any, Optional, List, Tuple, Callable, Type
from bs4 import BeautifulSoup

from src.utils.interfaces import MessageHandlerProtocol, MessageHandlerFactoryProtocol

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class BaseMessageHandler(MessageHandlerProtocol):
    """Base class for message handlers."""

    def can_handle(self, message_type: str) -> bool:
        """Check if this handler can process the given message type.

        Args:
            message_type: Type of message to check

        Returns:
            True if this handler can process the message type, False otherwise
        """
        return False

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from a message.

        Args:
            message: Message to extract data from

        Returns:
            Dictionary containing structured data extracted from the message
        """
        # Base implementation extracts common fields
        return {
            'id': message.get('id', ''),
            'timestamp': message.get('originalarrivaltime', ''),
            'sender_id': message.get('from', ''),
            'sender_name': message.get('displayName', ''),
            'message_type': message.get('messagetype', 'unknown'),
            'is_edited': bool(message.get('edittime', False)),
        }

class TextMessageHandler(BaseMessageHandler):
    """Handler for text messages."""

    def can_handle(self, message_type: str) -> bool:
        """Check if this handler can process the given message type.

        Args:
            message_type: Type of message to check

        Returns:
            True if this handler can process the message type, False otherwise
        """
        return message_type.lower() in ['text', 'richtext', 'richtext/html']

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from a text message.

        Args:
            message: Message to extract data from

        Returns:
            Dictionary containing structured data extracted from the message
        """
        # Get base data
        data = super().extract_structured_data(message)

        # Add text-specific data
        data['has_mentions'] = 'mentioned' in message
        data['has_emotions'] = 'emotions' in message

        return data

class MediaMessageHandler(BaseMessageHandler):
    """Handler for media messages (images, videos, files)."""

    def can_handle(self, message_type: str) -> bool:
        """Check if this handler can process the given message type.

        Args:
            message_type: Type of message to check

        Returns:
            True if this handler can process the message type, False otherwise
        """
        return message_type.lower() in ['media', 'image', 'video', 'file', 'uri']

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from a media message.

        Args:
            message: Message to extract data from

        Returns:
            Dictionary containing structured data extracted from the message
        """
        # Get base data
        data = super().extract_structured_data(message)

        # Add media-specific data
        data['attachments'] = []

        # Extract attachments
        if 'properties' in message and 'attachments' in message['properties']:
            for attachment in message['properties']['attachments']:
                data['attachments'].append({
                    'type': attachment.get('type', 'unknown'),
                    'name': attachment.get('name', ''),
                    'url': attachment.get('url', ''),
                    'content_type': attachment.get('contentType', ''),
                    'size': attachment.get('size', 0)
                })

        return data

class PollMessageHandler(BaseMessageHandler):
    """Handler for poll messages."""

    def can_handle(self, message_type: str) -> bool:
        """Check if this handler can process the given message type.

        Args:
            message_type: Type of message to check

        Returns:
            True if this handler can process the message type, False otherwise
        """
        return message_type.lower() == 'poll'

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from a poll message.

        Args:
            message: Message to extract data from

        Returns:
            Dictionary containing structured data extracted from the message
        """
        # Get base data
        data = super().extract_structured_data(message)

        # Extract poll data
        content = message.get('content', '')
        poll_data = {
            'question': '',
            'options': []
        }

        try:
            # Parse the HTML content
            soup = BeautifulSoup(content, 'html.parser')

            # Extract the poll question
            question_elem = soup.find('pollquestion')
            if question_elem:
                poll_data['question'] = question_elem.get_text(strip=True)

            # Extract poll options
            option_elems = soup.find_all('polloption')
            poll_data['options'] = [opt.get_text(strip=True) for opt in option_elems]

            # Extract from properties if available
            if 'properties' in message and 'pollData' in message['properties']:
                poll_info = message['properties']['pollData']
                poll_data.update({
                    'total_votes': poll_info.get('totalVotes', 0),
                    'is_anonymous': poll_info.get('isAnonymous', False),
                    'is_multiple_choice': poll_info.get('isMultipleChoice', False)
                })
        except Exception as e:
            logger.error(f"Error extracting poll data: {e}")

        data['poll_data'] = poll_data
        return data

class CallMessageHandler(BaseMessageHandler):
    """Handler for call messages."""

    def can_handle(self, message_type: str) -> bool:
        """Check if this handler can process the given message type.

        Args:
            message_type: Type of message to check

        Returns:
            True if this handler can process the message type, False otherwise
        """
        return message_type.lower() == 'call'

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from a call message.

        Args:
            message: Message to extract data from

        Returns:
            Dictionary containing structured data extracted from the message
        """
        # Get base data
        data = super().extract_structured_data(message)

        # Extract call data
        content = message.get('content', '')
        call_data = {
            'duration': '',
            'participants': []
        }

        try:
            # Parse the HTML content
            soup = BeautifulSoup(content, 'html.parser')

            # Extract call duration
            duration_elem = soup.find('duration')
            if duration_elem:
                call_data['duration'] = duration_elem.get_text(strip=True)

            # Extract call participants
            participant_elems = soup.find_all('participant')
            call_data['participants'] = [
                {
                    'id': p.get('id', ''),
                    'name': p.get('name', '')
                }
                for p in participant_elems
            ]

            # Extract from properties if available
            if 'properties' in message and 'callLog' in message['properties']:
                call_log = message['properties']['callLog']
                call_data.update({
                    'duration': call_log.get('duration', call_data['duration']),
                    'start_time': call_log.get('startTime', ''),
                    'end_time': call_log.get('endTime', ''),
                    'call_type': call_log.get('callType', 'unknown')
                })

                # Only update participants if not already extracted
                if not call_data['participants'] and 'participants' in call_log:
                    call_data['participants'] = call_log['participants']
        except Exception as e:
            logger.error(f"Error extracting call data: {e}")

        data['call_data'] = call_data
        return data

class LocationMessageHandler(BaseMessageHandler):
    """Handler for location messages."""

    def can_handle(self, message_type: str) -> bool:
        """Check if this handler can process the given message type.

        Args:
            message_type: Type of message to check

        Returns:
            True if this handler can process the message type, False otherwise
        """
        return message_type.lower() == 'location'

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from a location message.

        Args:
            message: Message to extract data from

        Returns:
            Dictionary containing structured data extracted from the message
        """
        # Get base data
        data = super().extract_structured_data(message)

        # Extract location data
        content = message.get('content', '')
        location_data = {
            'latitude': 0.0,
            'longitude': 0.0,
            'address': '',
            'name': ''
        }

        try:
            # Parse the HTML content
            soup = BeautifulSoup(content, 'html.parser')

            # Extract location information
            location_elem = soup.find('location')
            if location_elem:
                location_data.update({
                    'latitude': float(location_elem.get('latitude', 0.0)),
                    'longitude': float(location_elem.get('longitude', 0.0)),
                    'address': location_elem.get('address', ''),
                    'name': location_elem.get('name', '')
                })

            # Extract from properties if available
            if 'properties' in message and 'location' in message['properties']:
                location = message['properties']['location']
                location_data.update({
                    'latitude': location.get('latitude', location_data['latitude']),
                    'longitude': location.get('longitude', location_data['longitude']),
                    'address': location.get('address', location_data['address']),
                    'name': location.get('name', location_data['name'])
                })
        except Exception as e:
            logger.error(f"Error extracting location data: {e}")

        data['location_data'] = location_data
        return data

class ContactsMessageHandler(BaseMessageHandler):
    """Handler for contacts messages."""

    def can_handle(self, message_type: str) -> bool:
        """Check if this handler can process the given message type.

        Args:
            message_type: Type of message to check

        Returns:
            True if this handler can process the message type, False otherwise
        """
        return message_type.lower() == 'contacts'

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from a contacts message.

        Args:
            message: Message to extract data from

        Returns:
            Dictionary containing structured data extracted from the message
        """
        # Get base data
        data = super().extract_structured_data(message)

        # Extract contacts data
        content = message.get('content', '')
        contacts = []

        try:
            # Parse the HTML content
            soup = BeautifulSoup(content, 'html.parser')

            # Extract contact information
            contact_elems = soup.find_all('contact')
            for contact in contact_elems:
                contacts.append({
                    'name': contact.get('name', ''),
                    'phone': contact.get('phone', ''),
                    'email': contact.get('email', ''),
                    'mri': contact.get('mri', '')
                })

            # Extract from properties if available
            if 'properties' in message and 'contacts' in message['properties']:
                for contact in message['properties']['contacts']:
                    contacts.append({
                        'name': contact.get('name', ''),
                        'phone': contact.get('phone', ''),
                        'email': contact.get('email', ''),
                        'mri': contact.get('mri', '')
                    })
        except Exception as e:
            logger.error(f"Error extracting contacts data: {e}")

        data['contacts'] = contacts
        return data

class EventMessageHandler(BaseMessageHandler):
    """Handler for event messages (calls, added/removed users, etc.)."""

    def can_handle(self, message_type: str) -> bool:
        """Check if this handler can process the given message type.

        Args:
            message_type: Type of message to check

        Returns:
            True if this handler can process the message type, False otherwise
        """
        return message_type.lower() in ['event', 'systemalert', 'thread']

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from an event message.

        Args:
            message: Message to extract data from

        Returns:
            Dictionary containing structured data extracted from the message
        """
        # Get base data
        data = super().extract_structured_data(message)

        # Add event-specific data
        data['event_type'] = message.get('properties', {}).get('eventType', 'unknown')

        # Extract thread activity data if available
        if 'properties' in message and 'threadActivity' in message['properties']:
            thread_activity = message['properties']['threadActivity']
            data['thread_activity'] = {
                'type': thread_activity.get('type', 'unknown'),
                'members_added': thread_activity.get('membersAdded', []),
                'members_removed': thread_activity.get('membersRemoved', []),
                'topic_updated': thread_activity.get('topicUpdated', False),
                'history_disclosed': thread_activity.get('historyDisclosed', False)
            }

        return data

class UnknownMessageHandler(BaseMessageHandler):
    """Handler for unknown message types."""

    def can_handle(self, message_type: str) -> bool:
        """Check if this handler can process the given message type.

        Args:
            message_type: Type of message to check

        Returns:
            True if this handler can process the message type, False otherwise
        """
        return True  # Fallback handler for any message type

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from an unknown message.

        Args:
            message: Message to extract data from

        Returns:
            Dictionary containing structured data extracted from the message
        """
        # Get base data
        data = super().extract_structured_data(message)

        # Add raw properties for debugging
        if 'properties' in message:
            data['raw_properties'] = message['properties']

        return data

class SkypeMessageHandlerFactory(MessageHandlerFactoryProtocol):
    """Factory for creating message handlers based on message type."""

    def __init__(self):
        """Initialize the message handler factory."""
        # Register handlers in order of specificity
        self.handlers = [
            TextMessageHandler(),
            MediaMessageHandler(),
            PollMessageHandler(),
            CallMessageHandler(),
            LocationMessageHandler(),
            ContactsMessageHandler(),
            EventMessageHandler(),
            UnknownMessageHandler()  # Fallback handler
        ]
        logger.info("SkypeMessageHandlerFactory initialized with handlers")

    def get_handler(self, message_type: str) -> Optional[MessageHandlerProtocol]:
        """Get a handler for the specified message type.

        Args:
            message_type: Type of message to get a handler for

        Returns:
            A handler for the message type, or None if no handler is found
        """
        # Normalize message type
        normalized_type = message_type.lower() if message_type else 'unknown'

        # Find the first handler that can handle this message type
        for handler in self.handlers:
            if handler.can_handle(normalized_type):
                return handler

        # Should never reach here since UnknownMessageHandler handles everything
        logger.warning(f"No handler found for message type: {message_type}")
        return None

# Legacy function for backward compatibility
def get_handler_for_message_type(message_type: str) -> Optional[Callable[[Dict[str, Any]], Dict[str, Any]]]:
    """Legacy function to get a handler for the specified message type.

    Args:
        message_type: Type of message to get a handler for

    Returns:
        A function that can handle the message type, or None if no handler is found
    """
    factory = SkypeMessageHandlerFactory()
    handler = factory.get_handler(message_type)

    if handler:
        return handler.extract_structured_data
    return None

# Legacy function for backward compatibility
def extract_structured_data(message: Dict[str, Any]) -> Dict[str, Any]:
    """Legacy function to extract structured data from a message.

    Args:
        message: Message to extract data from

    Returns:
        Dictionary containing structured data extracted from the message
    """
    message_type = message.get('messagetype', 'unknown')
    factory = SkypeMessageHandlerFactory()
    handler = factory.get_handler(message_type)

    if handler:
        return handler.extract_structured_data(message)
    return {}