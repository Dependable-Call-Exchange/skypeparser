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

class EventMessageHandler(BaseMessageHandler):
    """Handler for event messages (calls, added/removed users, etc.)."""

    def can_handle(self, message_type: str) -> bool:
        """Check if this handler can process the given message type.

        Args:
            message_type: Type of message to check

        Returns:
            True if this handler can process the message type, False otherwise
        """
        return message_type.lower() in ['event', 'systemalert', 'call', 'thread']

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

        # Extract call data if available
        if 'properties' in message and 'callLog' in message['properties']:
            call_log = message['properties']['callLog']
            data['call_data'] = {
                'duration': call_log.get('duration', 0),
                'participants': call_log.get('participants', []),
                'start_time': call_log.get('startTime', ''),
                'end_time': call_log.get('endTime', '')
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

class PollHandler(MessageTypeHandler):
    """Handler for Poll message type."""

    @staticmethod
    def extract_data(message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract poll data from a message.

        Args:
            message (Dict[str, Any]): The message to extract data from

        Returns:
            Dict[str, Any]: Extracted poll data including question and options
        """
        content = message.get('content', '')
        result = {
            'poll_question': '',
            'poll_options': []
        }

        try:
            # Parse the HTML content
            soup = BeautifulSoup(content, 'html.parser')

            # Extract the poll question
            question_elem = soup.find('pollquestion')
            if question_elem:
                result['poll_question'] = question_elem.get_text(strip=True)

            # Extract poll options
            option_elems = soup.find_all('polloption')
            result['poll_options'] = [opt.get_text(strip=True) for opt in option_elems]

            logger.info(f"Extracted poll data: {result}")
        except Exception as e:
            logger.error(f"Error extracting poll data: {e}")

        return result


class CallHandler(MessageTypeHandler):
    """Handler for Event/Call message type."""

    @staticmethod
    def extract_data(message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract call data from a message.

        Args:
            message (Dict[str, Any]): The message to extract data from

        Returns:
            Dict[str, Any]: Extracted call data including duration and participants
        """
        content = message.get('content', '')
        result = {
            'call_duration': '',
            'call_participants': []
        }

        try:
            # Parse the HTML content
            soup = BeautifulSoup(content, 'html.parser')

            # Extract call duration
            duration_elem = soup.find('duration')
            if duration_elem:
                result['call_duration'] = duration_elem.get_text(strip=True)

            # Extract call participants
            participant_elems = soup.find_all('participant')
            result['call_participants'] = [
                {
                    'id': p.get('id', ''),
                    'name': p.get('name', '')
                }
                for p in participant_elems
            ]

            logger.info(f"Extracted call data: {result}")
        except Exception as e:
            logger.error(f"Error extracting call data: {e}")

        return result


class LocationHandler(MessageTypeHandler):
    """Handler for RichText/Location message type."""

    @staticmethod
    def extract_data(message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract location data from a message.

        Args:
            message (Dict[str, Any]): The message to extract data from

        Returns:
            Dict[str, Any]: Extracted location data including coordinates and address
        """
        content = message.get('content', '')
        result = {
            'location_latitude': '',
            'location_longitude': '',
            'location_address': ''
        }

        try:
            # Parse the HTML content
            soup = BeautifulSoup(content, 'html.parser')

            # Extract location coordinates
            location_elem = soup.find('location')
            if location_elem:
                result['location_latitude'] = location_elem.get('latitude', '')
                result['location_longitude'] = location_elem.get('longitude', '')

            # Extract location address
            address_elem = soup.find('address')
            if address_elem:
                result['location_address'] = address_elem.get_text(strip=True)

            logger.info(f"Extracted location data: {result}")
        except Exception as e:
            logger.error(f"Error extracting location data: {e}")

        return result


class ContactsHandler(MessageTypeHandler):
    """Handler for RichText/Contacts message type."""

    @staticmethod
    def extract_data(message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract contacts data from a message.

        Args:
            message (Dict[str, Any]): The message to extract data from

        Returns:
            Dict[str, Any]: Extracted contacts data
        """
        content = message.get('content', '')
        result = {
            'contacts': []
        }

        try:
            # Parse the HTML content
            soup = BeautifulSoup(content, 'html.parser')

            # Extract contacts
            contact_elems = soup.find_all('contact')
            result['contacts'] = [
                {
                    'name': c.get('name', ''),
                    'phone': c.get('phone', ''),
                    'email': c.get('email', '')
                }
                for c in contact_elems
            ]

            logger.info(f"Extracted contacts data: {result}")
        except Exception as e:
            logger.error(f"Error extracting contacts data: {e}")

        return result


class MediaHandler(MessageTypeHandler):
    """Handler for media message types (audio, video, files)."""

    @staticmethod
    def extract_data(message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract media data from a message.

        Args:
            message (Dict[str, Any]): The message to extract data from

        Returns:
            Dict[str, Any]: Extracted media data including file name, size, type, and additional metadata
        """
        content = message.get('content', '')
        message_type = message.get('messagetype', '')
        result = {
            'media_filename': '',
            'media_filesize': '',
            'media_filesize_formatted': '',
            'media_filetype': '',
            'media_url': '',
            'media_thumbnail_url': '',
            'media_width': '',
            'media_height': '',
            'media_duration': '',
            'media_description': ''
        }

        try:
            # Parse the HTML content
            soup = BeautifulSoup(content, 'html.parser')

            # Extract media information
            media_elem = soup.find(['uriobject', 'file', 'img', 'video', 'audio'])
            if media_elem:
                # Basic file information
                result['media_filename'] = media_elem.get('filename', '') or media_elem.get('name', '')
                result['media_filesize'] = media_elem.get('filesize', '')

                # Format file size if available
                if result['media_filesize']:
                    try:
                        size_bytes = int(result['media_filesize'])
                        if size_bytes < 1024:
                            result['media_filesize_formatted'] = f"{size_bytes} B"
                        elif size_bytes < 1024 * 1024:
                            result['media_filesize_formatted'] = f"{size_bytes / 1024:.1f} KB"
                        elif size_bytes < 1024 * 1024 * 1024:
                            result['media_filesize_formatted'] = f"{size_bytes / (1024 * 1024):.1f} MB"
                        else:
                            result['media_filesize_formatted'] = f"{size_bytes / (1024 * 1024 * 1024):.1f} GB"
                    except (ValueError, TypeError):
                        pass

                result['media_filetype'] = media_elem.get('filetype', '') or media_elem.get('type', '')
                result['media_url'] = media_elem.get('url', '') or media_elem.get('src', '')

                # Additional metadata
                result['media_thumbnail_url'] = media_elem.get('thumbnail_url', '') or media_elem.get('poster', '')
                result['media_width'] = media_elem.get('width', '')
                result['media_height'] = media_elem.get('height', '')
                result['media_duration'] = media_elem.get('duration', '')
                result['media_description'] = media_elem.get('description', '') or media_elem.get('alt', '')

            # Handle specific media types
            if 'Media_Album' in message_type:
                # Extract album information
                album_items = []
                for item in soup.find_all(['img', 'uriobject']):
                    album_items.append({
                        'url': item.get('url', '') or item.get('src', ''),
                        'thumbnail': item.get('thumbnail_url', '') or item.get('thumbnail', ''),
                        'width': item.get('width', ''),
                        'height': item.get('height', '')
                    })
                result['media_album_items'] = album_items
                result['media_album_count'] = len(album_items)

            logger.info(f"Extracted media data for {message_type}: {result}")
        except Exception as e:
            logger.error(f"Error extracting media data: {e}")

        return result


class MediaCardHandler(MessageTypeHandler):
    """Handler for RichText/Media_Card message type."""

    @staticmethod
    def extract_data(message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract media card data from a message.

        Args:
            message (Dict[str, Any]): The message to extract data from

        Returns:
            Dict[str, Any]: Extracted media card data
        """
        content = message.get('content', '')
        result = {
            'card_title': '',
            'card_description': '',
            'card_url': '',
            'card_thumbnail_url': '',
            'card_provider': ''
        }

        try:
            # Parse the HTML content
            soup = BeautifulSoup(content, 'html.parser')

            # Extract card information
            card_elem = soup.find('card')
            if card_elem:
                result['card_title'] = card_elem.get('title', '')
                result['card_description'] = card_elem.get('description', '')
                result['card_url'] = card_elem.get('url', '')
                result['card_thumbnail_url'] = card_elem.get('thumbnail_url', '')
                result['card_provider'] = card_elem.get('provider', '')

            # Try to extract from other elements if card element not found
            if not result['card_title']:
                title_elem = soup.find('title')
                if title_elem:
                    result['card_title'] = title_elem.get_text(strip=True)

            if not result['card_description']:
                desc_elem = soup.find('description')
                if desc_elem:
                    result['card_description'] = desc_elem.get_text(strip=True)

            logger.info(f"Extracted media card data: {result}")
        except Exception as e:
            logger.error(f"Error extracting media card data: {e}")

        return result


class PopCardHandler(MessageTypeHandler):
    """Handler for PopCard message type."""

    @staticmethod
    def extract_data(message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract pop card data from a message.

        Args:
            message (Dict[str, Any]): The message to extract data from

        Returns:
            Dict[str, Any]: Extracted pop card data
        """
        content = message.get('content', '')
        result = {
            'popcard_title': '',
            'popcard_content': '',
            'popcard_action': '',
            'popcard_type': ''
        }

        try:
            # Parse the HTML content
            soup = BeautifulSoup(content, 'html.parser')

            # Extract pop card information
            popcard_elem = soup.find('popcard')
            if popcard_elem:
                result['popcard_title'] = popcard_elem.get('title', '')
                result['popcard_type'] = popcard_elem.get('type', '')
                result['popcard_action'] = popcard_elem.get('action', '')

                # Extract content
                content_elem = popcard_elem.find('content')
                if content_elem:
                    result['popcard_content'] = content_elem.get_text(strip=True)

            logger.info(f"Extracted pop card data: {result}")
        except Exception as e:
            logger.error(f"Error extracting pop card data: {e}")

        return result


class TranslationHandler(MessageTypeHandler):
    """Handler for Translation message type."""

    @staticmethod
    def extract_data(message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract translation data from a message.

        Args:
            message (Dict[str, Any]): The message to extract data from

        Returns:
            Dict[str, Any]: Extracted translation data
        """
        content = message.get('content', '')
        result = {
            'translation_text': '',
            'translation_from_language': '',
            'translation_to_language': '',
            'translation_original_text': ''
        }

        try:
            # Parse the HTML content
            soup = BeautifulSoup(content, 'html.parser')

            # Extract translation information
            translation_elem = soup.find('translation')
            if translation_elem:
                result['translation_from_language'] = translation_elem.get('from', '')
                result['translation_to_language'] = translation_elem.get('to', '')
                result['translation_text'] = translation_elem.get_text(strip=True)

            # Extract original text
            original_elem = soup.find('original')
            if original_elem:
                result['translation_original_text'] = original_elem.get_text(strip=True)

            logger.info(f"Extracted translation data: {result}")
        except Exception as e:
            logger.error(f"Error extracting translation data: {e}")

        return result


class ThreadActivityHandler(MessageTypeHandler):
    """Handler for ThreadActivity/* message types."""

    @staticmethod
    def extract_data(message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract thread activity data from a message.

        Args:
            message (Dict[str, Any]): The message to extract data from

        Returns:
            Dict[str, Any]: Extracted thread activity data
        """
        content = message.get('content', '')
        message_type = message.get('messagetype', '')
        result = {
            'activity_type': '',
            'activity_members': [],
            'activity_value': '',
            'activity_initiator': ''
        }

        # Extract activity type from message type
        if '/' in message_type:
            result['activity_type'] = message_type.split('/', 1)[1]

        try:
            # Parse the HTML content
            soup = BeautifulSoup(content, 'html.parser')

            # Extract members for AddMember and DeleteMember
            if result['activity_type'] in ['AddMember', 'DeleteMember']:
                member_elems = soup.find_all('member')
                result['activity_members'] = [
                    {
                        'id': m.get('id', ''),
                        'name': m.get('name', '')
                    }
                    for m in member_elems
                ]

            # Extract value for updates (TopicUpdate, PictureUpdate, etc.)
            if 'Update' in result['activity_type']:
                value_elem = soup.find('value')
                if value_elem:
                    result['activity_value'] = value_elem.get_text(strip=True)

            # Extract initiator
            initiator_elem = soup.find('initiator')
            if initiator_elem:
                result['activity_initiator'] = initiator_elem.get('name', '') or initiator_elem.get('id', '')

            logger.info(f"Extracted thread activity data for {result['activity_type']}: {result}")
        except Exception as e:
            logger.error(f"Error extracting thread activity data: {e}")

        return result


class ScheduledCallHandler(MessageTypeHandler):
    """Handler for RichText/ScheduledCallInvite message type."""

    @staticmethod
    def extract_data(message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract scheduled call data from a message.

        Args:
            message (Dict[str, Any]): The message to extract data from

        Returns:
            Dict[str, Any]: Extracted scheduled call data including time and title
        """
        content = message.get('content', '')
        result = {
            'scheduled_call_title': '',
            'scheduled_call_time': '',
            'scheduled_call_duration': ''
        }

        try:
            # Parse the HTML content
            soup = BeautifulSoup(content, 'html.parser')

            # Extract scheduled call information
            call_elem = soup.find('scheduledcall')
            if call_elem:
                result['scheduled_call_title'] = call_elem.get('title', '')
                result['scheduled_call_time'] = call_elem.get('time', '')
                result['scheduled_call_duration'] = call_elem.get('duration', '')

            logger.info(f"Extracted scheduled call data: {result}")
        except Exception as e:
            logger.error(f"Error extracting scheduled call data: {e}")

        return result


# Map message types to their handlers
MESSAGE_TYPE_HANDLERS = {
    'Poll': PollHandler,
    'Event/Call': CallHandler,
    'RichText/Location': LocationHandler,
    'RichText/Contacts': ContactsHandler,
    'RichText/Media_AudioMsg': MediaHandler,
    'RichText/Media_Video': MediaHandler,
    'RichText/Media_GenericFile': MediaHandler,
    'RichText/UriObject': MediaHandler,
    'RichText/Media_Album': MediaHandler,
    'RichText/Media_Card': MediaCardHandler,
    'RichText/ScheduledCallInvite': ScheduledCallHandler,
    'PopCard': PopCardHandler,
    'Translation': TranslationHandler,
    'ThreadActivity/AddMember': ThreadActivityHandler,
    'ThreadActivity/DeleteMember': ThreadActivityHandler,
    'ThreadActivity/TopicUpdate': ThreadActivityHandler,
    'ThreadActivity/PictureUpdate': ThreadActivityHandler,
    'ThreadActivity/RoleUpdate': ThreadActivityHandler,
    'ThreadActivity/JoiningEnabledUpdate': ThreadActivityHandler,
    'ThreadActivity/HistoryDisclosedUpdate': ThreadActivityHandler,
    'ThreadActivity/E2EEHandshakeComplete': ThreadActivityHandler,
    'ThreadActivity/E2EEHandshakeAccept': ThreadActivityHandler,
    'ThreadActivity/E2EEHandshakeReject': ThreadActivityHandler,
    'ThreadActivity/E2EEHandshakeInvite': ThreadActivityHandler
}


def get_handler_for_message_type(message_type: str) -> Optional[MessageTypeHandler]:
    """
    Get the appropriate handler for a message type.

    Args:
        message_type (str): The message type

    Returns:
        Optional[MessageTypeHandler]: The handler for the message type, or None if not found
    """
    return MESSAGE_TYPE_HANDLERS.get(message_type)


def extract_structured_data(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Extract structured data from a message based on its type.

    Args:
        message (Dict[str, Any]): The message to extract data from

    Returns:
        Dict[str, Any]: Extracted structured data
    """
    message_type = message.get('messagetype', '')

    handler = get_handler_for_message_type(message_type)
    if handler:
        return handler.extract_data(message)

    return {}