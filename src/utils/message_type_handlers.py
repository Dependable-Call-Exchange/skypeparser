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
from datetime import datetime

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
        return message_type == 'Poll'

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from a poll message.

        Args:
            message: Poll message to extract data from

        Returns:
            Dictionary containing structured poll data
        """
        # Get base data from parent class
        data = super().extract_structured_data(message)

        try:
            # Extract poll content from content or HTML content
            content = message.get('content', '')
            if not content and 'properties' in message and 'htmlContent' in message['properties']:
                content = message['properties']['htmlContent']

            # Extract poll data using BeautifulSoup
            soup = BeautifulSoup(content, 'html.parser')

            # Extract poll title
            poll_title_elem = soup.select_one('.pollTitle')
            poll_title = poll_title_elem.get_text().strip() if poll_title_elem else 'Untitled Poll'
            data['poll_title'] = poll_title

            # Extract poll options and votes
            poll_options = []
            option_elems = soup.select('.pollOption')

            for option_elem in option_elems:
                option_text_elem = option_elem.select_one('.pollOptionText')
                option_text = option_text_elem.get_text().strip() if option_text_elem else 'Untitled Option'

                # Extract vote count if available
                vote_count = 0
                vote_count_elem = option_elem.select_one('.pollOptionVoteCount')
                if vote_count_elem:
                    vote_text = vote_count_elem.get_text().strip()
                    # Extract numeric vote count (e.g., "3 votes" -> 3)
                    vote_match = re.search(r'(\d+)', vote_text)
                    if vote_match:
                        vote_count = int(vote_match.group(1))

                # Check if this option is selected by the current user
                is_selected = 'selected' in option_elem.get('class', [])

                poll_options.append({
                    'text': option_text,
                    'vote_count': vote_count,
                    'is_selected': is_selected
                })

            data['poll_options'] = poll_options

            # Extract poll metadata
            poll_meta = {}

            # Extract poll creation time
            poll_created_elem = soup.select_one('.pollCreatedTime')
            if poll_created_elem:
                poll_created_text = poll_created_elem.get_text().strip()
                # Try to extract datetime from text
                poll_meta['created_at'] = poll_created_text

            # Extract poll status (open/closed)
            poll_status_elem = soup.select_one('.pollStatus')
            if poll_status_elem:
                poll_status_text = poll_status_elem.get_text().strip()
                poll_meta['status'] = 'closed' if 'closed' in poll_status_text.lower() else 'open'

            # Extract vote visibility
            poll_visibility_elem = soup.select_one('.pollVisibility')
            if poll_visibility_elem:
                visibility_text = poll_visibility_elem.get_text().strip()
                poll_meta['vote_visibility'] = 'public' if 'public' in visibility_text.lower() else 'private'

            # Extract poll creator
            poll_creator_elem = soup.select_one('.pollCreator')
            if poll_creator_elem:
                poll_meta['creator'] = poll_creator_elem.get_text().strip()

            # Extract total vote count
            total_votes_elem = soup.select_one('.pollTotalVotes')
            if total_votes_elem:
                total_votes_text = total_votes_elem.get_text().strip()
                votes_match = re.search(r'(\d+)', total_votes_text)
                if votes_match:
                    poll_meta['total_votes'] = int(votes_match.group(1))

            data['poll_metadata'] = poll_meta

            # Extract raw poll data if available in properties
            if 'properties' in message and 'polls' in message['properties']:
                try:
                    # Store raw poll data for advanced processing
                    polls_data = message['properties']['polls']
                    if isinstance(polls_data, str):
                        polls_data = json.loads(polls_data)
                    data['raw_poll_data'] = polls_data
                except Exception as e:
                    logger.warning(f"Error parsing raw poll data: {e}")

        except Exception as e:
            logger.warning(f"Error extracting poll data: {e}")
            # Include a basic description even if detailed extraction fails
            if 'poll_title' not in data:
                data['poll_title'] = 'Poll'

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

class ScheduledCallHandler(BaseMessageHandler):
    """Handler for scheduled call invitation messages."""

    def can_handle(self, message_type: str) -> bool:
        """Check if this handler can process the given message type.

        Args:
            message_type: Type of message to check

        Returns:
            True if this handler can process the message type, False otherwise
        """
        return message_type == 'RichText/ScheduledCallInvite'

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Extract structured data from a scheduled call message.

        Args:
            message: Scheduled call message to extract data from

        Returns:
            Dictionary containing structured scheduled call data
        """
        # Get base data from parent class
        data = super().extract_structured_data(message)

        try:
            # Extract call data from content or properties
            content = message.get('content', '')

            # Try to get HTML content from properties if available
            if 'properties' in message and 'htmlContent' in message['properties']:
                html_content = message['properties']['htmlContent']
                if html_content:
                    content = html_content

            # Extract schedule data using BeautifulSoup
            soup = BeautifulSoup(content, 'html.parser')

            # Initialize scheduled call data structure
            call_data = {
                'title': 'Scheduled Call',
                'start_time': None,
                'end_time': None,
                'duration_minutes': None,
                'organizer': None,
                'participants': [],
                'description': None,
                'meeting_link': None,
                'call_id': None
            }

            # Extract call title
            title_elem = soup.select_one('.callTitle, .meetingTitle')
            if title_elem:
                call_data['title'] = title_elem.get_text().strip()

            # Extract start time and date
            start_time_elem = soup.select_one('.callStartTime, .meetingTime')
            if start_time_elem:
                start_time_text = start_time_elem.get_text().strip()
                call_data['start_time'] = start_time_text

                # Try to parse the datetime if in a standard format
                try:
                    # Common formats to try
                    for fmt in [
                        '%Y-%m-%d %H:%M:%S',
                        '%Y-%m-%dT%H:%M:%S',
                        '%Y-%m-%d %H:%M',
                        '%B %d, %Y %I:%M %p'
                    ]:
                        try:
                            dt = datetime.strptime(start_time_text, fmt)
                            call_data['start_time'] = dt.isoformat()
                            break
                        except ValueError:
                            continue
                except Exception:
                    # Keep the original string if parsing fails
                    pass

            # Extract end time or duration
            end_time_elem = soup.select_one('.callEndTime, .meetingEndTime')
            if end_time_elem:
                end_time_text = end_time_elem.get_text().strip()
                call_data['end_time'] = end_time_text

                # Try to calculate duration if we have start and end times
                if call_data['start_time'] and call_data['end_time']:
                    try:
                        # Try to parse times and calculate duration
                        start_dt = datetime.fromisoformat(call_data['start_time'])
                        end_dt = datetime.fromisoformat(call_data['end_time'])
                        duration = (end_dt - start_dt).total_seconds() / 60
                        call_data['duration_minutes'] = int(duration)
                    except Exception:
                        # Ignore if we can't calculate
                        pass

            # Try to get duration directly if available
            duration_elem = soup.select_one('.callDuration, .meetingDuration')
            if duration_elem:
                duration_text = duration_elem.get_text().strip()
                # Try to parse duration string like "1 hour" or "30 minutes"
                duration_match = re.search(r'(\d+)\s*(hour|minute|min)', duration_text, re.IGNORECASE)
                if duration_match:
                    value = int(duration_match.group(1))
                    unit = duration_match.group(2).lower()
                    if 'hour' in unit:
                        call_data['duration_minutes'] = value * 60
                    else:
                        call_data['duration_minutes'] = value

            # Extract organizer
            organizer_elem = soup.select_one('.callOrganizer, .meetingOrganizer')
            if organizer_elem:
                call_data['organizer'] = organizer_elem.get_text().strip()

            # Extract participants
            participant_elems = soup.select('.callParticipant, .meetingParticipant')
            for elem in participant_elems:
                participant = elem.get_text().strip()
                if participant and participant not in call_data['participants']:
                    call_data['participants'].append(participant)

            # Extract meeting description
            description_elem = soup.select_one('.callDescription, .meetingDescription')
            if description_elem:
                call_data['description'] = description_elem.get_text().strip()

            # Extract meeting link
            link_elem = soup.select_one('a[href^="https://teams.microsoft.com/"], a[href^="https://meet.google.com/"], a[href^="https://zoom.us/"]')
            if link_elem and 'href' in link_elem.attrs:
                call_data['meeting_link'] = link_elem['href']

            # Try to extract call ID or meeting ID
            call_id_elem = soup.select_one('.callId, .meetingId')
            if call_id_elem:
                call_data['call_id'] = call_id_elem.get_text().strip()
            else:
                # Try to extract from the meeting link
                if call_data['meeting_link']:
                    # Teams meeting ID format
                    teams_match = re.search(r'meetingId=([a-zA-Z0-9_-]+)', call_data['meeting_link'])
                    if teams_match:
                        call_data['call_id'] = teams_match.group(1)

                    # Zoom meeting ID format
                    zoom_match = re.search(r'/j/(\d+)', call_data['meeting_link'])
                    if zoom_match:
                        call_data['call_id'] = zoom_match.group(1)

                    # Google Meet format
                    meet_match = re.search(r'meet.google.com/([a-zA-Z0-9-]+)', call_data['meeting_link'])
                    if meet_match:
                        call_data['call_id'] = meet_match.group(1)

            # Extract from properties if available
            if 'properties' in message:
                props = message['properties']

                # Look for scheduled call data in various property formats
                if 'scheduledCallData' in props:
                    try:
                        call_props = props['scheduledCallData']
                        if isinstance(call_props, str):
                            call_props = json.loads(call_props)

                        # Update with more precise data from properties
                        if 'title' in call_props:
                            call_data['title'] = call_props['title']
                        if 'startTime' in call_props:
                            call_data['start_time'] = call_props['startTime']
                        if 'endTime' in call_props:
                            call_data['end_time'] = call_props['endTime']
                        if 'organizer' in call_props:
                            call_data['organizer'] = call_props['organizer']
                        if 'meetingLink' in call_props:
                            call_data['meeting_link'] = call_props['meetingLink']
                        if 'callId' in call_props:
                            call_data['call_id'] = call_props['callId']
                    except Exception as e:
                        logger.warning(f"Error parsing scheduled call properties: {e}")

                # Store raw properties for debugging or future enhancements
                data['raw_call_properties'] = props

            # Store the extracted call data
            data['scheduled_call'] = call_data

        except Exception as e:
            logger.warning(f"Error extracting scheduled call data: {e}")
            # Include a basic description even if detailed extraction fails
            data['scheduled_call'] = {'title': 'Scheduled Call'}

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
            ScheduledCallHandler(),  # Add the new handler
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