#!/usr/bin/env python3
"""
Structured Data Extractor

This module provides an implementation of the StructuredDataExtractorProtocol
that leverages existing message handler functionality to extract structured data.
"""

import logging
from typing import Dict, Any, List, Optional

from src.utils.interfaces import StructuredDataExtractorProtocol
from src.utils.message_type_handlers import BaseMessageHandler

# Set up logging
logger = logging.getLogger(__name__)

class StructuredDataExtractor(StructuredDataExtractorProtocol):
    """
    Implementation of StructuredDataExtractorProtocol that leverages existing
    BaseMessageHandler functionality to extract structured data from messages.

    This class serves as a bridge between the ETL pipeline and the message
    handler system, allowing the Transformer to use the existing message
    extraction logic without direct dependency on the message handlers.
    """

    def __init__(self):
        """Initialize the structured data extractor."""
        # Create a base message handler for basic extraction
        self.base_handler = BaseMessageHandler()
        logger.debug("Initialized StructuredDataExtractor")

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract structured data from a message.

        Args:
            message: The raw message data

        Returns:
            Structured data extracted from the message
        """
        logger.debug(f"Extracting structured data from message: {message.get('id', 'unknown')}")

        # Use the base message handler to extract common fields
        structured_data = self.base_handler.extract_structured_data(message)

        # Extract additional content if available
        if 'content' in message:
            structured_data['content'] = message['content']

        # Extract HTML content if available
        if 'content_html' in message:
            structured_data['content_html'] = message['content_html']

        # Extract additional metadata from properties
        if 'properties' in message and isinstance(message['properties'], dict):
            properties = message['properties']
            structured_data['properties'] = properties

            # Extract specific properties if available
            if 'emotions' in properties:
                structured_data['has_emotions'] = True
                structured_data['emotions'] = properties['emotions']

            if 'mentioned' in properties:
                structured_data['has_mentions'] = True
                structured_data['mentions'] = properties['mentioned']

        logger.debug(f"Extracted structured data: {structured_data}")
        return structured_data

    def extract(self, raw_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract structured data from raw Skype export data.

        This method handles the new Skype export format with a 'messages' array
        containing user, export date, and conversations info.

        Args:
            raw_data: The raw data from the Skype export

        Returns:
            Structured data containing user_id, export_date, and conversations
        """
        logger.debug(f"Extracting structured data from raw data with keys: {list(raw_data.keys())}")

        # Default structure
        structured_data = {
            "user_id": "",
            "export_date": "",
            "conversations": []
        }

        # Check for new format with messages array
        if "messages" in raw_data and isinstance(raw_data["messages"], list) and len(raw_data["messages"]) > 0:
            message_obj = raw_data["messages"][0]

            # Extract user ID and export date
            structured_data["user_id"] = message_obj.get("userId", "")
            structured_data["export_date"] = message_obj.get("exportDate", "")

            # Extract conversations
            if "conversations" in message_obj:
                structured_data["conversations"] = message_obj.get("conversations", [])
                logger.debug(f"Found {len(structured_data['conversations'])} conversations in structured data")
            else:
                logger.warning("No conversations found in the message object")
        else:
            # Try to handle the data directly
            structured_data["user_id"] = raw_data.get("userId", raw_data.get("user_id", ""))
            structured_data["export_date"] = raw_data.get("exportDate", raw_data.get("export_date", ""))
            structured_data["conversations"] = raw_data.get("conversations", [])
            logger.debug(f"Using direct data extraction, found {len(structured_data['conversations'])} conversations")

        logger.debug(f"Extracted structured data with user_id: {structured_data['user_id']}, "
                     f"export_date: {structured_data['export_date']}, "
                     f"conversations count: {len(structured_data['conversations'])}")

        return structured_data