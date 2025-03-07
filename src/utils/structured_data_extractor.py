#!/usr/bin/env python3
"""
Structured Data Extractor

This module provides an implementation of the StructuredDataExtractorProtocol
that leverages existing message handler functionality to extract structured data.
"""

import logging
from typing import Dict, Any

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