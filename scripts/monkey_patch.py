#!/usr/bin/env python3
"""
Monkey Patch for Skype Parser

This script applies necessary monkey patches to fix compatibility issues
in the core parser. It must be imported before any other imports that would
use the patched classes.
"""

import logging
import os
import sys
import traceback
from typing import Any, Dict, Optional

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Setup basic logging for monkey patch
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("monkey_patch")

# Apply patches BEFORE importing ContentExtractor or any modules that use it
logger.info("Applying monkey patches...")

try:
    # Import the original class and function we need to patch
    from src.parser.content_extractor import (
        ContentExtractor,
        format_content_with_markup,
    )

    # Verify the ContentExtractor class exists
    logger.debug(f"Successfully imported ContentExtractor class: {ContentExtractor}")
    logger.debug(
        f"Successfully imported format_content_with_markup function: {format_content_with_markup}"
    )

    # Check if the function already exists before patching
    has_method = hasattr(ContentExtractor, "format_content_with_markup")
    logger.debug(
        f"ContentExtractor already has format_content_with_markup method: {has_method}"
    )

    # Define the method to add to ContentExtractor
    def patched_format_content_with_markup(
        self, content: str, structured_data: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Format message content with markup for better readability.

        This method is monkey patched onto the ContentExtractor class to
        provide compatibility with core_parser.py.

        Args:
            content: Message content to format
            structured_data: Optional structured data extracted from the message

        Returns:
            Formatted message content with markup
        """
        try:
            # Call the standalone function from the same module
            logger.debug(
                f"Patched format_content_with_markup called with content: {content[:50]}..."
            )

            # Ensure content is a string
            if not isinstance(content, str):
                logger.warning(
                    f"Content is not a string but {type(content)}, converting to string"
                )
                content = str(content)

            # Call the standalone function
            result = format_content_with_markup(content)
            logger.debug(f"Patched format_content_with_markup completed successfully")
            return result
        except Exception as e:
            logger.error(f"Error in patched format_content_with_markup: {e}")
            logger.error(traceback.format_exc())
            # Return the original content if the formatting fails
            return content

    # Apply the monkey patch
    if not has_method:
        ContentExtractor.format_content_with_markup = patched_format_content_with_markup
        logger.info("Successfully patched ContentExtractor.format_content_with_markup")
    else:
        logger.info(
            "ContentExtractor.format_content_with_markup already exists, no need to patch"
        )

    # Verify the patch was applied
    if hasattr(ContentExtractor, "format_content_with_markup"):
        logger.info("Verified: ContentExtractor has format_content_with_markup method")

        # Create an instance and test the method to ensure it works
        try:
            test_instance = ContentExtractor()
            test_result = test_instance.format_content_with_markup("Test content")
            logger.info(
                f"Test call to format_content_with_markup successful: {test_result[:20]}..."
            )
        except Exception as e:
            logger.error(f"Test call to format_content_with_markup failed: {e}")
            logger.error(traceback.format_exc())
    else:
        logger.error(
            "Verification failed: ContentExtractor still does not have format_content_with_markup method"
        )

except ImportError as e:
    logger.error(f"Import error applying monkey patch: {e}")
    logger.error(traceback.format_exc())
    raise
except Exception as e:
    logger.error(f"Error applying monkey patch: {e}")
    logger.error(traceback.format_exc())
    raise

logger.info("Monkey patching completed")
