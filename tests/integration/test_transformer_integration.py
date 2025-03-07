#!/usr/bin/env python3
"""
Test Transformer Component

This script tests the Transformer component of the ETL pipeline with sample extracted data.
It verifies that the transformer can properly process different message types and
demonstrates the dependency injection integration for message handlers.
"""

import os
import sys
import logging
import json
from typing import Dict, Any, List, Union
import re
from bs4 import BeautifulSoup

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Import required components
from src.db.etl.transformer import Transformer
from src.db.etl.extractor import Extractor
from src.db.etl.context import ETLContext
from src.utils.di import get_service_provider, get_service
from src.utils.interfaces import (
    TransformerProtocol,
    ContentExtractorProtocol,
    MessageHandlerFactoryProtocol,
    StructuredDataExtractorProtocol,
    FileHandlerProtocol
)
from src.utils.service_registry import register_core_services, register_all_services
from src.utils.file_handler import FileHandler
from src.utils.message_type_handlers import SkypeMessageHandlerFactory
from src.parser.content_extractor import ContentExtractor
from src.utils.structured_data_extractor import StructuredDataExtractor

# Create an adapter class that implements ContentExtractorProtocol
class ContentExtractorAdapter(ContentExtractorProtocol):
    """
    Adapter class that implements ContentExtractorProtocol and uses the existing ContentExtractor functionality.
    This bridges the gap between the protocol interface and the actual implementation.
    """

    def __init__(self):
        """Initialize the content extractor adapter."""
        self.extractor = ContentExtractor()
        logger.debug("Initialized ContentExtractorAdapter")

    def extract_content(self, message: Dict[str, Any]) -> str:
        """
        Extract cleaned content from a message.

        Args:
            message: The message data

        Returns:
            Cleaned content as a string
        """
        content = message.get('content', '')
        if not content:
            return ''

        # Extract structured data
        structured_data = self.extractor.extract_all(content)

        # Clean the content by removing HTML tags
        if BeautifulSoup:
            try:
                soup = BeautifulSoup(content, 'html.parser')
                cleaned_content = soup.get_text(separator=' ', strip=True)
            except Exception as e:
                logger.warning(f"Error cleaning content with BeautifulSoup: {e}")
                # Fall back to simple HTML tag removal
                cleaned_content = re.sub(r'<[^>]+>', ' ', content)
                cleaned_content = re.sub(r'\s+', ' ', cleaned_content).strip()
        else:
            # Simple HTML tag removal
            cleaned_content = re.sub(r'<[^>]+>', ' ', content)
            cleaned_content = re.sub(r'\s+', ' ', cleaned_content).strip()

        return cleaned_content

    def extract_html_content(self, message: Dict[str, Any]) -> str:
        """
        Extract HTML content from a message.

        Args:
            message: The message data

        Returns:
            HTML content as a string
        """
        return message.get('content', '')

    def extract_cleaned_content(self, content_html: str) -> str:
        """
        Extract cleaned content from HTML content.

        Args:
            content_html: The HTML content

        Returns:
            Cleaned content as a string
        """
        if not content_html:
            return ''

        # Clean the content by removing HTML tags
        if BeautifulSoup:
            try:
                soup = BeautifulSoup(content_html, 'html.parser')
                cleaned_content = soup.get_text(separator=' ', strip=True)
            except Exception as e:
                logger.warning(f"Error cleaning content with BeautifulSoup: {e}")
                # Fall back to simple HTML tag removal
                cleaned_content = re.sub(r'<[^>]+>', ' ', content_html)
                cleaned_content = re.sub(r'\s+', ' ', cleaned_content).strip()
        else:
            # Simple HTML tag removal
            cleaned_content = re.sub(r'<[^>]+>', ' ', content_html)
            cleaned_content = re.sub(r'\s+', ' ', cleaned_content).strip()

        return cleaned_content

def load_config(config_path='config/config.json'):
    """Load configuration from a JSON file."""
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        logger.error(f"Error: Configuration file not found at {config_path}")
        return None
    except json.JSONDecodeError:
        logger.error(f"Error: Invalid JSON in configuration file {config_path}")
        return None
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        return None

def get_db_config(config):
    """Extract database configuration from the config dictionary."""
    if not config or 'database' not in config:
        return None

    db_config = config['database']
    return {
        'dbname': db_config.get('dbname', 'skype_archive'),
        'user': db_config.get('user', 'postgres'),
        'password': db_config.get('password', ''),
        'host': db_config.get('host', 'localhost'),
        'port': db_config.get('port', 5432)
    }

def print_data_summary(data: Any):
    """Print a summary of the extracted data."""
    logger.info(f"Data type: {type(data)}")

    if isinstance(data, dict):
        logger.info(f"Data keys: {', '.join(data.keys())}")

        # Check for user information
        if 'user' in data:
            user = data['user']
            logger.info(f"User ID: {user.get('id', 'N/A')}, Display Name: {user.get('display_name', 'N/A')}")

        # Check for conversations
        if 'conversations' in data:
            conversations = data['conversations']
            if isinstance(conversations, dict):
                logger.info(f"Transformed {len(conversations)} conversations")

                # Print sample of message types
                message_types = set()
                message_count = 0

                for conv_id, conv_data in conversations.items():
                    if 'messages' in conv_data:
                        messages = conv_data['messages']
                        message_count += len(messages)

                        # Collect message types (sample first 100 messages)
                        for msg in messages[:100]:
                            if 'message_type' in msg:
                                message_types.add(msg['message_type'])

                logger.info(f"Total messages: {message_count}")
                logger.info(f"Message types found: {', '.join(message_types)}")

            elif isinstance(conversations, list):
                logger.info(f"Transformed {len(conversations)} conversations (list format)")

        # Check for metadata
        if 'metadata' in data:
            metadata = data['metadata']
            logger.info(f"Metadata: {metadata}")

    elif isinstance(data, list):
        logger.info(f"Data is a list with {len(data)} items")
        if data and isinstance(data[0], dict):
            sample_keys = list(data[0].keys())
            logger.info(f"Sample item keys: {', '.join(sample_keys[:5])}...")

def load_sample_data(file_path):
    """Load sample data from a file."""
    logger.info(f"Loading sample data from {file_path}")

    try:
        # Create a file handler
        file_handler = FileHandler()

        # Read the file
        data = file_handler.read_file(file_path)

        # Check if the data is in the expected format
        if isinstance(data, dict) and 'conversations' in data:
            logger.info("✅ Successfully loaded sample data")
            return data
        else:
            logger.error("❌ Data is not in the expected format")
            return None
    except Exception as e:
        logger.error(f"❌ Error loading sample data: {e}")
        return None

def test_transformer_with_direct_instantiation():
    """Test the transformer by directly instantiating it."""
    logger.info("Testing transformer with direct instantiation...")

    # Create an output directory if it doesn't exist
    output_dir = "output/transformer_test"
    os.makedirs(output_dir, exist_ok=True)

    # Load sample data
    sample_data_path = "temp_extract/messages.json"
    raw_data = load_sample_data(sample_data_path)

    if not raw_data:
        logger.error("Failed to load sample data")
        return

    # Create dependencies directly
    content_extractor = ContentExtractorAdapter()
    message_handler_factory = SkypeMessageHandlerFactory()
    structured_data_extractor = StructuredDataExtractor()

    # Create a transformer instance with direct dependencies
    transformer = Transformer(
        parallel_processing=False,  # Disable parallel processing for testing
        content_extractor=content_extractor,
        message_handler_factory=message_handler_factory,
        structured_data_extractor=structured_data_extractor
    )

    # Transform the data
    try:
        # Check if conversations is a list instead of a dictionary
        if 'conversations' in raw_data and isinstance(raw_data['conversations'], list):
            logger.info("Converting conversations from list to dictionary format...")
            conversations_dict = {}
            for conv in raw_data['conversations']:
                if 'id' in conv:
                    conversations_dict[conv['id']] = conv
                else:
                    # Generate a unique ID if none exists
                    import uuid
                    conversations_dict[str(uuid.uuid4())] = conv

            raw_data['conversations'] = conversations_dict
            logger.info(f"Converted {len(conversations_dict)} conversations to dictionary format")

        # Transform the data
        transformed_data = transformer.transform(raw_data)

        logger.info("✅ Successfully transformed data")
        print_data_summary(transformed_data)

        # Save the transformed data
        output_file = os.path.join(output_dir, "transformed_data.json")
        with open(output_file, 'w') as f:
            json.dump(transformed_data, f)
        logger.info(f"Saved transformed data to {output_file}")

    except Exception as e:
        logger.error(f"❌ Error transforming data: {e}")

def test_transformer_with_dependency_injection():
    """Test the transformer using dependency injection."""
    logger.info("Testing transformer with dependency injection...")

    # Load configuration
    config = load_config()
    if not config:
        logger.error("Failed to load configuration")
        return

    # Get database configuration
    db_config = get_db_config(config)
    if not db_config:
        logger.error("Failed to get database configuration")
        return

    # Register all services
    register_all_services(db_config=db_config)
    logger.info("Registered all services")

    # Create an output directory if it doesn't exist
    output_dir = "output/transformer_test_di"
    os.makedirs(output_dir, exist_ok=True)

    # Create a context with the db_config
    context = ETLContext(db_config=db_config, output_dir=output_dir)

    # Load sample data
    sample_data_path = "temp_extract/messages.json"
    raw_data = load_sample_data(sample_data_path)

    if not raw_data:
        logger.error("Failed to load sample data")
        return

    # Get the transformer from DI
    transformer = get_service(TransformerProtocol)

    if not transformer:
        logger.error("Failed to get transformer from DI")
        return

    # Set the context
    transformer.context = context

    # Transform the data
    try:
        # Check if conversations is a list instead of a dictionary
        if 'conversations' in raw_data and isinstance(raw_data['conversations'], list):
            logger.info("Converting conversations from list to dictionary format...")
            conversations_dict = {}
            for conv in raw_data['conversations']:
                if 'id' in conv:
                    conversations_dict[conv['id']] = conv
                else:
                    # Generate a unique ID if none exists
                    import uuid
                    conversations_dict[str(uuid.uuid4())] = conv

            raw_data['conversations'] = conversations_dict
            logger.info(f"Converted {len(conversations_dict)} conversations to dictionary format")

        # Transform the data
        transformed_data = transformer.transform(raw_data)

        logger.info("✅ Successfully transformed data using DI")
        print_data_summary(transformed_data)

        # Save the transformed data
        output_file = os.path.join(output_dir, "transformed_data.json")
        with open(output_file, 'w') as f:
            json.dump(transformed_data, f)
        logger.info(f"Saved transformed data to {output_file}")

    except Exception as e:
        logger.error(f"❌ Error transforming data using DI: {e}")

def main():
    """Main function."""
    logger.info("Starting transformer component test...")

    # Test with direct instantiation
    test_transformer_with_direct_instantiation()

    # Test with dependency injection
    test_transformer_with_dependency_injection()

    logger.info("Transformer component test completed")

if __name__ == '__main__':
    main()