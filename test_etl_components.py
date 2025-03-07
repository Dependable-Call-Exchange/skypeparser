#!/usr/bin/env python3
"""
Test ETL Components

This script tests the instantiation of key ETL components using dependency injection.
It verifies that the components can be created and basic operations can be performed.
"""

import sys
import os
import logging
from typing import Dict, Any

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Import DI components
from src.utils.di import get_service_provider, get_service
from src.utils.interfaces import (
    ExtractorProtocol,
    TransformerProtocol,
    LoaderProtocol,
    FileHandlerProtocol,
    ContentExtractorProtocol,
    MessageHandlerFactoryProtocol
)
from src.utils.service_registry import register_all_services
from src.db.etl.context import ETLContext

def load_config(config_path='config/config.json'):
    """Load configuration from a JSON file."""
    import json
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

def test_file_handler():
    """Test the FileHandler component."""
    logger.info("Testing FileHandler component...")

    try:
        # Get the file handler from the service provider
        file_handler = get_service(FileHandlerProtocol)

        # Test that we got a valid instance
        if file_handler:
            logger.info("✅ Successfully instantiated FileHandler")
            return True
        else:
            logger.error("❌ Failed to instantiate FileHandler")
            return False
    except Exception as e:
        logger.error(f"❌ Error testing FileHandler: {e}")
        return False

def test_content_extractor():
    """Test the ContentExtractor component."""
    logger.info("Testing ContentExtractor component...")

    try:
        # Get the content extractor from the service provider
        content_extractor = get_service(ContentExtractorProtocol)

        # Test that we got a valid instance
        if content_extractor:
            logger.info("✅ Successfully instantiated ContentExtractor")
            return True
        else:
            logger.error("❌ Failed to instantiate ContentExtractor")
            return False
    except Exception as e:
        logger.error(f"❌ Error testing ContentExtractor: {e}")
        return False

def test_message_handler_factory():
    """Test the MessageHandlerFactory component."""
    logger.info("Testing MessageHandlerFactory component...")

    try:
        # Get the message handler factory from the service provider
        message_handler_factory = get_service(MessageHandlerFactoryProtocol)

        # Test that we got a valid instance
        if message_handler_factory:
            logger.info("✅ Successfully instantiated MessageHandlerFactory")
            return True
        else:
            logger.error("❌ Failed to instantiate MessageHandlerFactory")
            return False
    except Exception as e:
        logger.error(f"❌ Error testing MessageHandlerFactory: {e}")
        return False

def test_etl_components():
    """Test the ETL components."""
    logger.info("Testing ETL components...")

    try:
        # Get the ETL components from the service provider
        extractor = get_service(ExtractorProtocol)
        transformer = get_service(TransformerProtocol)
        loader = get_service(LoaderProtocol)

        # Test that we got valid instances
        if extractor and transformer and loader:
            logger.info("✅ Successfully instantiated ETL components")
            return True
        else:
            logger.error("❌ Failed to instantiate ETL components")
            return False
    except Exception as e:
        logger.error(f"❌ Error testing ETL components: {e}")
        return False

def main():
    """Main function."""
    logger.info("Starting ETL components test...")

    # Load configuration
    config = load_config()
    if not config:
        logger.error("Failed to load configuration")
        return False

    # Get database configuration
    db_config = get_db_config(config)
    if not db_config:
        logger.error("Failed to get database configuration")
        return False

    # Register all services
    try:
        register_all_services(db_config=db_config)
        logger.info("✅ Successfully registered all services")
    except Exception as e:
        logger.error(f"❌ Error registering services: {e}")
        return False

    # Test components
    success = True
    success = success and test_file_handler()
    success = success and test_content_extractor()
    success = success and test_message_handler_factory()
    success = success and test_etl_components()

    if success:
        logger.info("✅ All component tests passed!")
    else:
        logger.error("❌ Some component tests failed")

    return success

if __name__ == '__main__':
    success = main()
    sys.exit(0 if success else 1)