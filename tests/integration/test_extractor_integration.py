#!/usr/bin/env python3
"""
Test Extractor Component

This script tests the Extractor component of the ETL pipeline with a sample Skype export file.
It verifies that the extractor can properly handle both JSON and TAR formats and
demonstrates the dependency injection integration.
"""

import os
import sys
import logging
import json
from typing import Dict, Any, List, Union

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
from src.db.etl.extractor import Extractor
from src.db.etl.context import ETLContext
from src.utils.di import get_service_provider, get_service
from src.utils.interfaces import ExtractorProtocol, FileHandlerProtocol
from src.utils.service_registry import register_core_services, register_all_services
from src.utils.file_handler import FileHandler
from src.utils.validation import validate_skype_data

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
        if 'conversations' in data:
            conversations = data['conversations']
            if isinstance(conversations, dict):
                logger.info(f"Extracted {len(conversations)} conversations")
            elif isinstance(conversations, list):
                logger.info(f"Extracted {len(conversations)} conversations (list format)")
    elif isinstance(data, list):
        logger.info(f"Extracted data is a list with {len(data)} items")
        if data and isinstance(data[0], dict):
            sample_keys = list(data[0].keys())
            logger.info(f"Sample item keys: {', '.join(sample_keys[:5])}...")

            # Try to convert list to expected format
            if 'id' in data[0] and 'content' in data[0]:
                logger.info("Attempting to convert list to expected format...")
                converted_data = {
                    'conversations': {
                        'default': {
                            'MessageList': data
                        }
                    }
                }
                try:
                    validate_skype_data(converted_data)
                    logger.info("✅ Successfully converted data to valid Skype format")
                    return converted_data
                except Exception as e:
                    logger.error(f"❌ Failed to convert data: {e}")

    return data

def test_extractor_with_direct_instantiation():
    """Test the extractor by directly instantiating it."""
    logger.info("Testing extractor with direct instantiation...")

    # Create an output directory if it doesn't exist
    output_dir = "output/extractor_test"
    os.makedirs(output_dir, exist_ok=True)

    # Create a file handler directly
    file_handler = FileHandler()

    # Create an extractor instance with the file handler
    extractor = Extractor(output_dir=output_dir, file_handler=file_handler)

    # Test with TAR file
    tar_file_path = "8_live_dave.leathers113_export.tar"
    if os.path.exists(tar_file_path):
        logger.info(f"Testing extraction from TAR file: {tar_file_path}")
        try:
            # Read the raw data first
            raw_data = file_handler.read_tarfile(tar_file_path, auto_select=True)
            logger.info(f"✅ Successfully read data from TAR file")

            # Examine and potentially convert the data
            converted_data = print_data_summary(raw_data)

            # If we have a valid format, try the extractor
            if isinstance(converted_data, dict) and 'conversations' in converted_data:
                logger.info("Testing extractor with converted data...")
                try:
                    # Save the converted data to a temporary file
                    temp_file = os.path.join(output_dir, "converted_data.json")
                    with open(temp_file, 'w') as f:
                        json.dump(converted_data, f)

                    # Extract from the temporary file
                    extracted_data = extractor.extract(file_path=temp_file)
                    logger.info(f"✅ Successfully extracted data from converted file")
                    print_data_summary(extracted_data)
                except Exception as e:
                    logger.error(f"❌ Error extracting from converted file: {e}")
        except Exception as e:
            logger.error(f"❌ Error reading from TAR file: {e}")
    else:
        logger.warning(f"TAR file not found: {tar_file_path}")

    # Test with JSON file from temp_extract
    json_file_path = "temp_extract/messages.json"
    if os.path.exists(json_file_path):
        logger.info(f"Testing extraction from JSON file: {json_file_path}")
        try:
            # Read the raw data first
            raw_data = file_handler.read_file(json_file_path)
            logger.info(f"✅ Successfully read data from JSON file")

            # Examine and potentially convert the data
            converted_data = print_data_summary(raw_data)

            # If we have a valid format, try the extractor
            if isinstance(converted_data, dict) and 'conversations' in converted_data:
                logger.info("Testing extractor with converted data...")
                try:
                    # Save the converted data to a temporary file
                    temp_file = os.path.join(output_dir, "converted_data.json")
                    with open(temp_file, 'w') as f:
                        json.dump(converted_data, f)

                    # Extract from the temporary file
                    extracted_data = extractor.extract(file_path=temp_file)
                    logger.info(f"✅ Successfully extracted data from converted file")
                    print_data_summary(extracted_data)
                except Exception as e:
                    logger.error(f"❌ Error extracting from converted file: {e}")
        except Exception as e:
            logger.error(f"❌ Error reading from JSON file: {e}")
    else:
        logger.warning(f"JSON file not found: {json_file_path}")

def test_extractor_with_dependency_injection():
    """Test the extractor using dependency injection."""
    logger.info("Testing extractor with dependency injection...")

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
    output_dir = "output/extractor_test_di"
    os.makedirs(output_dir, exist_ok=True)

    # Create a context with the db_config
    context = ETLContext(db_config=db_config, output_dir=output_dir)

    # Get the file handler from DI
    file_handler = get_service(FileHandlerProtocol)

    # Create an extractor with the file handler from DI
    extractor = Extractor(context=context, file_handler=file_handler)

    # Test with TAR file
    tar_file_path = "8_live_dave.leathers113_export.tar"
    if os.path.exists(tar_file_path):
        logger.info(f"Testing extraction from TAR file using DI: {tar_file_path}")
        try:
            # Read the raw data first
            raw_data = file_handler.read_tarfile(tar_file_path, auto_select=True)
            logger.info(f"✅ Successfully read data from TAR file using DI")

            # Examine and potentially convert the data
            converted_data = print_data_summary(raw_data)

            # If we have a valid format, try the extractor
            if isinstance(converted_data, dict) and 'conversations' in converted_data:
                logger.info("Testing extractor with converted data using DI...")
                try:
                    # Save the converted data to a temporary file
                    temp_file = os.path.join(output_dir, "converted_data.json")
                    with open(temp_file, 'w') as f:
                        json.dump(converted_data, f)

                    # Extract from the temporary file
                    extracted_data = extractor.extract(file_path=temp_file)
                    logger.info(f"✅ Successfully extracted data from converted file using DI")
                    print_data_summary(extracted_data)
                except Exception as e:
                    logger.error(f"❌ Error extracting from converted file using DI: {e}")
        except Exception as e:
            logger.error(f"❌ Error reading from TAR file using DI: {e}")
    else:
        logger.warning(f"TAR file not found: {tar_file_path}")

def main():
    """Main function."""
    logger.info("Starting extractor component test...")

    # Test with direct instantiation
    test_extractor_with_direct_instantiation()

    # Test with dependency injection
    test_extractor_with_dependency_injection()

    logger.info("Extractor component test completed")

if __name__ == '__main__':
    main()