#!/usr/bin/env python3
"""
Local Test Script for SkypeParser ETL Pipeline

This script validates the ETL pipeline structure and functionality locally, without
requiring database connectivity. It ensures that all components are properly
registered and can handle the input data correctly.

Usage:
    python test_etl_local.py [options]

Example:
    python test_etl_local.py --file 8_live_dave.leathers113_export.tar --extract-only
"""

import os
import sys
import json
import logging
import argparse
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# Configure logging
def setup_logging(log_dir: str = "logs") -> str:
    """Set up logging configuration."""
    os.makedirs(log_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f"etl_local_test_{timestamp}.log")

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

    return log_file

def parse_arguments() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Test SkypeParser ETL Pipeline locally',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument('-f', '--file',
                        default='8_live_dave.leathers113_export.tar',
                        help='Path to the Skype export file (TAR or JSON)')

    parser.add_argument('-o', '--output',
                        default='output',
                        help='Output directory for intermediary files')

    parser.add_argument('--extract-only',
                        action='store_true',
                        help='Only run the extraction phase')

    parser.add_argument('-v', '--verbose',
                        action='store_true',
                        help='Enable verbose logging')

    return parser.parse_args()

def validate_file(file_path: str) -> bool:
    """Validate the input file exists and has the correct format."""
    logger = logging.getLogger(__name__)

    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return False

    file_size = os.path.getsize(file_path)
    logger.info(f"Found file: {file_path} ({file_size} bytes)")

    return True

def test_extractor(file_path: str, output_dir: str) -> Tuple[bool, Dict[str, Any]]:
    """Test the extractor component with the given file."""
    logger = logging.getLogger(__name__)
    logger.info(f"Testing extractor with file: {file_path}")

    try:
        # Import required modules
        from src.utils.di import get_service_provider
        from src.utils.service_registry import register_core_services
        from src.utils.config import load_config
        from src.db.etl.context import ETLContext
        from src.db.etl.extractor import Extractor

        # Create output directory
        os.makedirs(output_dir, exist_ok=True)

        # Load config
        config = load_config('config/config.json')
        db_config = config.get('database', {})

        # Register core services if not already registered
        provider = get_service_provider()
        register_core_services(provider)

        # Create ETL context with mock DB config
        context = ETLContext(
            db_config=db_config,
            output_dir=output_dir
        )

        # Create extractor
        extractor = Extractor(context=context)

        # Extract data
        logger.info("Running extraction...")
        raw_data = extractor.extract(file_path)

        # Validate extraction result
        if not raw_data:
            logger.error("Extraction failed: No data returned")
            return False, {}

        # Save raw data for debugging
        raw_data_path = os.path.join(output_dir, f"raw_{os.path.basename(file_path)}.json")
        with open(raw_data_path, 'w') as f:
            json.dump(raw_data, f, indent=2)
        logger.info(f"Extracted data saved successfully to: {raw_data_path}")

        # Log extraction metrics
        if "messages" in raw_data:
            if isinstance(raw_data["messages"], list):
                # Old format
                logger.info(f"Found {len(raw_data['messages'])} message objects in extracted data")

                if len(raw_data["messages"]) > 0:
                    first_message = raw_data["messages"][0]
                    if "userId" in first_message and "exportDate" in first_message:
                        logger.info(f"Found metadata: userId={first_message['userId']}, exportDate={first_message['exportDate']}")

                    if "conversations" in first_message:
                        logger.info(f"Found {len(first_message['conversations'])} conversations")

            elif isinstance(raw_data["messages"], dict):
                # New format
                logger.info(f"Found 'messages' key in extracted data, this is a newer format")
                logger.info(f"Found {len(raw_data['messages'])} conversations")

                # Extract metadata if available
                user_id = raw_data.get("userId", "Unknown")
                export_date = raw_data.get("exportDate", "Unknown")
                logger.info(f"Found metadata: userId={user_id}, exportDate={export_date}")
        else:
            logger.warning("No 'messages' key found in extracted data")

        # Return success and raw data
        return True, raw_data

    except ImportError as e:
        logger.error(f"Failed to import required modules: {e}")
        return False, {}
    except Exception as e:
        logger.error(f"Error during extraction: {e}")
        logger.debug(traceback.format_exc())
        return False, {}

def test_transformer(raw_data: Dict[str, Any], output_dir: str) -> Tuple[bool, Dict[str, Any]]:
    """Test the transformer component with the given raw data."""
    logger = logging.getLogger(__name__)
    logger.info("Testing transformer...")

    try:
        # Import required modules
        from src.utils.di import get_service_provider
        from src.utils.service_registry import register_core_services
        from src.utils.config import load_config
        from src.db.etl.context import ETLContext
        from src.db.etl.transformer import Transformer

        # Load config
        config = load_config('config/config.json')
        db_config = config.get('database', {})

        # Register core services if not already registered
        provider = get_service_provider()
        register_core_services(provider)

        # Create ETL context with mock DB config
        context = ETLContext(
            db_config=db_config,
            output_dir=output_dir
        )

        # Create transformer
        transformer = Transformer(context=context)

        # Adapt raw data to the expected format if needed
        adapted_data = raw_data

        # Check if this is the newer format with 'messages' key
        if "messages" in raw_data and isinstance(raw_data["messages"], dict):
            logger.info("Adapting newer Skype export format to expected structure")

            # Extract user ID and export date from metadata if available
            user_id = raw_data.get("userId", "")
            export_date = raw_data.get("exportDate", "")

            # Create a structure that the transformer can handle
            adapted_data = {
                "messages": [
                    {
                        "userId": user_id,
                        "exportDate": export_date,
                        "conversations": []
                    }
                ]
            }

            # Extract conversations from the messages dictionary
            conversations = []
            for conv_id, conv_data in raw_data.get("messages", {}).items():
                if isinstance(conv_data, dict):
                    # Add conversation ID to the conversation data
                    conv_data["id"] = conv_id
                    conversations.append(conv_data)

            # Add conversations to the adapted data
            if conversations:
                adapted_data["messages"][0]["conversations"] = conversations
                logger.info(f"Added {len(conversations)} conversations to adapted data")

            # Save adapted data for debugging
            adapted_data_path = os.path.join(output_dir, "adapted_data.json")
            with open(adapted_data_path, 'w') as f:
                json.dump(adapted_data, f, indent=2)
            logger.info(f"Saved adapted data to {adapted_data_path}")

        # Transform data
        logger.info("Running transformation...")
        transformed_data = transformer.transform(adapted_data)

        # Validate transformation result
        if not transformed_data:
            logger.error("Transformation failed: No data returned")
            return False, {}

        # Check for required sections in transformed_data
        required_sections = ['user', 'conversations', 'messages', 'metadata']
        missing_sections = [section for section in required_sections if section not in transformed_data]

        if missing_sections:
            logger.error(f"Transformed data missing required sections: {missing_sections}")
            return False, transformed_data

        # Log transformation metrics
        user_id = transformed_data.get('user', {}).get('id')
        conversation_count = len(transformed_data.get('conversations', {}))
        message_count = len(transformed_data.get('messages', {}))

        logger.info(f"Transformation successful for user {user_id}")
        logger.info(f"Transformed {conversation_count} conversations and {message_count} messages")

        # Return success and transformed data
        return True, transformed_data

    except ImportError as e:
        logger.error(f"Failed to import required modules: {e}")
        return False, {}
    except Exception as e:
        logger.error(f"Error during transformation: {e}")
        logger.debug(traceback.format_exc())
        return False, {}

def generate_report(output_dir: str, extraction_success: bool, transformation_success: bool) -> str:
    """Generate a report of the test results."""
    os.makedirs(output_dir, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = os.path.join(output_dir, f"etl_local_test_report_{timestamp}.md")

    with open(report_file, 'w') as f:
        f.write("# SkypeParser ETL Pipeline Local Test Report\n\n")
        f.write(f"**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        # Overall status
        overall_status = "✅ PASSED" if extraction_success and (transformation_success or extraction_success) else "❌ FAILED"
        f.write(f"**Overall Status:** {overall_status}\n\n")

        # Test results
        f.write("## Test Results\n\n")
        f.write(f"1. **Extraction Phase:** {'✅ Passed' if extraction_success else '❌ Failed'}\n")

        if transformation_success is not None:
            f.write(f"2. **Transformation Phase:** {'✅ Passed' if transformation_success else '❌ Failed'}\n")

        # Recommendations
        f.write("\n## Recommendations\n\n")
        if not extraction_success:
            f.write("- Check the extraction module and file format\n")
            f.write("- Ensure all required services are properly registered\n")

        if transformation_success is not None and not transformation_success:
            f.write("- Verify the transformer module is properly configured\n")
            f.write("- Check for missing message type handlers or extractors\n")

        if extraction_success and (transformation_success or transformation_success is None):
            f.write("- The ETL pipeline components are functioning correctly\n")
            f.write("- Proceed with full end-to-end testing including database integration\n")

    return report_file

def main():
    """Main function to run the local ETL pipeline test."""
    # Parse arguments
    args = parse_arguments()

    # Setup logging
    log_file = setup_logging()
    logger = logging.getLogger(__name__)

    # Set log level
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    logger.info("Starting SkypeParser ETL Pipeline Local Test")

    try:
        # Validate input file
        if not validate_file(args.file):
            logger.error("Input file validation failed")
            return 1

        # Test extractor
        extraction_success, raw_data = test_extractor(args.file, args.output)

        # Stop here if extraction failed or extract-only is specified
        if not extraction_success or args.extract_only:
            transformation_success = None
        else:
            # Test transformer with the extracted data
            transformation_success, transformed_data = test_transformer(raw_data, args.output)

        # Generate report
        report_file = generate_report(args.output, extraction_success, transformation_success)
        logger.info(f"Report generated: {report_file}")

        # Determine exit code
        if args.extract_only:
            return 0 if extraction_success else 1
        else:
            return 0 if extraction_success and transformation_success else 1

    except Exception as e:
        logger.error(f"Error in local ETL test: {e}")
        logger.debug(traceback.format_exc())
        return 1

if __name__ == "__main__":
    sys.exit(main())