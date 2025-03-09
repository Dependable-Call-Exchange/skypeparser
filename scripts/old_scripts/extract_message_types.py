#!/usr/bin/env python3
"""
Script to extract message types from a Skype export file.

This script uses the message_type_extractor utility to extract all unique message types
from a Skype export file and generate a configuration file.
"""

import os
import sys
import logging
import argparse

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.utils.message_type_extractor import extract_message_types, generate_config, merge_with_existing

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(description='Extract message types from a Skype export file')
    parser.add_argument('--input', '-i', required=True, help='Path to the Skype export file')
    parser.add_argument('--output', '-o', default='config/message_types.json', help='Path to save the configuration file')
    parser.add_argument('--merge', '-m', action='store_true', help='Merge with existing configuration file')
    parser.add_argument('--report', '-r', action='store_true', help='Generate a detailed report')

    args = parser.parse_args()

    # Check if input file exists
    if not os.path.exists(args.input):
        logger.error(f"Input file {args.input} does not exist")
        return 1

    # Extract message types
    result = extract_message_types(args.input)

    if not result:
        logger.error("No message types found or error processing file")
        return 1

    # Generate configuration
    config = generate_config(result['unique_types'])

    # Merge with existing configuration if specified
    if args.merge and os.path.exists(args.output):
        config = merge_with_existing(config, args.output)

    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    # Save configuration
    with open(args.output, 'w', encoding='utf-8') as f:
        import json
        json.dump(config, f, indent=2)
    logger.info(f"Configuration saved to {args.output}")

    # Generate report if requested
    if args.report:
        report_path = args.output.replace('.json', '_report.txt')
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# Message Types Report\n\n")
            f.write(f"Total messages: {result['total_messages']}\n")
            f.write(f"Unique message types: {len(result['unique_types'])}\n\n")
            f.write("## Message Type Counts\n\n")
            for msg_type, count in result['type_counts'].items():
                percentage = (count / result['total_messages']) * 100
                f.write(f"- {msg_type}: {count} ({percentage:.2f}%)\n")

        logger.info(f"Report saved to {report_path}")

    return 0

if __name__ == "__main__":
    sys.exit(main())