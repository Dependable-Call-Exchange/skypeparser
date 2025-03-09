#!/usr/bin/env python3
"""
Message Type Extractor Utility

This script extracts all unique message types from a Skype export file
and generates a configuration file that can be used to update the message_types.json.
"""

import os
import json
import argparse
import logging
from typing import Dict, List, Set, Any
from collections import Counter

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_message_types(file_path: str) -> Dict[str, Any]:
    """
    Extract all unique message types from a Skype export file.

    Args:
        file_path (str): Path to the Skype export file

    Returns:
        Dict[str, Any]: Dictionary with message types and their counts
    """
    logger.info(f"Extracting message types from {file_path}")

    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        logger.error(f"Error loading file {file_path}: {e}")
        return {}

    # Extract all message types
    message_types = []
    type_counter = Counter()

    # Process each conversation
    for conversation in data.get('conversations', []):
        for message in conversation.get('MessageList', []):
            msg_type = message.get('messagetype', 'Unknown')
            if msg_type:
                message_types.append(msg_type)
                type_counter[msg_type] += 1

    # Create result dictionary
    result = {
        'unique_types': sorted(list(set(message_types))),
        'type_counts': dict(type_counter.most_common()),
        'total_messages': len(message_types)
    }

    logger.info(f"Found {len(result['unique_types'])} unique message types in {result['total_messages']} messages")
    return result

def generate_config(message_types: List[str], output_path: str = None) -> Dict[str, Any]:
    """
    Generate a configuration dictionary for the message types.

    Args:
        message_types (List[str]): List of unique message types
        output_path (str, optional): Path to save the configuration file

    Returns:
        Dict[str, Any]: Configuration dictionary
    """
    # Create default descriptions for each message type
    config = {
        "message_types": {},
        "default_message_format": "***Sent a {message_type}***"
    }

    for msg_type in message_types:
        # Generate a human-readable description
        if msg_type == "RichText":
            description = "***Text message***"
        elif "ThreadActivity" in msg_type:
            activity = msg_type.split('/')[1] if '/' in msg_type else msg_type
            description = f"***{activity} activity***"
        else:
            # Use the default format
            description = f"***Sent a {msg_type}***"

        config["message_types"][msg_type] = description

    # Save to file if output path is provided
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)
        logger.info(f"Configuration saved to {output_path}")

    return config

def merge_with_existing(new_config: Dict[str, Any], existing_path: str) -> Dict[str, Any]:
    """
    Merge new configuration with existing configuration.

    Args:
        new_config (Dict[str, Any]): New configuration dictionary
        existing_path (str): Path to existing configuration file

    Returns:
        Dict[str, Any]: Merged configuration dictionary
    """
    try:
        with open(existing_path, 'r', encoding='utf-8') as f:
            existing_config = json.load(f)
    except Exception as e:
        logger.warning(f"Could not load existing configuration from {existing_path}: {e}")
        return new_config

    # Merge message types, keeping existing descriptions
    for msg_type, description in new_config["message_types"].items():
        if msg_type not in existing_config.get("message_types", {}):
            existing_config.setdefault("message_types", {})[msg_type] = description

    return existing_config

def main():
    """Main function to run the script."""
    parser = argparse.ArgumentParser(description='Extract message types from Skype export file')
    parser.add_argument('file_path', help='Path to the Skype export file')
    parser.add_argument('--output', '-o', help='Path to save the configuration file')
    parser.add_argument('--merge', '-m', help='Path to existing configuration file to merge with')
    parser.add_argument('--report', '-r', action='store_true', help='Generate a detailed report')

    args = parser.parse_args()

    # Extract message types
    result = extract_message_types(args.file_path)

    if not result:
        logger.error("No message types found or error processing file")
        return

    # Generate configuration
    config = generate_config(result['unique_types'])

    # Merge with existing configuration if specified
    if args.merge and os.path.exists(args.merge):
        config = merge_with_existing(config, args.merge)

    # Save configuration if output path is specified
    if args.output:
        os.makedirs(os.path.dirname(args.output), exist_ok=True)
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=2)
        logger.info(f"Configuration saved to {args.output}")

    # Generate report if requested
    if args.report:
        report_path = args.output.replace('.json', '_report.txt') if args.output else 'message_types_report.txt'
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("# Message Types Report\n\n")
            f.write(f"Total messages: {result['total_messages']}\n")
            f.write(f"Unique message types: {len(result['unique_types'])}\n\n")
            f.write("## Message Type Counts\n\n")
            for msg_type, count in result['type_counts'].items():
                percentage = (count / result['total_messages']) * 100
                f.write(f"- {msg_type}: {count} ({percentage:.2f}%)\n")

        logger.info(f"Report saved to {report_path}")

if __name__ == "__main__":
    main()