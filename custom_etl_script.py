#!/usr/bin/env python3
"""
Simple Skype Export Parser

This script directly uses the core functionality to parse a Skype export file
and save the extracted data to JSON files, without the complexities of
database storage or dependency injection.
"""

import json
import logging
import os
import sys
from datetime import datetime

# Add the parent directory to the path to import the src modules
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.parser.core_parser import parse_skype_data
from src.utils.file_handler import read_file, read_tarfile
from src.utils.structured_logging import get_logger, setup_logging

# Setup logging
setup_logging(log_level="INFO", log_file="skype_parser.log")
logger = get_logger(__name__)


def main():
    """
    Main function to parse the Skype export file.
    """
    # Configuration
    export_file = "8_live_dave.leathers113_export.tar"
    user_display_name = "David Leathers"
    output_dir = "output"

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    logger.info(f"Starting Skype export parsing: {export_file}")

    try:
        # Step 1: Extract raw data from tarfile
        logger.info("Reading tarfile...")
        raw_data = read_tarfile(export_file)

        # Save raw data for inspection
        raw_output_path = os.path.join(
            output_dir, f"raw_{os.path.basename(export_file).replace('.tar', '.json')}"
        )
        with open(raw_output_path, "w", encoding="utf-8") as f:
            json.dump(raw_data, f, indent=2)

        logger.info(f"Raw data saved to: {raw_output_path}")

        # Step 2: Parse the data
        logger.info("Parsing Skype data...")
        parsed_data = parse_skype_data(raw_data, user_display_name)

        # Step 3: Save the parsed data
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parsed_output_path = os.path.join(output_dir, f"parsed_data_{timestamp}.json")
        with open(parsed_output_path, "w", encoding="utf-8") as f:
            json.dump(parsed_data, f, indent=2)

        # Step 4: Generate a report on the data
        user_id = raw_data.get("userId", "unknown")
        export_date = raw_data.get("exportDate", "unknown")
        conversation_count = len(parsed_data.get("conversations", []))
        message_count = sum(
            len(conv.get("messages", []))
            for conv in parsed_data.get("conversations", [])
        )

        logger.info(f"Parsing completed successfully.")
        logger.info(f"User ID: {user_id}")
        logger.info(f"Export Date: {export_date}")
        logger.info(f"Conversations: {conversation_count}")
        logger.info(f"Total Messages: {message_count}")
        logger.info(f"Parsed data saved to: {parsed_output_path}")

        # Generate a simple HTML report
        html_report_path = os.path.join(output_dir, f"report_{timestamp}.html")
        generate_html_report(parsed_data, html_report_path, user_display_name)
        logger.info(f"HTML report generated: {html_report_path}")

        return {
            "success": True,
            "user_id": user_id,
            "export_date": export_date,
            "conversation_count": conversation_count,
            "message_count": message_count,
            "output_file": parsed_output_path,
            "html_report": html_report_path,
        }

    except Exception as e:
        logger.error(f"Error processing Skype export: {str(e)}", exc_info=True)
        return {"success": False, "error": str(e)}


def generate_html_report(parsed_data, output_path, user_display_name):
    """
    Generate a simple HTML report from the parsed data.

    Args:
        parsed_data: Parsed Skype data
        output_path: Path to save the HTML report
        user_display_name: Display name of the user
    """
    conversations = parsed_data.get("conversations", [])

    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Skype Export Report for {user_display_name}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            h1, h2 {{ color: #0078d7; }}
            .conversation {{ margin-bottom: 20px; padding: 10px; border: 1px solid #ddd; border-radius: 5px; }}
            .conversation-header {{ background-color: #f0f0f0; padding: 10px; margin-bottom: 10px; }}
            .message {{ margin: 5px 0; padding: 5px; }}
            .message-from {{ font-weight: bold; }}
            .message-time {{ color: #666; font-size: 0.9em; }}
            .message-content {{ margin-top: 5px; }}
            .summary {{ background-color: #f8f8f8; padding: 10px; margin-bottom: 20px; }}
        </style>
    </head>
    <body>
        <h1>Skype Export Report</h1>
        <div class="summary">
            <h2>Summary</h2>
            <p><strong>User:</strong> {user_display_name}</p>
            <p><strong>Total Conversations:</strong> {len(conversations)}</p>
            <p><strong>Total Messages:</strong> {sum(len(conv.get('messages', [])) for conv in conversations)}</p>
            <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </div>

        <h2>Conversations</h2>
    """

    # Add conversations
    for i, conv in enumerate(
        conversations[:10], 1
    ):  # Limit to first 10 conversations for the report
        display_name = conv.get("displayName", "Unknown")
        messages = conv.get("messages", [])

        html += f"""
        <div class="conversation">
            <div class="conversation-header">
                <h3>Conversation {i}: {display_name}</h3>
                <p><strong>Messages:</strong> {len(messages)}</p>
            </div>
        """

        # Add messages (limit to 5 per conversation for the report)
        for msg in messages[:5]:
            from_name = msg.get("from", {}).get("displayName", "Unknown")
            timestamp = msg.get("composetime", "Unknown time")
            content = msg.get("content", "No content")

            html += f"""
            <div class="message">
                <div class="message-from">{from_name}</div>
                <div class="message-time">{timestamp}</div>
                <div class="message-content">{content}</div>
            </div>
            """

        if len(messages) > 5:
            html += f"<p><em>... and {len(messages) - 5} more messages</em></p>"

        html += "</div>"

    if len(conversations) > 10:
        html += f"<p><em>... and {len(conversations) - 10} more conversations</em></p>"

    html += """
    </body>
    </html>
    """

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)


if __name__ == "__main__":
    result = main()
    if result.get("success"):
        print(f"\nSuccessfully processed Skype export!")
        print(f"Conversations: {result.get('conversation_count')}")
        print(f"Messages: {result.get('message_count')}")
        print(f"Output files:")
        print(f"  - JSON: {result.get('output_file')}")
        print(f"  - HTML Report: {result.get('html_report')}")
    else:
        print(f"\nError processing Skype export: {result.get('error')}")
