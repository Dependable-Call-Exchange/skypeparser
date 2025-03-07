#!/usr/bin/env python3
"""
Simple Skype Export Parser

This script uses the core functionality to parse a Skype export file
and save the extracted data to JSON files, along with an HTML report.

Usage:
    python scripts/custom_etl_script.py -f <export_file> -u <user_display_name> [-o <output_directory>] [-v]

Example:
    python scripts/custom_etl_script.py -f 8_live_dave.leathers113_export.tar -u "David Leathers" -v
"""

import argparse
import json
import logging
import os
import sys
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

# Add the parent directory to the path to import the src modules
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# First, apply monkey patches before importing any other modules
from scripts.monkey_patch import logger as patch_logger

# Now import other modules
from src.parser.core_parser import parse_skype_data
from src.utils.di import ServiceProvider, get_service, get_service_provider
from src.utils.interfaces import FileHandlerProtocol
from src.utils.serialization import serialize_to_json
from src.utils.service_registry import register_core_services
from src.utils.structured_logging import get_logger, setup_logging

# Setup logging
setup_logging(level=logging.INFO, log_file="skype_parser.log", structured=True)
logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description="Parse a Skype export file and generate reports"
    )
    parser.add_argument(
        "-f", "--file", required=True, help="Path to the Skype export file (tar format)"
    )
    parser.add_argument(
        "-u", "--user", required=True, help="User display name for the export"
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default="output",
        help="Output directory for reports and data files",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    parser.add_argument(
        "--skip-raw",
        action="store_true",
        help="Skip saving raw data to JSON (useful for large files)",
    )
    return parser.parse_args()


def setup_environment(args: argparse.Namespace) -> None:
    """
    Setup the environment based on command-line arguments.

    Args:
        args: Command-line arguments
    """
    # Configure logging level based on verbose flag
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")
        # Also set monkey patch logger to debug
        patch_logger.setLevel(logging.DEBUG)

    # Ensure output directory exists
    os.makedirs(args.output_dir, exist_ok=True)
    logger.debug(f"Output directory: {args.output_dir}")

    # Validate input file exists
    if not os.path.exists(args.file):
        raise FileNotFoundError(f"Skype export file not found: {args.file}")

    if not os.path.isfile(args.file):
        raise ValueError(f"Path is not a file: {args.file}")

    logger.debug(f"Input file validated: {args.file}")


def initialize_di() -> ServiceProvider:
    """
    Initialize the dependency injection container.

    Returns:
        The configured ServiceProvider
    """
    provider = get_service_provider()
    register_core_services(provider)
    logger.debug("Dependency injection container initialized")
    return provider


def main() -> Dict[str, Any]:
    """
    Main function to parse the Skype export file.

    Returns:
        Dictionary with processing results
    """
    try:
        # Parse command-line arguments
        args = parse_args()

        # Setup environment (logging, directories, validation)
        setup_environment(args)

        # Initialize dependency injection
        initialize_di()

        logger.info(f"Starting Skype export parsing: {args.file}")

        # Get FileHandler from DI container
        file_handler = get_service(FileHandlerProtocol)

        # Step 1: Extract raw data from tarfile
        try:
            logger.info("Reading tarfile...")
            raw_data = file_handler.read_tarfile(args.file)
        except Exception as e:
            logger.error(f"Error reading tarfile: {str(e)}")
            raise ValueError(f"Failed to read export file: {str(e)}")

        # Validate raw data
        if not raw_data or not isinstance(raw_data, dict):
            raise ValueError("Invalid Skype export data format")

        # Save raw data for inspection using our serialization utility (optional)
        if not args.skip_raw:
            raw_output_path = os.path.join(
                args.output_dir,
                f"raw_{os.path.basename(args.file).replace('.tar', '.json')}",
            )
            logger.info(f"Saving raw data to: {raw_output_path}")
            serialize_to_json(raw_data, raw_output_path)
            logger.info(f"Raw data saved to: {raw_output_path}")
        else:
            logger.info("Skipping raw data save (--skip-raw flag used)")

        # Step 2: Parse the data
        try:
            logger.info("Parsing Skype data...")
            parsed_data = parse_skype_data(raw_data, args.user)

            # Log the type and structure of the parsed data for debugging
            logger.debug(f"Parsed data type: {type(parsed_data)}")
            if parsed_data and isinstance(parsed_data, dict):
                conversations = parsed_data.get("conversations", [])
                logger.debug(f"Conversations type: {type(conversations)}")
                logger.debug(
                    f"Number of conversations: {len(conversations) if isinstance(conversations, list) else 'not a list'}"
                )
                if isinstance(conversations, list) and conversations:
                    logger.debug(f"First conversation type: {type(conversations[0])}")
        except Exception as e:
            logger.error(f"Error parsing Skype data: {str(e)}")
            logger.error(traceback.format_exc())
            raise ValueError(f"Failed to parse Skype data: {str(e)}")

        # Validate parsed data more thoroughly
        if not parsed_data or not isinstance(parsed_data, dict):
            raise ValueError("Invalid parsed data structure: not a dictionary")

        if "conversations" not in parsed_data:
            raise ValueError(
                "Invalid parsed data structure: missing 'conversations' key"
            )

        conversations = parsed_data.get("conversations", [])
        if not isinstance(conversations, list):
            logger.warning(
                f"Conversations is not a list, but a {type(conversations)}. Converting to empty list."
            )
            conversations = []
            parsed_data["conversations"] = conversations

        # Step 3: Save the parsed data using our serialization utility
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        parsed_output_path = os.path.join(
            args.output_dir, f"parsed_data_{timestamp}.json"
        )
        # Use our serialization utility that handles datetime objects
        logger.info(f"Saving parsed data to: {parsed_output_path}")
        serialize_to_json(parsed_data, parsed_output_path)
        logger.info(f"Parsed data saved to: {parsed_output_path}")

        # Step 4: Generate a report on the data
        user_id = raw_data.get("userId", "unknown")
        export_date = raw_data.get("exportDate", "unknown")

        # Safely calculate conversation and message counts with type checking
        conversation_count = 0
        message_count = 0

        if isinstance(conversations, list):
            conversation_count = len(conversations)
            for conv in conversations:
                if isinstance(conv, dict):
                    messages = conv.get("messages", [])
                    if isinstance(messages, list):
                        message_count += len(messages)
                    else:
                        logger.warning(
                            f"Messages is not a list in conversation, but a {type(messages)}"
                        )
                else:
                    logger.warning(f"Found non-dictionary conversation: {type(conv)}")

        logger.info(f"Parsing completed successfully.")
        logger.info(f"User ID: {user_id}")
        logger.info(f"Export Date: {export_date}")
        logger.info(f"Conversations: {conversation_count}")
        logger.info(f"Total Messages: {message_count}")

        # Generate a simple HTML report
        html_report_path = os.path.join(args.output_dir, f"report_{timestamp}.html")
        logger.info(f"Generating HTML report: {html_report_path}")
        generate_html_report(parsed_data, html_report_path, args.user)
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

    except FileNotFoundError as e:
        logger.error(f"File not found: {str(e)}")
        return {"success": False, "error": "File not found", "details": str(e)}
    except ValueError as e:
        logger.error(f"Invalid data or argument: {str(e)}")
        return {"success": False, "error": "Invalid data", "details": str(e)}
    except Exception as e:
        logger.error(
            f"Unexpected error processing Skype export: {str(e)}", exc_info=True
        )
        return {"success": False, "error": "Unexpected error", "details": str(e)}


def generate_html_report(
    parsed_data: Dict[str, Any], output_path: str, user_display_name: str
) -> None:
    """
    Generate a simple HTML report from the parsed data.

    Args:
        parsed_data: Parsed Skype data
        output_path: Path to save the HTML report
        user_display_name: Display name of the user
    """
    try:
        # Safely extract conversations with type checking
        conversations = []
        if isinstance(parsed_data, dict) and "conversations" in parsed_data:
            conversations_data = parsed_data["conversations"]
            if isinstance(conversations_data, list):
                conversations = conversations_data
            else:
                logger.warning(
                    f"Conversations is not a list in parsed_data, but a {type(conversations_data)}"
                )

        # Calculate message count safely
        total_messages = 0
        for conv in conversations:
            if isinstance(conv, dict) and "messages" in conv:
                messages = conv["messages"]
                if isinstance(messages, list):
                    total_messages += len(messages)

        # Use a list for HTML parts to avoid inefficient string concatenation
        html_parts = []

        # HTML header and CSS
        html_parts.append(
            f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Skype Export Report for {html_escape(user_display_name)}</title>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
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
                <p><strong>User:</strong> {html_escape(user_display_name)}</p>
                <p><strong>Total Conversations:</strong> {len(conversations)}</p>
                <p><strong>Total Messages:</strong> {total_messages}</p>
                <p><strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
            </div>

            <h2>Conversations</h2>
        """
        )

        # Add conversations
        for i, conv in enumerate(
            conversations[:10], 1
        ):  # Limit to first 10 conversations for the report
            if not isinstance(conv, dict):
                logger.warning(
                    f"Skipping non-dictionary conversation of type {type(conv)}"
                )
                continue

            display_name = html_escape(conv.get("displayName", "Unknown"))

            # Safely get messages
            messages = []
            if "messages" in conv and isinstance(conv["messages"], list):
                messages = conv["messages"]
            else:
                logger.warning(
                    f"Missing or invalid messages in conversation {display_name}"
                )

            html_parts.append(
                f"""
            <div class="conversation">
                <div class="conversation-header">
                    <h3>Conversation {i}: {display_name}</h3>
                    <p><strong>Messages:</strong> {len(messages)}</p>
                </div>
            """
            )

            # Add messages (limit to 5 per conversation for the report)
            for msg in messages[:5]:
                if not isinstance(msg, dict):
                    logger.warning(
                        f"Skipping non-dictionary message of type {type(msg)}"
                    )
                    continue

                # Safely extract message data with defaults
                from_data = msg.get("from", {})
                from_name = "Unknown"
                if isinstance(from_data, dict):
                    from_name = html_escape(from_data.get("displayName", "Unknown"))
                elif isinstance(from_data, str):
                    from_name = html_escape(from_data)

                timestamp = html_escape(
                    msg.get(
                        "composetime", msg.get("originalarrivaltime", "Unknown time")
                    )
                )

                # Safely get content with fallback
                content = ""
                try:
                    content_raw = msg.get("content", "")
                    if isinstance(content_raw, str):
                        content = html_escape(content_raw)
                    else:
                        content = html_escape(str(content_raw))
                except Exception as e:
                    logger.warning(f"Error processing message content: {e}")
                    content = "<Error processing content>"

                html_parts.append(
                    f"""
                <div class="message">
                    <div class="message-from">{from_name}</div>
                    <div class="message-time">{timestamp}</div>
                    <div class="message-content">{content}</div>
                </div>
                """
                )

            if len(messages) > 5:
                html_parts.append(
                    f"<p><em>... and {len(messages) - 5} more messages</em></p>"
                )

            html_parts.append("</div>")

        if len(conversations) > 10:
            html_parts.append(
                f"<p><em>... and {len(conversations) - 10} more conversations</em></p>"
            )

        html_parts.append(
            """
        </body>
        </html>
        """
        )

        # Join all parts and write to file
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("".join(html_parts))

    except Exception as e:
        logger.error(f"Error generating HTML report: {str(e)}", exc_info=True)
        raise ValueError(f"Failed to generate HTML report: {str(e)}")


def html_escape(text: str) -> str:
    """
    Escape HTML special characters to prevent XSS vulnerabilities.

    Args:
        text: Text to escape

    Returns:
        Escaped text safe for HTML insertion
    """
    if not isinstance(text, str):
        try:
            text = str(text)
        except Exception as e:
            logger.warning(f"Failed to convert {type(text)} to string: {e}")
            return "[Non-string content]"

    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


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
        if "details" in result:
            print(f"Details: {result.get('details')}")
        print("\nRun with --verbose flag for more detailed logging.")
        sys.exit(1)
