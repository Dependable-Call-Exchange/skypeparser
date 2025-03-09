#!/usr/bin/env python
"""
Stream Skype Data - A memory-efficient Skype export parser

This script demonstrates how to use streaming processing to handle large Skype
export files without loading the entire dataset into memory. It's particularly
useful for exports containing millions of messages across thousands of conversations.

Usage:
    python scripts/stream_skype_data.py -f <skype_export_file> -u <your_display_name> [-v]

Options:
    -f, --file       Path to the Skype export file (TAR or JSON)
    -u, --user       Your display name as it appears in Skype
    -v, --verbose    Enable verbose logging
    -b, --batch      Batch size for conversation processing (default: 100)
    -o, --output     Output directory for reports (default: output)
"""

import argparse
import datetime
import logging
import os
import sys
import time
import warnings
from typing import Any, Dict, Optional

# Filter BeautifulSoup MarkupResemblesLocatorWarning
warnings.filterwarnings("ignore", category=UserWarning, module="bs4")

# Add the project root to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Import project modules
from src.parser.core_parser import parse_skype_data_streaming, stream_conversations
from src.utils.di import get_service
from src.utils.interfaces import FileHandlerProtocol
from src.utils.service_registry import register_core_services


def setup_logging(verbose: bool = False) -> None:
    """
    Set up logging configuration.

    Args:
        verbose: Whether to enable verbose logging
    """
    log_level = logging.DEBUG if verbose else logging.INFO
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    logging.basicConfig(
        level=log_level, format=log_format, handlers=[logging.StreamHandler()]
    )


def parse_arguments() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Process Skype export data with streaming"
    )

    parser.add_argument(
        "-f",
        "--file",
        required=True,
        help="Path to the Skype export file (TAR or JSON)",
    )
    parser.add_argument(
        "-u", "--user", required=True, help="Your display name as it appears in Skype"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose logging"
    )
    parser.add_argument(
        "-b",
        "--batch",
        type=int,
        default=100,
        help="Batch size for conversation processing",
    )
    parser.add_argument(
        "-o", "--output", default="output", help="Output directory for reports"
    )

    return parser.parse_args()


def progress_callback(key: str, value: Any) -> None:
    """
    Callback function for progress updates.

    Args:
        key: The statistic being updated
        value: The new value
    """
    logger = logging.getLogger(__name__)
    if key == "conversation_count":
        logger.info(f"Processed {value} conversations")
    elif key == "message_count":
        logger.info(f"Processed {value} messages")
    elif key == "user_id":
        logger.info(f"User ID: {value}")
    elif key == "export_date":
        logger.info(f"Export Date: {value}")


def generate_simple_report(stats: Dict[str, Any], output_dir: str) -> Optional[str]:
    """
    Generate a simple HTML report with the statistics.

    Args:
        stats: Statistics from the streaming process
        output_dir: Directory to save the report

    Returns:
        Path to the generated report or None if generation failed
    """
    try:
        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        # Generate timestamp for filename
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = os.path.join(output_dir, f"stream_report_{timestamp}.html")

        # Create HTML content
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Skype Export Streaming Report</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .container {{ max-width: 800px; margin: 0 auto; }}
                .header {{ background-color: #00aff0; color: white; padding: 10px; }}
                .stats {{ margin-top: 20px; }}
                .stat-item {{ margin-bottom: 10px; }}
                .footer {{ margin-top: 30px; font-size: 12px; color: #666; }}
            </style>
        </head>
        <body>
            <div class="container">
                <div class="header">
                    <h1>Skype Export Streaming Report</h1>
                    <p>Generated on {datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
                </div>

                <div class="stats">
                    <h2>Export Statistics</h2>

                    <div class="stat-item">
                        <strong>User ID:</strong> {stats.get('user_id', 'Unknown')}
                    </div>

                    <div class="stat-item">
                        <strong>Export Date:</strong> {stats.get('export_date', 'Unknown')}
                    </div>

                    <div class="stat-item">
                        <strong>Export Time:</strong> {stats.get('export_time', 'Unknown')}
                    </div>

                    <div class="stat-item">
                        <strong>Conversations:</strong> {stats.get('conversation_count', 0):,}
                    </div>

                    <div class="stat-item">
                        <strong>Messages:</strong> {stats.get('message_count', 0):,}
                    </div>

                    <div class="stat-item">
                        <strong>Processing Time:</strong> {stats.get('duration_seconds', 0):.2f} seconds
                    </div>
                </div>

                <div class="footer">
                    <p>This report was generated using the SkypeParser streaming processor.</p>
                </div>
            </div>
        </body>
        </html>
        """

        # Write to file
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(html_content)

        return report_path

    except Exception as e:
        logging.getLogger(__name__).error(f"Failed to generate report: {e}")
        return None


def main() -> None:
    """Main function for the streaming processor."""
    # Parse arguments
    args = parse_arguments()

    # Set up logging
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    # Log start
    logger.info(f"Starting streaming process for {args.file}")
    logger.info(f"User display name: {args.user}")

    # Register core services (no database needed for streaming)
    register_core_services()

    # Get FileHandler service
    file_handler = get_service(FileHandlerProtocol)

    try:
        # Check file existence
        if not os.path.exists(args.file):
            logger.error(f"File not found: {args.file}")
            sys.exit(1)

        # Process using streaming
        logger.info("Starting streaming process...")
        start_time = time.time()

        # Use the streaming function with callback
        stats = parse_skype_data_streaming(
            file_path=args.file, user_display_name=args.user, callback=progress_callback
        )

        # Log results
        processing_time = time.time() - start_time
        logger.info(f"Streaming completed in {processing_time:.2f} seconds")
        logger.info(f"Conversations: {stats['conversation_count']:,}")
        logger.info(f"Messages: {stats['message_count']:,}")

        # Generate report
        report_path = generate_simple_report(stats, args.output)
        if report_path:
            logger.info(f"Report generated: {report_path}")

    except KeyboardInterrupt:
        logger.info("Process interrupted by user")
        sys.exit(1)

    except Exception as e:
        logger.error(f"Error processing file: {e}")
        import traceback

        logger.debug(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
