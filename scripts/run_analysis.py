#!/usr/bin/env python3
"""
Skype Parser Analysis Script

This script provides a command-line interface for generating reports and visualizations
from Skype data that has been processed by the ETL pipeline.
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from typing import Dict, Any, Optional

# Add the parent directory to the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.analysis.reporting import SkypeReportGenerator
from src.analysis.visualization import SkypeDataVisualizer
from src.analysis.queries import SkypeQueryExamples
from src.utils.config import load_config, get_db_config
from src.utils.di import get_service
from src.utils.interfaces import DatabaseConnectionProtocol
from src.utils.service_registry import register_all_services

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('skype_analysis.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments.

    Returns:
        An argparse.Namespace object containing the parsed arguments.
    """
    parser = argparse.ArgumentParser(description='Generate reports and visualizations from Skype data')

    parser.add_argument('--export-id', type=int, required=True,
                        help='The ID of the export to analyze')

    parser.add_argument('--output-dir', type=str, default='output/analysis',
                        help='Directory to save output files (default: output/analysis)')

    parser.add_argument('--config', type=str, default='config/config.json',
                        help='Path to configuration file (default: config/config.json)')

    parser.add_argument('--report-type', type=str, choices=['summary', 'full', 'custom'], default='full',
                        help='Type of report to generate (default: full)')

    parser.add_argument('--visualize', action='store_true',
                        help='Generate visualizations')

    parser.add_argument('--query', type=str,
                        help='Run a specific query (e.g., "find_conversations_with_keyword:hello")')

    parser.add_argument('--format', type=str, choices=['json', 'csv', 'text'], default='json',
                        help='Output format for reports (default: json)')

    parser.add_argument('--verbose', action='store_true',
                        help='Enable verbose output')

    return parser.parse_args()

def setup_environment(config_path: str, verbose: bool = False) -> None:
    """
    Set up the environment for analysis.

    Args:
        config_path: Path to the configuration file.
        verbose: Whether to enable verbose output.
    """
    # Set logging level
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Load configuration
    config = load_config(config_path)
    db_config = get_db_config(config)

    # Register services
    register_all_services(db_config=db_config)

    logger.info("Environment set up successfully")

def generate_report(export_id: int, report_type: str, output_dir: str, output_format: str) -> Dict[str, Any]:
    """
    Generate a report for a Skype export.

    Args:
        export_id: The ID of the export to analyze.
        report_type: The type of report to generate ('summary', 'full', 'custom').
        output_dir: Directory to save output files.
        output_format: Output format for reports ('json', 'csv', 'text').

    Returns:
        A dictionary containing the report data.
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Create report generator
    report_generator = SkypeReportGenerator()

    # Generate report
    if report_type == 'summary':
        report = report_generator.get_export_summary(export_id)
    elif report_type == 'full':
        report = report_generator.generate_full_report(export_id)
    else:  # custom
        # For custom reports, we'll include a subset of the full report
        report = {
            "summary": report_generator.get_export_summary(export_id),
            "message_type_distribution": report_generator.get_message_type_distribution(export_id),
            "top_senders": report_generator.get_top_senders(export_id)
        }

    # Save report to file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    if output_format == 'json':
        output_path = os.path.join(output_dir, f"report_{export_id}_{timestamp}.json")
        with open(output_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
    elif output_format == 'csv':
        # For CSV, we'll create separate files for each section of the report
        for section_name, section_data in report.items():
            if isinstance(section_data, list):
                output_path = os.path.join(output_dir, f"{section_name}_{export_id}_{timestamp}.csv")
                with open(output_path, 'w') as f:
                    if section_data:
                        # Write header
                        f.write(','.join(section_data[0].keys()) + '\n')
                        # Write data
                        for item in section_data:
                            f.write(','.join(str(value) for value in item.values()) + '\n')
            elif isinstance(section_data, dict):
                output_path = os.path.join(output_dir, f"{section_name}_{export_id}_{timestamp}.csv")
                with open(output_path, 'w') as f:
                    # Write header and data
                    f.write('key,value\n')
                    for key, value in section_data.items():
                        f.write(f"{key},{value}\n")
    else:  # text
        output_path = os.path.join(output_dir, f"report_{export_id}_{timestamp}.txt")
        with open(output_path, 'w') as f:
            f.write(f"Skype Export Analysis Report\n")
            f.write(f"Export ID: {export_id}\n")
            f.write(f"Generated: {timestamp}\n\n")

            for section_name, section_data in report.items():
                f.write(f"=== {section_name.upper()} ===\n\n")
                if isinstance(section_data, list):
                    for item in section_data:
                        for key, value in item.items():
                            f.write(f"{key}: {value}\n")
                        f.write("\n")
                elif isinstance(section_data, dict):
                    for key, value in section_data.items():
                        f.write(f"{key}: {value}\n")
                f.write("\n")

    logger.info(f"Report saved to {output_path}")

    return report

def generate_visualizations(export_id: int, output_dir: str) -> Dict[str, Optional[str]]:
    """
    Generate visualizations for a Skype export.

    Args:
        export_id: The ID of the export to visualize.
        output_dir: Directory to save output files.

    Returns:
        A dictionary mapping visualization names to file paths.
    """
    # Create output directory if it doesn't exist
    visualization_dir = os.path.join(output_dir, 'visualizations')
    os.makedirs(visualization_dir, exist_ok=True)

    # Create visualizer
    visualizer = SkypeDataVisualizer(output_dir=visualization_dir)

    # Generate visualizations
    visualizations = visualizer.generate_all_visualizations(export_id)

    logger.info(f"Visualizations saved to {visualization_dir}")

    return visualizations

def run_query(export_id: int, query_string: str, output_dir: str, output_format: str) -> Dict[str, Any]:
    """
    Run a specific query on a Skype export.

    Args:
        export_id: The ID of the export to analyze.
        query_string: The query to run (e.g., "find_conversations_with_keyword:hello").
        output_dir: Directory to save output files.
        output_format: Output format for reports ('json', 'csv', 'text').

    Returns:
        A dictionary containing the query results.
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Create query examples
    query_examples = SkypeQueryExamples()

    # Parse query string
    parts = query_string.split(':', 1)
    query_name = parts[0]
    query_args = parts[1] if len(parts) > 1 else None

    # Run query
    result = None

    if query_name == 'find_conversations_with_keyword':
        if query_args:
            result = query_examples.find_conversations_with_keyword(export_id, query_args)
        else:
            logger.error("Keyword required for find_conversations_with_keyword query")
            return {"error": "Keyword required for find_conversations_with_keyword query"}
    elif query_name == 'get_conversation_timeline':
        if query_args:
            parts = query_args.split(',')
            conversation_id = parts[0]
            interval = parts[1] if len(parts) > 1 else 'day'
            result = query_examples.get_conversation_timeline(export_id, conversation_id, interval)
        else:
            logger.error("Conversation ID required for get_conversation_timeline query")
            return {"error": "Conversation ID required for get_conversation_timeline query"}
    elif query_name == 'get_user_activity_timeline':
        if query_args:
            parts = query_args.split(',')
            sender_name = parts[0]
            interval = parts[1] if len(parts) > 1 else 'day'
            result = query_examples.get_user_activity_timeline(export_id, sender_name, interval)
        else:
            logger.error("Sender name required for get_user_activity_timeline query")
            return {"error": "Sender name required for get_user_activity_timeline query"}
    elif query_name == 'get_conversation_participants':
        if query_args:
            result = query_examples.get_conversation_participants(export_id, query_args)
        else:
            logger.error("Conversation ID required for get_conversation_participants query")
            return {"error": "Conversation ID required for get_conversation_participants query"}
    elif query_name == 'get_message_length_by_sender':
        limit = int(query_args) if query_args and query_args.isdigit() else 10
        result = query_examples.get_message_length_by_sender(export_id, limit)
    elif query_name == 'get_attachment_statistics':
        result = query_examples.get_attachment_statistics(export_id)
    else:
        logger.error(f"Unknown query: {query_name}")
        return {"error": f"Unknown query: {query_name}"}

    # Save result to file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    if output_format == 'json':
        output_path = os.path.join(output_dir, f"query_{query_name}_{export_id}_{timestamp}.json")
        with open(output_path, 'w') as f:
            json.dump(result, f, indent=2, default=str)
    elif output_format == 'csv':
        output_path = os.path.join(output_dir, f"query_{query_name}_{export_id}_{timestamp}.csv")
        with open(output_path, 'w') as f:
            if isinstance(result, list) and result:
                # Write header
                f.write(','.join(result[0].keys()) + '\n')
                # Write data
                for item in result:
                    f.write(','.join(str(value) for value in item.values()) + '\n')
            elif isinstance(result, dict):
                # Write header and data
                f.write('key,value\n')
                for key, value in result.items():
                    if isinstance(value, list):
                        # For nested lists, we'll create a separate section
                        f.write(f"{key},<see below>\n")
                        if value:
                            f.write('\n')
                            f.write(','.join(value[0].keys()) + '\n')
                            for item in value:
                                f.write(','.join(str(v) for v in item.values()) + '\n')
                    else:
                        f.write(f"{key},{value}\n")
    else:  # text
        output_path = os.path.join(output_dir, f"query_{query_name}_{export_id}_{timestamp}.txt")
        with open(output_path, 'w') as f:
            f.write(f"Skype Export Query Results\n")
            f.write(f"Query: {query_string}\n")
            f.write(f"Export ID: {export_id}\n")
            f.write(f"Generated: {timestamp}\n\n")

            if isinstance(result, list):
                for item in result:
                    for key, value in item.items():
                        f.write(f"{key}: {value}\n")
                    f.write("\n")
            elif isinstance(result, dict):
                for key, value in result.items():
                    if isinstance(value, list):
                        f.write(f"{key}:\n")
                        for item in value:
                            f.write("  ")
                            if isinstance(item, dict):
                                f.write(", ".join(f"{k}: {v}" for k, v in item.items()))
                            else:
                                f.write(str(item))
                            f.write("\n")
                    else:
                        f.write(f"{key}: {value}\n")

    logger.info(f"Query results saved to {output_path}")

    return {"query": query_string, "result": result}

def main() -> None:
    """
    Main function for the analysis script.
    """
    # Parse command-line arguments
    args = parse_args()

    try:
        # Set up environment
        setup_environment(args.config, args.verbose)

        # Create output directory if it doesn't exist
        os.makedirs(args.output_dir, exist_ok=True)

        # Run analysis based on arguments
        if args.query:
            # Run specific query
            result = run_query(args.export_id, args.query, args.output_dir, args.format)
            logger.info(f"Query executed successfully")
        else:
            # Generate report
            report = generate_report(args.export_id, args.report_type, args.output_dir, args.format)
            logger.info(f"Report generated successfully")

            # Generate visualizations if requested
            if args.visualize:
                visualizations = generate_visualizations(args.export_id, args.output_dir)
                logger.info(f"Visualizations generated successfully")

        logger.info("Analysis completed successfully")

    except Exception as e:
        logger.error(f"Error during analysis: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()