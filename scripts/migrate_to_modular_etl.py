#!/usr/bin/env python3
"""
Migration Script for Modular ETL Pipeline

This script helps users migrate from the old SkypeETLPipeline to the new modular ETL pipeline.
It scans Python files for imports and usage of the old ETL pipeline and suggests replacements.
"""

import os
import re
import argparse
import logging
from typing import List, Dict, Any, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Patterns to search for
IMPORT_PATTERN = re.compile(r'from\s+src\.db\.etl_pipeline\s+import\s+SkypeETLPipeline')
IMPORT_AS_PATTERN = re.compile(r'from\s+src\.db\.etl_pipeline\s+import\s+SkypeETLPipeline\s+as\s+(\w+)')
USAGE_PATTERN = re.compile(r'(\w+)\s*=\s*SkypeETLPipeline\(')

# Replacements
IMPORT_REPLACEMENT = 'from src.db import ETLPipeline'
USAGE_REPLACEMENT = '{} = ETLPipeline('

def scan_file(file_path: str) -> Dict[str, Any]:
    """
    Scan a file for imports and usage of the old ETL pipeline.

    Args:
        file_path: Path to the file to scan

    Returns:
        Dict containing information about the file and suggested replacements
    """
    with open(file_path, 'r') as f:
        content = f.read()

    # Find imports
    imports = IMPORT_PATTERN.findall(content)
    imports_as = IMPORT_AS_PATTERN.findall(content)

    # Find usage
    usage = USAGE_PATTERN.findall(content)

    return {
        'file_path': file_path,
        'imports': len(imports),
        'imports_as': imports_as,
        'usage': usage,
        'has_old_etl': bool(imports or imports_as or usage)
    }

def scan_directory(directory: str) -> List[Dict[str, Any]]:
    """
    Scan a directory for Python files using the old ETL pipeline.

    Args:
        directory: Directory to scan

    Returns:
        List of dictionaries containing information about each file
    """
    results = []

    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = os.path.join(root, file)
                result = scan_file(file_path)
                if result['has_old_etl']:
                    results.append(result)

    return results

def generate_migration_report(results: List[Dict[str, Any]]) -> str:
    """
    Generate a migration report based on the scan results.

    Args:
        results: List of dictionaries containing information about each file

    Returns:
        String containing the migration report
    """
    if not results:
        return "No files found using the old ETL pipeline."

    report = "Migration Report\n"
    report += "===============\n\n"
    report += f"Found {len(results)} files using the old ETL pipeline:\n\n"

    for result in results:
        report += f"File: {result['file_path']}\n"

        if result['imports']:
            report += "  - Replace import:\n"
            report += "    from src.db.etl_pipeline import SkypeETLPipeline\n"
            report += "    with:\n"
            report += "    from src.db import ETLPipeline\n\n"

        if result['imports_as']:
            report += "  - Replace import as:\n"
            for alias in result['imports_as']:
                report += f"    from src.db.etl_pipeline import SkypeETLPipeline as {alias}\n"
                report += "    with:\n"
                report += f"    from src.db import ETLPipeline as {alias}\n\n"

        if result['usage']:
            report += "  - Replace usage:\n"
            for var in result['usage']:
                report += f"    {var} = SkypeETLPipeline(...)\n"
                report += "    with:\n"
                report += f"    {var} = ETLPipeline(...)\n\n"

        report += "  - Update configuration parameters:\n"
        report += "    - The new ETLPipeline accepts a 'context' parameter for advanced configuration\n"
        report += "    - The 'db_config' parameter is a dictionary instead of individual parameters\n"
        report += "    - The 'performance_config' parameter is replaced by individual parameters\n\n"

        report += "  - Update method calls:\n"
        report += "    - The new ETLPipeline has a slightly different API\n"
        report += "    - Check the documentation for details\n\n"

    report += "For more information, see the migration guide in docs/MIGRATION.md\n"

    return report

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Migrate from old ETL pipeline to new modular ETL pipeline')
    parser.add_argument('directory', help='Directory to scan')
    parser.add_argument('--output', help='Output file for migration report')
    args = parser.parse_args()

    logger.info(f"Scanning directory: {args.directory}")
    results = scan_directory(args.directory)

    report = generate_migration_report(results)

    if args.output:
        with open(args.output, 'w') as f:
            f.write(report)
        logger.info(f"Migration report written to: {args.output}")
    else:
        print(report)

if __name__ == '__main__':
    main()