#!/usr/bin/env python3
"""
Script to analyze the directory structure of the SkypeParser project.

This script examines the src directory and:
1. Identifies all Python files
2. Categorizes them by their probable domain (messages, files, etc.)
3. Analyzes import statements to create a dependency graph
4. Generates a JSON report with the analysis results

Usage:
    python analyze_directory.py [source_dir] [output_file]

Where:
    source_dir - The directory to analyze (default: src)
    output_file - JSON file to save the analysis (default: reorganization_analysis.json)
"""

import os
import re
import sys
import json
import ast
from pathlib import Path
import logging
from typing import Dict, List, Set, Tuple, Any, Optional
import networkx as nx

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Dictionary to map file paths to domains based on filename patterns
DOMAIN_PATTERNS = {
    'data_handlers': [
        r'.*handler\.py$',
        r'.*_handler\.py$',
        r'handler_registry\.py$',
        r'data_inserter\.py$'
    ],
    'messages': [
        r'message.*\.py$',
        r'.*message.*\.py$'
    ],
    'attachments': [
        r'attachment.*\.py$',
        r'.*attachment.*\.py$'
    ],
    'conversations': [
        r'conversation.*\.py$',
        r'.*conversation.*\.py$'
    ],
    'files': [
        r'file.*\.py$',
        r'.*file.*\.py$',
        r'tar_extractor\.py$'
    ],
    'validation': [
        r'.*validator\.py$',
        r'validation\.py$',
        r'.*validation.*\.py$',
        r'schema_validation\.py$'
    ],
    'monitoring': [
        r'progress_tracker\.py$',
        r'checkpoint_manager\.py$',
        r'memory_monitor\.py$',
        r'phase_manager\.py$'
    ],
    'logging': [
        r'.*logging.*\.py$',
        r'log.*\.py$',
        r'error.*\.py$'
    ],
    'core_utils': [
        r'.*\.py$'  # This is a catch-all pattern for any Python file
    ]
}


def get_imports_from_file(file_path: Path) -> List[Tuple[str, str]]:
    """
    Extract import statements from a Python file.

    Args:
        file_path: Path to the Python file

    Returns:
        List of tuples (module, names) where names is a comma-separated string of imported names
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        try:
            parsed = ast.parse(content)
        except SyntaxError as e:
            logger.error(f"Syntax error in {file_path}: {e}")
            return []

        imports = []

        for node in ast.walk(parsed):
            if isinstance(node, ast.Import):
                for name in node.names:
                    imports.append((name.name, name.asname or name.name))
            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    module = node.module
                    if node.level > 0:  # It's a relative import
                        module = '.' * node.level + module
                    names = [name.name for name in node.names]
                    imports.append((module, ', '.join(names)))

        return imports
    except Exception as e:
        logger.error(f"Error processing imports in {file_path}: {e}")
        return []


def guess_domain(file_path: Path) -> str:
    """
    Guess the domain a file belongs to based on its name and content.

    Args:
        file_path: Path to the file

    Returns:
        The name of the domain (e.g., "messages", "files", etc.)
    """
    file_str = str(file_path)

    # First check filename patterns
    for domain, patterns in DOMAIN_PATTERNS.items():
        # Skip the catch-all domain during pattern matching
        if domain == 'core_utils':
            continue

        for pattern in patterns:
            if re.match(pattern, file_path.name, re.IGNORECASE):
                return domain

    # If no patterns match, check the current directory name
    parent_dir = file_path.parent.name
    if parent_dir == 'handlers':
        return 'data_handlers'

    # Default to utils for any file that doesn't match a specific domain
    return 'core_utils'


def build_dependency_graph(files_info: Dict[str, Dict]) -> nx.DiGraph:
    """
    Build a directed graph of file dependencies.

    Args:
        files_info: Dictionary of file information including imports

    Returns:
        NetworkX DiGraph representing dependencies
    """
    G = nx.DiGraph()

    # Add all files as nodes
    for file_path, info in files_info.items():
        G.add_node(file_path, **info)

    # Add edges for dependencies
    for file_path, info in files_info.items():
        for imported_module, _ in info.get('imports', []):
            # Try to match the import to a file
            for other_file, other_info in files_info.items():
                module_parts = imported_module.split('.')
                if module_parts[-1] in other_file:
                    G.add_edge(file_path, other_file)
                    break

    return G


def analyze_directory(directory: Path) -> Dict[str, Any]:
    """
    Analyze the directory structure and dependencies.

    Args:
        directory: Path to the directory to analyze

    Returns:
        Dictionary with analysis results
    """
    results = {
        'total_files': 0,
        'files_by_domain': {},
        'domains': set(),
        'files_info': {}
    }

    # Initialize domain counts
    for domain in DOMAIN_PATTERNS.keys():
        results['files_by_domain'][domain] = []
        results['domains'].add(domain)

    # Analyze Python files
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.py'):
                file_path = Path(root) / file
                # Use os.path.relpath instead of Path.relative_to to avoid errors
                rel_path = os.path.relpath(file_path, Path.cwd())

                # Skip __init__.py files for now
                if file == '__init__.py':
                    continue

                # Get imports from file
                imports = get_imports_from_file(file_path)

                # Guess which domain this file belongs to
                domain = guess_domain(file_path)

                # Store file info
                results['files_info'][rel_path] = {
                    'domain': domain,
                    'imports': imports,
                    'filename': file,
                    'directory': str(file_path.parent)
                }

                # Update domain lists
                results['files_by_domain'][domain].append(rel_path)
                results['domains'].add(domain)

                results['total_files'] += 1

    # Count files by domain
    domain_counts = {domain: len(files) for domain, files in results['files_by_domain'].items()}
    results['domain_counts'] = domain_counts

    # Build dependency graph
    try:
        dependency_graph = build_dependency_graph(results['files_info'])

        # Convert graph to dictionary for JSON serialization
        results['dependency_graph'] = {
            'nodes': list(dependency_graph.nodes()),
            'edges': list(dependency_graph.edges())
        }

        # Add centrality measures
        results['centrality'] = {
            'degree': nx.degree_centrality(dependency_graph),
            'in_degree': nx.in_degree_centrality(dependency_graph),
            'out_degree': nx.out_degree_centrality(dependency_graph)
        }
    except Exception as e:
        logger.error(f"Error building dependency graph: {e}")
        results['dependency_graph'] = {'nodes': [], 'edges': []}
        results['centrality'] = {}

    return results


def save_results(results: Dict[str, Any], output_file: Path) -> None:
    """
    Save analysis results to a JSON file.

    Args:
        results: Analysis results dictionary
        output_file: Path to save the results
    """
    # Convert sets to lists for JSON serialization
    results['domains'] = list(results['domains'])

    # Convert any Path objects to strings
    def clean_for_json(obj):
        if isinstance(obj, Path):
            return str(obj)
        elif isinstance(obj, dict):
            return {k: clean_for_json(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_for_json(item) for item in obj]
        elif isinstance(obj, set):
            return list(obj)
        else:
            return obj

    cleaned_results = clean_for_json(results)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(cleaned_results, f, indent=2)

    logger.info(f"Analysis results saved to {output_file}")


def generate_reorganization_mapping(results: Dict[str, Any]) -> Dict[str, str]:
    """
    Generate a mapping of source files to target locations.

    Args:
        results: Analysis results dictionary

    Returns:
        Dictionary mapping source file paths to target paths
    """
    mapping = {}

    for file_path, info in results['files_info'].items():
        domain = info['domain']
        filename = info['filename']

        # Skip __init__.py files
        if filename == '__init__.py':
            continue

        # Construct target path
        current = Path(file_path)
        target = Path('src') / domain / filename

        mapping[str(current)] = str(target)

    return mapping


def main():
    """
    Main entry point for the script.
    """
    # Get arguments
    if len(sys.argv) > 1:
        source_dir = Path(sys.argv[1])
    else:
        source_dir = Path('src')

    if len(sys.argv) > 2:
        output_file = Path(sys.argv[2])
    else:
        output_file = Path('reorganization_analysis.json')

    if not source_dir.exists() or not source_dir.is_dir():
        logger.error(f"Directory {source_dir} does not exist or is not a directory")
        sys.exit(1)

    # Analyze directory
    logger.info(f"Analyzing directory {source_dir}...")
    results = analyze_directory(source_dir)

    # Generate reorganization mapping
    mapping = generate_reorganization_mapping(results)
    results['reorganization_mapping'] = mapping

    # Save results
    save_results(results, output_file)

    # Print summary
    logger.info(f"Analysis complete. Found {results['total_files']} Python files.")
    logger.info("Files by domain:")
    for domain, count in results['domain_counts'].items():
        logger.info(f"  {domain}: {count} files")

    logger.info(f"Full results saved to {output_file}")


if __name__ == "__main__":
    main()