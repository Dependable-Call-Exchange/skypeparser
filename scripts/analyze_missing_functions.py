#!/usr/bin/env python3
"""
Script to analyze reorganization test results and identify missing functions.

This script parses the reorganization_test_results.md file, extracts information
about missing imports, and creates a structured report that can be used to update
the add_standalone_functions.py script.
"""

import re
import os
import subprocess
from collections import defaultdict
from typing import Dict, List, Set, Tuple

def run_tests() -> None:
    """Run the reorganization tests and generate fresh results."""
    print("Running reorganization tests...")
    subprocess.run(["python", "scripts/test_reorganization.py"], check=False)
    print("Tests completed. Analyzing results...")

def parse_test_results() -> Dict[str, List[Tuple[str, str]]]:
    """
    Parse the reorganization_test_results.md file and extract information about missing imports.

    Returns:
        A dictionary where keys are source modules trying to import,
        and values are lists of (missing_function, target_module) tuples.
    """
    results_file = "reorganization_test_results.md"
    if not os.path.exists(results_file):
        print(f"Error: {results_file} not found. Please run the reorganization tests first.")
        return {}

    missing_imports = defaultdict(list)

    with open(results_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Extract import errors using regex
    import_error_pattern = r'\| (src\.[a-zA-Z_.]+) \| ❌ Error: cannot import name \'([a-zA-Z_]+)\' from \'(src\.[a-zA-Z_.]+)\''
    matches = re.findall(import_error_pattern, content)

    for source_module, missing_function, target_module in matches:
        missing_imports[source_module].append((missing_function, target_module))

    return missing_imports

def analyze_missing_functions(missing_imports: Dict[str, List[Tuple[str, str]]]) -> Dict[str, Set[str]]:
    """
    Group missing functions by their target modules.

    Returns:
        A dictionary where keys are target modules where functions should be added,
        and values are sets of function names to add.
    """
    functions_by_module = defaultdict(set)

    for source_imports in missing_imports.values():
        for function_name, target_module in source_imports:
            functions_by_module[target_module].add(function_name)

    return functions_by_module

def generate_report(missing_imports: Dict[str, List[Tuple[str, str]]],
                   functions_by_module: Dict[str, Set[str]]) -> None:
    """Generate a report summarizing missing functions."""
    if not missing_imports:
        print("No import errors found.")
        return

    print("\n=== Missing Functions Report ===\n")

    # Summary by target module
    print("== Functions to add by module ==")
    for target_module, functions in functions_by_module.items():
        print(f"\n{target_module}:")
        for function in sorted(functions):
            print(f"  - {function}")

    # Detailed import dependencies
    print("\n\n== Import dependencies by source module ==")
    for source_module, imports in sorted(missing_imports.items()):
        print(f"\n{source_module} requires:")
        grouped_by_target = defaultdict(list)
        for function, target in imports:
            grouped_by_target[target].append(function)

        for target, functions in grouped_by_target.items():
            function_list = ", ".join(functions)
            print(f"  - {function_list} from {target}")

    # Generate MODULE_CONFIG snippets
    print("\n\n== Suggested MODULE_CONFIG additions ==")
    for target_module, functions in functions_by_module.items():
        # Extract module filename from path
        filename = os.path.basename(target_module.replace('.', '/'))

        # Get class name based on module name (make an educated guess)
        module_name = target_module.split('.')[-1]
        words = re.findall(r'[A-Za-z][a-z]*', module_name.replace('_', ' ').title().replace(' ', ''))
        class_name = ''.join(words)

        if module_name.endswith('_monitor'):
            class_name = 'MemoryMonitor'
        elif module_name == 'structured_logging':
            class_name = 'StructuredFormatter'
        elif module_name == 'reporting':
            class_name = 'SkypeReportGenerator'

        # Generate instance variable from class name
        instance_var = f"_global_{module_name}"
        instance_creation = f"{class_name}()"

        print(f"\n# For {target_module}:")
        print(f'    "{target_module.replace(".", "/")}.py": {{')
        print(f'        "class_name": "{class_name}",')
        print(f'        "instance_var": "{instance_var}",')
        print(f'        "instance_creation": "{instance_creation}",')
        print('        "methods_to_wrap": [')
        for function in sorted(functions):
            print(f'            "{function}",')
        print('        ],')
        print('    },')

def main():
    """Main function."""
    # Run tests to get fresh results
    run_tests()

    # Parse test results
    missing_imports = parse_test_results()

    # Analyze missing functions
    functions_by_module = analyze_missing_functions(missing_imports)

    # Generate report
    generate_report(missing_imports, functions_by_module)

    print("\nReport completed. Use this information to update scripts/add_standalone_functions.py")
    print("After updating the configuration, run the script again to add the missing functions.")

if __name__ == "__main__":
    main()