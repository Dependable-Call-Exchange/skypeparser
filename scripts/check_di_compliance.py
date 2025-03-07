#!/usr/bin/env python
"""
Pre-commit hook to check for direct service instantiation in constructors.
This helps enforce the Dependency Inversion Principle by ensuring services
are injected rather than directly instantiated.
"""

import ast
import sys
from pathlib import Path
from typing import List, Optional, Set, Tuple


# Service classes that should be injected, not instantiated directly
SERVICE_CLASSES = {
    # ETL pipeline components
    'ETLPipeline', 'TestableETLPipeline', 'ETLContext', 'Extractor',
    'Transformer', 'Loader', 'PipelineManager',
    # Database components
    'DatabaseConnection',
    # File components
    'FileHandler', 'FileReader', 'FileWriter',
    # Parser components
    'MessageParser', 'StructuredDataExtractor', 'ContentExtractor',
    # Service classes
    'MessageHandlerFactory'
}

# System utility classes that are acceptable to instantiate
UTILITY_CLASSES = {
    'Path', 'datetime', 'dict', 'list', 'set', 'int', 'str', 'float',
    'OrderedDict', 'defaultdict', 'Counter', 'deque', 'namedtuple',
    'logging.Logger'
}


class DirectInstantiationVisitor(ast.NodeVisitor):
    """AST visitor that finds direct instantiation of service classes in constructors."""

    def __init__(self):
        self.violations = []
        self.current_function = None
        self.is_in_init = False

    def visit_ClassDef(self, node):
        """Visit a class definition."""
        self.generic_visit(node)

    def visit_FunctionDef(self, node):
        """Visit a function definition, tracking if we're in __init__."""
        old_function = self.current_function
        old_in_init = self.is_in_init

        self.current_function = node.name
        self.is_in_init = node.name == '__init__'

        self.generic_visit(node)

        self.current_function = old_function
        self.is_in_init = old_in_init

    def visit_Call(self, node):
        """Visit a function or class call."""
        if not self.is_in_init:
            # We only care about instantiations in __init__
            self.generic_visit(node)
            return

        # Check if this is a class instantiation
        if isinstance(node.func, ast.Name):
            class_name = node.func.id
            if class_name in SERVICE_CLASSES and class_name not in UTILITY_CLASSES:
                self.violations.append({
                    'line': node.lineno,
                    'col': node.col_offset,
                    'class_name': class_name
                })

        self.generic_visit(node)


def check_file(filepath: str) -> List[dict]:
    """Check a file for DI violations."""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    try:
        tree = ast.parse(content)
    except SyntaxError:
        # If we can't parse the file, skip it
        return []

    visitor = DirectInstantiationVisitor()
    visitor.visit(tree)

    return visitor.violations


def main(files: List[str]) -> int:
    """Run the check on all provided files."""
    exit_code = 0

    for filepath in files:
        if not filepath.endswith('.py'):
            continue

        violations = check_file(filepath)

        if violations:
            exit_code = 1
            print(f"DI violations in {filepath}:")
            for v in violations:
                print(f"  Line {v['line']}: Direct instantiation of service class '{v['class_name']}' in constructor")
                print("  Consider injecting this dependency instead of creating it directly")
                print()

    return exit_code


if __name__ == '__main__':
    sys.exit(main(sys.argv[1:]))