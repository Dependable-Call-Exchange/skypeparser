#!/usr/bin/env python3
"""
Script to automatically add standalone wrapper functions for class methods.

This script analyzes Python files, identifies class methods, and generates
corresponding standalone functions that wrap the class methods.

Improvements include:
- Incremental syntax error detection and fixing before AST parsing.
- Enhanced function signature extraction with proper handling of defaults and annotations.
- Logging and handling for newly detected syntax errors.
"""

import os
import re
import ast
from typing import Dict, List, Tuple, Optional, Set

# Configuration for each module
MODULE_CONFIG = {
    "src/analysis/queries.py": {
        "class_name": "SkypeQueryExamples",
        "instance_var": "_global_query_examples",
        "instance_creation": "SkypeQueryExamples()",
        "methods_to_wrap": [
            "get_attachment_statistics",
        ],
    },
    "src/analysis/reporting.py": {
        "class_name": "SkypeReportGenerator",
        "instance_var": "_global_report_generator",
        "instance_creation": "SkypeReportGenerator()",
        "methods_to_wrap": [
            "get_export_summary",
            "get_conversation_statistics",
            "get_message_type_distribution",
            "get_activity_by_hour",
        ],
    },
    "src/monitoring/memory_monitor.py": {
        "class_name": "MemoryMonitor",
        "instance_var": "_global_memory_monitor",
        "instance_creation": "MemoryMonitor()",
        "methods_to_wrap": [
            "check_memory",
            "get_peak_memory",
            "reset_peak_memory",
        ],
    },
    "src/monitoring/checkpoint_manager.py": {
        # These are already handled manually
    },
}

# Special case for structured_logging.py - decorator is a function not a class method
STANDALONE_FUNCTIONS = {
    "src/logging/structured_logging.py": [
        {
            "name": "decorator",
            "wrapper_code": """
def decorator(func):
    \"\"\"
    Decorator for adding logging capabilities to functions.
    This is a convenience function that forwards to the timing_decorator
    function in the structured_logging module.

    Args:
        func: The function to decorate

    Returns:
        Decorated function
    \"\"\"
    from src.logging.structured_logging import timing_decorator
    return timing_decorator()(func)
"""
        }
    ]
}

# Predefined files and patterns with known syntax errors to fix
SYNTAX_ERRORS = {
    "src/monitoring/memory_monitor.py": [
        {
            "pattern": r"(\s*=\s*\d+\))",
            "fix_type": "unmatched_paren"
        },
        {
            "pattern": r"from src\.logging\.new_structured_logging import \(\n\n# Global instance for standalone functions\n_global_memory_monitor = None\n\n",
            "replacement": "from src.logging.new_structured_logging import (\n\n",
            "fix_type": "custom"
        }
    ]
}

########################
# Helper: Try Parsing
########################
def try_parse_file(file_path: str) -> ast.Module:
    """
    Parse a Python file into an AST. Raises SyntaxError if file
    cannot be parsed.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    return ast.parse(content, filename=file_path)


########################
# Syntax Fixing
########################
def fix_unmatched_parenthesis(line: str) -> str:
    """
    Attempt to remove one unmatched ')' if the line has more closing
    parentheses than opening parentheses.
    """
    open_count = line.count("(")
    close_count = line.count(")")
    while close_count > open_count and line.endswith(")"):
        line = line[:-1]
        close_count -= 1
    return line

def apply_known_fixes(file_path: str, lines: List[str]) -> bool:
    """
    Applies any known, predefined pattern fixes (SYNTAX_ERRORS dict)
    to the given file.
    Returns True if any fix was made, otherwise False.
    """
    changed = False
    if file_path in SYNTAX_ERRORS:
        for err in SYNTAX_ERRORS[file_path]:
            pattern = err["pattern"]
            fix_type = err["fix_type"]

            if fix_type == "custom" and "replacement" in err:
                # Handle multi-line patterns with custom replacement
                content = '\n'.join(lines)
                if re.search(pattern, content, re.MULTILINE):
                    new_content = re.sub(pattern, err["replacement"], content, flags=re.MULTILINE)
                    if new_content != content:
                        print(f"Fixed custom syntax error in {file_path}")
                        new_lines = new_content.split('\n')
                        lines.clear()
                        lines.extend(new_lines)
                        changed = True
            else:
                # Handle single-line patterns
                for i, line in enumerate(lines):
                    match = re.search(pattern, line)
                    if match:
                        old_line = line
                        if fix_type == "unmatched_paren":
                            line = fix_unmatched_parenthesis(line)
                        # Potential room for other fix types here

                        if line != old_line:
                            lines[i] = line
                            print(f"Fixed syntax error in {file_path} (predefined pattern).")
                            print(f"  Old: {old_line.strip()}")
                            print(f"  New: {line.strip()}")
                            changed = True
    return changed

def attempt_incremental_syntax_fixes(file_path: str) -> None:
    """
    Attempt to fix syntax errors incrementally before AST parsing.
    We repeatedly check for syntax errors. If encountered, we
    apply known or discovered fixes, then re-check, up to a max attempt count.
    """
    max_attempts = 5
    attempt = 0

    while attempt < max_attempts:
        attempt += 1
        try:
            # Test parse
            try_parse_file(file_path)
            # If parsing succeeded, break the loop
            return
        except SyntaxError as e:
            print(f"SyntaxError in {file_path} at line {e.lineno}: {e.msg}")
            # Read lines
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.read().split('\n')

            # If the line is in range, attempt a naive unmatched parenthesis fix
            # or apply known pattern fixes
            changed = False
            if apply_known_fixes(file_path, lines):
                changed = True
            else:
                # Attempt naive fix for unmatched closing parenthesis
                line_index = e.lineno - 1
                if 0 <= line_index < len(lines):
                    old_line = lines[line_index]
                    new_line = fix_unmatched_parenthesis(lines[line_index])
                    if new_line != old_line:
                        print("Applied naive unmatched parenthesis fix:")
                        print(f"  Old: {old_line}")
                        print(f"  New: {new_line}")
                        lines[line_index] = new_line
                        changed = True

            if changed:
                # Write updated lines
                with open(file_path, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(lines))
            else:
                # We can't fix it automatically; log and break
                print(f"No known fix for syntax error in {file_path} line {e.lineno}.")
                break
    # If we exit the while loop, either we succeeded or we gave up.


########################
# AST-based Helpers
########################
def find_class_method(tree: ast.Module, class_name: str, method_name: str) -> Optional[ast.FunctionDef]:
    """Find a method in a class by name."""
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, ast.FunctionDef) and item.name == method_name:
                    return item
    return None

def get_method_signature(method: ast.FunctionDef) -> Tuple[List[str], List[Optional[ast.expr]], List[Optional[ast.expr]]]:
    """
    Extract method signature information: argument names, default values, and annotations.
    Skips 'self'. Returns lists that line up: args[i], defaults[i], annotations[i].
    Note: defaults[i] or annotations[i] can be None if not present.
    """
    args = []
    defaults = []
    annotations = []

    # total arguments excluding 'self'
    arg_names = [a.arg for a in method.args.args if a.arg != "self"]

    # Identify default values
    total_args = len(method.args.args)
    defaults_count = len(method.args.defaults)
    # E.g. if we have 3 total args (including self), 2 defaults, etc.

    # Collect arg objects for skipping self
    shifted_defaults = [None] * (len(arg_names) - defaults_count) + method.args.defaults

    # Build parallel lists
    idx = 0
    for a in method.args.args:
        if a.arg == "self":
            continue
        args.append(a.arg)
        ann = getattr(a, "annotation", None)
        annotations.append(ann)
        defaults.append(shifted_defaults[idx] if idx < len(shifted_defaults) else None)
        idx += 1

    return args, defaults, annotations

def get_method_docstring(method: ast.FunctionDef) -> str:
    """Extract docstring from a method."""
    return ast.get_docstring(method) or ""

########################
# Generate Standalone Function
########################
def generate_standalone_function(
    file_path: str,
    class_name: str,
    method_name: str,
    instance_var: str,
    instance_creation: str
) -> str:
    """
    Generate a standalone function that wraps a class method in the given file.
    Returns an empty string if the method is not found or parse fails.
    """
    try:
        tree = try_parse_file(file_path)
    except SyntaxError:
        print(f"Cannot parse {file_path} even after incremental fixes. Skipping.")
        return ""

    method = find_class_method(tree, class_name, method_name)
    if not method:
        print(f"Method {method_name} not found in class {class_name} (file: {file_path}).")
        return ""

    args, defaults, annotations = get_method_signature(method)
    docstring = get_method_docstring(method)

    # Construct function signature
    params_list = []
    for i, arg in enumerate(args):
        param_str = arg
        # Add type annotation if present
        if annotations[i]:
            param_str += f": {ast.unparse(annotations[i])}"
        # Add default value if present
        if defaults[i]:
            param_str += f" = {ast.unparse(defaults[i])}"
        params_list.append(param_str)

    signature = ", ".join(params_list)

    # Return annotation
    returns_str = ""
    if method.returns is not None:
        returns_str = f" -> {ast.unparse(method.returns)}"

    # Construct call arg string
    args_str = ", ".join(args)

    # Generate the standalone function code
    function_code = f"""
def {method_name}({signature}){returns_str}:
    \"\"\"
    {docstring}

    This is a convenience function that creates a {class_name} instance
    and calls its {method_name} method.
    \"\"\"
    global {instance_var}
    if {instance_var} is None:
        {instance_var} = {instance_creation}
    return {instance_var}.{method_name}({args_str})
"""
    return function_code

########################
# File Modification Helpers
########################
def add_function_to_file(file_path: str, function_code: str) -> None:
    """
    Append the new function to the file if it doesn't already exist.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    if function_code.strip() in content:
        # Already there
        print(f"Function already exists in {file_path}, skipping insertion.")
        return

    # Append
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
        f.write("\n")
        f.write(function_code)

def add_global_instance_if_needed(file_path: str, instance_var: str) -> None:
    """
    Insert a global instance variable if it's not found in the file.
    Placed just after the last import statement.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    if f"{instance_var} = " in content:
        return  # Already defined

    import_pattern = r'(^import .*$|^from .* import .*$)'
    matches = list(re.finditer(import_pattern, content, re.MULTILINE))
    insertion_text = f"\n\n# Global instance for standalone functions\n{instance_var} = None\n"

    if not matches:
        # No imports, just put it at the top after docstring
        # Attempt to find end of docstring or beginning of file
        docstring_pattern = r'("""[\s\S]*?"""|\'\'\'[\s\S]*?\'\'\')'
        docstring_match = re.search(docstring_pattern, content)
        if docstring_match:
            insert_pos = docstring_match.end()
        else:
            insert_pos = 0
        new_content = content[:insert_pos] + insertion_text + content[insert_pos:]
    else:
        last_import_end = matches[-1].end()
        new_content = content[:last_import_end] + insertion_text + content[last_import_end:]

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(new_content)

########################
# Main Logic
########################
def add_custom_standalone_functions():
    """Add custom standalone functions that aren't class methods."""
    for file_path, functions in STANDALONE_FUNCTIONS.items():
        if not os.path.exists(file_path):
            print(f"File {file_path} does not exist, skipping.")
            continue

        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        for func in functions:
            if func["name"] + "(" in content:
                print(f"Function {func['name']} already exists in {file_path}, skipping.")
                continue

            print(f"Adding custom standalone function {func['name']} to {file_path}")
            with open(file_path, 'a', encoding='utf-8') as f:
                f.write("\n")
                f.write(func["wrapper_code"])

def main():
    """
    Main function:
    1. Incrementally fix syntax errors.
    2. Re-parse files to confirm correctness.
    3. Generate standalone wrappers for specified modules/methods.
    4. Add custom standalone functions for special cases.
    """
    # Step 1: Attempt syntax fixes for all known files
    all_files = set(SYNTAX_ERRORS.keys()) | set(MODULE_CONFIG.keys()) | set(STANDALONE_FUNCTIONS.keys())
    for file_path in all_files:
        if not os.path.exists(file_path):
            print(f"File {file_path} does not exist, skipping.")
            continue
        attempt_incremental_syntax_fixes(file_path)

    # Step 2: Generate standalone functions for each module
    for file_path, config in MODULE_CONFIG.items():
        if not config:
            print(f"Skipping {file_path} as it's already handled manually.")
            continue
        if not os.path.exists(file_path):
            print(f"File {file_path} does not exist, skipping.")
            continue

        class_name = config["class_name"]
        instance_var = config["instance_var"]
        instance_creation = config["instance_creation"]
        methods_to_wrap = config["methods_to_wrap"]

        # Ensure global instance variable is present
        add_global_instance_if_needed(file_path, instance_var)

        # For each method in the config
        for method_name in methods_to_wrap:
            function_code = generate_standalone_function(
                file_path, class_name, method_name, instance_var, instance_creation
            )
            if function_code.strip():
                print(f"Adding standalone function {method_name} to {file_path}")
                add_function_to_file(file_path, function_code)

    # Step 3: Add custom standalone functions
    add_custom_standalone_functions()

if __name__ == "__main__":
    main()