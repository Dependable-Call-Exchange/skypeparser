#!/usr/bin/env python3
"""
CLI Command Migration Helper

This script helps users transition from the command-line interfaces of the deprecated
scripts (skype_to_postgres.py and store_skype_export.py) to the new run_etl_pipeline_enhanced.py
script.

Usage:
    python scripts/migrate_cli_commands.py "<old_command>"

Example:
    python scripts/migrate_cli_commands.py "python src/db/skype_to_postgres.py -f export.tar -u 'John Doe' -d skype_db"
"""

import sys
import re
import argparse
import shlex

# Mapping of old command patterns to new command patterns
COMMAND_MAPPINGS = {
    # skype_to_postgres.py
    r"python\s+(?:src/db/|)skype_to_postgres\.py\s+(.*)": "python scripts/run_etl_pipeline_enhanced.py {0}",

    # store_skype_export.py
    r"python\s+(?:src/db/|)store_skype_export\.py\s+(.*)": "python scripts/run_etl_pipeline_enhanced.py {0}",

    # etl_pipeline.py
    r"python\s+(?:src/db/|)etl_pipeline\.py\s+(.*)": "python scripts/run_etl_pipeline_enhanced.py {0}",
}

# Mapping of old arguments to new arguments
ARG_MAPPINGS = {
    # Common arguments
    "-f": "-f",
    "--file": "-f",
    "-u": "-u",
    "--user": "-u",
    "-d": "-d",
    "--database": "-d",
    "-H": "-H",
    "--host": "-H",
    "-P": "-P",
    "--port": "-P",
    "-U": "-U",
    "--username": "-U",
    "-W": "-W",
    "--password": "-W",
    "--create-tables": "--create-tables",
    "--select-json": "--select-json",

    # New arguments with no direct mapping
    "--output": "--output-dir",
    "--output-dir": "--output-dir",
    "--parallel": "--parallel",
    "--memory": "--memory-limit",
    "--memory-limit": "--memory-limit",
    "--batch-size": "--batch-size",
    "--chunk-size": "--chunk-size",
    "--workers": "--max-workers",
    "--max-workers": "--max-workers",
    "--download-attachments": "--download-attachments",
    "--attachments-dir": "--attachments-dir",
    "--no-thumbnails": "--no-thumbnails",
    "--config": "--config",
    "--non-interactive": "--non-interactive",
    "--verbose": "--verbose",
}

def parse_command(command: str) -> tuple:
    """
    Parse a command string into a command and arguments.

    Args:
        command: Command string

    Returns:
        Tuple of (command, args)
    """
    parts = shlex.split(command)
    cmd = parts[0]
    for i, part in enumerate(parts[1:], 1):
        if part.endswith('.py'):
            cmd = f"{cmd} {part}"
            args = parts[i+1:]
            return cmd, args

    return cmd, parts[1:]

def migrate_command(command: str) -> str:
    """
    Migrate a command from the old format to the new format.

    Args:
        command: Old command string

    Returns:
        New command string
    """
    # Check if the command matches any of the patterns
    for pattern, template in COMMAND_MAPPINGS.items():
        match = re.match(pattern, command)
        if match:
            args = match.group(1)
            return template.format(migrate_args(args))

    # If no pattern matches, return the original command
    return command

def migrate_args(args_str: str) -> str:
    """
    Migrate arguments from the old format to the new format.

    Args:
        args_str: Old arguments string

    Returns:
        New arguments string
    """
    # Parse arguments
    args = shlex.split(args_str)
    new_args = []

    i = 0
    while i < len(args):
        arg = args[i]

        # Check if the argument is a flag or an option
        if arg.startswith('-'):
            # Check if the argument has a mapping
            if arg in ARG_MAPPINGS:
                new_arg = ARG_MAPPINGS[arg]
                new_args.append(new_arg)

                # Check if the argument has a value
                if i + 1 < len(args) and not args[i + 1].startswith('-'):
                    new_args.append(args[i + 1])
                    i += 2
                else:
                    i += 1
            else:
                # If no mapping exists, keep the original argument
                new_args.append(arg)

                # Check if the argument has a value
                if i + 1 < len(args) and not args[i + 1].startswith('-'):
                    new_args.append(args[i + 1])
                    i += 2
                else:
                    i += 1
        else:
            # If not a flag or option, keep the original argument
            new_args.append(arg)
            i += 1

    # Join arguments back into a string
    return ' '.join(new_args)

def print_command_comparison(old_command: str, new_command: str):
    """
    Print a comparison of the old and new commands.

    Args:
        old_command: Old command string
        new_command: New command string
    """
    print("Old command:")
    print(f"  {old_command}")
    print("\nNew command:")
    print(f"  {new_command}")
    print("\nChanges:")

    # Parse commands
    old_cmd, old_args = parse_command(old_command)
    new_cmd, new_args = parse_command(new_command)

    # Compare commands
    if old_cmd != new_cmd:
        print(f"  - Command changed from '{old_cmd}' to '{new_cmd}'")

    # Compare arguments
    old_arg_dict = {}
    new_arg_dict = {}

    i = 0
    while i < len(old_args):
        if old_args[i].startswith('-'):
            if i + 1 < len(old_args) and not old_args[i + 1].startswith('-'):
                old_arg_dict[old_args[i]] = old_args[i + 1]
                i += 2
            else:
                old_arg_dict[old_args[i]] = True
                i += 1
        else:
            i += 1

    i = 0
    while i < len(new_args):
        if new_args[i].startswith('-'):
            if i + 1 < len(new_args) and not new_args[i + 1].startswith('-'):
                new_arg_dict[new_args[i]] = new_args[i + 1]
                i += 2
            else:
                new_arg_dict[new_args[i]] = True
                i += 1
        else:
            i += 1

    # Find added, removed, and changed arguments
    added = set(new_arg_dict.keys()) - set(old_arg_dict.keys())
    removed = set(old_arg_dict.keys()) - set(new_arg_dict.keys())
    common = set(old_arg_dict.keys()) & set(new_arg_dict.keys())
    changed = {arg for arg in common if old_arg_dict[arg] != new_arg_dict[arg]}

    if added:
        print("  - Added arguments:")
        for arg in sorted(added):
            print(f"      {arg} = {new_arg_dict[arg]}")

    if removed:
        print("  - Removed arguments:")
        for arg in sorted(removed):
            print(f"      {arg} = {old_arg_dict[arg]}")

    if changed:
        print("  - Changed arguments:")
        for arg in sorted(changed):
            print(f"      {arg}: {old_arg_dict[arg]} -> {new_arg_dict[arg]}")

    # Print mapped arguments
    mapped = {}
    for old_arg, new_arg in ARG_MAPPINGS.items():
        if old_arg in old_arg_dict and new_arg in new_arg_dict:
            mapped[old_arg] = new_arg

    if mapped:
        print("  - Mapped arguments:")
        for old_arg, new_arg in sorted(mapped.items()):
            if old_arg != new_arg:
                print(f"      {old_arg} -> {new_arg}")

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description='Migrate CLI commands from deprecated scripts to run_etl_pipeline_enhanced.py')
    parser.add_argument('command', help='Old command to migrate')
    args = parser.parse_args()

    # Migrate command
    new_command = migrate_command(args.command)

    # Print comparison
    print_command_comparison(args.command, new_command)

    print("\nRecommended next steps:")
    print("  1. Review the new command to ensure it does what you expect")
    print("  2. Run the new command to verify it works correctly")
    print("  3. Update any scripts or documentation that use the old command")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/migrate_cli_commands.py \"<old_command>\"")
        print("Example: python scripts/migrate_cli_commands.py \"python src/db/skype_to_postgres.py -f export.tar -u 'John Doe' -d skype_db\"")
        sys.exit(1)

    main()