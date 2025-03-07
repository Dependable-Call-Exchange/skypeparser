#!/usr/bin/env python3
"""
Master script to run all fix scripts.

This script runs all the fix scripts to address the issues in the TestableETLPipeline
class and related files.
"""

import os
import sys
import argparse
import importlib.util
import subprocess
from typing import List, Dict, Any, Optional, Tuple

def import_script(script_path: str) -> Optional[Any]:
    """
    Import a Python script module.

    Args:
        script_path: Path to the script to import

    Returns:
        The imported module or None if import fails
    """
    try:
        spec = importlib.util.spec_from_file_location("module.name", script_path)
        if spec is None or spec.loader is None:
            print(f"Error: Could not load spec for {script_path}")
            return None

        module = importlib.util.module_from_spec(spec)
        if module is None:
            print(f"Error: Could not create module from spec for {script_path}")
            return None

        spec.loader.exec_module(module)
        return module
    except Exception as e:
        print(f"Error importing {script_path}: {e}")
        return None

def run_script_directly(script_path: str, args: List[str]) -> int:
    """
    Run a Python script as a subprocess.

    Args:
        script_path: Path to the script to run
        args: Arguments to pass to the script

    Returns:
        The exit code from the script
    """
    print(f"Running {script_path}...")

    # Check if the script exists
    if not os.path.exists(script_path):
        print(f"Script not found: {script_path}")
        return 1

    # Build the command
    cmd = [sys.executable, script_path] + args

    try:
        # Run the script
        result = subprocess.run(cmd, check=False)
        print(f"Finished running {script_path} with exit code {result.returncode}")
        return result.returncode
    except Exception as e:
        print(f"Error running {script_path}: {e}")
        return 1

def run_script(script_path: str, args: List[str], use_subprocess: bool = False) -> int:
    """
    Run a Python script.

    Args:
        script_path: Path to the script to run
        args: Arguments to pass to the script
        use_subprocess: Whether to run the script as a subprocess

    Returns:
        The exit code from the script (0 for success, non-zero for error)
    """
    # Check if the script exists
    if not os.path.exists(script_path):
        print(f"Script not found: {script_path}")
        return 1

    if use_subprocess:
        return run_script_directly(script_path, args)

    # Import the script
    module = import_script(script_path)
    if module is None:
        return 1

    # Set up sys.argv for the module
    old_argv = sys.argv
    sys.argv = [script_path] + args

    try:
        # Call the main function
        print(f"Running {script_path}...")
        if hasattr(module, "main"):
            result = module.main()
            print(f"Finished running {script_path}")
            return result if isinstance(result, int) else 0
        else:
            print(f"No main function found in {script_path}")
            return 1
    except Exception as e:
        print(f"Error running {script_path}: {e}")
        return 1
    finally:
        # Restore sys.argv
        sys.argv = old_argv

def main():
    """Main function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Run all fix scripts.")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without modifying files")
    parser.add_argument("--verbose", action="store_true", help="Show verbose output")
    parser.add_argument("--dir", default=None, help="Directory to search for files (default: src and tests directories)")
    parser.add_argument("--file", default=None, help="Specific file to fix")
    parser.add_argument("--subprocess", action="store_true", help="Run scripts as subprocesses")
    parser.add_argument("--skip", nargs="+", choices=["etl", "testable", "test"], help="Skip specific scripts")
    args = parser.parse_args()

    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    print("Starting all fixes...")

    # Define the scripts to run with their arguments
    scripts = []

    # Add scripts unless they're explicitly skipped
    if args.skip is None or "etl" not in args.skip:
        script_args = []
        if args.dry_run:
            script_args.append("--dry-run")
        if args.verbose:
            script_args.append("--verbose")
        if args.dir:
            script_args.extend(["--dir", args.dir])
        if args.file:
            script_args.extend(["--file", args.file])

        scripts.append((os.path.join(script_dir, "fix_etl_pipeline.py"), script_args))

    if args.skip is None or "testable" not in args.skip:
        script_args = []
        if args.dry_run:
            script_args.append("--dry-run")
        if args.verbose:
            script_args.append("--verbose")
        if args.file and "testable_etl_pipeline" in args.file:
            script_args.extend(["--file", args.file])

        scripts.append((os.path.join(script_dir, "fix_testable_etl_pipeline.py"), script_args))

    if args.skip is None or "test" not in args.skip:
        script_args = []
        if args.dry_run:
            script_args.append("--dry-run")
        if args.verbose:
            script_args.append("--verbose")
        if args.file and "test_example" in args.file:
            script_args.extend(["--file", args.file])

        scripts.append((os.path.join(script_dir, "fix_test_example.py"), script_args))

    # Run each script
    exit_codes = []
    for script_path, script_args in scripts:
        exit_code = run_script(script_path, script_args, args.subprocess)
        exit_codes.append(exit_code)

    # Check if all scripts succeeded
    if all(code == 0 for code in exit_codes):
        print("\nAll fixes completed successfully!")
    else:
        print("\nSome fixes failed:")
        for (script_path, _), code in zip(scripts, exit_codes):
            script_name = os.path.basename(script_path)
            status = "Success" if code == 0 else f"Failed with exit code {code}"
            print(f"  {script_name}: {status}")

    print("\nNext steps:")
    if args.dry_run:
        print("1. Run the scripts again without --dry-run to apply the changes")
    else:
        print("1. Run the tests to verify the fixes")
        print("   python -m pytest tests/examples/refactored_test_example.py -vv --log-cli-level=DEBUG")

    print("2. Review the changes and make any necessary adjustments")
    print("3. Commit the changes to version control")

    # Return non-zero if any script failed
    return 0 if all(code == 0 for code in exit_codes) else 1

if __name__ == "__main__":
    sys.exit(main())