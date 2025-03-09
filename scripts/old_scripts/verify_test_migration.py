#!/usr/bin/env python3
"""
Test Migration Verification Script

This script verifies that the test coverage remains consistent
after migrating from legacy test files to pytest-style tests.

Usage:
    python scripts/verify_test_migration.py --old tests/unit/test_etl_pipeline.py --new tests/unit/test_etl_pipeline_pytest.py

The script will:
1. Run both test files and collect coverage data
2. Compare the coverage to ensure the new tests cover the same (or more) code
3. Generate a report of any coverage discrepancies
"""

import argparse
import subprocess
import sys
import json
import os
from pathlib import Path
from typing import Dict, Any, List, Set, Tuple


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Test Migration Verification")
    parser.add_argument("--old", required=True, help="Path to the old test file")
    parser.add_argument("--new", required=True, help="Path to the new migrated test file")
    parser.add_argument("--module", help="Specific module to check coverage for")
    parser.add_argument("--report", action="store_true", help="Generate a detailed report")
    parser.add_argument("--output", default="migration_verification_report.md",
                       help="Output file for the report")

    return parser.parse_args()


def run_coverage(test_file: str, module: str = None) -> Dict[str, Any]:
    """Run coverage on a test file and return results."""
    coverage_file = f"{Path(test_file).stem}_coverage.json"

    # Build module filter option
    module_filter = f"--cov={module}" if module else "--cov"

    # Run pytest with coverage
    cmd = [
        "python", "-m", "pytest", test_file,
        module_filter, "--cov-report=json", f"--cov-report=json:{coverage_file}",
        "-v"
    ]

    try:
        subprocess.run(cmd, check=True, capture_output=True, text=True)

        # Load coverage data
        with open(coverage_file, "r") as f:
            coverage_data = json.load(f)

        # Clean up coverage file
        os.remove(coverage_file)

        return coverage_data

    except subprocess.CalledProcessError as e:
        print(f"Error running tests: {e}")
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        return {}
    except Exception as e:
        print(f"Error: {e}")
        return {}


def extract_coverage_lines(coverage_data: Dict[str, Any], module: str = None) -> Dict[str, Set[int]]:
    """Extract covered lines from coverage data."""
    result = {}

    for file_path, file_data in coverage_data.get("files", {}).items():
        # Skip files not in the specified module if a module is provided
        if module and module not in file_path:
            continue

        # Get executed line numbers
        executed_lines = set()
        for line_no, count in enumerate(file_data.get("executed_lines", []), 1):
            if count > 0:
                executed_lines.add(line_no)

        result[file_path] = executed_lines

    return result


def compare_coverage(old_coverage: Dict[str, Set[int]],
                    new_coverage: Dict[str, Set[int]]) -> Dict[str, Dict[str, Any]]:
    """Compare coverage between old and new tests."""
    result = {}

    # Get all files from both coverages
    all_files = set(old_coverage.keys()) | set(new_coverage.keys())

    for file_path in all_files:
        old_lines = old_coverage.get(file_path, set())
        new_lines = new_coverage.get(file_path, set())

        # Calculate missing lines
        missing_in_new = old_lines - new_lines
        added_in_new = new_lines - old_lines

        # Calculate coverage percentages
        old_count = len(old_lines)
        new_count = len(new_lines)

        result[file_path] = {
            "old_lines": old_count,
            "new_lines": new_count,
            "missing_in_new": missing_in_new,
            "added_in_new": added_in_new,
            "coverage_change": new_count - old_count,
            "coverage_percentage": (new_count / old_count * 100) if old_count > 0 else 100
        }

    return result


def generate_report(comparison: Dict[str, Dict[str, Any]],
                   old_file: str, new_file: str,
                   output_file: str) -> None:
    """Generate a detailed report of the coverage comparison."""
    with open(output_file, "w") as f:
        f.write(f"# Test Migration Verification Report\n\n")
        f.write(f"Comparing coverage between:\n")
        f.write(f"- **Old test file:** `{old_file}`\n")
        f.write(f"- **New test file:** `{new_file}`\n\n")

        # Write summary
        total_old_lines = sum(data["old_lines"] for data in comparison.values())
        total_new_lines = sum(data["new_lines"] for data in comparison.values())
        total_missing = sum(len(data["missing_in_new"]) for data in comparison.values())
        total_added = sum(len(data["added_in_new"]) for data in comparison.values())

        coverage_percentage = (total_new_lines / total_old_lines * 100) if total_old_lines > 0 else 100

        f.write("## Summary\n\n")
        f.write(f"- **Total lines covered in old tests:** {total_old_lines}\n")
        f.write(f"- **Total lines covered in new tests:** {total_new_lines}\n")
        f.write(f"- **Coverage change:** {total_new_lines - total_old_lines:+} lines\n")
        f.write(f"- **Coverage percentage:** {coverage_percentage:.2f}%\n")
        f.write(f"- **Lines missing in new tests:** {total_missing}\n")
        f.write(f"- **Lines added in new tests:** {total_added}\n\n")

        # Overall status
        if total_missing == 0:
            f.write("**✅ PASSED:** The new tests cover all lines covered by the old tests.\n\n")
        else:
            f.write("**❌ FAILED:** The new tests do not cover all lines covered by the old tests.\n\n")

        # Write file details
        f.write("## Detailed Results\n\n")

        f.write("| File | Old Coverage | New Coverage | Change | Status |\n")
        f.write("|------|-------------|-------------|--------|--------|\n")

        for file_path, data in comparison.items():
            file_name = file_path.split("/")[-1]
            old_lines = data["old_lines"]
            new_lines = data["new_lines"]
            change = data["coverage_change"]

            status = "✅" if len(data["missing_in_new"]) == 0 else "❌"

            f.write(f"| {file_name} | {old_lines} | {new_lines} | {change:+} | {status} |\n")

        # Write missing lines details for each file
        f.write("\n## Missing Coverage Details\n\n")

        missing_found = False
        for file_path, data in comparison.items():
            if len(data["missing_in_new"]) > 0:
                missing_found = True
                file_name = file_path.split("/")[-1]
                f.write(f"### {file_name}\n\n")
                f.write("Lines covered in the old tests but missing in the new tests:\n\n")
                f.write("```\n")
                for line in sorted(data["missing_in_new"]):
                    f.write(f"Line {line}\n")
                f.write("```\n\n")

        if not missing_found:
            f.write("No missing coverage found! All lines covered by the old tests are also covered by the new tests.\n\n")


def main() -> int:
    """Main function."""
    args = parse_args()

    print(f"Verifying test migration from '{args.old}' to '{args.new}'...")

    # Check if files exist
    if not os.path.exists(args.old):
        print(f"Error: Old test file '{args.old}' not found")
        return 1

    if not os.path.exists(args.new):
        print(f"Error: New test file '{args.new}' not found")
        return 1

    # Run coverage on both files
    print(f"Running coverage on old test file...")
    old_coverage_data = run_coverage(args.old, args.module)

    print(f"Running coverage on new test file...")
    new_coverage_data = run_coverage(args.new, args.module)

    if not old_coverage_data or not new_coverage_data:
        print("Error: Failed to collect coverage data")
        return 1

    # Extract covered lines
    old_covered_lines = extract_coverage_lines(old_coverage_data, args.module)
    new_covered_lines = extract_coverage_lines(new_coverage_data, args.module)

    # Compare coverage
    comparison = compare_coverage(old_covered_lines, new_covered_lines)

    # Generate report if requested
    if args.report:
        generate_report(comparison, args.old, args.new, args.output)
        print(f"Report generated: {args.output}")

    # Check if all old lines are covered in the new tests
    missing_lines = sum(len(data["missing_in_new"]) for data in comparison.values())

    if missing_lines == 0:
        print("✅ SUCCESS: The new tests cover all lines covered by the old tests")
        return 0
    else:
        print(f"❌ FAILURE: The new tests are missing coverage for {missing_lines} lines")
        if not args.report:
            print("Run with --report to generate a detailed report")
        return 1


if __name__ == "__main__":
    sys.exit(main())