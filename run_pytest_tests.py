#!/usr/bin/env python3
"""
Enhanced Test Runner for SkypeParser (Single-File Version)

This script runs pytest-based tests with detailed logging and generates
comprehensive reports of the test results, including coverage analysis
and performance metrics.

Key Improvements:
- Uses `logging` instead of `print` for better configurability.
- Includes a `--directory` argument to specify the base test directory.
- Refactored the command-building logic to avoid popping items.
- Handles subprocess exit codes and errors more robustly.
- Maintains coverage parsing and fallback manual parsing if needed.
"""

import argparse
import json
import logging
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any

# Configure a basic logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments for the pytest runner."""
    parser = argparse.ArgumentParser(description="Enhanced pytest runner with reporting")

    # Test type selection
    parser.add_argument(
        "--run-type",
        choices=["unit", "integration", "all"],
        default="unit",
        help="Type of tests to run",
    )

    # Base test directory
    parser.add_argument(
        "--directory",
        default="tests",
        help="Base directory containing test folders (default: 'tests')",
    )

    # Test discovery options
    parser.add_argument(
        "--pattern",
        default="test_*.py",
        help="File pattern for test discovery (default: 'test_*.py')",
    )
    parser.add_argument(
        "--file",
        default=None,
        help="Run a specific test file (overrides --pattern if provided)",
    )

    # Test execution options
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Verbose output",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop on first failure",
    )
    parser.add_argument(
        "--markers",
        default=None,
        help="Only run tests with specific markers",
    )

    return parser.parse_args()


def create_output_directory(output_dir: str) -> Path:
    """
    Create the output directory if it doesn't exist.

    Args:
        output_dir: Path to the output directory

    Returns:
        Path object for the output directory
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path


def build_pytest_command(args: argparse.Namespace, timestamp: Optional[str] = None) -> Tuple[List[str], str]:
    """
    Build a pytest command based on the provided arguments.

    Args:
        args: The parsed command-line arguments.
        timestamp: Optional timestamp string for the report file.

    Returns:
        (cmd, json_report_path):
            cmd: A list of strings forming the pytest command.
            json_report_path: The path where the JSON report will be stored.
    """
    timestamp = timestamp or datetime.now().strftime("%Y%m%d_%H%M%S")
    json_report_path = f"test_reports/pytest_report_{timestamp}.json"

    # Base command
    cmd = ["python", "-m", "pytest"]

    # Determine test directory based on --run-type
    # If run_type is 'all', we use the entire tests folder
    # Otherwise, use subfolder like "tests/unit" or "tests/integration"
    if args.run_type == "all":
        test_dir = args.directory
    else:
        test_dir = os.path.join(args.directory, args.run_type)

    # Decide whether to target a single file or a directory/pattern
    if args.file:
        # --file overrides directory or pattern
        if os.path.isabs(args.file):
            # Use absolute path directly
            target_path = args.file
        else:
            # Check if file path already includes tests/ directory
            if args.file.startswith('tests/'):
                # Use file path as-is
                target_path = args.file
            else:
                # Combine base test directory with relative file path
                target_path = os.path.join(test_dir, args.file)
        cmd.append(target_path)
    else:
        # If no file specified, either run entire directory or match pattern
        if args.pattern and "*" in args.pattern:
            # If pattern includes wildcard, pass '-k <pattern>'
            # We append the test directory for discovery
            cmd.append(test_dir)
            # Example: pattern "test_*.py" => we strip ".py" and the asterisk
            # for -k usage
            pattern_name = args.pattern.replace("*", "").replace(".py", "")
            # If pattern_name ends up empty (e.g., 'test_*.py'), handle it
            pattern_name = pattern_name if pattern_name else "test_"
            cmd.append(f"-k={pattern_name}")
        elif args.pattern:
            # If pattern is a single file name without wildcards
            # just append the pattern under the test directory
            # (e.g., tests/unit/test_something.py)
            pattern_path = os.path.join(test_dir, args.pattern)
            cmd.append(pattern_path)
        else:
            # Default to just the test directory if no pattern is provided
            cmd.append(test_dir)

    # Verbosity
    if args.verbose:
        cmd.append("-v")

    # Add logging level
    # You can tweak the log level based on user inputs if desired
    cmd.append("--log-cli-level=DEBUG")

    # Fail-fast
    if args.fail_fast:
        cmd.append("-x")

    # Markers
    if args.markers:
        cmd.append("-m")
        cmd.append(args.markers)

    # JSON report
    cmd.append("--json-report")
    cmd.append(f"--json-report-file={json_report_path}")

    return cmd, json_report_path


def parse_json_report(json_report_path: str) -> Dict[str, Any]:
    """
    Parse the JSON report file generated by pytest-json-report.

    Args:
        json_report_path: Path to the JSON report file.

    Returns:
        A dictionary with test statistics.
    """
    try:
        with open(json_report_path, "r") as f:
            report = json.load(f)

        summary = report.get("summary", {})
        duration = float(report.get("duration", 0.0))

        results = {
            "total": summary.get("total", 0),
            "passed": summary.get("passed", 0),
            "failed": summary.get("failed", 0),
            "skipped": summary.get("skipped", 0),
            "errors": summary.get("errors", 0),
            "warnings": summary.get("warnings", 0),
            "duration": duration,
        }
        return results
    except FileNotFoundError:
        logger.warning("JSON report file not found: %s", json_report_path)
        return {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "skipped": 0,
            "errors": 0,
            "warnings": 0,
            "duration": 0.0,
        }
    except json.JSONDecodeError as e:
        logger.error("Error decoding JSON report file: %s\n%s", json_report_path, e)
        return {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "skipped": 0,
            "errors": 0,
            "warnings": 0,
            "duration": 0.0,
        }
    except Exception as e:
        logger.error("Error parsing JSON report: %s", e)
        return {
            "total": 0,
            "passed": 0,
            "failed": 0,
            "skipped": 0,
            "errors": 0,
            "warnings": 0,
            "duration": 0.0,
        }


def parse_coverage_results(output: str) -> Dict[str, Any]:
    """
    Fallback coverage parser if you only have text output.
    For best reliability, consider using coverage JSON/XML outputs instead.

    Args:
        output: Coverage output as a string

    Returns:
        Dictionary containing coverage statistics
    """
    result = {
        "total_coverage": 0.0,
        "module_coverage": {},
        "missing_coverage": [],
    }

    coverage_lines = []
    capture = False

    for line in output.splitlines():
        if "---------- coverage:" in line:
            capture = True
            continue

        if capture and "TOTAL" in line:
            coverage_lines.append(line)
            break

        if capture and line.strip():
            coverage_lines.append(line)

    for line in coverage_lines:
        parts = [p for p in line.split() if p]
        if len(parts) >= 4:
            module = parts[0]
            statements = int(parts[1])
            missed = int(parts[2])
            coverage_pct = float(parts[3].rstrip("%"))

            if module != "TOTAL":
                result["module_coverage"][module] = {
                    "statements": statements,
                    "missed": missed,
                    "coverage": coverage_pct,
                }
                if missed > 0 and coverage_pct < 80:
                    result["missing_coverage"].append(
                        {
                            "module": module,
                            "statements": statements,
                            "missed": missed,
                            "coverage": coverage_pct,
                        }
                    )
            else:
                result["total_coverage"] = coverage_pct

    return result


def generate_markdown_report(
    output_dir: Path,
    timestamp: str,
    start_time: float,
    end_time: float,
    command: List[str],
    test_results: Dict[str, Any],
    coverage_results: Optional[Dict[str, Any]] = None,
) -> str:
    """
    Generate a detailed test report in Markdown format.

    Args:
        output_dir: Path to the output directory
        timestamp: Timestamp for the report
        start_time: Test execution start time
        end_time: Test execution end time
        command: The pytest command that was run
        test_results: Parsed test results from JSON or fallback parsing
        coverage_results: Optional parsed coverage results

    Returns:
        The path to the generated Markdown report file as a string.
    """
    report_file = output_dir / f"test_summary_{timestamp}.md"

    duration = end_time - start_time

    with open(report_file, "w", encoding="utf-8") as f:
        f.write("# SkypeParser Test Execution Summary\n\n")

        # Test execution details
        f.write("## Test Execution Details\n\n")
        f.write(f"- **Date and Time:** {datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"- **Duration:** {duration:.2f} seconds\n")
        f.write(f"- **Command:** `{' '.join(command)}`\n\n")

        # Test summary
        total_tests = test_results.get("total", 0)
        passed = test_results.get("passed", 0)
        failed = test_results.get("failed", 0)
        skipped = test_results.get("skipped", 0)
        errors = test_results.get("errors", 0)
        warnings = test_results.get("warnings", 0)

        f.write("## Test Summary\n\n")
        f.write(f"- **Total Tests:** {total_tests}\n")
        pass_pct = (passed / total_tests * 100) if total_tests > 0 else 0.0
        f.write(f"- **Passed:** {passed} ({pass_pct:.1f}%)\n")
        f.write(f"- **Failed:** {failed}\n")
        f.write(f"- **Skipped:** {skipped}\n")
        f.write(f"- **Errors:** {errors}\n")
        f.write(f"- **Warnings:** {warnings}\n\n")

        # Coverage results
        if coverage_results:
            f.write("## Coverage Summary\n\n")
            f.write(f"- **Total Coverage:** {coverage_results['total_coverage']:.1f}%\n\n")

            # Module coverage
            f.write("### Module Coverage\n\n")
            f.write("| Module | Statements | Coverage |\n")
            f.write("|--------|------------|----------|\n")
            for module, data in sorted(coverage_results["module_coverage"].items(), key=lambda x: x[1]["coverage"]):
                f.write(f"| {module} | {data['statements']} | {data['coverage']:.1f}% |\n")

            # Missing coverage
            if coverage_results["missing_coverage"]:
                f.write("\n### Modules Needing Coverage Improvement\n\n")
                f.write("| Module | Statements | Missed | Coverage |\n")
                f.write("|--------|------------|--------|----------|\n")
                for data in sorted(coverage_results["missing_coverage"], key=lambda x: x["coverage"]):
                    f.write(
                        f"| {data['module']} | {data['statements']} | {data['missed']} "
                        f"| {data['coverage']:.1f}% |\n"
                    )

    return str(report_file)


def run_tests(args: argparse.Namespace) -> Dict[str, Any]:
    """
    Orchestrate the test run:
      - Create the output dir
      - Build the pytest command
      - Run tests via subprocess
      - Parse JSON report and coverage if relevant
      - Generate markdown summary
      - Return overall results dictionary

    Args:
        args: Command-line arguments

    Returns:
        A dictionary containing overall test results.
    """
    # Ensure we have a directory for JSON reports and the final markdown report
    os.makedirs("test_reports", exist_ok=True)

    # Timestamp for file naming
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Build the command
    pytest_cmd, json_report_path = build_pytest_command(args, timestamp)

    # Log the command details
    logger.info("Running tests with command: %s", " ".join(pytest_cmd))
    logger.info("Test type: %s", args.run_type)
    logger.info("Test directory: %s", args.directory)
    logger.info("Test pattern: %s", args.pattern)
    if args.file:
        logger.info("Using specific file: %s", args.file)
    if args.markers:
        logger.info("Test markers: %s", args.markers)

    start_time = time.time()

    # Run the tests
    process = subprocess.run(pytest_cmd, capture_output=True, text=True)
    end_time = time.time()

    # Log any errors or warnings from subprocess
    if process.returncode != 0:
        logger.warning("Pytest process finished with a non-zero exit code: %d", process.returncode)
    if process.stderr:
        logger.debug("Pytest stderr output:\n%s", process.stderr)

    # Parse JSON report
    results = parse_json_report(json_report_path)
    if results["total"] == 0:
        logger.warning("No tests were reported in the JSON. Attempting fallback checks or no tests found.")

    # Optionally parse coverage from process.stdout if you run coverage in the command
    # For example, if your pytest command includes `--cov` and prints coverage to stdout:
    # coverage_results = parse_coverage_results(process.stdout)

    # For now, let's keep coverage_results as None or parse manually if needed:
    coverage_results = None

    # Create final test summary
    output_dir = create_output_directory("test_reports")
    report_path = generate_markdown_report(
        output_dir=output_dir,
        timestamp=timestamp,
        start_time=start_time,
        end_time=end_time,
        command=pytest_cmd,
        test_results=results,
        coverage_results=coverage_results,
    )

    logger.info("Test summary generated at: %s", report_path)
    logger.info("Completed in %.2f seconds.", end_time - start_time)

    # Return the overall results (could be used by CI for pass/fail checks)
    return results


if __name__ == "__main__":
    cli_args = parse_args()
    results_dict = run_tests(cli_args)
    # Optionally, exit with pytest's return code if you want your CI to fail correctly:
    sys.exit(0 if results_dict["failed"] == 0 and results_dict["errors"] == 0 else 1)
