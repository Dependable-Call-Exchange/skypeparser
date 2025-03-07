#!/usr/bin/env python3
"""
Performance tests for ETL pipelines.

This module provides performance tests for comparing the original ETL pipeline
with the new modular ETL pipeline. It measures execution time, memory usage,
and resource utilization for various dataset sizes.
"""

import os
import sys
import time
import json
import tempfile
import logging
import argparse
import statistics
from typing import Dict, Any, List, Tuple
import psutil
import pytest

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl import ETLPipeline as OriginalETLPipeline
from src.db.etl import ETLPipeline as ModularETLPipeline
from tests.fixtures import BASIC_SKYPE_DATA, COMPLEX_SKYPE_DATA
from src.utils.config import load_config, get_db_config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Performance test configurations
TEST_ITERATIONS = 3  # Number of times to run each test for averaging
DATASET_SIZES = {
    'small': 1,      # Multiplier for small dataset
    'medium': 5,     # Multiplier for medium dataset
    'large': 10      # Multiplier for large dataset
}


class PerformanceTest:
    """Base class for performance testing."""

    def __init__(self, db_config: Dict[str, Any]):
        """Initialize the performance test.

        Args:
            db_config: Database configuration
        """
        self.db_config = db_config
        self.temp_dir = tempfile.mkdtemp()
        self.output_dir = os.path.join(self.temp_dir, 'output')
        os.makedirs(self.output_dir, exist_ok=True)

        # Create test data files
        self.test_files = self._create_test_files()

        # Initialize results
        self.results = {
            'original': {},
            'modular': {}
        }

    def _create_test_files(self) -> Dict[str, Dict[str, str]]:
        """Create test data files of various sizes.

        Returns:
            Dict mapping dataset size to file path
        """
        test_files = {}

        for size, multiplier in DATASET_SIZES.items():
            test_files[size] = self._create_dataset(size, multiplier)

        return test_files

    def _create_dataset(self, size: str, multiplier: int) -> Dict[str, str]:
        """Create a dataset of the specified size.

        Args:
            size: Size name ('small', 'medium', 'large')
            multiplier: Multiplier for the dataset size

        Returns:
            Dict mapping dataset type to file path
        """
        dataset_files = {}

        # Create basic dataset
        basic_data = self._multiply_dataset(BASIC_SKYPE_DATA, multiplier)
        basic_file = os.path.join(self.temp_dir, f'basic_{size}.json')
        with open(basic_file, 'w') as f:
            json.dump(basic_data, f)
        dataset_files['basic'] = basic_file

        # Create complex dataset
        complex_data = self._multiply_dataset(COMPLEX_SKYPE_DATA, multiplier)
        complex_file = os.path.join(self.temp_dir, f'complex_{size}.json')
        with open(complex_file, 'w') as f:
            json.dump(complex_data, f)
        dataset_files['complex'] = complex_file

        return dataset_files

    def _multiply_dataset(self, data: Dict[str, Any], multiplier: int) -> Dict[str, Any]:
        """Multiply the dataset by duplicating conversations.

        Args:
            data: Original dataset
            multiplier: Number of times to duplicate conversations

        Returns:
            Multiplied dataset
        """
        result = data.copy()

        # Duplicate conversations
        original_conversations = data.get('conversations', [])
        new_conversations = []

        for i in range(multiplier):
            for conv in original_conversations:
                new_conv = conv.copy()
                new_conv['id'] = f"{new_conv['id']}_{i}"
                new_conversations.append(new_conv)

        result['conversations'] = new_conversations
        return result

    def run_tests(self) -> Dict[str, Any]:
        """Run all performance tests.

        Returns:
            Dict containing test results
        """
        for size in DATASET_SIZES.keys():
            logger.info(f"Running tests for {size} dataset")

            # Test original pipeline
            original_results = self._test_original_pipeline(size)
            self.results['original'][size] = original_results

            # Test modular pipeline
            modular_results = self._test_modular_pipeline(size)
            self.results['modular'][size] = modular_results

            # Log comparison
            self._log_comparison(size, original_results, modular_results)

        return self.results

    def _test_original_pipeline(self, size: str) -> Dict[str, Any]:
        """Test the original ETL pipeline.

        Args:
            size: Dataset size ('small', 'medium', 'large')

        Returns:
            Dict containing test results
        """
        logger.info(f"Testing original pipeline with {size} dataset")

        results = {
            'basic': self._run_original_pipeline(self.test_files[size]['basic']),
            'complex': self._run_original_pipeline(self.test_files[size]['complex'])
        }

        return results

    def _run_original_pipeline(self, file_path: str) -> Dict[str, Any]:
        """Run the original ETL pipeline.

        Args:
            file_path: Path to the test file

        Returns:
            Dict containing test results
        """
        times = []
        memory_usages = []
        cpu_usages = []

        for i in range(TEST_ITERATIONS):
            logger.info(f"Original pipeline iteration {i+1}/{TEST_ITERATIONS}")

            # Create pipeline
            pipeline = OriginalETLPipeline(
                db_config=self.db_config,
                output_dir=self.output_dir
            )

            # Measure performance
            start_time = time.time()
            process = psutil.Process(os.getpid())
            start_memory = process.memory_info().rss / (1024 * 1024)  # MB

            # Run pipeline
            result = pipeline.run_pipeline(
                file_path=file_path,
                user_display_name='Test User'
            )

            # Measure end stats
            end_time = time.time()
            end_memory = process.memory_info().rss / (1024 * 1024)  # MB
            cpu_percent = process.cpu_percent()

            # Calculate metrics
            execution_time = end_time - start_time
            memory_usage = end_memory - start_memory

            # Store results
            times.append(execution_time)
            memory_usages.append(memory_usage)
            cpu_usages.append(cpu_percent)

            logger.info(f"Original pipeline execution time: {execution_time:.2f} seconds")
            logger.info(f"Original pipeline memory usage: {memory_usage:.2f} MB")
            logger.info(f"Original pipeline CPU usage: {cpu_percent:.2f}%")

        # Calculate averages
        avg_time = statistics.mean(times)
        avg_memory = statistics.mean(memory_usages)
        avg_cpu = statistics.mean(cpu_usages)

        return {
            'execution_time': avg_time,
            'memory_usage': avg_memory,
            'cpu_usage': avg_cpu,
            'raw_times': times,
            'raw_memory_usages': memory_usages,
            'raw_cpu_usages': cpu_usages
        }

    def _test_modular_pipeline(self, size: str) -> Dict[str, Any]:
        """Test the modular ETL pipeline.

        Args:
            size: Dataset size ('small', 'medium', 'large')

        Returns:
            Dict containing test results
        """
        logger.info(f"Testing modular pipeline with {size} dataset")

        results = {
            'basic': self._run_modular_pipeline(self.test_files[size]['basic']),
            'complex': self._run_modular_pipeline(self.test_files[size]['complex'])
        }

        return results

    def _run_modular_pipeline(self, file_path: str) -> Dict[str, Any]:
        """Run the modular ETL pipeline.

        Args:
            file_path: Path to the test file

        Returns:
            Dict containing test results
        """
        times = []
        memory_usages = []
        cpu_usages = []

        for i in range(TEST_ITERATIONS):
            logger.info(f"Modular pipeline iteration {i+1}/{TEST_ITERATIONS}")

            # Create pipeline
            pipeline = ModularETLPipeline(
                db_config=self.db_config,
                output_dir=self.output_dir
            )

            # Measure performance
            start_time = time.time()
            process = psutil.Process(os.getpid())
            start_memory = process.memory_info().rss / (1024 * 1024)  # MB

            # Run pipeline
            result = pipeline.run_pipeline(
                file_path=file_path,
                user_display_name='Test User'
            )

            # Measure end stats
            end_time = time.time()
            end_memory = process.memory_info().rss / (1024 * 1024)  # MB
            cpu_percent = process.cpu_percent()

            # Calculate metrics
            execution_time = end_time - start_time
            memory_usage = end_memory - start_memory

            # Store results
            times.append(execution_time)
            memory_usages.append(memory_usage)
            cpu_usages.append(cpu_percent)

            logger.info(f"Modular pipeline execution time: {execution_time:.2f} seconds")
            logger.info(f"Modular pipeline memory usage: {memory_usage:.2f} MB")
            logger.info(f"Modular pipeline CPU usage: {cpu_percent:.2f}%")

        # Calculate averages
        avg_time = statistics.mean(times)
        avg_memory = statistics.mean(memory_usages)
        avg_cpu = statistics.mean(cpu_usages)

        return {
            'execution_time': avg_time,
            'memory_usage': avg_memory,
            'cpu_usage': avg_cpu,
            'raw_times': times,
            'raw_memory_usages': memory_usages,
            'raw_cpu_usages': cpu_usages
        }

    def _log_comparison(self, size: str, original_results: Dict[str, Any],
                      modular_results: Dict[str, Any]) -> None:
        """Log a comparison of the original and modular pipeline results.

        Args:
            size: Dataset size ('small', 'medium', 'large')
            original_results: Results from the original pipeline
            modular_results: Results from the modular pipeline
        """
        for dataset_type in ['basic', 'complex']:
            orig = original_results[dataset_type]
            mod = modular_results[dataset_type]

            time_diff = mod['execution_time'] - orig['execution_time']
            time_percent = (time_diff / orig['execution_time']) * 100 if orig['execution_time'] > 0 else 0

            memory_diff = mod['memory_usage'] - orig['memory_usage']
            memory_percent = (memory_diff / orig['memory_usage']) * 100 if orig['memory_usage'] > 0 else 0

            logger.info(f"=== {size.upper()} {dataset_type.upper()} DATASET COMPARISON ===")
            logger.info(f"Execution time: Original={orig['execution_time']:.2f}s, Modular={mod['execution_time']:.2f}s")
            logger.info(f"Time difference: {time_diff:.2f}s ({time_percent:.2f}%)")
            logger.info(f"Memory usage: Original={orig['memory_usage']:.2f}MB, Modular={mod['memory_usage']:.2f}MB")
            logger.info(f"Memory difference: {memory_diff:.2f}MB ({memory_percent:.2f}%)")
            logger.info(f"CPU usage: Original={orig['cpu_usage']:.2f}%, Modular={mod['cpu_usage']:.2f}%")
            logger.info("=" * 50)

    def cleanup(self) -> None:
        """Clean up temporary files."""
        import shutil
        shutil.rmtree(self.temp_dir)


def get_db_config() -> Dict[str, Any]:
    """Get database configuration for testing.

    Returns:
        Dict containing database configuration
    """
    try:
        # Try to load from config file
        config = load_config('config/config.json')
        db_config = get_db_config(config)
    except Exception:
        # Fall back to default test configuration
        db_config = {
            'dbname': 'test_skype_logs',
            'user': 'postgres',
            'password': '',
            'host': 'localhost',
            'port': 5432
        }
    return db_config


def save_results(results: Dict[str, Any], output_file: str) -> None:
    """Save test results to a file.

    Args:
        results: Test results
        output_file: Path to the output file
    """
    with open(output_file, 'w') as f:
        json.dump(results, f, indent=2)
    logger.info(f"Results saved to {output_file}")


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Run ETL pipeline performance tests')
    parser.add_argument('--output', '-o', default='performance_results.json',
                      help='Output file for test results')
    parser.add_argument('--iterations', '-i', type=int, default=TEST_ITERATIONS,
                      help='Number of iterations for each test')
    return parser.parse_args()


@pytest.mark.performance
def test_etl_performance():
    """Run ETL pipeline performance tests."""
    # Get database configuration
    db_config = get_db_config()

    # Create and run performance tests
    test = PerformanceTest(db_config)
    try:
        results = test.run_tests()
        assert results is not None, "Performance test results should not be None"
    finally:
        test.cleanup()


def main():
    """Main function."""
    args = parse_args()

    # Update test iterations
    global TEST_ITERATIONS
    TEST_ITERATIONS = args.iterations

    # Get database configuration
    db_config = get_db_config()

    # Create and run performance tests
    test = PerformanceTest(db_config)
    try:
        results = test.run_tests()
        save_results(results, args.output)
    finally:
        test.cleanup()


if __name__ == '__main__':
    main()