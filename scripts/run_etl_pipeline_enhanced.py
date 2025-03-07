#!/usr/bin/env python3
"""
Enhanced Skype Parser ETL Pipeline CLI

This script provides an enhanced command-line interface for running the Skype Parser ETL pipeline.
It includes additional options, progress reporting, and improved logging.

Usage:
    python scripts/run_etl_pipeline_enhanced.py -f path/to/skype_export.tar -u "Your Name"
    python scripts/run_etl_pipeline_enhanced.py -f path/to/skype_export.tar -u "Your Name" --resume
    python scripts/run_etl_pipeline_enhanced.py -f path/to/skype_export.tar -u "Your Name" --parallel
"""

import os
import sys
import argparse
import logging
import json
import time
import datetime
import threading
from typing import Dict, Any, Optional, List, Tuple
from pathlib import Path
from tqdm import tqdm

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.db.etl import ETLPipeline
from src.utils.config import load_config, get_db_config
from src.utils.validation import validate_skype_data
from src.utils.di import get_service
from src.utils.interfaces import ProgressTrackerProtocol

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('etl_pipeline.log')
    ]
)
logger = logging.getLogger(__name__)


class ProgressReporter:
    """
    A class for reporting progress of the ETL pipeline.
    """

    def __init__(self, total_steps: int = 100, update_interval: float = 0.5):
        """
        Initialize the progress reporter.

        Args:
            total_steps: Total number of steps in the pipeline
            update_interval: Interval in seconds between progress updates
        """
        self.total_steps = total_steps
        self.current_step = 0
        self.current_phase = "Initializing"
        self.phase_progress = 0
        self.phase_total = 0
        self.start_time = time.time()
        self.update_interval = update_interval
        self.last_update_time = 0
        self.running = False
        self.progress_bar = None
        self.lock = threading.Lock()

    def start(self):
        """Start the progress reporter."""
        self.start_time = time.time()
        self.running = True
        self.progress_bar = tqdm(
            total=self.total_steps,
            desc=f"{self.current_phase}",
            unit="step",
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
        )

    def stop(self):
        """Stop the progress reporter."""
        self.running = False
        if self.progress_bar:
            self.progress_bar.close()

    def update(self, step: int = None, phase: str = None, phase_progress: int = None, phase_total: int = None):
        """
        Update the progress.

        Args:
            step: Current step
            phase: Current phase
            phase_progress: Progress within the current phase
            phase_total: Total steps in the current phase
        """
        with self.lock:
            current_time = time.time()

            # Update step if provided
            if step is not None:
                self.current_step = step

            # Update phase if provided
            if phase is not None:
                self.current_phase = phase

            # Update phase progress if provided
            if phase_progress is not None:
                self.phase_progress = phase_progress

            # Update phase total if provided
            if phase_total is not None:
                self.phase_total = phase_total

            # Update progress bar if enough time has passed
            if current_time - self.last_update_time >= self.update_interval:
                self.last_update_time = current_time

                if self.progress_bar:
                    # Update progress bar
                    self.progress_bar.n = self.current_step

                    # Update description
                    if self.phase_total > 0:
                        phase_percent = int(100 * self.phase_progress / self.phase_total)
                        self.progress_bar.set_description(f"{self.current_phase} ({phase_percent}%)")
                    else:
                        self.progress_bar.set_description(f"{self.current_phase}")

                    # Refresh display
                    self.progress_bar.refresh()


class EnhancedETLPipeline(ETLPipeline):
    """
    Enhanced ETL pipeline with progress reporting.
    """

    def __init__(self, progress_reporter: ProgressReporter = None, **kwargs):
        """
        Initialize the enhanced ETL pipeline.

        Args:
            progress_reporter: Progress reporter instance
            **kwargs: Additional arguments to pass to the parent class
        """
        super().__init__(**kwargs)
        self.progress_reporter = progress_reporter

    def run_pipeline(self, file_path: str, user_display_name: str = "", resume_from_checkpoint: bool = False) -> Dict[str, Any]:
        """
        Run the ETL pipeline with progress reporting.

        Args:
            file_path: Path to the Skype export file
            user_display_name: User display name for the export
            resume_from_checkpoint: Whether to resume from a checkpoint

        Returns:
            Dict containing the results of the ETL pipeline
        """
        # Start progress reporter
        if self.progress_reporter:
            self.progress_reporter.start()
            self.progress_reporter.update(step=0, phase="Initializing")

        try:
            # Initialize the pipeline
            self.initialize_pipeline(file_path, user_display_name, resume_from_checkpoint)

            # Extract phase
            if self.progress_reporter:
                self.progress_reporter.update(step=10, phase="Extracting")

            self.extract_data()

            # Transform phase
            if self.progress_reporter:
                self.progress_reporter.update(step=40, phase="Transforming")

            self.transform_data()

            # Load phase
            if self.progress_reporter:
                self.progress_reporter.update(step=70, phase="Loading")

            self.load_data()

            # Finalize
            if self.progress_reporter:
                self.progress_reporter.update(step=100, phase="Completed")

            return self.get_results()
        except Exception as e:
            if self.progress_reporter:
                self.progress_reporter.update(phase=f"Error: {str(e)}")
            raise
        finally:
            if self.progress_reporter:
                self.progress_reporter.stop()


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description='Run the Skype Parser ETL pipeline to process Skype export data',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Input file options
    parser.add_argument('-f', '--file', required=True,
                        help='Path to the Skype export file (.tar or .json)')
    parser.add_argument('-u', '--user', default='',
                        help='User display name for the export')

    # Configuration options
    parser.add_argument('-c', '--config', default='config/config.json',
                        help='Path to the configuration file')
    parser.add_argument('-o', '--output', default='output',
                        help='Output directory for intermediate files')

    # Database options
    db_group = parser.add_argument_group('Database Options')
    db_group.add_argument('--db-name',
                        help='Database name (overrides config file)')
    db_group.add_argument('--db-user',
                        help='Database user (overrides config file)')
    db_group.add_argument('--db-password',
                        help='Database password (overrides config file)')
    db_group.add_argument('--db-host',
                        help='Database host (overrides config file)')
    db_group.add_argument('--db-port', type=int,
                        help='Database port (overrides config file)')
    db_group.add_argument('--db-schema',
                        help='Database schema (overrides config file)')
    db_group.add_argument('--db-ssl-mode',
                        help='Database SSL mode (overrides config file)')
    db_group.add_argument('--db-ssl-root-cert',
                        help='Database SSL root certificate (overrides config file)')

    # Performance options
    perf_group = parser.add_argument_group('Performance Options')
    perf_group.add_argument('-m', '--memory', type=int, default=1024,
                        help='Memory limit in MB')
    perf_group.add_argument('-p', '--parallel', action='store_true',
                        help='Enable parallel processing')
    perf_group.add_argument('-s', '--chunk-size', type=int, default=1000,
                        help='Chunk size for batch processing')
    perf_group.add_argument('-b', '--batch-size', type=int, default=100,
                        help='Batch size for database operations')
    perf_group.add_argument('-w', '--workers', type=int,
                        help='Maximum number of worker threads/processes')

    # Checkpoint options
    checkpoint_group = parser.add_argument_group('Checkpoint Options')
    checkpoint_group.add_argument('-r', '--resume', action='store_true',
                        help='Resume from the latest checkpoint')
    checkpoint_group.add_argument('--checkpoint',
                        help='Resume from a specific checkpoint file')
    checkpoint_group.add_argument('--list-checkpoints', action='store_true',
                        help='List available checkpoints and exit')
    checkpoint_group.add_argument('--checkpoint-interval', type=int, default=1000,
                        help='Number of items to process before creating a checkpoint')
    checkpoint_group.add_argument('--checkpoint-dir',
                        help='Directory to store checkpoints (defaults to output_dir/checkpoints)')

    # Attachment options
    attachment_group = parser.add_argument_group('Attachment Options')
    attachment_group.add_argument('--download-attachments', action='store_true',
                        help='Download attachments from URLs')
    attachment_group.add_argument('--attachments-dir',
                        help='Directory to store downloaded attachments (defaults to output_dir/attachments)')
    attachment_group.add_argument('--no-thumbnails', action='store_true',
                        help='Disable thumbnail generation for image attachments')
    attachment_group.add_argument('--no-metadata', action='store_true',
                        help='Disable metadata extraction from attachments')
    attachment_group.add_argument('--max-attachment-size', type=int, default=10,
                        help='Maximum attachment size in MB')
    attachment_group.add_argument('--attachment-timeout', type=int, default=30,
                        help='Timeout for attachment downloads in seconds')

    # Output options
    output_group = parser.add_argument_group('Output Options')
    output_group.add_argument('--json-output', action='store_true',
                        help='Output results in JSON format')
    output_group.add_argument('--csv-output', action='store_true',
                        help='Output results in CSV format')
    output_group.add_argument('--no-progress', action='store_true',
                        help='Disable progress reporting')
    output_group.add_argument('--quiet', action='store_true',
                        help='Suppress all output except errors')

    # Analysis options
    analysis_group = parser.add_argument_group('Analysis Options')
    analysis_group.add_argument('--analyze', action='store_true',
                        help='Run analysis after ETL pipeline completes')
    analysis_group.add_argument('--visualize', action='store_true',
                        help='Generate visualizations after ETL pipeline completes')
    analysis_group.add_argument('--report-type', choices=['summary', 'full', 'custom'], default='summary',
                        help='Type of report to generate if --analyze is specified')

    # Debug options
    debug_group = parser.add_argument_group('Debug Options')
    debug_group.add_argument('-v', '--verbose', action='store_true',
                        help='Enable verbose logging')
    debug_group.add_argument('-d', '--debug', action='store_true',
                        help='Enable debug logging')
    debug_group.add_argument('--dry-run', action='store_true',
                        help='Validate input and configuration without running the pipeline')
    debug_group.add_argument('--log-file',
                        help='Path to log file (defaults to etl_pipeline.log)')
    debug_group.add_argument('--log-format',
                        help='Log format string')

    return parser.parse_args()


def setup_logging(args: argparse.Namespace) -> None:
    """Set up logging based on command line arguments.

    Args:
        args: Command line arguments
    """
    # Determine log level
    log_level = logging.INFO
    if args.debug:
        log_level = logging.DEBUG
    elif args.verbose:
        log_level = logging.INFO
    elif args.quiet:
        log_level = logging.ERROR

    # Determine log file
    log_file = args.log_file or 'etl_pipeline.log'

    # Determine log format
    log_format = args.log_format or '%(asctime)s - %(levelname)s - %(name)s - %(message)s'
    date_format = '%Y-%m-%d %H:%M:%S'

    # Configure logging
    logging.basicConfig(
        level=log_level,
        format=log_format,
        datefmt=date_format,
        handlers=[
            logging.StreamHandler() if not args.quiet else logging.NullHandler(),
            logging.FileHandler(log_file)
        ]
    )

    logger.info(f"Log level set to: {logging.getLevelName(log_level)}")
    logger.info(f"Logging to file: {log_file}")


def get_config(args: argparse.Namespace) -> Dict[str, Any]:
    """Load configuration from file and override with command line arguments.

    Args:
        args: Command line arguments

    Returns:
        Dict containing configuration
    """
    # Load configuration from file
    try:
        config = load_config(args.config)
        logger.info(f"Loaded configuration from {args.config}")
    except Exception as e:
        logger.warning(f"Failed to load configuration from {args.config}: {e}")
        config = {}

    # Get database configuration
    db_config = get_db_config(config)

    # Override with command line arguments
    if args.db_name:
        db_config['dbname'] = args.db_name
    if args.db_user:
        db_config['user'] = args.db_user
    if args.db_password:
        db_config['password'] = args.db_password
    if args.db_host:
        db_config['host'] = args.db_host
    if args.db_port:
        db_config['port'] = args.db_port
    if args.db_schema:
        db_config['schema'] = args.db_schema
    if args.db_ssl_mode:
        db_config['sslmode'] = args.db_ssl_mode
    if args.db_ssl_root_cert:
        db_config['sslrootcert'] = args.db_ssl_root_cert

    # Ensure we have a valid database configuration
    required_fields = ['dbname', 'user']
    missing_fields = [f for f in required_fields if f not in db_config]
    if missing_fields:
        logger.error(f"Missing required database configuration fields: {missing_fields}")
        logger.error("Please provide these fields in the config file or as command line arguments")
        sys.exit(1)

    # Set up checkpoint directory
    checkpoint_dir = args.checkpoint_dir or os.path.join(args.output, 'checkpoints')

    # Set up attachments directory
    attachments_dir = args.attachments_dir or os.path.join(args.output, 'attachments')

    return {
        'db_config': db_config,
        'output_dir': args.output,
        'memory_limit_mb': args.memory,
        'parallel_processing': args.parallel,
        'chunk_size': args.chunk_size,
        'batch_size': args.batch_size,
        'max_workers': args.workers,
        'download_attachments': args.download_attachments,
        'attachments_dir': attachments_dir,
        'generate_thumbnails': not args.no_thumbnails,
        'extract_metadata': not args.no_metadata,
        'checkpoint_dir': checkpoint_dir,
        'checkpoint_interval': args.checkpoint_interval,
        'max_attachment_size_mb': args.max_attachment_size,
        'attachment_timeout_seconds': args.attachment_timeout
    }


def validate_input_file(file_path: str) -> None:
    """Validate that the input file exists and is a valid Skype export file.

    Args:
        file_path: Path to the input file

    Raises:
        FileNotFoundError: If the file does not exist
        ValueError: If the file is not a valid Skype export file
    """
    # Check that the file exists
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Input file not found: {file_path}")

    # Check file extension
    file_ext = os.path.splitext(file_path)[1].lower()
    if file_ext not in ['.tar', '.json']:
        logger.warning(f"Unexpected file extension: {file_ext}. Expected .tar or .json")

    # Check file size
    file_size_mb = os.path.getsize(file_path) / (1024 * 1024)
    logger.info(f"Input file size: {file_size_mb:.2f} MB")

    logger.info(f"Input file validated: {file_path}")


def list_available_checkpoints(pipeline: ETLPipeline) -> None:
    """List available checkpoints and exit.

    Args:
        pipeline: ETL pipeline instance
    """
    checkpoints = pipeline.get_available_checkpoints()

    if not checkpoints:
        logger.info("No checkpoints available")
        return

    logger.info(f"Available checkpoints ({len(checkpoints)}):")
    for i, checkpoint in enumerate(checkpoints, 1):
        checkpoint_path = Path(checkpoint)
        checkpoint_size_kb = os.path.getsize(checkpoint) / 1024
        checkpoint_time = datetime.datetime.fromtimestamp(os.path.getmtime(checkpoint))
        logger.info(f"{i}. {checkpoint_path.name} - {checkpoint_time} - {checkpoint_size_kb:.2f} KB")


def run_etl_pipeline(args: argparse.Namespace, config: Dict[str, Any]) -> Dict[str, Any]:
    """Run the ETL pipeline with the given arguments and configuration.

    Args:
        args: Command line arguments
        config: Configuration dictionary

    Returns:
        Dict containing the results of the ETL pipeline
    """
    # Create progress reporter if enabled
    progress_reporter = None
    if not args.no_progress and not args.quiet:
        progress_reporter = ProgressReporter()

    # Create the ETL pipeline
    pipeline = EnhancedETLPipeline(progress_reporter=progress_reporter, **config)

    # List checkpoints if requested
    if args.list_checkpoints:
        list_available_checkpoints(pipeline)
        sys.exit(0)

    # Dry run if requested
    if args.dry_run:
        logger.info("Dry run mode: validating input and configuration")
        logger.info(f"Input file: {args.file}")
        logger.info(f"User display name: {args.user}")
        logger.info(f"Configuration: {json.dumps(config, indent=2, default=str)}")
        logger.info("Dry run completed successfully")
        sys.exit(0)

    # Resume from checkpoint if requested
    if args.checkpoint:
        logger.info(f"Resuming from checkpoint: {args.checkpoint}")
        pipeline = EnhancedETLPipeline.load_from_checkpoint(
            checkpoint_file=args.checkpoint,
            db_config=config['db_config'],
            progress_reporter=progress_reporter
        )
    elif args.resume:
        # Get the latest checkpoint
        checkpoints = pipeline.get_available_checkpoints()
        if checkpoints:
            latest_checkpoint = checkpoints[-1]
            logger.info(f"Resuming from latest checkpoint: {latest_checkpoint}")
            pipeline = EnhancedETLPipeline.load_from_checkpoint(
                checkpoint_file=latest_checkpoint,
                db_config=config['db_config'],
                progress_reporter=progress_reporter
            )
        else:
            logger.warning("No checkpoints available to resume from")

    # Run the pipeline
    logger.info(f"Running ETL pipeline with file: {args.file}")
    start_time = time.time()
    result = pipeline.run_pipeline(
        file_path=args.file,
        user_display_name=args.user,
        resume_from_checkpoint=args.resume or args.checkpoint is not None
    )
    end_time = time.time()

    # Add execution time to result
    result['execution_time_seconds'] = end_time - start_time
    result['execution_time_formatted'] = str(datetime.timedelta(seconds=int(end_time - start_time)))

    return result


def run_analysis(args: argparse.Namespace, result: Dict[str, Any]) -> None:
    """Run analysis on the processed data.

    Args:
        args: Command line arguments
        result: Results from the ETL pipeline
    """
    if not args.analyze:
        return

    logger.info("Running analysis on processed data")

    try:
        # Import analysis modules
        from src.analysis.reporting import SkypeReportGenerator
        from src.analysis.visualization import SkypeDataVisualizer

        # Get export ID from result
        export_id = result.get('export_id')
        if not export_id:
            logger.error("Cannot run analysis: Export ID not found in results")
            return

        # Create output directory
        analysis_dir = os.path.join(args.output, 'analysis')
        os.makedirs(analysis_dir, exist_ok=True)

        # Generate report
        logger.info(f"Generating {args.report_type} report")
        report_generator = SkypeReportGenerator()

        if args.report_type == 'summary':
            report = report_generator.get_export_summary(export_id)
        elif args.report_type == 'full':
            report = report_generator.generate_full_report(export_id)
        else:  # custom
            report = {
                "summary": report_generator.get_export_summary(export_id),
                "message_type_distribution": report_generator.get_message_type_distribution(export_id),
                "top_senders": report_generator.get_top_senders(export_id)
            }

        # Save report to file
        report_file = os.path.join(analysis_dir, f"report_{export_id}.json")
        with open(report_file, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        logger.info(f"Report saved to: {report_file}")

        # Generate visualizations if requested
        if args.visualize:
            logger.info("Generating visualizations")
            visualization_dir = os.path.join(analysis_dir, 'visualizations')
            visualizer = SkypeDataVisualizer(output_dir=visualization_dir)
            visualizations = visualizer.generate_all_visualizations(export_id)
            logger.info(f"Visualizations saved to: {visualization_dir}")

    except ImportError as e:
        logger.error(f"Cannot run analysis: {e}")
        logger.error("Make sure the analysis module is installed")
    except Exception as e:
        logger.exception(f"Error running analysis: {e}")


def print_results(result: Dict[str, Any]) -> None:
    """Print the results of the ETL pipeline.

    Args:
        result: Results from the ETL pipeline
    """
    # Check if the pipeline completed successfully
    if result.get('status') == 'completed':
        logger.info("ETL pipeline completed successfully")
        logger.info(f"Export ID: {result.get('export_id')}")
        logger.info(f"Task ID: {result.get('task_id')}")
        logger.info(f"Execution time: {result.get('execution_time_formatted')}")

        # Print conversation and message counts
        logger.info(f"Processed {result.get('conversation_count', 0)} conversations")
        logger.info(f"Processed {result.get('message_count', 0)} messages")

        # Print phase statistics
        for phase_name, phase_data in result.get('phases', {}).items():
            logger.info(f"{phase_name.capitalize()} phase: {phase_data.get('status', 'unknown')}")

            # Print phase-specific statistics
            if phase_name == 'extract':
                logger.info(f"  Extracted {phase_data.get('conversation_count', 0)} conversations")
            elif phase_name == 'transform':
                logger.info(f"  Transformed {phase_data.get('processed_conversations', 0)} conversations")
                logger.info(f"  Transformed {phase_data.get('processed_messages', 0)} messages")
            elif phase_name == 'load':
                logger.info(f"  Export ID: {phase_data.get('export_id', 'unknown')}")

        # Print attachment statistics if available
        if 'attachment_count' in result:
            logger.info(f"Processed {result.get('attachment_count', 0)} attachments")
            logger.info(f"Downloaded {result.get('downloaded_attachment_count', 0)} attachments")
            logger.info(f"Generated {result.get('thumbnail_count', 0)} thumbnails")
    else:
        logger.error(f"ETL pipeline failed: {result.get('error', 'Unknown error')}")

        # Print phase statuses
        for phase_name, phase_data in result.get('phases', {}).items():
            logger.info(f"{phase_name.capitalize()} phase: {phase_data.get('status', 'unknown')}")


def save_results(result: Dict[str, Any], output_dir: str, args: argparse.Namespace) -> None:
    """Save the results of the ETL pipeline to a file.

    Args:
        result: Results from the ETL pipeline
        output_dir: Output directory
        args: Command line arguments
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Generate output file path
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')

    # Save as JSON if requested
    if args.json_output or not args.csv_output:
        json_file = os.path.join(
            output_dir,
            f"etl_result_{result.get('task_id', 'unknown')}_{timestamp}.json"
        )

        try:
            with open(json_file, 'w') as f:
                json.dump(result, f, indent=2, default=str)
            logger.info(f"Results saved to: {json_file}")
        except Exception as e:
            logger.error(f"Failed to save results to {json_file}: {e}")

    # Save as CSV if requested
    if args.csv_output:
        csv_file = os.path.join(
            output_dir,
            f"etl_result_{result.get('task_id', 'unknown')}_{timestamp}.csv"
        )

        try:
            with open(csv_file, 'w') as f:
                # Write header
                f.write("key,value\n")

                # Write top-level fields
                for key, value in result.items():
                    if isinstance(value, dict):
                        continue
                    f.write(f"{key},{value}\n")

                # Write phase data
                for phase_name, phase_data in result.get('phases', {}).items():
                    for key, value in phase_data.items():
                        f.write(f"{phase_name}_{key},{value}\n")

            logger.info(f"Results saved to: {csv_file}")
        except Exception as e:
            logger.error(f"Failed to save results to {csv_file}: {e}")


def main():
    """Main function."""
    # Parse command line arguments
    args = parse_args()

    # Set up logging
    setup_logging(args)

    try:
        # Validate input file
        validate_input_file(args.file)

        # Get configuration
        config = get_config(args)

        # Run the ETL pipeline
        result = run_etl_pipeline(args, config)

        # Print the results
        print_results(result)

        # Save the results
        save_results(result, config['output_dir'], args)

        # Run analysis if requested
        run_analysis(args, result)

        # Exit with success
        sys.exit(0)
    except FileNotFoundError as e:
        logger.error(f"File not found: {e}")
        sys.exit(1)
    except ValueError as e:
        logger.error(f"Invalid input: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Error running ETL pipeline: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()