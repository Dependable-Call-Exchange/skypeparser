# Enhanced CLI for Skype Parser

This document provides information about the enhanced command-line interface (CLI) for the Skype Parser tool.

## Overview

The enhanced CLI provides additional options, progress reporting, and improved logging compared to the standard CLI. It is designed to make it easier to use the Skype Parser tool for processing large Skype export files.

## Prerequisites

Before using the enhanced CLI, you need to:

1. Install the required dependencies:
   ```bash
   pip install tqdm
   ```

2. If you want to use the analysis and visualization features, you also need to install:
   ```bash
   pip install matplotlib pandas
   ```

## Usage

The enhanced CLI can be accessed through the `scripts/run_etl_pipeline_enhanced.py` script. Here's the basic usage:

```bash
python scripts/run_etl_pipeline_enhanced.py -f path/to/skype_export.tar -u "Your Name" [options]
```

### Options

The enhanced CLI provides a wide range of options, organized into several groups:

#### Input File Options

- `-f, --file`: Path to the Skype export file (.tar or .json) (required)
- `-u, --user`: User display name for the export

#### Configuration Options

- `-c, --config`: Path to the configuration file (default: `config/config.json`)
- `-o, --output`: Output directory for intermediate files (default: `output`)

#### Database Options

- `--db-name`: Database name (overrides config file)
- `--db-user`: Database user (overrides config file)
- `--db-password`: Database password (overrides config file)
- `--db-host`: Database host (overrides config file)
- `--db-port`: Database port (overrides config file)
- `--db-schema`: Database schema (overrides config file)
- `--db-ssl-mode`: Database SSL mode (overrides config file)
- `--db-ssl-root-cert`: Database SSL root certificate (overrides config file)

#### Performance Options

- `-m, --memory`: Memory limit in MB (default: 1024)
- `-p, --parallel`: Enable parallel processing
- `-s, --chunk-size`: Chunk size for batch processing (default: 1000)
- `-b, --batch-size`: Batch size for database operations (default: 100)
- `-w, --workers`: Maximum number of worker threads/processes

#### Checkpoint Options

- `-r, --resume`: Resume from the latest checkpoint
- `--checkpoint`: Resume from a specific checkpoint file
- `--list-checkpoints`: List available checkpoints and exit
- `--checkpoint-interval`: Number of items to process before creating a checkpoint (default: 1000)
- `--checkpoint-dir`: Directory to store checkpoints (defaults to `output_dir/checkpoints`)

#### Attachment Options

- `--download-attachments`: Download attachments from URLs
- `--attachments-dir`: Directory to store downloaded attachments (defaults to `output_dir/attachments`)
- `--no-thumbnails`: Disable thumbnail generation for image attachments
- `--no-metadata`: Disable metadata extraction from attachments
- `--max-attachment-size`: Maximum attachment size in MB (default: 10)
- `--attachment-timeout`: Timeout for attachment downloads in seconds (default: 30)

#### Output Options

- `--json-output`: Output results in JSON format
- `--csv-output`: Output results in CSV format
- `--no-progress`: Disable progress reporting
- `--quiet`: Suppress all output except errors

#### Analysis Options

- `--analyze`: Run analysis after ETL pipeline completes
- `--visualize`: Generate visualizations after ETL pipeline completes
- `--report-type`: Type of report to generate if `--analyze` is specified (`summary`, `full`, `custom`) (default: `summary`)

#### Debug Options

- `-v, --verbose`: Enable verbose logging
- `-d, --debug`: Enable debug logging
- `--dry-run`: Validate input and configuration without running the pipeline
- `--log-file`: Path to log file (defaults to `etl_pipeline.log`)
- `--log-format`: Log format string

## Examples

Here are some examples of how to use the enhanced CLI:

### Basic Usage

```bash
python scripts/run_etl_pipeline_enhanced.py -f path/to/skype_export.tar -u "Your Name"
```

### Resuming from a Checkpoint

```bash
python scripts/run_etl_pipeline_enhanced.py -f path/to/skype_export.tar -u "Your Name" --resume
```

### Parallel Processing

```bash
python scripts/run_etl_pipeline_enhanced.py -f path/to/skype_export.tar -u "Your Name" --parallel
```

### Downloading Attachments

```bash
python scripts/run_etl_pipeline_enhanced.py -f path/to/skype_export.tar -u "Your Name" --download-attachments
```

### Running Analysis and Generating Visualizations

```bash
python scripts/run_etl_pipeline_enhanced.py -f path/to/skype_export.tar -u "Your Name" --analyze --visualize
```

### Dry Run

```bash
python scripts/run_etl_pipeline_enhanced.py -f path/to/skype_export.tar -u "Your Name" --dry-run
```

### Listing Available Checkpoints

```bash
python scripts/run_etl_pipeline_enhanced.py -f path/to/skype_export.tar -u "Your Name" --list-checkpoints
```

### Customizing Output

```bash
python scripts/run_etl_pipeline_enhanced.py -f path/to/skype_export.tar -u "Your Name" --json-output --csv-output
```

### Customizing Logging

```bash
python scripts/run_etl_pipeline_enhanced.py -f path/to/skype_export.tar -u "Your Name" --debug --log-file custom_log.log
```

## Progress Reporting

The enhanced CLI includes progress reporting using the `tqdm` library. This provides a progress bar that shows the current status of the ETL pipeline, including:

- The current phase (Extracting, Transforming, Loading)
- The overall progress as a percentage
- The estimated time remaining
- The processing rate

If you don't want to see the progress bar, you can use the `--no-progress` option.

## Logging Improvements

The enhanced CLI includes improved logging compared to the standard CLI:

- More detailed log messages
- Configurable log level (`--debug`, `--verbose`, `--quiet`)
- Configurable log file (`--log-file`)
- Configurable log format (`--log-format`)

## Analysis and Visualization

The enhanced CLI includes options for running analysis and generating visualizations after the ETL pipeline completes:

- `--analyze`: Run analysis on the processed data
- `--visualize`: Generate visualizations of the processed data
- `--report-type`: Type of report to generate (`summary`, `full`, `custom`)

The analysis results and visualizations are saved to the `output/analysis` directory.

## Dry Run Mode

The enhanced CLI includes a dry run mode that validates the input file and configuration without actually running the ETL pipeline. This is useful for checking that everything is set up correctly before starting a potentially long-running process.

To use dry run mode, use the `--dry-run` option:

```bash
python scripts/run_etl_pipeline_enhanced.py -f path/to/skype_export.tar -u "Your Name" --dry-run
```

## Checkpoint Management

The enhanced CLI includes improved checkpoint management compared to the standard CLI:

- List available checkpoints with `--list-checkpoints`
- Resume from a specific checkpoint with `--checkpoint`
- Resume from the latest checkpoint with `--resume`
- Configure the checkpoint interval with `--checkpoint-interval`
- Configure the checkpoint directory with `--checkpoint-dir`

## Attachment Handling

The enhanced CLI includes improved attachment handling compared to the standard CLI:

- Download attachments with `--download-attachments`
- Configure the attachments directory with `--attachments-dir`
- Disable thumbnail generation with `--no-thumbnails`
- Disable metadata extraction with `--no-metadata`
- Configure the maximum attachment size with `--max-attachment-size`
- Configure the attachment download timeout with `--attachment-timeout`

## Output Formats

The enhanced CLI can output results in different formats:

- JSON format with `--json-output`
- CSV format with `--csv-output`

If neither option is specified, the results are saved in JSON format by default.

## Quiet Mode

The enhanced CLI includes a quiet mode that suppresses all output except errors. This is useful for running the ETL pipeline in a script or as a background process.

To use quiet mode, use the `--quiet` option:

```bash
python scripts/run_etl_pipeline_enhanced.py -f path/to/skype_export.tar -u "Your Name" --quiet
```

## Conclusion

The enhanced CLI provides a more user-friendly and feature-rich interface for the Skype Parser tool. It includes progress reporting, improved logging, analysis and visualization options, and many other features that make it easier to use the tool for processing large Skype export files.