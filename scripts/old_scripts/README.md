# SkypeParser Scripts

This directory contains various scripts for parsing, analyzing, and managing Skype export data. Below is a guide to each script and its functionality.

## ETL Pipeline Scripts

### `run_skype_parser.py`
Basic command-line interface for parsing Skype export data.

**Usage:**
```bash
python scripts/run_skype_parser.py -f <skype_export_file> -u <your_display_name>
```

**Key Options:**
- `-f, --file`: Path to the Skype export file (TAR or JSON)
- `-u, --user`: Your display name as it appears in Skype
- `-v, --verbose`: Enable verbose logging
- `--skip-raw`: Skip generating raw JSON output files

### `custom_etl_script.py`
A simple Skype export parser that extracts data from a Skype export file and saves it to JSON files, without database storage. It also generates an HTML report of the parsed data.

**Usage:**
```bash
python scripts/custom_etl_script.py -f <skype_export_file> -u <your_display_name> -v
```

**Key Options:**
- `-f, --file`: Path to the Skype export file (TAR or JSON)
- `-u, --user`: Your display name as it appears in Skype
- `-v, --verbose`: Enable verbose logging
- `-o, --output`: Output directory for files (default: 'output')
- `--skip-raw`: Skip generating raw JSON output files

## Streaming Processing Scripts

### `stream_skype_data.py`
Memory-efficient processing of large Skype export files using streaming techniques. This script is designed for very large exports (>500MB or >100,000 messages) that might cause memory issues with standard processing.

**Usage:**
```bash
python scripts/stream_skype_data.py -f <skype_export_file> -u <your_display_name> -v
```

**Key Options:**
- `-f, --file`: Path to the Skype export file (TAR or JSON)
- `-u, --user`: Your display name as it appears in Skype
- `-v, --verbose`: Enable verbose logging
- `-b, --batch`: Batch size for conversation processing (default: 100)
- `-o, --output`: Output directory for reports (default: 'output')

**Features:**
- Memory-efficient streaming processing
- Works with both TAR and JSON files
- Generates simple HTML reports with statistics
- Progress tracking with detailed logging
- Minimal memory footprint for large exports

**Example:**
```bash
# Process a large Skype export file with verbose logging
python scripts/stream_skype_data.py -f exports/large_skype_export.tar -u "Jane Doe" -v

# Customize batch size and output directory
python scripts/stream_skype_data.py -f exports/large_skype_export.tar -u "Jane Doe" -b 500 -o custom_output
```

## Utility Scripts

### `monkey_patch.py`
This script applies monkey patches to fix issues with the BeautifulSoup warning filters and missing methods in the ContentExtractor class.

**Usage:**
```bash
# This script is typically imported rather than executed directly
from scripts.monkey_patch import apply_patches
apply_patches()
```

**Key Features:**
- Silences BeautifulSoup MarkupResemblesLocatorWarning warnings
- Adds missing format_content_with_markup method to ContentExtractor class
- Provides better error handling for content formatting
- Logs detailed information about patch application

## Benchmarking Scripts

### `benchmark_parsers.py`
Compares performance between standard and streaming parsing methods.

**Usage:**
```bash
python scripts/benchmark_parsers.py -f <skype_export_file> -u <your_display_name>
```

**Key Options:**
- `-f, --file`: Path to the Skype export file to benchmark
- `-u, --user`: Your display name as it appears in Skype
- `-i, --iterations`: Number of benchmark iterations (default: 3)
- `-m, --memory-profiling`: Enable memory profiling (requires memory_profiler)

## Performance Considerations

When deciding which script to use, consider these factors:

1. **Standard Processing (`run_skype_parser.py` or `custom_etl_script.py`)**:
   - Best for exports < 500MB
   - Suitable for < 100,000 messages
   - Provides detailed analysis and full HTML reports
   - Requires more memory

2. **Streaming Processing (`stream_skype_data.py`)**:
   - Best for exports > 500MB
   - Efficient for > 100,000 messages
   - Provides basic statistics and simple reports
   - Uses minimal memory
   - Faster for very large exports

For most users with typical Skype exports, the standard scripts will work well. The streaming script is designed specifically for users with extremely large export files or limited system resources.

## Requirements

All scripts require the SkypeParser core libraries. The streaming script additionally requires:

```bash
pip install ijson
```

## See Also

For more detailed information about processing large files, see:
- [Processing Large Files](../docs/user_guide/processing_large_files.md)
