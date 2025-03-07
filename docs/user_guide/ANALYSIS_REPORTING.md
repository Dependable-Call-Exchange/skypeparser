# Skype Parser Analysis and Reporting

This document provides information about the analysis and reporting functionality in the Skype Parser tool.

## Overview

The Skype Parser tool includes functionality for analyzing and reporting on Skype data that has been processed by the ETL pipeline. This functionality is provided by the `src/analysis` module and can be accessed through the `scripts/run_analysis.py` script.

## Prerequisites

Before using the analysis and reporting functionality, you need to:

1. Process a Skype export using the ETL pipeline
2. Have the export ID of the processed data
3. Install the optional visualization dependencies if you want to generate visualizations:
   ```bash
   pip install matplotlib pandas
   ```

## Usage

The analysis and reporting functionality can be accessed through the `scripts/run_analysis.py` script. Here's the basic usage:

```bash
python scripts/run_analysis.py --export-id <export_id> [options]
```

### Options

- `--export-id`: The ID of the export to analyze (required)
- `--output-dir`: Directory to save output files (default: `output/analysis`)
- `--config`: Path to configuration file (default: `config/config.json`)
- `--report-type`: Type of report to generate (`summary`, `full`, `custom`) (default: `full`)
- `--visualize`: Generate visualizations
- `--query`: Run a specific query (e.g., `find_conversations_with_keyword:hello`)
- `--format`: Output format for reports (`json`, `csv`, `text`) (default: `json`)
- `--verbose`: Enable verbose output

## Reports

The analysis and reporting functionality can generate different types of reports:

### Summary Report

A summary report includes basic information about the export, such as the number of conversations, messages, and the date range.

```bash
python scripts/run_analysis.py --export-id <export_id> --report-type summary
```

### Full Report

A full report includes all available information about the export, including:

- Export summary
- Conversation statistics
- Message type distribution
- Activity by hour
- Activity by day of week
- Top senders
- Message length statistics

```bash
python scripts/run_analysis.py --export-id <export_id> --report-type full
```

### Custom Report

A custom report includes a subset of the full report, specifically:

- Export summary
- Message type distribution
- Top senders

```bash
python scripts/run_analysis.py --export-id <export_id> --report-type custom
```

## Visualizations

The analysis and reporting functionality can generate visualizations of the data. To generate visualizations, use the `--visualize` option:

```bash
python scripts/run_analysis.py --export-id <export_id> --visualize
```

This will generate the following visualizations:

- Message type distribution (pie chart)
- Activity by hour (bar chart)
- Activity by day of week (bar chart)
- Top senders (horizontal bar chart)
- Conversation statistics (horizontal bar chart)

Visualizations are saved to the `output/analysis/visualizations` directory by default.

## Queries

The analysis and reporting functionality includes several predefined queries that can be used to extract specific information from the data. To run a query, use the `--query` option:

```bash
python scripts/run_analysis.py --export-id <export_id> --query <query_name>:<query_args>
```

### Available Queries

- `find_conversations_with_keyword:<keyword>`: Find conversations containing a specific keyword
- `get_conversation_timeline:<conversation_id>[,<interval>]`: Get a timeline of message activity for a specific conversation
- `get_user_activity_timeline:<sender_name>[,<interval>]`: Get a timeline of message activity for a specific user
- `get_conversation_participants:<conversation_id>`: Get a list of participants in a specific conversation
- `get_message_length_by_sender[:<limit>]`: Get average message length by sender
- `get_attachment_statistics`: Get statistics about attachments in a Skype export

### Examples

Find conversations containing the word "hello":

```bash
python scripts/run_analysis.py --export-id <export_id> --query find_conversations_with_keyword:hello
```

Get a timeline of message activity for a specific conversation, grouped by day:

```bash
python scripts/run_analysis.py --export-id <export_id> --query get_conversation_timeline:19:skypeid,day
```

Get a timeline of message activity for a specific user, grouped by week:

```bash
python scripts/run_analysis.py --export-id <export_id> --query get_user_activity_timeline:"John Doe",week
```

Get a list of participants in a specific conversation:

```bash
python scripts/run_analysis.py --export-id <export_id> --query get_conversation_participants:19:skypeid
```

Get average message length by sender, limited to the top 5:

```bash
python scripts/run_analysis.py --export-id <export_id> --query get_message_length_by_sender:5
```

Get statistics about attachments in a Skype export:

```bash
python scripts/run_analysis.py --export-id <export_id> --query get_attachment_statistics
```

## Output Formats

The analysis and reporting functionality can output reports and query results in different formats:

- `json`: JSON format (default)
- `csv`: CSV format
- `text`: Plain text format

To specify the output format, use the `--format` option:

```bash
python scripts/run_analysis.py --export-id <export_id> --format csv
```

## Programmatic Usage

The analysis and reporting functionality can also be used programmatically in your own Python code:

```python
from src.analysis.reporting import SkypeReportGenerator
from src.analysis.visualization import SkypeDataVisualizer
from src.analysis.queries import SkypeQueryExamples

# Create a report generator
report_generator = SkypeReportGenerator()

# Generate a report
report = report_generator.generate_full_report(export_id)

# Create a visualizer
visualizer = SkypeDataVisualizer()

# Generate visualizations
visualizations = visualizer.generate_all_visualizations(export_id)

# Create a query examples object
query_examples = SkypeQueryExamples()

# Run a query
results = query_examples.find_conversations_with_keyword(export_id, "hello")
```

## Extending the Functionality

The analysis and reporting functionality can be extended by adding new methods to the `SkypeReportGenerator`, `SkypeDataVisualizer`, and `SkypeQueryExamples` classes.

For example, to add a new query, you would add a new method to the `SkypeQueryExamples` class:

```python
def get_most_active_hours(self, export_id: int, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Get the most active hours of the day.

    Args:
        export_id: The ID of the export to analyze.
        limit: The maximum number of hours to return.

    Returns:
        A list of dictionaries containing the most active hours.
    """
    query = """
        SELECT
            EXTRACT(HOUR FROM timestamp) as hour,
            COUNT(*) as message_count
        FROM
            skype_messages
        WHERE
            export_id = %s
        GROUP BY
            hour
        ORDER BY
            message_count DESC
        LIMIT %s
    """

    result = self.db_connection.execute_query(query, (export_id, limit))

    return result if result else []
```

Then, you would update the `run_query` function in `scripts/run_analysis.py` to handle the new query:

```python
elif query_name == 'get_most_active_hours':
    limit = int(query_args) if query_args and query_args.isdigit() else 5
    result = query_examples.get_most_active_hours(export_id, limit)
```