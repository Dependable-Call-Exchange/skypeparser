# Skype Parser Analysis and Reporting

## Overview

The Skype Parser tool provides robust analysis and reporting functionality for processed Skype export data. This capability is implemented in the `src/analysis` module and can be accessed via the `scripts/run_analysis.py` script.

## Prerequisites

Before utilizing the analysis and reporting features, ensure you have:

1. Processed a Skype export through the ETL pipeline.
2. The export ID of the processed data.
3. Installed the necessary visualization dependencies if generating visualizations:
   ```bash
   pip install matplotlib pandas
   ```

## Usage

The `scripts/run_analysis.py` script serves as the primary interface for analysis and reporting:

```bash
python scripts/run_analysis.py --export-id <export_id> [options]
```

### Options

- `--export-id`: Specifies the export ID to analyze (required).
- `--output-dir`: Sets the directory for output files (default: `output/analysis`).
- `--config`: Defines a configuration file (default: `config/config.json`).
- `--report-type`: Chooses the type of report (`summary`, `full`, `custom`) (default: `full`).
- `--visualize`: Enables visualization generation.
- `--query`: Executes a specific query (e.g., `find_conversations_with_keyword:hello`).
- `--format`: Determines output format (`json`, `csv`, `text`) (default: `json`).
- `--verbose`: Activates verbose output for detailed logs.

## Reports

### Summary Report

Provides an overview of the export, including:
- Total conversations
- Total messages
- Date range

```bash
python scripts/run_analysis.py --export-id <export_id> --report-type summary
```

### Full Report

Offers comprehensive insights such as:
- Export summary
- Conversation statistics
- Message type distribution
- Activity trends (hourly/daily)
- Top senders
- Message length statistics

```bash
python scripts/run_analysis.py --export-id <export_id> --report-type full
```

### Custom Report

Generates a tailored subset of the full report:
- Export summary
- Message type distribution
- Top senders

```bash
python scripts/run_analysis.py --export-id <export_id> --report-type custom
```

## Visualizations

To generate graphical insights, use the `--visualize` option:

```bash
python scripts/run_analysis.py --export-id <export_id> --visualize
```

Generated visualizations include:
- **Message Type Distribution** (Pie Chart)
- **Activity by Hour** (Bar Chart)
- **Activity by Day of Week** (Bar Chart)
- **Top Senders** (Horizontal Bar Chart)
- **Conversation Statistics** (Horizontal Bar Chart)

By default, visualizations are saved in `output/analysis/visualizations`.

## Queries

The tool supports predefined queries for targeted analysis. Use the `--query` option:

```bash
python scripts/run_analysis.py --export-id <export_id> --query <query_name>:<query_args>
```

### Available Queries

- `find_conversations_with_keyword:<keyword>` – Identifies conversations containing a specified keyword.
- `get_conversation_timeline:<conversation_id>[,<interval>]` – Retrieves conversation activity timeline.
- `get_user_activity_timeline:<sender_name>[,<interval>]` – Retrieves user activity timeline.
- `get_conversation_participants:<conversation_id>` – Lists participants in a conversation.
- `get_message_length_by_sender[:<limit>]` – Computes average message length by sender.
- `get_attachment_statistics` – Provides statistics on Skype attachments.

#### Example Queries

**Find conversations with "hello":**
```bash
python scripts/run_analysis.py --export-id <export_id> --query find_conversations_with_keyword:hello
```

**Retrieve user activity timeline grouped by week:**
```bash
python scripts/run_analysis.py --export-id <export_id> --query get_user_activity_timeline:"John Doe",week
```

## Output Formats

Analysis results can be exported in multiple formats:
- `json` (default)
- `csv`
- `text`

Specify the format using:
```bash
python scripts/run_analysis.py --export-id <export_id> --format csv
```

## Programmatic Usage

The reporting and visualization features can be used within custom Python scripts:

```python
from src.analysis.reporting import SkypeReportGenerator
from src.analysis.visualization import SkypeDataVisualizer
from src.analysis.queries import SkypeQueryExamples

# Initialize report generator
report_generator = SkypeReportGenerator()
report = report_generator.generate_full_report(export_id)

# Initialize visualizer
visualizer = SkypeDataVisualizer()
visualizations = visualizer.generate_all_visualizations(export_id)

# Execute a query
query_examples = SkypeQueryExamples()
results = query_examples.find_conversations_with_keyword(export_id, "hello")
```

## Extending Functionality

### Adding New Queries

To extend query functionality, add methods in `SkypeQueryExamples`:

```python
def get_most_active_hours(self, export_id: int, limit: int = 5) -> List[Dict[str, Any]]:
    """
    Retrieves the most active messaging hours.
    """
    query = """
        SELECT EXTRACT(HOUR FROM timestamp) as hour, COUNT(*) as message_count
        FROM skype_messages WHERE export_id = %s
        GROUP BY hour ORDER BY message_count DESC LIMIT %s
    """
    return self.db_connection.execute_query(query, (export_id, limit))
```

Then, update `scripts/run_analysis.py` to support the new query:

```python
elif query_name == 'get_most_active_hours':
    limit = int(query_args) if query_args and query_args.isdigit() else 5
    result = query_examples.get_most_active_hours(export_id, limit)
```

## Summary

The Skype Parser analysis and reporting module offers powerful tools for extracting meaningful insights from Skype data. With structured reports, advanced queries, and visualization support, users can efficiently analyze communication patterns and trends. The modular design allows for easy extension and integration into broader analytical workflows.

