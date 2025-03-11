# SkypeParser Documentation

Welcome to the SkypeParser documentation. This documentation provides comprehensive information about the SkypeParser project, a Python tool for parsing, analyzing, and storing Skype export data.

## Documentation Structure

The documentation is organized into the following sections:

### [Project Summary](PROJECT_SUMMARY.md)

A comprehensive overview of the SkypeParser project, including its key features, architecture, and components.

### [Getting Started](getting-started/)

Quick start guides for installation and basic usage:

- [Installation Guide](getting-started/installation.md) - Installation instructions
- [Basic Usage Guide](getting-started/basic-usage.md) - Basic usage examples
- [Configuration Guide](getting-started/configuration.md) - Configuration options

### [User Guide](user-guide/)

Comprehensive documentation for end users:

- [CLI Reference](user-guide/cli-reference.md) - Command-line interface reference
- [Data Analysis](user-guide/data-analysis.md) - Analyzing Skype data
- [Visualization](user-guide/visualization.md) - Visualizing Skype data
- [Large Datasets](user-guide/large-datasets.md) - Processing large datasets
- [Message Types](user-guide/message-types.md) - Information about supported message types
- [Content Extraction](user-guide/content-extraction.md) - Details on content extraction features
- [Checkpoint Resumption](user-guide/checkpoint-resumption.md) - Resuming processing from checkpoints
- [Non-Interactive Mode](user-guide/non-interactive.md) - Running in non-interactive mode
- [Database Integration](user-guide/database/) - Database setup and integration
- [Web App Development](user-guide/web-app.md) - Building web applications with SkypeParser

### [Developer Guide](developer-guide/)

Documentation for developers contributing to the project:

- [Architecture](developer-guide/architecture.md) - System architecture overview
- [Modularization](developer-guide/modularization.md) - Modularization strategy
- [SOLID Principles](developer-guide/solid-principles.md) - SOLID principles implementation
- [Dependency Injection](developer-guide/dependency-injection.md) - Dependency injection framework
- [ETL Context API](developer-guide/etl-context-api.md) - ETL context API documentation
- [Factory Pattern](developer-guide/factory-pattern.md) - Factory pattern implementation
- [Error Handling](developer-guide/error-handling.md) - Error handling guidelines
- [Contributing](developer-guide/contributing.md) - Contribution guidelines
- [Testing](developer-guide/testing.md) - Testing guidelines and framework
- [Performance](developer-guide/performance.md) - Performance optimization guidelines
- [Input Validation](developer-guide/input-validation.md) - Input validation guidelines
- [API Reference](developer-guide/api-reference.md) - API documentation

### [Implementation Details](implementation/)

Details on specific implementation aspects:

- [ETL Pipeline](implementation/etl-pipeline.md) - ETL pipeline implementation details
- [Validation](implementation/validation.md) - Validation implementation details
- [Loader Optimization](implementation/loader-optimization.md) - Loader optimization details
- [Logging](implementation/logging.md) - Logging implementation details
- [Refactoring](implementation/refactoring.md) - Refactoring summary and details
- [Roadmap](implementation/roadmap.md) - Project roadmap and planned improvements
- [Completed Features](implementation/completed-features.md) - List of completed features
- [Monkey Patching](implementation/MONKEYPATCH_GUIDE.md) - Guide to monkey patching in the project
- [File Path Constraints](implementation/file_path_constraint_fix.md) - Information about file path constraint fixes
- [ETL Implementation Summary](implementation/SUMMARY.md) - Summary of ETL implementation

## Common Workflows

### Basic Parsing

The simplest workflow is to parse a Skype export file and output the conversations to text files:

```bash
python -m src.parser.skype_parser path/to/skype_export.tar -t -o output_dir -u "Your Name"
```

See the [Basic Usage Guide](getting-started/basic-usage.md) for more details.

### Database Storage

To store the parsed data in a PostgreSQL database:

```bash
python -m src.parser.skype_parser path/to/skype_export.tar -t --store-db --db-name skype_archive --db-user postgres -u "Your Name"
```

See [PostgreSQL Setup](user-guide/database/postgres.md) for more details.

### ETL Pipeline

For more advanced usage, you can use the ETL (Extract, Transform, Load) pipeline:

```bash
python scripts/run_etl_pipeline.py --file path/to/skype_export.tar --user "Your Name"
```

See [ETL Pipeline](implementation/etl-pipeline.md) for more details.

### Data Analysis and Visualization

After processing your Skype data, you can analyze and visualize it using the provided tools:

```python
from src.analysis.queries import SkypeQueryExamples
from src.utils.db_connection import DatabaseConnection

# Create database connection
db_conn = DatabaseConnection(
    host='localhost',
    port=5432,
    dbname='skype_data',
    user='postgres',
    password='password'
)

# Create query object
queries = SkypeQueryExamples(db_conn)

# Get message count by conversation
message_counts = queries.get_message_count_by_conversation()
for conv_name, count in message_counts:
    print(f"{conv_name}: {count} messages")
```

See [Data Analysis](user-guide/data-analysis.md) and [Visualization](user-guide/visualization.md) for more details.

### Building a Web App

You can build a web application to browse and search your Skype data:

```bash
# Start the backend API
cd backend
python app.py

# Start the frontend
cd frontend
npm start
```

See [Web App Development](user-guide/web-app.md) for more details.

## Getting Help

If you encounter issues or have questions, please:

1. Check the documentation in the `docs` directory
2. Review the examples in the `examples` directory
3. Examine the logs for error messages
4. Open an issue on the GitHub repository
