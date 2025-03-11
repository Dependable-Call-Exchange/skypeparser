# Data Visualization Guide

This guide explains how to use the visualization features of SkypeParser to create visual representations of your Skype data.

## Overview

SkypeParser includes built-in visualization capabilities that allow you to generate charts and graphs from your Skype data. These visualizations can help you understand patterns in your conversations, identify active participants, and analyze message frequency over time.

## Prerequisites

To use the visualization features, you need:

1. Skype data processed and stored in a database
2. Python with the following packages installed:
   ```bash
   pip install matplotlib pandas seaborn
   ```

## Basic Visualization

The simplest way to generate visualizations is to use the `--visualize` flag with the analysis script:

```bash
python -m src.analysis.visualize --export-id <export_id> --output-dir visualizations
```

This will generate a set of standard visualizations and save them to the specified output directory.

## Standard Visualizations

SkypeParser can generate the following standard visualizations:

### Message Activity Over Time

![Message Activity](../assets/images/message_activity_example.png)

This visualization shows the number of messages sent over time, allowing you to identify periods of high and low activity.

```bash
python -m src.analysis.visualize --export-id <export_id> --type activity_timeline
```

### Message Distribution by User

![User Distribution](../assets/images/user_distribution_example.png)

This visualization shows the distribution of messages among users, helping you identify the most active participants.

```bash
python -m src.analysis.visualize --export-id <export_id> --type user_distribution
```

### Message Types

![Message Types](../assets/images/message_types_example.png)

This visualization shows the distribution of different message types (text, images, calls, etc.).

```bash
python -m src.analysis.visualize --export-id <export_id> --type message_types
```

### Activity Heatmap

![Activity Heatmap](../assets/images/activity_heatmap_example.png)

This visualization shows message activity by day of week and hour of day, helping you identify when conversations are most active.

```bash
python -m src.analysis.visualize --export-id <export_id> --type activity_heatmap
```

### Conversation Network

![Conversation Network](../assets/images/conversation_network_example.png)

This visualization shows the network of conversations and participants, helping you understand the relationships between different users and conversations.

```bash
python -m src.analysis.visualize --export-id <export_id> --type conversation_network
```

## Custom Visualizations

You can create custom visualizations using the SkypeParser API:

```python
from src.analysis.visualization import SkypeDataVisualizer
from src.utils.db_connection import DatabaseConnection

# Create database connection
db_conn = DatabaseConnection(
    host='localhost',
    port=5432,
    dbname='skype_data',
    user='postgres',
    password='password'
)

# Create visualizer
visualizer = SkypeDataVisualizer(db_conn)

# Generate custom visualization
visualizer.create_custom_visualization(
    export_id=1,
    query="""
        SELECT
            DATE_TRUNC('day', timestamp) as day,
            COUNT(*) as message_count
        FROM
            skype_messages
        WHERE
            export_id = %s
        GROUP BY
            day
        ORDER BY
            day
    """,
    query_params=(1,),
    x_column='day',
    y_column='message_count',
    title='Messages per Day',
    output_file='messages_per_day.png',
    chart_type='line'
)
```

## Interactive Visualizations

For more advanced interactive visualizations, you can use the SkypeParser data with tools like Plotly or Dash:

```python
import plotly.express as px
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

# Get message count by day
message_counts = queries.get_message_count_by_day(export_id=1)

# Create interactive visualization
fig = px.line(
    message_counts,
    x='day',
    y='message_count',
    title='Messages per Day'
)
fig.show()
```

## Exporting Visualizations

Visualizations can be exported in various formats:

```bash
# Export as PNG
python -m src.analysis.visualize --export-id <export_id> --type activity_timeline --format png

# Export as SVG
python -m src.analysis.visualize --export-id <export_id> --type activity_timeline --format svg

# Export as PDF
python -m src.analysis.visualize --export-id <export_id> --type activity_timeline --format pdf
```

## Customizing Visualization Appearance

You can customize the appearance of visualizations using a configuration file:

```yaml
# visualization_config.yaml
theme: 'dark_background'  # or 'default', 'seaborn', 'ggplot', etc.
figure:
  figsize: [12, 8]
  dpi: 300
colors:
  palette: 'viridis'  # or 'plasma', 'inferno', 'magma', etc.
  custom_colors:
    - '#FF5733'
    - '#33FF57'
    - '#3357FF'
fonts:
  family: 'sans-serif'
  size: 12
```

Then use the configuration file:

```bash
python -m src.analysis.visualize --export-id <export_id> --config visualization_config.yaml
```

## Batch Visualization

You can generate multiple visualizations at once:

```bash
python -m src.analysis.visualize --export-id <export_id> --types activity_timeline,user_distribution,message_types
```

## Troubleshooting

### Common Issues

1. **Missing Dependencies**:
   If you encounter errors about missing dependencies, install the required packages:
   ```bash
   pip install matplotlib pandas seaborn plotly
   ```

2. **No Data to Visualize**:
   Ensure that your database contains data for the specified export ID.

3. **Memory Issues**:
   For large datasets, you may need to filter the data or increase your system's memory.

### Getting Help

If you encounter issues that aren't covered here, please:

1. Check the [documentation](../README.md) for more information
2. Look for similar issues in the project's issue tracker
3. Ask for help in the project's discussion forum

## Next Steps

Now that you understand how to create visualizations with SkypeParser, you can:

- Explore the [Data Analysis Guide](data-analysis.md) for more advanced analysis techniques
- Learn about [Large Datasets](large-datasets.md) if you're working with a large amount of data
- Check out the [API Reference](../developer-guide/api-reference.md) for more details on the visualization API