"""
Visualization Module for Skype Parser

This module provides functionality for generating visualizations of Skype data
that has been processed by the ETL pipeline.
"""

import logging
import os
from typing import Dict, List, Any, Optional, Tuple
import json
from datetime import datetime

# Import visualization libraries conditionally to avoid hard dependencies
try:
    import matplotlib
    matplotlib.use('Agg')  # Use non-interactive backend
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.ticker import MaxNLocator
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

from src.analysis.reporting import SkypeReportGenerator

logger = logging.getLogger(__name__)

class SkypeDataVisualizer:
    """
    A class for generating visualizations of Skype data.

    This class provides methods for generating various visualizations of Skype data
    that has been processed by the ETL pipeline and stored in a PostgreSQL database.
    """

    def __init__(self, report_generator: Optional[SkypeReportGenerator] = None, output_dir: str = "output/visualizations"):
        """
        Initialize the SkypeDataVisualizer.

        Args:
            report_generator: A SkypeReportGenerator object. If None, a new one will be created.
            output_dir: The directory where visualizations will be saved.
        """
        self.report_generator = report_generator or SkypeReportGenerator()
        self.output_dir = output_dir

        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)

        # Check if visualization libraries are available
        if not MATPLOTLIB_AVAILABLE:
            logger.warning("Matplotlib is not installed. Visualizations will not be generated.")

        if not PANDAS_AVAILABLE:
            logger.warning("Pandas is not installed. Some visualizations may not be available.")

        logger.info("SkypeDataVisualizer initialized")

    def _check_visualization_libraries(self) -> bool:
        """
        Check if visualization libraries are available.

        Returns:
            True if visualization libraries are available, False otherwise.
        """
        if not MATPLOTLIB_AVAILABLE:
            logger.warning("Matplotlib is not installed. Visualizations cannot be generated.")
            return False
        return True

    def visualize_message_type_distribution(self, export_id: int, filename: str = "message_type_distribution.png") -> Optional[str]:
        """
        Generate a pie chart of message type distribution.

        Args:
            export_id: The ID of the export to visualize.
            filename: The filename for the visualization.

        Returns:
            The path to the saved visualization, or None if visualization could not be generated.
        """
        if not self._check_visualization_libraries():
            return None

        # Get message type distribution data
        data = self.report_generator.get_message_type_distribution(export_id)

        if not data:
            logger.warning(f"No message type distribution data found for export ID {export_id}")
            return None

        # Create figure
        plt.figure(figsize=(10, 8))

        # Extract data for pie chart
        labels = [item["message_type"] for item in data]
        sizes = [item["count"] for item in data]

        # Create pie chart
        plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
        plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle

        # Add title
        plt.title(f"Message Type Distribution (Export ID: {export_id})")

        # Save figure
        output_path = os.path.join(self.output_dir, filename)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"Message type distribution visualization saved to {output_path}")

        return output_path

    def visualize_activity_by_hour(self, export_id: int, filename: str = "activity_by_hour.png") -> Optional[str]:
        """
        Generate a bar chart of activity by hour.

        Args:
            export_id: The ID of the export to visualize.
            filename: The filename for the visualization.

        Returns:
            The path to the saved visualization, or None if visualization could not be generated.
        """
        if not self._check_visualization_libraries():
            return None

        # Get activity by hour data
        data = self.report_generator.get_activity_by_hour(export_id)

        if not data:
            logger.warning(f"No activity by hour data found for export ID {export_id}")
            return None

        # Create figure
        plt.figure(figsize=(12, 6))

        # Extract data for bar chart
        hours = [int(item["hour"]) for item in data]
        counts = [item["message_count"] for item in data]

        # Create bar chart
        plt.bar(hours, counts, color='skyblue')

        # Add labels and title
        plt.xlabel('Hour of Day (24-hour format)')
        plt.ylabel('Number of Messages')
        plt.title(f"Message Activity by Hour of Day (Export ID: {export_id})")

        # Set x-axis ticks
        plt.xticks(range(0, 24))

        # Add grid
        plt.grid(axis='y', linestyle='--', alpha=0.7)

        # Save figure
        output_path = os.path.join(self.output_dir, filename)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"Activity by hour visualization saved to {output_path}")

        return output_path

    def visualize_activity_by_day_of_week(self, export_id: int, filename: str = "activity_by_day_of_week.png") -> Optional[str]:
        """
        Generate a bar chart of activity by day of week.

        Args:
            export_id: The ID of the export to visualize.
            filename: The filename for the visualization.

        Returns:
            The path to the saved visualization, or None if visualization could not be generated.
        """
        if not self._check_visualization_libraries():
            return None

        # Get activity by day of week data
        data = self.report_generator.get_activity_by_day_of_week(export_id)

        if not data:
            logger.warning(f"No activity by day of week data found for export ID {export_id}")
            return None

        # Create figure
        plt.figure(figsize=(10, 6))

        # Sort data by day of week
        data.sort(key=lambda x: x["day_of_week"])

        # Extract data for bar chart
        days = [item["day_name"] for item in data]
        counts = [item["message_count"] for item in data]

        # Create bar chart
        plt.bar(days, counts, color='lightgreen')

        # Add labels and title
        plt.xlabel('Day of Week')
        plt.ylabel('Number of Messages')
        plt.title(f"Message Activity by Day of Week (Export ID: {export_id})")

        # Add grid
        plt.grid(axis='y', linestyle='--', alpha=0.7)

        # Save figure
        output_path = os.path.join(self.output_dir, filename)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"Activity by day of week visualization saved to {output_path}")

        return output_path

    def visualize_top_senders(self, export_id: int, limit: int = 10, filename: str = "top_senders.png") -> Optional[str]:
        """
        Generate a horizontal bar chart of top message senders.

        Args:
            export_id: The ID of the export to visualize.
            limit: The maximum number of senders to include.
            filename: The filename for the visualization.

        Returns:
            The path to the saved visualization, or None if visualization could not be generated.
        """
        if not self._check_visualization_libraries():
            return None

        # Get top senders data
        data = self.report_generator.get_top_senders(export_id, limit)

        if not data:
            logger.warning(f"No top senders data found for export ID {export_id}")
            return None

        # Create figure
        plt.figure(figsize=(12, 8))

        # Extract data for bar chart
        senders = [item["sender_name"] for item in data]
        counts = [item["message_count"] for item in data]

        # Reverse the order for better visualization
        senders.reverse()
        counts.reverse()

        # Create horizontal bar chart
        plt.barh(senders, counts, color='coral')

        # Add labels and title
        plt.xlabel('Number of Messages')
        plt.ylabel('Sender')
        plt.title(f"Top {len(data)} Message Senders (Export ID: {export_id})")

        # Add grid
        plt.grid(axis='x', linestyle='--', alpha=0.7)

        # Add count labels to the bars
        for i, count in enumerate(counts):
            plt.text(count + 0.5, i, str(count), va='center')

        # Save figure
        output_path = os.path.join(self.output_dir, filename)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"Top senders visualization saved to {output_path}")

        return output_path

    def visualize_conversation_statistics(self, export_id: int, limit: int = 10, filename: str = "conversation_statistics.png") -> Optional[str]:
        """
        Generate a horizontal bar chart of conversation statistics.

        Args:
            export_id: The ID of the export to visualize.
            limit: The maximum number of conversations to include.
            filename: The filename for the visualization.

        Returns:
            The path to the saved visualization, or None if visualization could not be generated.
        """
        if not self._check_visualization_libraries():
            return None

        # Get conversation statistics data
        data = self.report_generator.get_conversation_statistics(export_id, limit)

        if not data:
            logger.warning(f"No conversation statistics data found for export ID {export_id}")
            return None

        # Create figure
        plt.figure(figsize=(12, 8))

        # Extract data for bar chart
        conversations = [item["display_name"] for item in data]
        counts = [item["message_count"] for item in data]

        # Reverse the order for better visualization
        conversations.reverse()
        counts.reverse()

        # Create horizontal bar chart
        plt.barh(conversations, counts, color='lightblue')

        # Add labels and title
        plt.xlabel('Number of Messages')
        plt.ylabel('Conversation')
        plt.title(f"Top {len(data)} Conversations by Message Count (Export ID: {export_id})")

        # Add grid
        plt.grid(axis='x', linestyle='--', alpha=0.7)

        # Add count labels to the bars
        for i, count in enumerate(counts):
            plt.text(count + 0.5, i, str(count), va='center')

        # Save figure
        output_path = os.path.join(self.output_dir, filename)
        plt.savefig(output_path, dpi=300, bbox_inches='tight')
        plt.close()

        logger.info(f"Conversation statistics visualization saved to {output_path}")

        return output_path

    def generate_all_visualizations(self, export_id: int) -> Dict[str, Optional[str]]:
        """
        Generate all visualizations for a Skype export.

        Args:
            export_id: The ID of the export to visualize.

        Returns:
            A dictionary mapping visualization names to file paths.
        """
        visualizations = {
            "message_type_distribution": self.visualize_message_type_distribution(export_id),
            "activity_by_hour": self.visualize_activity_by_hour(export_id),
            "activity_by_day_of_week": self.visualize_activity_by_day_of_week(export_id),
            "top_senders": self.visualize_top_senders(export_id),
            "conversation_statistics": self.visualize_conversation_statistics(export_id)
        }

        # Create a JSON file with visualization paths
        metadata = {
            "export_id": export_id,
            "generated_at": datetime.now().isoformat(),
            "visualizations": {k: v for k, v in visualizations.items() if v is not None}
        }

        metadata_path = os.path.join(self.output_dir, f"visualizations_metadata_{export_id}.json")
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)

        logger.info(f"Generated {sum(1 for v in visualizations.values() if v is not None)} visualizations for export ID {export_id}")

        return visualizations