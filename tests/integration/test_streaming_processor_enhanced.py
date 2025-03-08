#!/usr/bin/env python3
"""
Enhanced tests for the streaming processor.

This test suite focuses on testing the streaming processor's checkpoint creation,
resume functionality, and handling of large files and interruptions.
"""

import json
import os
import sys
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch

import pytest

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.db.etl.streaming_processor import StreamingProcessor
from src.utils.config import get_db_config
from tests.fixtures import (
    SkypeConversationFactory,
    SkypeDataFactory,
    SkypeMessageFactory,
    is_db_available,
    test_db_connection,
)


def create_large_streaming_file(
    file_path, conversation_count=5, messages_per_conversation=200
):
    """Create a large file for streaming tests."""
    large_data = SkypeDataFactory.build(
        userId="streaming_test_user",
        exportDate="2023-01-01T12:00:00Z",
        conversations=[
            SkypeConversationFactory.build(
                id=f"conversation{i}",
                displayName=f"Streaming Test Conversation {i}",
                MessageList=[
                    SkypeMessageFactory.build(
                        id=f"msg_{i}_{j}",
                        from_id=f"user{j % 5}",
                        from_name=f"User {j % 5}",
                        content=f"Streaming test message {j} in conversation {i}",
                    )
                    for j in range(messages_per_conversation)
                ],
            )
            for i in range(conversation_count)
        ],
    )

    with open(file_path, "w") as f:
        json.dump(large_data, f)

    return file_path


class SimulatedInterruption(Exception):
    """Exception to simulate an interruption during processing."""

    pass


@pytest.mark.integration
@pytest.mark.streaming
class TestStreamingProcessorEnhanced(unittest.TestCase):
    """Enhanced tests for the streaming processor."""

    def setUp(self):
        """Set up the test environment."""
        # Skip integration tests unless database is available
        if not is_db_available():
            self.skipTest("Integration tests disabled. Database not available.")

        # Set up temporary directories
        self.temp_dir = tempfile.mkdtemp()
        self.test_dir = os.path.join(self.temp_dir, "test_output")
        self.checkpoint_dir = os.path.join(self.temp_dir, "checkpoints")
        os.makedirs(self.test_dir, exist_ok=True)
        os.makedirs(self.checkpoint_dir, exist_ok=True)

        # Get database configuration
        self.db_config = get_test_db_config()

        # Create test file
        self.test_file = os.path.join(self.temp_dir, "streaming_data.json")
        create_large_streaming_file(
            self.test_file, conversation_count=3, messages_per_conversation=100
        )

    def tearDown(self):
        """Clean up test fixtures."""
        # Remove temporary directory
        import shutil

        shutil.rmtree(self.temp_dir)

    def test_basic_streaming_processing(self):
        """Test basic streaming processing functionality."""
        # Create streaming processor
        processor = StreamingProcessor(
            db_config=self.db_config,
            output_dir=self.test_dir,
            checkpoint_dir=self.checkpoint_dir,
            batch_size=10,  # Small batch size for testing
        )

        # Process file
        result = processor.process_file(
            file_path=self.test_file, user_display_name="Streaming Test User"
        )

        # Verify processing success
        self.assertTrue(
            result["success"],
            f"Streaming processing failed: {result.get('error', 'Unknown error')}",
        )

        # Verify processed count
        self.assertEqual(
            result["processed_count"], 300, "Expected 300 messages to be processed"
        )

        # Verify data in database
        with test_db_connection(self.db_config) as conn:
            with conn.cursor() as cursor:
                # Check conversations
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM skype_conversations
                    WHERE export_id = %s
                """,
                    (result["export_id"],),
                )
                conversation_count = cursor.fetchone()[0]
                self.assertEqual(
                    conversation_count, 3, "Expected 3 conversations in database"
                )

                # Check messages
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM skype_messages
                    WHERE conversation_id IN (
                        SELECT conversation_id FROM skype_conversations
                        WHERE export_id = %s
                    )
                """,
                    (result["export_id"],),
                )
                message_count = cursor.fetchone()[0]
                self.assertEqual(
                    message_count, 300, "Expected 300 messages in database"
                )

    def test_checkpoint_creation(self):
        """Test checkpoint creation during streaming processing."""
        # Create streaming processor with small checkpoint interval
        processor = StreamingProcessor(
            db_config=self.db_config,
            output_dir=self.test_dir,
            checkpoint_dir=self.checkpoint_dir,
            batch_size=10,
            checkpoint_interval=50,  # Create checkpoint every 50 messages
        )

        # Process file
        result = processor.process_file(
            file_path=self.test_file, user_display_name="Checkpoint Test User"
        )

        # Verify processing success
        self.assertTrue(
            result["success"],
            f"Streaming processing failed: {result.get('error', 'Unknown error')}",
        )

        # Get checkpoint files
        checkpoint_files = [
            f for f in os.listdir(self.checkpoint_dir) if f.startswith("checkpoint_")
        ]

        # Verify at least one checkpoint was created
        self.assertGreater(len(checkpoint_files), 0, "No checkpoint files were created")

        # Load the latest checkpoint
        latest_checkpoint = os.path.join(
            self.checkpoint_dir, sorted(checkpoint_files)[-1]
        )
        with open(latest_checkpoint, "r") as f:
            checkpoint_data = json.load(f)

        # Verify checkpoint data
        self.assertIn("export_id", checkpoint_data, "Checkpoint missing export_id")
        self.assertIn(
            "processed_count", checkpoint_data, "Checkpoint missing processed_count"
        )
        self.assertEqual(
            checkpoint_data["processed_count"],
            300,
            "Checkpoint shows incorrect processed count",
        )

    def test_interruption_and_resume(self):
        """Test interruption during streaming and resumption from checkpoint."""
        # Create a processor that will be interrupted
        processor = StreamingProcessor(
            db_config=self.db_config,
            output_dir=self.test_dir,
            checkpoint_dir=self.checkpoint_dir,
            batch_size=10,
            checkpoint_interval=25,  # Create frequent checkpoints
        )

        # Override the process_message method to interrupt after processing 100 messages
        original_process = processor._process_message
        processed_count = [0]  # Use list for mutable closure

        def interrupted_process(message, conversation_id, **kwargs):
            result = original_process(message, conversation_id, **kwargs)
            processed_count[0] += 1
            if processed_count[0] >= 100:
                raise SimulatedInterruption("Simulated interruption after 100 messages")
            return result

        # Patch the process_message method
        with patch.object(
            processor, "_process_message", side_effect=interrupted_process
        ):
            try:
                # This should be interrupted
                processor.process_file(
                    file_path=self.test_file, user_display_name="Interrupted Test User"
                )
                self.fail("Expected SimulatedInterruption was not raised")
            except SimulatedInterruption:
                # This is expected
                pass

        # Get the checkpoint files
        checkpoint_files = [
            f for f in os.listdir(self.checkpoint_dir) if f.startswith("checkpoint_")
        ]
        self.assertGreater(
            len(checkpoint_files),
            0,
            "No checkpoint files were created during interruption",
        )

        # Load the latest checkpoint
        latest_checkpoint = os.path.join(
            self.checkpoint_dir, sorted(checkpoint_files)[-1]
        )
        with open(latest_checkpoint, "r") as f:
            checkpoint_data = json.load(f)

        # Verify checkpoint
        self.assertIn("export_id", checkpoint_data, "Checkpoint missing export_id")
        self.assertIn(
            "processed_count", checkpoint_data, "Checkpoint missing processed_count"
        )
        self.assertGreaterEqual(
            checkpoint_data["processed_count"], 75, "Checkpoint processed count too low"
        )
        self.assertLessEqual(
            checkpoint_data["processed_count"],
            100,
            "Checkpoint processed count too high",
        )

        # Create a new processor to resume processing
        resume_processor = StreamingProcessor.load_from_checkpoint(
            checkpoint_path=latest_checkpoint,
            db_config=self.db_config,
            output_dir=self.test_dir,
            checkpoint_dir=self.checkpoint_dir,
        )

        # Resume processing
        resume_result = resume_processor.process_file(
            file_path=self.test_file, resume=True
        )

        # Verify resume success
        self.assertTrue(
            resume_result["success"],
            f"Resume processing failed: {resume_result.get('error', 'Unknown error')}",
        )

        # Verify total processed messages
        self.assertEqual(
            resume_result["processed_count"],
            300,
            "Total processed count should be 300 after resume",
        )

        # Verify data in database
        with test_db_connection(self.db_config) as conn:
            with conn.cursor() as cursor:
                # Check export
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM skype_raw_exports
                    WHERE export_id = %s
                """,
                    (checkpoint_data["export_id"],),
                )
                export_count = cursor.fetchone()[0]
                self.assertEqual(export_count, 1, "Export record not found in database")

                # Check messages
                cursor.execute(
                    """
                    SELECT COUNT(*) FROM skype_messages
                    WHERE conversation_id IN (
                        SELECT conversation_id FROM skype_conversations
                        WHERE export_id = %s
                    )
                """,
                    (checkpoint_data["export_id"],),
                )
                message_count = cursor.fetchone()[0]
                self.assertEqual(
                    message_count, 300, "Expected 300 messages in database after resume"
                )

    def test_multiple_interruptions_and_resumptions(self):
        """Test multiple interruptions and resumptions during streaming processing."""
        # Create initial processor
        processor = StreamingProcessor(
            db_config=self.db_config,
            output_dir=self.test_dir,
            checkpoint_dir=self.checkpoint_dir,
            batch_size=10,
            checkpoint_interval=20,  # Frequent checkpoints
        )

        # First interruption at 100 messages
        with patch.object(
            processor,
            "_process_message",
            side_effect=lambda m, c, **kw: self._interrupt_after(
                processor._process_message, m, c, 100, **kw
            ),
        ):
            try:
                processor.process_file(
                    file_path=self.test_file, user_display_name="Multi-Interrupted Test"
                )
                self.fail("Expected first interruption was not raised")
            except SimulatedInterruption:
                pass

        # Get the latest checkpoint
        checkpoint_files = sorted(
            [f for f in os.listdir(self.checkpoint_dir) if f.startswith("checkpoint_")]
        )
        latest_checkpoint = os.path.join(self.checkpoint_dir, checkpoint_files[-1])

        # Create second processor to resume
        processor2 = StreamingProcessor.load_from_checkpoint(
            checkpoint_path=latest_checkpoint,
            db_config=self.db_config,
            output_dir=self.test_dir,
            checkpoint_dir=self.checkpoint_dir,
            batch_size=10,
            checkpoint_interval=20,
        )

        # Second interruption at 200 messages
        with patch.object(
            processor2,
            "_process_message",
            side_effect=lambda m, c, **kw: self._interrupt_after(
                processor2._process_message, m, c, 200, **kw
            ),
        ):
            try:
                processor2.process_file(file_path=self.test_file, resume=True)
                self.fail("Expected second interruption was not raised")
            except SimulatedInterruption:
                pass

        # Get the latest checkpoint after second interruption
        checkpoint_files = sorted(
            [f for f in os.listdir(self.checkpoint_dir) if f.startswith("checkpoint_")]
        )
        latest_checkpoint = os.path.join(self.checkpoint_dir, checkpoint_files[-1])

        # Create final processor to complete the processing
        processor3 = StreamingProcessor.load_from_checkpoint(
            checkpoint_path=latest_checkpoint,
            db_config=self.db_config,
            output_dir=self.test_dir,
            checkpoint_dir=self.checkpoint_dir,
        )

        # Complete processing
        final_result = processor3.process_file(file_path=self.test_file, resume=True)

        # Verify final success
        self.assertTrue(
            final_result["success"],
            f"Final processing failed: {final_result.get('error', 'Unknown error')}",
        )

        # Verify total processed messages
        self.assertEqual(
            final_result["processed_count"],
            300,
            "Total processed count should be 300 after multiple resumes",
        )

    def test_checkpoint_file_location(self):
        """Test customizing checkpoint file location."""
        # Create a custom checkpoint directory
        custom_checkpoint_dir = os.path.join(self.temp_dir, "custom_checkpoints")
        os.makedirs(custom_checkpoint_dir, exist_ok=True)

        # Create processor with custom checkpoint directory
        processor = StreamingProcessor(
            db_config=self.db_config,
            output_dir=self.test_dir,
            checkpoint_dir=custom_checkpoint_dir,
            checkpoint_interval=50,
        )

        # Process file
        result = processor.process_file(
            file_path=self.test_file, user_display_name="Custom Checkpoint Test"
        )

        # Verify processing success
        self.assertTrue(
            result["success"],
            f"Processing failed: {result.get('error', 'Unknown error')}",
        )

        # Check for checkpoint files in custom directory
        checkpoint_files = [
            f for f in os.listdir(custom_checkpoint_dir) if f.startswith("checkpoint_")
        ]
        self.assertGreater(
            len(checkpoint_files),
            0,
            "No checkpoint files were created in custom directory",
        )

    def test_automatic_checkpoint_cleanup(self):
        """Test automatic cleanup of old checkpoints."""
        # Create a processor with checkpoint cleanup enabled
        processor = StreamingProcessor(
            db_config=self.db_config,
            output_dir=self.test_dir,
            checkpoint_dir=self.checkpoint_dir,
            checkpoint_interval=20,
            max_checkpoint_files=2,  # Keep only 2 most recent checkpoints
        )

        # Process file (this should create multiple checkpoints)
        result = processor.process_file(
            file_path=self.test_file, user_display_name="Checkpoint Cleanup Test"
        )

        # Verify processing success
        self.assertTrue(
            result["success"],
            f"Processing failed: {result.get('error', 'Unknown error')}",
        )

        # Check number of checkpoint files
        checkpoint_files = [
            f for f in os.listdir(self.checkpoint_dir) if f.startswith("checkpoint_")
        ]
        self.assertLessEqual(
            len(checkpoint_files),
            2,
            f"Too many checkpoint files: {len(checkpoint_files)}, expected at most 2",
        )

    def _interrupt_after(
        self, original_method, message, conversation_id, threshold, **kwargs
    ):
        """Helper method to interrupt processing after a certain number of messages."""
        # Get current processed count from processor being patched
        processed_count = kwargs.get("_current_count", 0)

        # Call original method
        result = original_method(message, conversation_id, **kwargs)

        # Increment processed count
        processed_count += 1

        # Interrupt if threshold reached
        if processed_count >= threshold:
            raise SimulatedInterruption(
                f"Simulated interruption after {threshold} messages"
            )

        return result


# Helper function to get test database configuration
def get_test_db_config():
    """Get database configuration for testing."""
    from tests.fixtures import get_test_db_config

    return get_test_db_config()


if __name__ == "__main__":
    unittest.main()
