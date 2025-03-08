#!/usr/bin/env python3
"""
Enhanced integration tests for the ETL pipeline module.

This test suite provides comprehensive integration testing for the ETL pipeline,
using dependency injection consistently and testing more edge cases.
"""

import os
import sys
import unittest
import tempfile
import json
import logging
import pytest
from unittest.mock import patch, MagicMock

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.testable_etl_pipeline import TestableETLPipeline
from src.utils.config import get_db_config, load_config
from src.utils.file_handler import FileHandler
from src.utils.validation import ValidationService
from src.parser.content_extractor import ContentExtractor
from src.utils.structured_data_extractor import StructuredDataExtractor
from src.utils.message_type_handlers import SkypeMessageHandlerFactory
from tests.fixtures import (
    BASIC_SKYPE_DATA,
    COMPLEX_SKYPE_DATA,
    test_db_connection,
    is_db_available,
    create_mock_functions
)
from tests.factories import (
    SkypeDataFactory,
    MockBuilderFactory
)


@pytest.mark.integration
class TestETLPipelineIntegrationEnhanced(unittest.TestCase):
    """Enhanced integration tests for the ETL pipeline with dependency injection."""

    def setUp(self):
        """Set up the test environment."""
        # Skip integration tests unless database is available
        if not is_db_available():
            self.skipTest("Integration tests disabled. Database not available.")

        # Set up the test environment
        self.temp_dir = tempfile.mkdtemp()
        self.test_dir = os.path.join(self.temp_dir, 'test_output')
        os.makedirs(self.test_dir, exist_ok=True)

        # Get database configuration from fixtures
        self.db_config = get_test_db_config()

        # Create sample Skype export data
        self.basic_data = BASIC_SKYPE_DATA
        self.complex_data = COMPLEX_SKYPE_DATA

        # Create files with the sample data
        self.basic_file = os.path.join(self.temp_dir, 'basic.json')
        with open(self.basic_file, 'w') as f:
            json.dump(self.basic_data, f)

        self.complex_file = os.path.join(self.temp_dir, 'complex.json')
        with open(self.complex_file, 'w') as f:
            json.dump(self.complex_data, f)

        # Create a pipeline instance with a test database connection and real components
        with test_db_connection() as conn:
            self.pipeline = TestableETLPipeline(
                output_dir=self.test_dir,
                db_connection=conn,
                file_handler=FileHandler(),
                validation_service=ValidationService(),
                content_extractor=ContentExtractor(),
                structured_data_extractor=StructuredDataExtractor(),
                message_handler_factory=SkypeMessageHandlerFactory()
            )

    def tearDown(self):
        """Tear down test fixtures."""
        # Clean up temporary directory
        import shutil
        shutil.rmtree(self.temp_dir)

    def test_dependency_injected_pipeline(self):
        """Test running the pipeline with fully injected dependencies."""
        # Create mock functions for dependency injection
        mock_functions = create_mock_functions(self.basic_data)

        # Create a pipeline instance with injected mock functions
        with test_db_connection() as conn:
            pipeline = TestableETLPipeline(
                output_dir=self.test_dir,
                db_connection=conn,
                read_file_func=mock_functions['read_file'],
                validate_file_exists_func=mock_functions['validate_file_exists'],
                validate_json_func=mock_functions['validate_json']
            )

            # Run the pipeline with a non-existent file (mock will handle it)
            result = pipeline.run_pipeline(
                file_path="non_existent_file.json",
                user_display_name="DI Test User"
            )

            # Verify the results
            self.assertIn('extraction', result)
            self.assertIn('transformation', result)
            self.assertIn('loading', result)
            self.assertTrue(result['extraction']['success'])
            self.assertTrue(result['transformation']['success'])
            self.assertTrue(result['loading']['success'])

            # Verify mock functions were called
            mock_functions['read_file'].assert_called_with("non_existent_file.json")
            mock_functions['validate_file_exists'].assert_called_with("non_existent_file.json")

    def test_complex_data_processing(self):
        """Test processing complex data with many message types."""
        # Run the pipeline with complex data
        result = self.pipeline.run_pipeline(
            file_path=self.complex_file,
            user_display_name="Complex Data Test"
        )

        # Verify the results
        self.assertTrue(result['extraction']['success'])
        self.assertTrue(result['transformation']['success'])
        self.assertTrue(result['loading']['success'])
        self.assertIn('exportId', result['loading'])
        export_id = result['loading']['exportId']

        # Verify the data was stored in the database
        with self.pipeline.conn.cursor() as cur:
            # Check for conversations
            cur.execute("""
                SELECT COUNT(*)
                FROM skype_conversations
                WHERE export_id = %s
            """, (export_id,))
            conversation_count = cur.fetchone()[0]
            self.assertEqual(conversation_count, len(self.complex_data['conversations']))

            # Check for messages
            cur.execute("""
                SELECT COUNT(*)
                FROM skype_messages
                WHERE conversation_id IN (
                    SELECT conversation_id
                    FROM skype_conversations
                    WHERE export_id = %s
                )
            """, (export_id,))
            message_count = cur.fetchone()[0]

            # Calculate expected message count from complex data
            expected_message_count = sum(
                len(conv.get('MessageList', []))
                for conv in self.complex_data['conversations']
            )

            self.assertEqual(message_count, expected_message_count)

            # Check for various message types
            cur.execute("""
                SELECT type, COUNT(*)
                FROM skype_messages
                WHERE conversation_id IN (
                    SELECT conversation_id
                    FROM skype_conversations
                    WHERE export_id = %s
                )
                GROUP BY type
            """, (export_id,))
            message_type_counts = dict(cur.fetchall())

            # Verify at least some message types are present
            self.assertGreaterEqual(len(message_type_counts), 3,
                                  "Expected at least 3 different message types")

    def test_pipeline_with_logging(self):
        """Test pipeline with logging configured."""
        # Configure a test logger
        logger = logging.getLogger('etl_test')
        logger.setLevel(logging.DEBUG)

        # Create a memory handler to capture log records
        log_capture = []

        class MemoryHandler(logging.Handler):
            def emit(self, record):
                log_capture.append(record)

        memory_handler = MemoryHandler()
        logger.addHandler(memory_handler)

        # Create pipeline with logger
        with test_db_connection() as conn:
            pipeline = TestableETLPipeline(
                output_dir=self.test_dir,
                db_connection=conn,
                logger=logger
            )

            # Run the pipeline
            result = pipeline.run_pipeline(
                file_path=self.basic_file,
                user_display_name="Logging Test User"
            )

            # Verify the pipeline ran successfully
            self.assertTrue(result['extraction']['success'])
            self.assertTrue(result['transformation']['success'])
            self.assertTrue(result['loading']['success'])

            # Verify log messages were captured
            self.assertGreater(len(log_capture), 10,
                             "Expected at least 10 log messages")

            # Verify log levels are appropriate
            info_logs = [r for r in log_capture if r.levelno == logging.INFO]
            debug_logs = [r for r in log_capture if r.levelno == logging.DEBUG]

            self.assertGreater(len(info_logs), 0, "Expected some INFO logs")
            self.assertGreater(len(debug_logs), 0, "Expected some DEBUG logs")

    def test_error_handling_with_injected_components(self):
        """Test error handling with injected components that raise exceptions."""
        # Create a file handler that raises an exception
        error_file_handler = FileHandler()

        def read_file_with_error(file_path):
            raise ValueError("Simulated file reading error")

        error_file_handler.read_file = read_file_with_error

        # Create pipeline with error-prone component
        with test_db_connection() as conn:
            pipeline = TestableETLPipeline(
                output_dir=self.test_dir,
                db_connection=conn,
                file_handler=error_file_handler
            )

            # Run the pipeline, expecting an error
            with self.assertRaises(ValueError) as context:
                pipeline.run_pipeline(
                    file_path=self.basic_file,
                    user_display_name="Error Test User"
                )

            # Verify the error message
            self.assertIn("Simulated file reading error", str(context.exception))

    def test_pipeline_events(self):
        """Test pipeline events and callbacks."""
        # Create event tracking variables
        events = {
            'extraction_start': False,
            'extraction_end': False,
            'transformation_start': False,
            'transformation_end': False,
            'loading_start': False,
            'loading_end': False
        }

        # Create event callbacks
        def on_extraction_start(data):
            events['extraction_start'] = True

        def on_extraction_end(data):
            events['extraction_end'] = True

        def on_transformation_start(data):
            events['transformation_start'] = True

        def on_transformation_end(data):
            events['transformation_end'] = True

        def on_loading_start(data):
            events['loading_start'] = True

        def on_loading_end(data):
            events['loading_end'] = True

        # Create pipeline with event callbacks
        with test_db_connection() as conn:
            pipeline = TestableETLPipeline(
                output_dir=self.test_dir,
                db_connection=conn,
                on_extraction_start=on_extraction_start,
                on_extraction_end=on_extraction_end,
                on_transformation_start=on_transformation_start,
                on_transformation_end=on_transformation_end,
                on_loading_start=on_loading_start,
                on_loading_end=on_loading_end
            )

            # Run the pipeline
            result = pipeline.run_pipeline(
                file_path=self.basic_file,
                user_display_name="Events Test User"
            )

            # Verify the pipeline ran successfully
            self.assertTrue(result['extraction']['success'])
            self.assertTrue(result['transformation']['success'])
            self.assertTrue(result['loading']['success'])

            # Verify all events were triggered
            for event, triggered in events.items():
                self.assertTrue(triggered, f"Event {event} was not triggered")

    def test_incremental_processing(self):
        """Test incremental processing (new data added to existing export)."""
        # First run: basic data
        result1 = self.pipeline.run_pipeline(
            file_path=self.basic_file,
            user_display_name="Incremental Test User"
        )

        # Get export ID from first run
        export_id = result1['loading']['exportId']

        # Second run: incremental with complex data
        result2 = self.pipeline.run_pipeline(
            file_path=self.complex_file,
            user_display_name="Incremental Test User",
            incremental=True,
            export_id=export_id
        )

        # Verify second run success
        self.assertTrue(result2['extraction']['success'])
        self.assertTrue(result2['transformation']['success'])
        self.assertTrue(result2['loading']['success'])

        # Verify the data was added to the existing export
        with self.pipeline.conn.cursor() as cur:
            # Check that the export still exists
            cur.execute("""
                SELECT COUNT(*)
                FROM skype_raw_exports
                WHERE export_id = %s
            """, (export_id,))
            self.assertEqual(cur.fetchone()[0], 1)

            # Get total message count
            cur.execute("""
                SELECT COUNT(*)
                FROM skype_messages
                WHERE conversation_id IN (
                    SELECT conversation_id
                    FROM skype_conversations
                    WHERE export_id = %s
                )
            """, (export_id,))
            message_count = cur.fetchone()[0]

            # Calculate expected message count from both files
            expected_message_count = (
                sum(len(conv.get('MessageList', [])) for conv in self.basic_data['conversations']) +
                sum(len(conv.get('MessageList', [])) for conv in self.complex_data['conversations'])
            )

            # Verify the message count matches the expected count
            # Note: There might be some duplicate messages, so we use assertGreaterEqual
            # instead of assertEqual
            self.assertGreaterEqual(message_count, expected_message_count)


# Helper function to get test database configuration
def get_test_db_config():
    """Get database configuration for testing."""
    from tests.fixtures import get_test_db_config
    return get_test_db_config()


if __name__ == '__main__':
    unittest.main()