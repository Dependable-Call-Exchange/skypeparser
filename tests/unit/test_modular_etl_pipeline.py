#!/usr/bin/env python3
"""
Unit tests for the modular ETL pipeline manager.

This test suite provides comprehensive testing for the modular ETL pipeline,
focusing on the orchestration of the extraction, transformation, and loading phases.
"""

import os
import sys
import unittest
from unittest.mock import patch, MagicMock, call

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl.pipeline_manager import ETLPipeline
import pytest

@pytest.mark.modular_etl
class TestModularETLPipeline(unittest.TestCase):
    """Test cases for the modular ETL pipeline manager."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a test database configuration
        self.db_config = {
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password',
            'host': 'localhost',
            'port': 5432
        }

        # Sample raw data for testing
        self.raw_data = {
            'conversations': [
                {
                    'id': 'conv1',
                    'displayName': 'Test Conversation 1',
                    'MessageList': [
                        {'id': 'msg1', 'content': 'Hello world'}
                    ]
                }
            ]
        }

        # Sample transformed data for testing
        self.transformed_data = {
            'metadata': {
                'user_display_name': 'Test User',
                'export_time': '2023-01-01T12:00:00Z',
                'total_conversations': 1,
                'total_messages': 1
            },
            'conversations': {
                'conv1': {
                    'display_name': 'Test Conversation 1',
                    'first_message_time': '2023-01-01T12:00:00Z',
                    'last_message_time': '2023-01-01T12:00:00Z',
                    'message_count': 1,
                    'messages': [
                        {
                            'timestamp': '2023-01-01T12:00:00Z',
                            'sender_id': 'user1',
                            'sender_name': 'User 1',
                            'message_type': 'RichText',
                            'content': 'Hello world',
                            'cleaned_content': 'Hello world',
                            'is_edited': False
                        }
                    ]
                }
            }
        }

    @patch('src.db.etl.pipeline_manager.Extractor')
    @patch('src.db.etl.pipeline_manager.Transformer')
    @patch('src.db.etl.pipeline_manager.Loader')
    @patch('src.db.etl.pipeline_manager.ProgressTracker')
    @patch('src.db.etl.pipeline_manager.MemoryMonitor')
    def test_init(self, mock_memory_monitor, mock_progress_tracker,
                mock_loader, mock_transformer, mock_extractor):
        """Test initialization of the ETLPipeline class."""
        # Set up mocks
        mock_extractor.return_value = MagicMock()
        mock_transformer.return_value = MagicMock()
        mock_loader.return_value = MagicMock()
        mock_progress_tracker.return_value = MagicMock()
        mock_memory_monitor.return_value = MagicMock()

        # Create a pipeline instance
        pipeline = ETLPipeline(
            db_config=self.db_config,
            output_dir='test_output',
            memory_limit_mb=2048,
            parallel_processing=True,
            chunk_size=2000
        )

        # Verify the components were created
        mock_extractor.assert_called_once_with(output_dir='test_output')
        mock_transformer.assert_called_once_with(
            parallel_processing=True,
            chunk_size=2000
        )
        mock_loader.assert_called_once_with(db_config=self.db_config)
        mock_progress_tracker.assert_called_once()
        mock_memory_monitor.assert_called_once_with(memory_limit_mb=2048)

        # Verify the components were stored
        self.assertEqual(pipeline.extractor, mock_extractor.return_value)
        self.assertEqual(pipeline.transformer, mock_transformer.return_value)
        self.assertEqual(pipeline.loader, mock_loader.return_value)
        self.assertEqual(pipeline.progress_tracker, mock_progress_tracker.return_value)
        self.assertEqual(pipeline.memory_monitor, mock_memory_monitor.return_value)
        self.assertEqual(pipeline.output_dir, 'test_output')

    @patch('src.db.etl.pipeline_manager.Extractor')
    @patch('src.db.etl.pipeline_manager.Transformer')
    @patch('src.db.etl.pipeline_manager.Loader')
    @patch('src.db.etl.pipeline_manager.ProgressTracker')
    @patch('src.db.etl.pipeline_manager.MemoryMonitor')
    def test_run_pipeline_success(self, mock_memory_monitor, mock_progress_tracker,
                               mock_loader, mock_transformer, mock_extractor):
        """Test running the pipeline successfully."""
        # Set up mocks
        mock_extractor_instance = MagicMock()
        mock_transformer_instance = MagicMock()
        mock_loader_instance = MagicMock()
        mock_progress_tracker_instance = MagicMock()
        mock_memory_monitor_instance = MagicMock()

        mock_extractor.return_value = mock_extractor_instance
        mock_transformer.return_value = mock_transformer_instance
        mock_loader.return_value = mock_loader_instance
        mock_progress_tracker.return_value = mock_progress_tracker_instance
        mock_memory_monitor.return_value = mock_memory_monitor_instance

        # Set up return values
        mock_extractor_instance.extract.return_value = self.raw_data
        mock_transformer_instance.transform.return_value = self.transformed_data
        mock_loader_instance.load.return_value = 123  # Export ID

        mock_progress_tracker_instance.finish_phase.return_value = {
            'phase': 'test',
            'duration_seconds': 1.0,
            'processed_messages': 1
        }

        # Create a pipeline instance
        pipeline = ETLPipeline(db_config=self.db_config)

        # Run the pipeline
        result = pipeline.run_pipeline(
            file_path='test.tar',
            user_display_name='Test User'
        )

        # Verify the components were called
        mock_loader_instance.connect_db.assert_called_once()
        mock_extractor_instance.extract.assert_called_once_with(
            file_path='test.tar',
            file_obj=None
        )
        mock_transformer_instance.transform.assert_called_once_with(
            self.raw_data,
            'Test User'
        )
        mock_loader_instance.load.assert_called_once_with(
            self.raw_data,
            self.transformed_data,
            'test.tar'
        )
        mock_loader_instance.close_db.assert_called_once()

        # Verify the progress tracker was used
        self.assertEqual(mock_progress_tracker_instance.start_phase.call_count, 3)
        self.assertEqual(mock_progress_tracker_instance.finish_phase.call_count, 3)

        # Verify the memory monitor was used
        self.assertEqual(mock_memory_monitor_instance.check_memory.call_count, 6)

        # Verify the result
        self.assertTrue(result['success'])
        self.assertEqual(result['export_id'], 123)
        self.assertEqual(len(result['phases']), 3)

    @patch('src.db.etl.pipeline_manager.Extractor')
    @patch('src.db.etl.pipeline_manager.Transformer')
    @patch('src.db.etl.pipeline_manager.Loader')
    @patch('src.db.etl.pipeline_manager.ProgressTracker')
    @patch('src.db.etl.pipeline_manager.MemoryMonitor')
    def test_run_pipeline_extraction_error(self, mock_memory_monitor, mock_progress_tracker,
                                        mock_loader, mock_transformer, mock_extractor):
        """Test error handling during extraction phase."""
        # Set up mocks
        mock_extractor_instance = MagicMock()
        mock_loader_instance = MagicMock()

        mock_extractor.return_value = mock_extractor_instance
        mock_loader.return_value = mock_loader_instance

        # Set up extraction to raise an exception
        mock_extractor_instance.extract.side_effect = Exception("Extraction error")

        # Create a pipeline instance
        pipeline = ETLPipeline(db_config=self.db_config)

        # Run the pipeline
        result = pipeline.run_pipeline(file_path='test.tar')

        # Verify the result
        self.assertFalse(result['success'])
        self.assertEqual(result['error'], "Extraction error")

        # Verify the database connection was closed
        mock_loader_instance.close_db.assert_called_once()

    @patch('src.db.etl.pipeline_manager.Extractor')
    @patch('src.db.etl.pipeline_manager.Transformer')
    @patch('src.db.etl.pipeline_manager.Loader')
    @patch('src.db.etl.pipeline_manager.ProgressTracker')
    @patch('src.db.etl.pipeline_manager.MemoryMonitor')
    def test_run_pipeline_transformation_error(self, mock_memory_monitor, mock_progress_tracker,
                                           mock_loader, mock_transformer, mock_extractor):
        """Test error handling during transformation phase."""
        # Set up mocks
        mock_extractor_instance = MagicMock()
        mock_transformer_instance = MagicMock()
        mock_loader_instance = MagicMock()

        mock_extractor.return_value = mock_extractor_instance
        mock_transformer.return_value = mock_transformer_instance
        mock_loader.return_value = mock_loader_instance

        # Set up extraction to succeed but transformation to fail
        mock_extractor_instance.extract.return_value = self.raw_data
        mock_transformer_instance.transform.side_effect = Exception("Transformation error")

        # Create a pipeline instance
        pipeline = ETLPipeline(db_config=self.db_config)

        # Run the pipeline
        result = pipeline.run_pipeline(file_path='test.tar')

        # Verify the result
        self.assertFalse(result['success'])
        self.assertEqual(result['error'], "Transformation error")

        # Verify the database connection was closed
        mock_loader_instance.close_db.assert_called_once()

    @patch('src.db.etl.pipeline_manager.Extractor')
    @patch('src.db.etl.pipeline_manager.Transformer')
    @patch('src.db.etl.pipeline_manager.Loader')
    @patch('src.db.etl.pipeline_manager.ProgressTracker')
    @patch('src.db.etl.pipeline_manager.MemoryMonitor')
    def test_run_pipeline_loading_error(self, mock_memory_monitor, mock_progress_tracker,
                                     mock_loader, mock_transformer, mock_extractor):
        """Test error handling during loading phase."""
        # Set up mocks
        mock_extractor_instance = MagicMock()
        mock_transformer_instance = MagicMock()
        mock_loader_instance = MagicMock()

        mock_extractor.return_value = mock_extractor_instance
        mock_transformer.return_value = mock_transformer_instance
        mock_loader.return_value = mock_loader_instance

        # Set up extraction and transformation to succeed but loading to fail
        mock_extractor_instance.extract.return_value = self.raw_data
        mock_transformer_instance.transform.return_value = self.transformed_data
        mock_loader_instance.load.side_effect = Exception("Loading error")

        # Create a pipeline instance
        pipeline = ETLPipeline(db_config=self.db_config)

        # Run the pipeline
        result = pipeline.run_pipeline(file_path='test.tar')

        # Verify the result
        self.assertFalse(result['success'])
        self.assertEqual(result['error'], "Loading error")

        # Verify the database connection was closed
        mock_loader_instance.close_db.assert_called_once()

    @patch('src.db.etl.pipeline_manager.Extractor')
    @patch('src.db.etl.pipeline_manager.Transformer')
    @patch('src.db.etl.pipeline_manager.Loader')
    @patch('src.db.etl.pipeline_manager.ProgressTracker')
    @patch('src.db.etl.pipeline_manager.MemoryMonitor')
    def test_run_extraction_phase(self, mock_memory_monitor, mock_progress_tracker,
                               mock_loader, mock_transformer, mock_extractor):
        """Test the extraction phase."""
        # Set up mocks
        mock_extractor_instance = MagicMock()
        mock_progress_tracker_instance = MagicMock()
        mock_memory_monitor_instance = MagicMock()

        mock_extractor.return_value = mock_extractor_instance
        mock_progress_tracker.return_value = mock_progress_tracker_instance
        mock_memory_monitor.return_value = mock_memory_monitor_instance

        # Set up return values
        mock_extractor_instance.extract.return_value = self.raw_data
        mock_progress_tracker_instance.finish_phase.return_value = {
            'phase': 'extract',
            'duration_seconds': 1.0
        }

        # Create a pipeline instance
        pipeline = ETLPipeline(db_config=self.db_config)

        # Run the extraction phase
        results = {}
        raw_data = pipeline._run_extraction_phase('test.tar', None, results)

        # Verify the extractor was called
        mock_extractor_instance.extract.assert_called_once_with(
            file_path='test.tar',
            file_obj=None
        )

        # Verify the progress tracker was used
        mock_progress_tracker_instance.start_phase.assert_called_once_with('extract')
        mock_progress_tracker_instance.finish_phase.assert_called_once()

        # Verify the memory monitor was used
        self.assertEqual(mock_memory_monitor_instance.check_memory.call_count, 2)

        # Verify the result
        self.assertEqual(raw_data, self.raw_data)
        self.assertEqual(results['phases']['extract'],
                       mock_progress_tracker_instance.finish_phase.return_value)

    @patch('src.db.etl.pipeline_manager.Extractor')
    @patch('src.db.etl.pipeline_manager.Transformer')
    @patch('src.db.etl.pipeline_manager.Loader')
    @patch('src.db.etl.pipeline_manager.ProgressTracker')
    @patch('src.db.etl.pipeline_manager.MemoryMonitor')
    def test_run_transformation_phase(self, mock_memory_monitor, mock_progress_tracker,
                                   mock_loader, mock_transformer, mock_extractor):
        """Test the transformation phase."""
        # Set up mocks
        mock_transformer_instance = MagicMock()
        mock_progress_tracker_instance = MagicMock()
        mock_memory_monitor_instance = MagicMock()

        mock_transformer.return_value = mock_transformer_instance
        mock_progress_tracker.return_value = mock_progress_tracker_instance
        mock_memory_monitor.return_value = mock_memory_monitor_instance

        # Set up return values
        mock_transformer_instance.transform.return_value = self.transformed_data
        mock_progress_tracker_instance.finish_phase.return_value = {
            'phase': 'transform',
            'duration_seconds': 1.0
        }

        # Create a pipeline instance
        pipeline = ETLPipeline(db_config=self.db_config)

        # Run the transformation phase
        results = {}
        transformed_data = pipeline._run_transformation_phase(
            self.raw_data,
            'Test User',
            results
        )

        # Verify the transformer was called
        mock_transformer_instance.transform.assert_called_once_with(
            self.raw_data,
            'Test User'
        )

        # Verify the progress tracker was used
        mock_progress_tracker_instance.start_phase.assert_called_once_with(
            'transform',
            total_conversations=1,
            total_messages=1
        )
        mock_progress_tracker_instance.finish_phase.assert_called_once()

        # Verify the memory monitor was used
        self.assertEqual(mock_memory_monitor_instance.check_memory.call_count, 2)

        # Verify the result
        self.assertEqual(transformed_data, self.transformed_data)
        self.assertEqual(results['phases']['transform'],
                       mock_progress_tracker_instance.finish_phase.return_value)

    @patch('src.db.etl.pipeline_manager.Extractor')
    @patch('src.db.etl.pipeline_manager.Transformer')
    @patch('src.db.etl.pipeline_manager.Loader')
    @patch('src.db.etl.pipeline_manager.ProgressTracker')
    @patch('src.db.etl.pipeline_manager.MemoryMonitor')
    def test_run_loading_phase(self, mock_memory_monitor, mock_progress_tracker,
                            mock_loader, mock_transformer, mock_extractor):
        """Test the loading phase."""
        # Set up mocks
        mock_loader_instance = MagicMock()
        mock_progress_tracker_instance = MagicMock()
        mock_memory_monitor_instance = MagicMock()

        mock_loader.return_value = mock_loader_instance
        mock_progress_tracker.return_value = mock_progress_tracker_instance
        mock_memory_monitor.return_value = mock_memory_monitor_instance

        # Set up return values
        mock_loader_instance.load.return_value = 123  # Export ID
        mock_progress_tracker_instance.finish_phase.return_value = {
            'phase': 'load',
            'duration_seconds': 1.0
        }

        # Create a pipeline instance
        pipeline = ETLPipeline(db_config=self.db_config)

        # Run the loading phase
        results = {}
        export_id = pipeline._run_loading_phase(
            self.raw_data,
            self.transformed_data,
            'test.tar',
            results
        )

        # Verify the loader was called
        mock_loader_instance.load.assert_called_once_with(
            self.raw_data,
            self.transformed_data,
            'test.tar'
        )

        # Verify the progress tracker was used
        mock_progress_tracker_instance.start_phase.assert_called_once_with(
            'load',
            total_conversations=1,
            total_messages=1
        )
        mock_progress_tracker_instance.finish_phase.assert_called_once()

        # Verify the memory monitor was used
        self.assertEqual(mock_memory_monitor_instance.check_memory.call_count, 2)

        # Verify the result
        self.assertEqual(export_id, 123)
        self.assertEqual(results['phases']['load'],
                       mock_progress_tracker_instance.finish_phase.return_value)

if __name__ == '__main__':
    unittest.main()