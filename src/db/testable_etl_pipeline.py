#!/usr/bin/env python3
"""
Testable ETL Pipeline for Skype Export Data

This module provides a testable version of the ETL pipeline with dependency injection
support for easier testing. It allows injecting mock objects for file reading,
database connections, and other dependencies.
"""

import logging
import json
import os
from typing import Dict, Any, Optional, Callable, BinaryIO, List, Union
from unittest.mock import patch, MagicMock

# Import from the modular ETL pipeline instead of the deprecated one
from src.db.etl import ETLPipeline
from src.db.etl.context import ETLContext
from src.db.etl.extractor import Extractor
from src.db.etl.transformer import Transformer
from src.db.etl.loader import Loader

# Import necessary protocols and implementation
from src.utils.interfaces import (
    FileHandlerProtocol,
    ContentExtractorProtocol,
    StructuredDataExtractorProtocol,
    MessageHandlerFactoryProtocol,
    DatabaseConnectionProtocol
)
from src.utils.di import get_service_provider, get_service

# Import from service registry
from src.utils.service_registry import register_core_services, register_database_connection, register_etl_services

# Import additional modules
from src.utils.db_connection import DatabaseConnection
from src.parser.content_extractor import ContentExtractor
from src.utils.message_type_handlers import SkypeMessageHandlerFactory
from src.utils.structured_data_extractor import StructuredDataExtractor

# Configure logging
logger = logging.getLogger(__name__)

# Create a mock file handler class that wraps the read_file_func
class MockFileHandler(FileHandlerProtocol):
    """Mock file handler for testing that wraps the provided read_file_func."""

    def __init__(self,
                 read_file_func: Optional[Callable[[str, Optional[str]], str]] = None,
                 tar_extract_func: Optional[Callable[[str, Optional[str]], Dict[str, Any]]] = None):
        self.read_file_func = read_file_func
        self.tar_extract_func = tar_extract_func
        logger.debug("MockFileHandler initialized")

    def read_file(self, file_path: str) -> Dict[str, Any]:
        """Read file using the provided read_file_func or return empty dict."""
        logger.debug(f"MockFileHandler.read_file called with {file_path}")
        if self.read_file_func:
            try:
                # Handle both patch objects and regular functions
                if hasattr(self.read_file_func, 'side_effect') and self.read_file_func.side_effect:
                    # If it's a patch with side_effect, call the side_effect directly
                    logger.debug("Calling patch side_effect function")
                    return self.read_file_func.side_effect(file_path)
                else:
                    # Otherwise call the function directly
                    logger.debug("Calling read_file_func directly")
                    return self.read_file_func(file_path)
            except Exception as e:
                logger.error(f"Error in read_file: {str(e)}", exc_info=True)
                return {}
        logger.warning("No read_file_func provided, returning empty dict")
        return {}

    def read_file_object(self, file_obj: BinaryIO) -> Dict[str, Any]:
        """Read file object."""
        logger.debug("MockFileHandler.read_file_object called")
        try:
            # Simple implementation that reads the file object and returns parsed JSON
            content = file_obj.read()
            if isinstance(content, bytes):
                content = content.decode('utf-8')
            return json.loads(content)
        except Exception as e:
            logger.error(f"Error in read_file_object: {str(e)}", exc_info=True)
            return {}

    def read_tarfile(self, file_path: str, auto_select: bool = False) -> Dict[str, Any]:
        """Read tarfile using the provided tar_extract_func or return empty dict."""
        logger.debug(f"MockFileHandler.read_tarfile called with {file_path}")
        if self.tar_extract_func:
            try:
                # Handle both patch objects and regular functions
                if hasattr(self.tar_extract_func, 'side_effect') and self.tar_extract_func.side_effect:
                    # If it's a patch with side_effect, call the side_effect directly
                    logger.debug("Calling tar_extract_func side_effect")
                    return self.tar_extract_func.side_effect(file_path, None if not auto_select else "auto")
                else:
                    # Otherwise call the function directly
                    logger.debug("Calling tar_extract_func directly")
                    return self.tar_extract_func(file_path, None if not auto_select else "auto")
            except Exception as e:
                logger.error(f"Error in read_tarfile: {str(e)}", exc_info=True)
                return {}
        logger.warning("No tar_extract_func provided, returning empty dict")
        return {}

class TestableETLPipeline:
    """
    A testable version of the ETL pipeline with dependency injection support.

    This class allows injecting mock objects for file reading, database connections,
    and other dependencies, making it easier to test the ETL pipeline in isolation.
    """

    def __init__(
        self,
        db_config: Dict[str, Any],
        use_di: bool = False,
        read_file_func: Optional[Callable[[str, Optional[str]], str]] = None,
        tar_extract_func: Optional[Callable[[str, Optional[str]], Dict[str, Any]]] = None,
        validate_file_exists_func: Optional[Callable[[str], bool]] = None,
        validate_json_file_func: Optional[Callable[[str], bool]] = None,
        validate_user_display_name_func: Optional[Callable[[str], str]] = None,
        db_connection: Optional[Any] = None,
        content_extractor: Optional[ContentExtractorProtocol] = None,
        structured_data_extractor: Optional[StructuredDataExtractorProtocol] = None,
        message_handler_factory: Optional[MessageHandlerFactoryProtocol] = None
    ) -> None:
        """Initialize the TestableETLPipeline.

        Args:
            db_config: Database configuration
            use_di: Whether to use dependency injection
            read_file_func: Function to read files
            tar_extract_func: Function to extract tar files
            validate_file_exists_func: Function to validate file existence
            validate_json_file_func: Function to validate JSON files
            validate_user_display_name_func: Function to validate user display name
            db_connection: Database connection
            content_extractor: Content extractor
            structured_data_extractor: Structured data extractor
            message_handler_factory: Message handler factory
        """
        self.db_config = db_config
        self.use_di = use_di

        # Create the ETL context first
        self.context = ETLContext(db_config=self.db_config)

        # Extract side_effects from any patch objects that were passed
        self.read_file_func = self._extract_mock_function(read_file_func)
        self.tar_extract_func = self._extract_mock_function(tar_extract_func)
        self.validate_file_exists_func = self._extract_mock_function(validate_file_exists_func)
        self.validate_json_file_func = self._extract_mock_function(validate_json_file_func)
        self.validate_user_display_name_func = self._extract_mock_function(validate_user_display_name_func)

        logger.debug(f"Initializing TestableETLPipeline with use_di={use_di}")

        # Initialize the file handler
        self.file_handler = MockFileHandler(
            read_file_func=self.read_file_func,
            tar_extract_func=self.tar_extract_func
        )
        logger.debug("MockFileHandler initialized")

        # Initialize pipeline
        if use_di:
            self.pipeline = self._create_pipeline_with_di(
                db_connection=db_connection,
                content_extractor=content_extractor,
                structured_data_extractor=structured_data_extractor,
                message_handler_factory=message_handler_factory
            )
        else:
            self.pipeline = self._create_pipeline_without_di(
                db_connection=db_connection,
                content_extractor=content_extractor,
                structured_data_extractor=structured_data_extractor,
                message_handler_factory=message_handler_factory
            )

    def _extract_mock_function(self, mock_obj):
        """Extract the actual function from a mock object if necessary.

        Args:
            mock_obj: Either a function, a mock, or a patch object

        Returns:
            The actual function to use
        """
        if mock_obj is None:
            return None

        # If it's a patch object that hasn't been started
        if hasattr(mock_obj, 'start') and callable(mock_obj.start):
            try:
                # Try to get the side_effect
                if hasattr(mock_obj, 'side_effect') and mock_obj.side_effect is not None:
                    return mock_obj.side_effect
                # If no side_effect, start the mock and return the mock itself
                started_mock = mock_obj.start()
                return started_mock
            except Exception as e:
                logger.error(f"Error extracting function from mock: {str(e)}")
                return mock_obj

        # If it's a MagicMock with a side_effect
        if hasattr(mock_obj, 'side_effect') and mock_obj.side_effect is not None:
            return mock_obj.side_effect

        # Otherwise, assume it's a callable and return it directly
        return mock_obj

    def _create_pipeline_with_di(self, db_connection, content_extractor, structured_data_extractor, message_handler_factory):
        # Use the dependency injection container
        from src.utils.di import ServiceProvider
        provider = ServiceProvider()

        # Register database connection
        register_database_connection(db_config=self.db_config, provider=provider)

        # Register the context we created
        provider.register_singleton(ETLContext, self.context)

        # Register the file handler with our mock functions
        if self.file_handler:
            provider.register_singleton(FileHandlerProtocol, self.file_handler)

        # Register dependencies if provided
        if content_extractor:
            provider.register_singleton(ContentExtractorProtocol, content_extractor)
        if structured_data_extractor:
            provider.register_singleton(StructuredDataExtractorProtocol, structured_data_extractor)
        if message_handler_factory:
            provider.register_singleton(MessageHandlerFactoryProtocol, message_handler_factory)
        if db_connection:
            provider.register_singleton("db_connection", db_connection)

        # Register ETL services with our context
        register_etl_services(db_config=self.db_config, provider=provider)

        logger.debug("Creating ETLPipeline with dependency injection")
        self.pipeline = ETLPipeline(db_config=self.db_config, context=self.context, use_di=True)
        return self.pipeline

    def _create_pipeline_without_di(self, db_connection, content_extractor, structured_data_extractor, message_handler_factory):
        # Create manually without DI
        logger.debug("Creating ETLPipeline without dependency injection")
        self.pipeline = ETLPipeline(db_config=self.db_config, context=self.context, use_di=False)

        # Create the extractor with our file handler explicitly
        logger.debug("Creating Extractor with explicit file handler")
        self.pipeline.extractor = Extractor(context=self.context, file_handler=self.file_handler)

        # Create a transformer with our mock dependencies
        logger.debug("Creating custom Transformer for testing")
        self.pipeline.transformer = MockTransformer(
            context=self.context,
            content_extractor=content_extractor,
            message_handler_factory=message_handler_factory,
            structured_data_extractor=structured_data_extractor
        )

        self.pipeline.loader = Loader(context=self.context, db_connection=db_connection)

        # Inject dependencies if provided
        if self.read_file_func:
            logger.debug("Injecting read_file_func")
            self.pipeline.extractor.read_file = self.read_file_func

        if self.tar_extract_func:
            logger.debug("Injecting tar_extract_func")
            self.pipeline.extractor.extract_tar = self.tar_extract_func

        # Directly patch the extract method to use validate_json_file
        if self.validate_json_file_func:
            logger.debug("Patching extract method to use validate_json_file")
            self._patch_extract_method(self.pipeline.extractor)

        # Directly assign the validation function to the extractor
        if self.validate_file_exists_func:
            logger.debug("Patching validate_file_exists")
            # Replace the validate_file_exists function in the extractor
            import types
            from src.utils.validation import validate_file_exists as original_validate

            # Create a function that replaces the original with our mock
            def patched_validate(file_path, *args, **kwargs):
                return self.validate_file_exists_func(file_path)

            # Replace the validate_file_exists in the module namespace
            import src.utils.validation
            self._original_validate = src.utils.validation.validate_file_exists
            src.utils.validation.validate_file_exists = patched_validate

        if self.validate_user_display_name_func:
            self.pipeline.extractor.validate_user_display_name = self.validate_user_display_name_func

        return self.pipeline

    def __del__(self):
        """Restore original validate function when object is deleted."""
        if hasattr(self, '_original_validate'):
            import src.utils.validation
            src.utils.validation.validate_file_exists = self._original_validate

    def run_pipeline(
        self,
        file_path: Optional[str] = None,
        file_obj: Optional[BinaryIO] = None,
        is_tar: bool = False,
        user_display_name: Optional[str] = None,
        resume_from_checkpoint: bool = False,
        checkpoint_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Run the ETL pipeline with the injected dependencies.

        Args:
            file_path: Path to the Skype export file
            file_obj: File object (alternative to file_path)
            is_tar: Whether the file is a tar file
            user_display_name: Display name of the user
            resume_from_checkpoint: Whether to resume from a checkpoint
            checkpoint_id: ID of the checkpoint to resume from

        Returns:
            Dictionary with the results of the pipeline run
        """
        # Use a patch context manager to override os.path.exists and os.path.isfile
        with patch('os.path.exists', return_value=True), patch('os.path.isfile', return_value=True):
            return self.pipeline.run_pipeline(
                file_path=file_path,
                file_obj=file_obj,
                user_display_name=user_display_name,
                resume_from_checkpoint=resume_from_checkpoint
            )

    def extract(
        self,
        file_path: Optional[str] = None,
        file_obj: Optional[BinaryIO] = None
    ) -> Dict[str, Any]:
        """
        Extract data from a Skype export file.

        Args:
            file_path: Path to the Skype export file
            file_obj: File object (alternative to file_path)

        Returns:
            Dictionary with the extracted data
        """
        # Use a patch context manager to override os.path.exists
        with patch('os.path.exists', return_value=True), patch('os.path.isfile', return_value=True):
            return self.pipeline.extractor.extract(
                file_path=file_path,
                file_obj=file_obj
            )

    def transform(
        self,
        raw_data: Dict[str, Any],
        user_display_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Transform raw Skype data into a structured format.

        Args:
            raw_data: Raw Skype data
            user_display_name: Display name of the user

        Returns:
            Dictionary with the transformed data
        """
        return self.pipeline.transformer.transform(
            raw_data=raw_data,
            user_display_name=user_display_name
        )

    def load(
        self,
        raw_data: Dict[str, Any],
        transformed_data: Dict[str, Any],
        file_path: Optional[str] = None
    ) -> str:
        """
        Load transformed Skype data into the database.

        Args:
            raw_data: Raw Skype data
            transformed_data: Transformed Skype data
            file_path: Path to the Skype export file

        Returns:
            Export ID
        """
        return self.pipeline.loader.load(
            raw_data=raw_data,
            transformed_data=transformed_data,
            file_path=file_path
        )

    def save_checkpoint(
        self,
        phase: str,
        data: Dict[str, Any],
        checkpoint_id: Optional[str] = None
    ) -> str:
        """
        Save a checkpoint for the current pipeline state.

        Args:
            phase: Current phase of the pipeline
            data: Data to save in the checkpoint
            checkpoint_id: Optional ID for the checkpoint

        Returns:
            ID of the saved checkpoint
        """
        return self.pipeline.save_checkpoint(
            phase=phase,
            data=data,
            checkpoint_id=checkpoint_id
        )

    def load_checkpoint(
        self,
        checkpoint_id: str
    ) -> Dict[str, Any]:
        """
        Load a checkpoint by ID.

        Args:
            checkpoint_id: ID of the checkpoint to load

        Returns:
            Checkpoint data
        """
        return self.pipeline.load_checkpoint(checkpoint_id=checkpoint_id)

    def get_available_checkpoints(self) -> List[str]:
        """
        Get a list of available checkpoint IDs.

        Returns:
            List of checkpoint IDs
        """
        return self.pipeline.get_available_checkpoints()

    def connect_db(self):
        """Connect to the database."""
        self.pipeline.loader.connect_db()

    def close_db(self):
        """Close the database connection."""
        self.pipeline.loader.close_db()

    @classmethod
    def load_from_checkpoint(
        cls,
        checkpoint_id: str,
        db_config: Optional[Dict[str, Any]] = None,
        output_dir: Optional[str] = None
    ) -> 'TestableETLPipeline':
        """
        Create a new pipeline instance from a checkpoint.

        Args:
            checkpoint_id: ID of the checkpoint to load
            db_config: Database configuration dictionary
            output_dir: Directory for output files

        Returns:
            New TestableETLPipeline instance with the checkpoint loaded
        """
        pipeline = cls(db_config=db_config, output_dir=output_dir)
        pipeline.load_checkpoint(checkpoint_id)
        return pipeline

    def _patch_extract_method(self, extractor):
        """Patch the extract method to use validate_json_file_func."""
        original_extract = extractor.extract
        validate_json_file_func = self.validate_json_file_func

        def patched_extract(file_path, *args, **kwargs):
            logger.debug(f"Using patched extract method for {file_path}")
            try:
                # Call the validation function and return its result
                result = validate_json_file_func(file_path)
                logger.debug(f"Validation function returned: {type(result)}")
                # Ensure we have a dictionary
                if not isinstance(result, dict):
                    try:
                        # Try to parse as JSON string
                        parsed_result = json.loads(result) if isinstance(result, str) else None
                        if isinstance(parsed_result, dict):
                            return parsed_result
                    except Exception:
                        pass
                    # If we still don't have a dict, raise an error
                    raise ValueError("Extracted data must be a dictionary")
                return result
            except Exception as e:
                logger.error(f"Error in patched extract: {str(e)}")
                raise

        # Replace the extract method
        extractor.extract = patched_extract

class MockTransformer(Transformer):
    """Mock transformer that properly handles ETLContext API differences."""

    def transform(self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None) -> Dict[str, Any]:
        """Transform raw Skype export data into a structured format.

        Args:
            raw_data: Raw Skype export data
            user_display_name: Optional display name of the user

        Returns:
            Transformed data
        """
        if self.context:
            # Use start_phase instead of set_phase
            self.context.start_phase('transform')

        logger.debug(f"Transforming data with context: {self.context}")

        # Initialize tracking counters
        total_conversations = 0
        total_messages = 0

        # Initialize the transformed output structure
        transformed_data = {
            'task_id': self.context.task_id if self.context else 'mock-task-id',
            'conversations': [],
            'conversation_mapping': {}
        }

        # Add user and export info
        if user_display_name:
            transformed_data['user_display_name'] = user_display_name

        if raw_data.get('userId'):
            transformed_data['user_id'] = raw_data['userId']

        if raw_data.get('exportDate'):
            transformed_data['export_date'] = raw_data['exportDate']

        # Process conversations
        if raw_data.get('conversations'):
            for conv in raw_data['conversations']:
                # Add basic conversation data
                conversation = {
                    'id': conv.get('id', f'unknown-{total_conversations}'),
                    'display_name': conv.get('displayName', 'Unknown Conversation'),
                    'messages': []
                }

                # Process messages
                if conv.get('MessageList'):
                    for msg in conv['MessageList']:
                        message = {
                            'id': msg.get('id', f'unknown-{total_messages}'),
                            'timestamp': msg.get('originalarrivaltime', ''),
                            'sender_id': msg.get('from_id', ''),
                            'sender_name': msg.get('from_name', ''),
                            'content': msg.get('content', ''),
                            'type': msg.get('messagetype', 'Unknown')
                        }
                        conversation['messages'].append(message)
                        total_messages += 1

                transformed_data['conversations'].append(conversation)
                # Add to conversation mapping
                transformed_data['conversation_mapping'][conversation['id']] = conversation
                total_conversations += 1

        # Add counts to transformed data
        transformed_data['total_conversations'] = total_conversations
        transformed_data['total_messages'] = total_messages

        if self.context:
            # Use end_phase instead of set_phase
            self.context.end_phase('transform')

        return transformed_data

if __name__ == "__main__":
    # This module is not meant to be run directly
    # It should be imported and used as a library
    print("This module is not meant to be run directly.")
    print("Please import it and use the TestableETLPipeline class instead.")
    print("See the README.md file for usage examples.")