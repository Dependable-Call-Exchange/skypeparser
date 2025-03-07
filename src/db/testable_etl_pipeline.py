#!/usr/bin/env python3
"""
Testable ETL Pipeline for Skype Export Data

This module provides a testable version of the ETL pipeline with dependency injection
support for easier testing. It allows injecting mock objects for file reading,
database connections, and other dependencies.
"""

import json
import logging
import os
import warnings
from typing import Any, BinaryIO, Callable, Dict, List, Optional, Union
from unittest.mock import MagicMock, patch

# Import from the modular ETL pipeline instead of the deprecated one
from src.db.etl import ETLPipeline
from src.db.etl.context import ETLContext
from src.db.etl.extractor import Extractor
from src.db.etl.loader import Loader
from src.db.etl.transformer import Transformer
from src.parser.content_extractor import ContentExtractor

# Import additional modules
from src.utils.db_connection import DatabaseConnection
from src.utils.di import get_service, get_service_provider

# Import necessary protocols and implementation
from src.utils.interfaces import (
    ContentExtractorProtocol,
    DatabaseConnectionProtocol,
    FileHandlerProtocol,
    MessageHandlerFactoryProtocol,
    StructuredDataExtractorProtocol,
    ValidationServiceProtocol,
)
from src.utils.message_type_handlers import SkypeMessageHandlerFactory

# Import from service registry
from src.utils.service_registry import (
    register_core_services,
    register_database_connection,
    register_etl_services,
)
from src.utils.structured_data_extractor import StructuredDataExtractor
from src.utils.validation import ValidationService

# Configure logging
logger = logging.getLogger(__name__)


# Create a mock file handler class that wraps the read_file_func
class MockFileHandler(FileHandlerProtocol):
    """Mock file handler for testing that wraps the provided read_file_func."""

    def __init__(
        self,
        read_file_func: Optional[Callable[[str, Optional[str]], Dict[str, Any]]] = None,
        read_file_object_func: Optional[
            Callable[[BinaryIO, Optional[str]], Dict[str, Any]]
        ] = None,
        tar_extract_func: Optional[
            Callable[[str, Optional[str]], Dict[str, Any]]
        ] = None,
    ):
        """Initialize the mock file handler.

        Args:
            read_file_func: Function to read files
            read_file_object_func: Function to read file objects
            tar_extract_func: Function to extract tar files
        """
        self.read_file_func = read_file_func
        self.read_file_object_func = read_file_object_func
        self.tar_extract_func = tar_extract_func

    def read_file(self, file_path: str) -> Dict[str, Any]:
        """Read a file using the provided read_file_func.

        Args:
            file_path: Path to the file

        Returns:
            Dict[str, Any]: The file contents
        """
        if self.read_file_func:
            logger.debug(f"Using mock read_file_func for {file_path}")
            return self.read_file_func(file_path)
        else:
            logger.debug(
                f"No mock read_file_func provided, returning empty dict for {file_path}"
            )
            return {}

    def read_file_object(self, file_obj: BinaryIO) -> Dict[str, Any]:
        """Read a file object using the provided read_file_object_func.

        Args:
            file_obj: File object to read

        Returns:
            Dict[str, Any]: The file contents
        """
        if self.read_file_object_func:
            logger.debug("Using mock read_file_object_func")
            return self.read_file_object_func(file_obj)
        else:
            logger.debug("No mock read_file_object_func provided, returning empty dict")
            return {}

    def read_tarfile(self, file_path: str, auto_select: bool = False) -> Dict[str, Any]:
        """Extract a tar file using the provided tar_extract_func.

        Args:
            file_path: Path to the tar file
            auto_select: Whether to automatically select the first file

        Returns:
            Dict[str, Any]: The extracted file contents
        """
        if self.tar_extract_func:
            logger.debug(f"Using mock tar_extract_func for {file_path}")
            return self.tar_extract_func(file_path)
        else:
            logger.debug(
                f"No mock tar_extract_func provided, returning empty dict for {file_path}"
            )
            return {}


class MockValidationService(ValidationServiceProtocol):
    """Mock validation service for testing."""

    def __init__(
        self,
        validate_file_exists_func: Optional[Callable[[str], bool]] = None,
        validate_file_object_func: Optional[Callable[[BinaryIO], bool]] = None,
        validate_json_file_func: Optional[Callable[[str], Dict[str, Any]]] = None,
        validate_user_display_name_func: Optional[Callable[[str], str]] = None,
    ):
        """Initialize the mock validation service.

        Args:
            validate_file_exists_func: Function to validate file existence
            validate_file_object_func: Function to validate file objects
            validate_json_file_func: Function to validate JSON files
            validate_user_display_name_func: Function to validate user display name
        """
        self.validate_file_exists_func = validate_file_exists_func
        self.validate_file_object_func = validate_file_object_func
        self.validate_json_file_func = validate_json_file_func
        self.validate_user_display_name_func = validate_user_display_name_func

        # Create mock methods for testing
        self.validate_file_exists_mock = MagicMock(
            side_effect=self.validate_file_exists
        )
        self.validate_file_object_mock = MagicMock(
            side_effect=self.validate_file_object
        )
        self.validate_user_display_name_mock = MagicMock(
            side_effect=self.validate_user_display_name
        )

    def validate_file_exists(
        self,
        path: str,
        base_dir: Optional[str] = None,
        allow_absolute: bool = False,
        allow_symlinks: bool = False,
    ) -> bool:
        """Validate that a file exists using the provided validate_file_exists_func.

        Args:
            path: Path to validate
            base_dir: Base directory for relative paths
            allow_absolute: Whether to allow absolute paths
            allow_symlinks: Whether to allow symlinks

        Returns:
            bool: Whether the file exists
        """
        if self.validate_file_exists_func:
            logger.debug(f"Using mock validate_file_exists_func for {path}")
            return self.validate_file_exists_func(path)
        else:
            logger.debug(
                f"No mock validate_file_exists_func provided, returning True for {path}"
            )
            return True

    def validate_file_object(self, file_obj: BinaryIO) -> bool:
        """Validate a file object using the provided validate_file_object_func.

        Args:
            file_obj: File object to validate

        Returns:
            bool: Whether the file object is valid
        """
        if self.validate_file_object_func:
            logger.debug("Using mock validate_file_object_func")
            return self.validate_file_object_func(file_obj)
        else:
            logger.debug("No mock validate_file_object_func provided, returning True")
            return True

    def validate_json_file(
        self,
        file_path: str,
        base_dir: Optional[str] = None,
        allow_absolute: bool = False,
        allow_symlinks: bool = False,
    ) -> Dict[str, Any]:
        """Validate a JSON file using the provided validate_json_file_func.

        Args:
            file_path: Path to the JSON file
            base_dir: Base directory for relative paths
            allow_absolute: Whether to allow absolute paths
            allow_symlinks: Whether to allow symlinks

        Returns:
            Dict[str, Any]: The validated JSON data
        """
        if self.validate_json_file_func:
            logger.debug(f"Using mock validate_json_file_func for {file_path}")
            return self.validate_json_file_func(file_path)
        else:
            logger.debug(
                f"No mock validate_json_file_func provided, returning empty dict for {file_path}"
            )
            return {}

    def validate_user_display_name(self, name: str) -> str:
        """Validate a user display name using the provided validate_user_display_name_func.

        Args:
            name: User display name to validate

        Returns:
            str: The validated user display name
        """
        if self.validate_user_display_name_func:
            logger.debug(f"Using mock validate_user_display_name_func for {name}")
            return self.validate_user_display_name_func(name)
        else:
            logger.debug(
                f"No mock validate_user_display_name_func provided, returning {name}"
            )
            return name


class ImprovedTestableETLPipeline:
    """
    An improved testable version of the ETL pipeline with dependency injection support.

    This class follows the same pattern as SOLIDSkypeETLPipeline, making it easier to
    test the ETL pipeline in isolation by injecting mock objects for all dependencies.
    """

    def __init__(
        self,
        db_config: Dict[str, Any],
        file_handler: FileHandlerProtocol,
        validation_service: ValidationServiceProtocol,
        db_connection: DatabaseConnectionProtocol,
        content_extractor: ContentExtractorProtocol,
        structured_data_extractor: StructuredDataExtractorProtocol,
        message_handler_factory: MessageHandlerFactoryProtocol,
    ) -> None:
        """
        Initialize the ImprovedTestableETLPipeline with all required dependencies.

        Args:
            db_config: Database configuration dictionary
            file_handler: File handler for reading Skype export files
            validation_service: Service for validating input data
            db_connection: Database connection for loading data
            content_extractor: Extractor for message content
            structured_data_extractor: Extractor for structured data
            message_handler_factory: Factory for message handlers
        """
        # Create the ETL context
        self.context: ETLContext = ETLContext(db_config=db_config)

        # Create the pipeline components
        self.extractor: Extractor = Extractor(
            context=self.context,
            file_handler=file_handler,
            validation_service=validation_service,
        )
        self.transformer: Transformer = Transformer(
            context=self.context,
            content_extractor=content_extractor,
            structured_data_extractor=structured_data_extractor,
            message_handler_factory=message_handler_factory,
        )
        self.loader: Loader = Loader(context=self.context, db_connection=db_connection)

        # Store dependencies for later use
        self.file_handler: FileHandlerProtocol = file_handler
        self.validation_service: ValidationServiceProtocol = validation_service
        self.db_connection: DatabaseConnectionProtocol = db_connection
        self.db_config: Dict[str, Any] = db_config

    def run_pipeline(
        self,
        file_path: Optional[str] = None,
        file_obj: Optional[BinaryIO] = None,
        user_display_name: Optional[str] = None,
        resume_from_checkpoint: bool = False,
    ) -> Dict[str, Any]:
        """
        Run the complete ETL pipeline.

        Args:
            file_path: Path to the Skype export file
            file_obj: File-like object containing Skype export data
            user_display_name: Display name of the user
            resume_from_checkpoint: Whether to resume from a checkpoint

        Returns:
            Dict[str, Any]: Results of the pipeline run with extraction, transformation, and loading sections
        """
        # Validate input parameters
        if file_path is None and file_obj is None:
            raise ValueError("Either file_path or file_obj must be provided")

        # Check if we need to use the mock validation methods
        if hasattr(self.validation_service, "validate_file_exists_mock"):
            # Use the mock method for file validation
            if file_path is not None:
                self.validation_service.validate_file_exists_mock(file_path)

        if hasattr(self.validation_service, "validate_user_display_name_mock"):
            # Use the mock method for user display name validation
            if user_display_name is not None:
                self.validation_service.validate_user_display_name_mock(
                    user_display_name
                )

        # Extract data
        raw_data = self.extract(file_path=file_path, file_obj=file_obj)

        # Transform data
        transformed_data = self.transform(
            raw_data=raw_data, user_display_name=user_display_name
        )

        # Load data
        load_id = self.load(
            raw_data=raw_data, transformed_data=transformed_data, file_path=file_path
        )

        # Return results
        return {
            "extraction": {"success": True, "data": raw_data},
            "transformation": {"success": True, "data": transformed_data},
            "loading": {"success": True, "id": load_id},
        }

    def extract(
        self, file_path: Optional[str] = None, file_obj: Optional[BinaryIO] = None
    ) -> Dict[str, Any]:
        """
        Extract data from a Skype export file.

        Args:
            file_path: Path to the Skype export file
            file_obj: File-like object containing Skype export data

        Returns:
            Dict[str, Any]: The extracted data
        """
        return self.extractor.extract(file_path=file_path, file_obj=file_obj)

    def transform(
        self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Transform raw Skype export data into a structured format.

        Args:
            raw_data: Raw Skype export data
            user_display_name: Display name of the user

        Returns:
            Dict[str, Any]: The transformed data
        """
        return self.transformer.transform(
            raw_data=raw_data, user_display_name=user_display_name
        )

    def load(
        self,
        raw_data: Dict[str, Any],
        transformed_data: Dict[str, Any],
        file_path: Optional[str] = None,
    ) -> int:
        """
        Load transformed data into the database.

        Args:
            raw_data: Raw Skype export data
            transformed_data: Transformed data
            file_path: Path to the Skype export file (for reference)

        Returns:
            int: ID of the loaded data
        """
        return self.loader.load(
            raw_data=raw_data, transformed_data=transformed_data, file_source=file_path
        )

    def connect_db(self) -> None:
        """Connect to the database."""
        if hasattr(self.db_connection, "connect"):
            self.db_connection.connect()

    def close_db(self) -> None:
        """Close the database connection."""
        if hasattr(self.db_connection, "close"):
            self.db_connection.close()


def create_testable_etl_pipeline(
    db_config: Dict[str, Any],
    file_handler: Optional[FileHandlerProtocol] = None,
    validation_service: Optional[ValidationServiceProtocol] = None,
    db_connection: Optional[DatabaseConnectionProtocol] = None,
    content_extractor: Optional[ContentExtractorProtocol] = None,
    structured_data_extractor: Optional[StructuredDataExtractorProtocol] = None,
    message_handler_factory: Optional[MessageHandlerFactoryProtocol] = None,
    read_file_func: Optional[Callable[[str], Dict[str, Any]]] = None,
    read_file_object_func: Optional[Callable[[BinaryIO], Dict[str, Any]]] = None,
    tar_extract_func: Optional[Callable[[str], Dict[str, Any]]] = None,
    validate_file_exists_func: Optional[Callable[[str], bool]] = None,
    validate_file_object_func: Optional[Callable[[BinaryIO], bool]] = None,
    validate_json_file_func: Optional[Callable[[str], Dict[str, Any]]] = None,
    validate_user_display_name_func: Optional[Callable[[str], str]] = None,
) -> ImprovedTestableETLPipeline:
    """
    Create an ImprovedTestableETLPipeline with all the necessary dependencies.

    This factory function creates an ImprovedTestableETLPipeline with either the provided
    dependencies or default implementations. It also allows providing mock functions
    for specific operations.

    Args:
        db_config: Database configuration dictionary
        file_handler: File handler for reading Skype export files
        validation_service: Service for validating input data
        db_connection: Database connection for loading data
        content_extractor: Extractor for message content
        structured_data_extractor: Extractor for structured data
        message_handler_factory: Factory for message handlers
        read_file_func: Function to read files
        read_file_object_func: Function to read file objects
        tar_extract_func: Function to extract tar files
        validate_file_exists_func: Function to validate file existence
        validate_file_object_func: Function to validate file objects
        validate_json_file_func: Function to validate JSON files
        validate_user_display_name_func: Function to validate user display name

    Returns:
        ImprovedTestableETLPipeline: A fully configured testable ETL pipeline
    """
    # Create mock file handler if specific mock functions are provided
    if file_handler is None and (
        read_file_func or read_file_object_func or tar_extract_func
    ):
        file_handler = MockFileHandler(
            read_file_func=read_file_func,
            read_file_object_func=read_file_object_func,
            tar_extract_func=tar_extract_func,
        )
    elif file_handler is None:
        file_handler = FileHandlerProtocol()

    # Create mock validation service if specific mock functions are provided
    if validation_service is None and (
        validate_file_exists_func
        or validate_file_object_func
        or validate_json_file_func
        or validate_user_display_name_func
    ):
        validation_service = MockValidationService(
            validate_file_exists_func=validate_file_exists_func,
            validate_file_object_func=validate_file_object_func,
            validate_json_file_func=validate_json_file_func,
            validate_user_display_name_func=validate_user_display_name_func,
        )
    elif validation_service is None:
        validation_service = ValidationService()

    # Create default implementations if not provided
    if db_connection is None:
        db_connection = DatabaseConnection(db_config)

    if content_extractor is None:
        content_extractor = ContentExtractor()

    if structured_data_extractor is None:
        structured_data_extractor = StructuredDataExtractor()

    if message_handler_factory is None:
        message_handler_factory = SkypeMessageHandlerFactory()

    # Create and return the pipeline
    return ImprovedTestableETLPipeline(
        db_config=db_config,
        file_handler=file_handler,
        validation_service=validation_service,
        db_connection=db_connection,
        content_extractor=content_extractor,
        structured_data_extractor=structured_data_extractor,
        message_handler_factory=message_handler_factory,
    )


# Keep the original TestableETLPipeline for backward compatibility
class TestableETLPipeline:
    """
    A testable version of the ETL pipeline with dependency injection support.

    This class allows injecting mock objects for file reading, database connections,
    and other dependencies, making it easier to test the ETL pipeline in isolation.

    Note: This class is deprecated. Please use ImprovedTestableETLPipeline instead.
    """

    def __init__(
        self,
        db_config: Dict[str, Any],
        use_di: bool = False,
        read_file_func: Optional[Callable[[str, Optional[str]], str]] = None,
        tar_extract_func: Optional[
            Callable[[str, Optional[str]], Dict[str, Any]]
        ] = None,
        validate_file_exists_func: Optional[Callable[[str], bool]] = None,
        validate_json_file_func: Optional[Callable[[str], bool]] = None,
        validate_user_display_name_func: Optional[Callable[[str], str]] = None,
        db_connection: Optional[Any] = None,
        content_extractor: Optional[ContentExtractorProtocol] = None,
        structured_data_extractor: Optional[StructuredDataExtractorProtocol] = None,
        message_handler_factory: Optional[MessageHandlerFactoryProtocol] = None,
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
        warnings.warn(
            "TestableETLPipeline is deprecated. Please use ImprovedTestableETLPipeline instead.",
            DeprecationWarning,
            stacklevel=2,
        )

        # Create an ImprovedTestableETLPipeline instead
        if not use_di:
            # Create a mock file handler
            file_handler = MockFileHandler(
                read_file_func=read_file_func, tar_extract_func=tar_extract_func
            )

            # Create a mock validation service
            validation_service = MockValidationService(
                validate_file_exists_func=validate_file_exists_func,
                validate_json_file_func=validate_json_file_func,
                validate_user_display_name_func=validate_user_display_name_func,
            )

            # Create the improved pipeline
            self._pipeline = create_testable_etl_pipeline(
                db_config=db_config,
                file_handler=file_handler,
                validation_service=validation_service,
                db_connection=db_connection,
                content_extractor=content_extractor,
                structured_data_extractor=structured_data_extractor,
                message_handler_factory=message_handler_factory,
            )
        else:
            # Use dependency injection
            from src.utils.di import ServiceProvider

            provider = ServiceProvider()

            # Register services
            register_core_services(provider=provider)
            register_database_connection(db_config=db_config, provider=provider)
            register_etl_services(db_config=db_config, provider=provider)

            # Create the pipeline
            self._pipeline = ETLPipeline(db_config=db_config, use_di=True)

        # Store the context for backward compatibility
        self.context = self._pipeline.context

        # Store the components for backward compatibility
        self.extractor = self._pipeline.extractor
        self.transformer = self._pipeline.transformer
        self.loader = self._pipeline.loader

        # Store the original functions for cleanup
        self._original_validate = None
        self._original_user_validate = None

    def __del__(self):
        """Clean up any patched functions."""
        # Restore original functions if they were patched
        if self._original_validate:
            import src.utils.validation

            src.utils.validation.validate_file_exists = self._original_validate

        if self._original_user_validate:
            import src.utils.validation

            src.utils.validation.validate_user_display_name = (
                self._original_user_validate
            )

    def run_pipeline(
        self,
        file_path: Optional[str] = None,
        file_obj: Optional[BinaryIO] = None,
        user_display_name: Optional[str] = None,
        resume_from_checkpoint: bool = False,
    ) -> Dict[str, Any]:
        """
        Run the complete ETL pipeline.

        Args:
            file_path: Path to the Skype export file
            file_obj: File-like object containing Skype export data
            user_display_name: Display name of the user
            resume_from_checkpoint: Whether to resume from a checkpoint

        Returns:
            Dict[str, Any]: Results of the pipeline run
        """
        return self._pipeline.run_pipeline(
            file_path=file_path,
            file_obj=file_obj,
            user_display_name=user_display_name,
            resume_from_checkpoint=resume_from_checkpoint,
        )

    def extract(
        self, file_path: Optional[str] = None, file_obj: Optional[BinaryIO] = None
    ) -> Dict[str, Any]:
        """
        Extract data from a Skype export file.

        Args:
            file_path: Path to the Skype export file
            file_obj: File-like object containing Skype export data

        Returns:
            Dict[str, Any]: The extracted data
        """
        return self._pipeline.extract(file_path=file_path, file_obj=file_obj)

    def transform(
        self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Transform raw Skype export data into a structured format.

        Args:
            raw_data: Raw Skype export data
            user_display_name: Display name of the user

        Returns:
            Dict[str, Any]: The transformed data
        """
        return self._pipeline.transform(
            raw_data=raw_data, user_display_name=user_display_name
        )

    def load(
        self,
        raw_data: Dict[str, Any],
        transformed_data: Dict[str, Any],
        file_path: Optional[str] = None,
    ) -> int:
        """
        Load transformed data into the database.

        Args:
            raw_data: Raw Skype export data
            transformed_data: Transformed data
            file_path: Path to the Skype export file (for reference)

        Returns:
            int: ID of the loaded data
        """
        return self._pipeline.load(
            raw_data=raw_data, transformed_data=transformed_data, file_path=file_path
        )

    def connect_db(self) -> None:
        """Connect to the database."""
        self._pipeline.connect_db()

    def close_db(self) -> None:
        """Close the database connection."""
        self._pipeline.close_db()


# For backward compatibility
class MockTransformer(Transformer):
    """Mock transformer for testing."""

    def transform(
        self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """Transform raw Skype export data into a structured format.

        Args:
            raw_data: Raw Skype export data
            user_display_name: Optional display name of the user

        Returns:
            Dict[str, Any]: Transformed data
        """
        if self.context:
            self.context.start_phase("transformation")

        logger.debug(f"MockTransformer.transform called with context: {self.context}")

        # Create a simple transformed data structure
        transformed_data = {"conversations": []}

        # Extract conversations from raw data
        if "conversations" in raw_data:
            for conversation in raw_data["conversations"]:
                # Skip conversations without a display name
                if not conversation.get("displayName"):
                    continue

                # Create a transformed conversation
                transformed_conversation = {
                    "id": conversation.get("id", ""),
                    "display_name": conversation.get("displayName", ""),
                    "messages": [],
                }

                # Extract messages from the conversation
                if "MessageList" in conversation:
                    for message in conversation["MessageList"]:
                        # Create a transformed message
                        transformed_message = {
                            "id": message.get("id", ""),
                            "content": message.get("content", ""),
                            "timestamp": message.get("originalarrivaltime", ""),
                            "sender": message.get("from", ""),
                        }

                        # Add the message to the conversation
                        transformed_conversation["messages"].append(transformed_message)

                # Add the conversation to the transformed data
                transformed_data["conversations"].append(transformed_conversation)

        # End the transformation phase
        if self.context:
            self.context.end_phase(
                {
                    "status": "completed",
                    "conversations_processed": len(transformed_data["conversations"]),
                    "messages_processed": sum(
                        len(c["messages"]) for c in transformed_data["conversations"]
                    ),
                }
            )

        return transformed_data


if __name__ == "__main__":
    # This module is not meant to be run directly
    # It should be imported and used as a library
    print("This module is not meant to be run directly.")
    print("Please import it and use the TestableETLPipeline class instead.")
    print("See the README.md file for usage examples.")
