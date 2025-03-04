#!/usr/bin/env python3
"""
Testable ETL Pipeline for Skype Export Data

This module extends the SkypeETLPipeline class to make it more testable
by allowing dependency injection of file operations, validation functions,
and database connections. This makes it easier to write unit tests without
extensive patching.
"""

import os
import json
import logging
from typing import Dict, List, Optional, Tuple, Any, BinaryIO, Callable

from .etl_pipeline import SkypeETLPipeline, timestamp_parser

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class TestableETLPipeline(SkypeETLPipeline):
    """
    Extends the SkypeETLPipeline class to make it more testable through dependency injection.
    """

    def __init__(
        self,
        db_config: Dict[str, Any] = None,
        output_dir: Optional[str] = None,
        # File operations
        read_file_func: Optional[Callable] = None,
        read_file_object_func: Optional[Callable] = None,
        read_tarfile_func: Optional[Callable] = None,
        read_tarfile_object_func: Optional[Callable] = None,
        # Validation functions
        validate_file_exists_func: Optional[Callable] = None,
        validate_directory_func: Optional[Callable] = None,
        validate_json_file_func: Optional[Callable] = None,
        validate_tar_file_func: Optional[Callable] = None,
        validate_file_object_func: Optional[Callable] = None,
        validate_skype_data_func: Optional[Callable] = None,
        validate_user_display_name_func: Optional[Callable] = None,
        validate_db_config_func: Optional[Callable] = None,
        # Database connection
        db_connection: Optional[Any] = None,
        # Config loader
        load_config_func: Optional[Callable] = None,
        get_db_config_func: Optional[Callable] = None,
        get_message_type_description_func: Optional[Callable] = None,
    ):
        """
        Initialize the testable ETL pipeline with injectable dependencies.

        Args:
            db_config (dict, optional): Database configuration
            output_dir (str, optional): Directory to output files to
            read_file_func (callable, optional): Function to read files
            read_file_object_func (callable, optional): Function to read file objects
            read_tarfile_func (callable, optional): Function to read tar files
            read_tarfile_object_func (callable, optional): Function to read tar file objects
            validate_file_exists_func (callable, optional): Function to validate file existence
            validate_directory_func (callable, optional): Function to validate directories
            validate_json_file_func (callable, optional): Function to validate JSON files
            validate_tar_file_func (callable, optional): Function to validate tar files
            validate_file_object_func (callable, optional): Function to validate file objects
            validate_skype_data_func (callable, optional): Function to validate Skype data
            validate_user_display_name_func (callable, optional): Function to validate user display names
            validate_db_config_func (callable, optional): Function to validate database config
            db_connection (object, optional): Database connection object
            load_config_func (callable, optional): Function to load configuration
            get_db_config_func (callable, optional): Function to get database configuration
            get_message_type_description_func (callable, optional): Function to get message type descriptions
        """
        # Initialize parent class
        super().__init__(db_config, output_dir)

        # Store injected dependencies
        # File operations
        self._read_file = read_file_func
        self._read_file_object = read_file_object_func
        self._read_tarfile = read_tarfile_func
        self._read_tarfile_object = read_tarfile_object_func

        # Validation functions
        self._validate_file_exists = validate_file_exists_func
        self._validate_directory = validate_directory_func
        self._validate_json_file = validate_json_file_func
        self._validate_tar_file = validate_tar_file_func
        self._validate_file_object = validate_file_object_func
        self._validate_skype_data = validate_skype_data_func
        self._validate_user_display_name = validate_user_display_name_func
        self._validate_db_config = validate_db_config_func

        # Database connection
        if db_connection:
            self.conn = db_connection

        # Config functions
        self._load_config = load_config_func
        self._get_db_config = get_db_config_func
        self._get_message_type_description = get_message_type_description_func

        # Import default implementations if not provided
        self._import_default_implementations()

    def _import_default_implementations(self):
        """
        Import default implementations for functions that weren't injected.
        """
        # File operations
        if self._read_file is None:
            from ..utils.file_handler import read_file
            self._read_file = read_file

        if self._read_file_object is None:
            from ..utils.file_handler import read_file_object
            self._read_file_object = read_file_object

        if self._read_tarfile is None:
            from ..utils.file_handler import read_tarfile
            self._read_tarfile = read_tarfile

        if self._read_tarfile_object is None:
            from ..utils.file_handler import read_tarfile_object
            self._read_tarfile_object = read_tarfile_object

        # Validation functions
        if self._validate_file_exists is None:
            from ..utils.validation import validate_file_exists
            self._validate_file_exists = validate_file_exists

        if self._validate_directory is None:
            from ..utils.validation import validate_directory
            self._validate_directory = validate_directory

        if self._validate_json_file is None:
            from ..utils.validation import validate_json_file
            self._validate_json_file = validate_json_file

        if self._validate_tar_file is None:
            from ..utils.validation import validate_tar_file
            self._validate_tar_file = validate_tar_file

        if self._validate_file_object is None:
            from ..utils.validation import validate_file_object
            self._validate_file_object = validate_file_object

        if self._validate_skype_data is None:
            from ..utils.validation import validate_skype_data
            self._validate_skype_data = validate_skype_data

        if self._validate_user_display_name is None:
            from ..utils.validation import validate_user_display_name
            self._validate_user_display_name = validate_user_display_name

        if self._validate_db_config is None:
            from ..utils.validation import validate_db_config
            self._validate_db_config = validate_db_config

        # Config functions
        if self._load_config is None:
            from ..utils.config import load_config
            self._load_config = load_config

        if self._get_db_config is None:
            from ..utils.config import get_db_config
            self._get_db_config = get_db_config

        if self._get_message_type_description is None:
            from ..utils.config import get_message_type_description
            self._get_message_type_description = get_message_type_description

    def connect_db(self) -> None:
        """
        Connect to the database using the provided configuration.
        Override to use injected connection if available.
        """
        if self.conn:
            logger.info("Using injected database connection")
            return

        super().connect_db()

    def extract(self, file_path: str = None, file_obj: BinaryIO = None) -> Dict[str, Any]:
        """
        Extract raw data from a Skype export file (tar archive or JSON).
        Override to use injected functions.

        Args:
            file_path (str, optional): Path to the Skype export file
            file_obj (BinaryIO, optional): File-like object containing the Skype export

        Returns:
            dict: The raw data extracted from the file

        Raises:
            ValidationError: If the input is invalid
            ValueError: If neither file_path nor file_obj is provided
        """
        logger.info("Starting extraction phase")

        if not file_path and not file_obj:
            error_msg = "Either file_path or file_obj must be provided"
            logger.error(error_msg)
            raise ValueError(error_msg)

        try:
            # Determine if we're dealing with a file path or file object
            if file_path:
                # Validate file exists and is readable
                try:
                    self._validate_file_exists(file_path)
                except Exception as e:
                    logger.error(f"File validation error: {e}")
                    raise

                # Process based on file type
                if file_path.endswith('.tar'):
                    try:
                        self._validate_tar_file(file_path)
                        raw_data = self._read_tarfile(file_path, auto_select=True)
                        logger.info(f"Extracted data from tar file: {file_path}")
                    except Exception as e:
                        logger.error(f"TAR file validation error: {e}")
                        raise
                else:
                    try:
                        raw_data = self._validate_json_file(file_path)
                        logger.info(f"Read data from JSON file: {file_path}")
                    except Exception as e:
                        logger.error(f"JSON file validation error: {e}")
                        raise
            elif file_obj:
                # Validate file object
                try:
                    self._validate_file_object(file_obj, allowed_extensions=['.json', '.tar'])
                except Exception as e:
                    logger.error(f"File object validation error: {e}")
                    raise

                # Try to determine file type from name if available
                if hasattr(file_obj, 'name') and file_obj.name.endswith('.tar'):
                    raw_data = self._read_tarfile_object(file_obj, auto_select=True)
                    logger.info("Extracted data from uploaded tar file")
                else:
                    # Assume JSON if not a tar file
                    raw_data = self._read_file_object(file_obj)
                    logger.info("Read data from uploaded JSON file")

            # Validate the extracted data structure
            try:
                self._validate_skype_data(raw_data)
            except Exception as e:
                logger.error(f"Skype data validation error: {e}")
                raise

            # Store raw data if output directory is specified
            if self.output_dir and file_path:
                raw_output_path = os.path.join(self.output_dir, 'raw_data.json')
                with open(raw_output_path, 'w', encoding='utf-8') as f:
                    json.dump(raw_data, f, indent=2)
                logger.info(f"Raw data saved to {raw_output_path}")

            return raw_data

        except Exception as e:
            logger.error(f"Error during extraction phase: {e}")
            raise

    def transform(self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Transform the raw data into a structured format.
        Override to use injected validation function.

        Args:
            raw_data (dict): The raw data extracted from the Skype export
            user_display_name (str, optional): The display name to use for the user

        Returns:
            dict: The transformed data

        Raises:
            ValidationError: If the input data is invalid
        """
        logger.info("Starting transformation phase")

        try:
            # Validate and prepare data
            self._validate_raw_data(raw_data)

            # Extract and process metadata
            transformed_data = self._process_metadata(raw_data, user_display_name)

            # Process conversations
            self._process_conversations(raw_data, transformed_data)

            # Store transformed data if output directory is specified
            self._save_transformed_data(transformed_data)

            return transformed_data

        except Exception as e:
            logger.error(f"Error during transformation phase: {e}")
            raise

    def _validate_raw_data(self, raw_data: Dict[str, Any]) -> None:
        """
        Validate the raw data structure.
        Override to use injected validation function.

        Args:
            raw_data (dict): The raw data to validate

        Raises:
            ValidationError: If the input data is invalid
        """
        try:
            self._validate_skype_data(raw_data)
        except Exception as e:
            logger.error(f"Raw data validation error: {e}")
            raise

    def _process_metadata(self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Extract and process metadata from raw data.
        Override to use injected validation function.

        Args:
            raw_data (dict): The raw data extracted from the Skype export
            user_display_name (str, optional): The display name to use for the user

        Returns:
            dict: The initial transformed data structure with metadata
        """
        # Extract key metadata
        user_id = raw_data['userId']
        export_date_time = raw_data['exportDate']
        export_date_str, export_time_str, export_datetime = timestamp_parser(export_date_time)
        conversations = raw_data['conversations']

        # Validate and sanitize user display name
        if user_display_name:
            try:
                user_display_name = self._validate_user_display_name(user_display_name)
            except Exception as e:
                logger.warning(f"User display name validation error: {e}. Using user ID instead.")
                user_display_name = user_id
        else:
            user_display_name = user_id

        # Initialize the transformed data structure
        transformed_data = {
            'metadata': {
                'userId': user_id,
                'userDisplayName': user_display_name,
                'exportDate': export_date_time,
                'exportDateFormatted': f"{export_date_str} {export_time_str}",
                'conversationCount': len(conversations)
            },
            'conversations': {}
        }

        return transformed_data

    def _type_parser(self, msg_type: str) -> str:
        """
        Map message types to their human-readable descriptions.
        Override to use injected function.

        Args:
            msg_type (str): Skype message type

        Returns:
            str: Human-readable description
        """
        return self._get_message_type_description(self.config, msg_type)


if __name__ == "__main__":
    # This module is not meant to be run directly
    # It should be imported and used as a library
    print("This module is not meant to be run directly.")
    print("Please import it and use the TestableETLPipeline class instead.")
    print("See the README.md file for usage examples.")