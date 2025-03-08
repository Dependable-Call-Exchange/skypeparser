#!/usr/bin/env python3
"""
Interface Definitions

This module defines protocol classes (interfaces) for the main dependencies
in the Skype Parser project. These protocols establish clear contracts that
implementations must fulfill, improving type safety and testability.
"""

from typing import (
    Any,
    BinaryIO,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    Protocol,
    Tuple,
    TypeVar,
)


# Content extraction interfaces
class ContentExtractorProtocol(Protocol):
    """Protocol for content extractors that process message content."""

    def extract_content(self, message: Dict[str, Any]) -> str:
        """
        Extract cleaned content from a message.

        Args:
            message: The message data

        Returns:
            Cleaned content as a string
        """
        ...

    def extract_html_content(self, message: Dict[str, Any]) -> str:
        """
        Extract HTML content from a message.

        Args:
            message: The message data

        Returns:
            HTML content as a string
        """
        ...


# Structured data extraction interface
class StructuredDataExtractorProtocol(Protocol):
    """Protocol for extractors that convert raw message data to structured data."""

    def extract_structured_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract structured data from a message.

        Args:
            message: The raw message data

        Returns:
            Structured data extracted from the message
        """
        ...


# Message handling interfaces
class MessageHandlerProtocol(Protocol):
    """Protocol for message type handlers."""

    def can_handle(self, message_type: str) -> bool:
        """
        Check if this handler can process the given message type.

        Args:
            message_type: The type of message

        Returns:
            True if this handler can process the message type
        """
        ...

    def extract_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extract structured data from a message.

        Args:
            message: The message data

        Returns:
            Structured data extracted from the message
        """
        ...


class MessageHandlerFactoryProtocol(Protocol):
    """Protocol for factories that create message handlers."""

    def get_handler(self, message_type: str) -> MessageHandlerProtocol:
        """
        Get a handler for the specified message type.

        Args:
            message_type: The type of message

        Returns:
            A handler that can process the message type
        """
        ...


# File handling interfaces
class FileHandlerProtocol(Protocol):
    """Protocol for file handlers that read Skype export files."""

    def read_file(self, file_path: str) -> Dict[str, Any]:
        """
        Read data from a file.

        Args:
            file_path: Path to the file

        Returns:
            The data read from the file
        """
        ...

    def read_file_object(self, file_obj: BinaryIO, file_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Read data from a file object.

        Args:
            file_obj: File-like object
            file_name: Optional name of the file

        Returns:
            The data read from the file object
        """
        ...

    def read_tarfile(self, file_path: str, auto_select: bool = False, select_json: Optional[int] = None) -> Dict[str, Any]:
        """
        Read data from a tar file.

        Args:
            file_path: Path to the tar file
            auto_select: Whether to automatically select the main data file
            select_json: Index of the JSON file to select (0-based)

        Returns:
            The data read from the tar file
        """
        ...

    def read_tarfile_object(self, file_obj: BinaryIO, auto_select: bool = False, select_json: Optional[int] = None) -> Dict[str, Any]:
        """
        Read data from a tar file object.

        Args:
            file_obj: File object for the tar file
            auto_select: Whether to automatically select the main data file
            select_json: Index of the JSON file to select (0-based)

        Returns:
            The data read from the tar file
        """
        ...

    def read_tarfile_streaming(
        self, file_path: str, auto_select: bool = False
    ) -> Iterator[Tuple[str, Any]]:
        """
        Read data from a tar file using streaming JSON processing.

        This method uses ijson for memory-efficient processing of large JSON files.
        It yields (path, item) tuples for each item in the JSON file.

        Args:
            file_path: Path to the tar file
            auto_select: Whether to automatically select the main data file

        Yields:
            Tuples of (path, item) where path is the JSON path and item is the value
        """
        ...


# Database interfaces
class DatabaseConnectionProtocol(Protocol):
    """Protocol for database connections."""

    def connect(self) -> None:
        """
        Connect to the database.

        Raises:
            Exception: If connection fails
        """
        ...

    def disconnect(self) -> None:
        """Close the database connection."""
        ...

    def execute(self, query: str, params: Optional[Dict[str, Any]] = None) -> Any:
        """
        Execute a database query.

        Args:
            query: SQL query to execute
            params: Query parameters

        Returns:
            Query result
        """
        ...

    def execute_batch(self, query: str, params_list: List[Dict[str, Any]]) -> None:
        """
        Execute a batch of database queries.

        Args:
            query: SQL query to execute
            params_list: List of parameter dictionaries
        """
        ...

    def fetch_one(self, query: str, params: Optional[Dict[str, Any]] = None) -> Optional[Tuple]:
        """
        Execute a query and fetch one result.

        Args:
            query: SQL query to execute
            params: Parameters for the query

        Returns:
            A single result row or None if no results
        """
        ...

    def commit(self) -> None:
        """
        Commit the current transaction.
        """
        ...

    def rollback(self) -> None:
        """
        Rollback the current transaction.
        """
        ...


class ConnectionPoolProtocol(Protocol):
    """Protocol for database connection pools."""

    def get_connection(self) -> Tuple[Any, Any]:
        """
        Get a connection from the pool.

        Returns:
            A tuple containing (connection, cursor)

        Raises:
            Exception: If unable to get a connection from the pool
        """
        ...

    def release_connection(self, conn: Any, cursor: Any) -> None:
        """
        Release a connection back to the pool.

        Args:
            conn: The connection to release
            cursor: The cursor to close
        """
        ...

    def close_all(self) -> None:
        """
        Close all connections in the pool.
        """
        ...

    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the connection pool.

        Returns:
            Dictionary with pool statistics
        """
        ...


# Repository interfaces
T = TypeVar("T")


class RepositoryProtocol(Protocol, Generic[T]):
    """Protocol for repositories that store and retrieve data."""

    def get_by_id(self, id: Any) -> Optional[T]:
        """
        Get an entity by its ID.

        Args:
            id: Entity ID

        Returns:
            The entity if found, None otherwise
        """
        ...

    def get_all(self) -> List[T]:
        """
        Get all entities.

        Returns:
            List of all entities
        """
        ...

    def add(self, entity: T) -> T:
        """
        Add a new entity.

        Args:
            entity: Entity to add

        Returns:
            The added entity
        """
        ...

    def update(self, entity: T) -> T:
        """
        Update an existing entity.

        Args:
            entity: Entity to update

        Returns:
            The updated entity
        """
        ...

    def delete(self, id: Any) -> bool:
        """
        Delete an entity by its ID.

        Args:
            id: Entity ID

        Returns:
            True if the entity was deleted, False otherwise
        """
        ...


# ETL pipeline interfaces
class ExtractorProtocol(Protocol):
    """Protocol for extractors that extract data from sources."""

    def extract(
        self, file_path: Optional[str] = None, file_obj: Optional[BinaryIO] = None
    ) -> Dict[str, Any]:
        """
        Extract data from a source.

        Args:
            file_path: Path to the file to extract from
            file_obj: File-like object to extract from

        Returns:
            The extracted data
        """
        ...


class TransformerProtocol(Protocol):
    """Protocol for transformers that transform raw data."""

    def transform(
        self, raw_data: Dict[str, Any], user_display_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Transform raw data into structured format.

        Args:
            raw_data: Raw data to transform
            user_display_name: Display name of the user

        Returns:
            The transformed data
        """
        ...


class LoaderProtocol(Protocol):
    """Protocol for loaders that load data into a destination."""

    def load(
        self,
        raw_data: Dict[str, Any],
        transformed_data: Dict[str, Any],
        file_source: Optional[str] = None,
    ) -> int:
        """
        Load data into a destination.

        Args:
            raw_data: Raw data from the extractor
            transformed_data: Transformed data from the transformer
            file_source: Source of the data

        Returns:
            ID of the loaded data
        """
        ...

    def connect_db(self) -> None:
        """Connect to the database."""
        ...

    def close_db(self) -> None:
        """Close the database connection."""
        ...


# Validation interfaces
class ValidationServiceProtocol(Protocol):
    """Protocol for validation services that validate input data."""

    def validate_file_exists(
        self,
        path: str,
        base_dir: Optional[str] = None,
        allow_absolute: bool = False,
        allow_symlinks: bool = False,
    ) -> bool:
        """
        Validate that a file exists and passes path safety checks.

        Args:
            path (str): Path to validate
            base_dir (str, optional): Base directory that all paths should be within
            allow_absolute (bool): Whether to allow absolute paths
            allow_symlinks (bool): Whether to allow symbolic links

        Returns:
            bool: True if the file exists and passes safety checks

        Raises:
            ValidationError: If the file does not exist or fails safety checks
        """
        ...

    def validate_json_file(
        self,
        file_path: str,
        base_dir: Optional[str] = None,
        allow_absolute: bool = False,
        allow_symlinks: bool = False,
    ) -> Dict[str, Any]:
        """
        Validate and parse a JSON file, ensuring it passes path safety checks.

        Args:
            file_path (str): Path to the JSON file to validate
            base_dir (str, optional): Base directory that all paths should be within
            allow_absolute (bool): Whether to allow absolute paths
            allow_symlinks (bool): Whether to allow symbolic links

        Returns:
            dict: Parsed JSON data

        Raises:
            ValidationError: If the file is not a valid JSON file or fails safety checks
            json.JSONDecodeError: If the file is not valid JSON
        """
        ...

    def validate_file_object(self, file_obj: BinaryIO) -> bool:
        """
        Validate that a file object is valid and readable.

        Args:
            file_obj: File object to validate

        Returns:
            bool: True if the file object is valid and readable

        Raises:
            ValidationError: If the file object is not valid or readable
        """
        ...

    def validate_user_display_name(self, name: str) -> str:
        """
        Validate and sanitize a user display name.

        Args:
            name (str): User display name to validate

        Returns:
            str: Sanitized user display name

        Raises:
            ValidationError: If the user display name is not valid
        """
        ...
