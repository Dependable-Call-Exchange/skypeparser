"""
Loader module for the ETL pipeline.

This module handles loading transformed Skype data into the database,
including raw exports, conversations, and messages.
"""

import datetime
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple, Union
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_READ_COMMITTED

from src.utils.di import get_service
from src.utils.interfaces import DatabaseConnectionProtocol, LoaderProtocol
from src.utils.validation import validate_db_config
from src.utils.new_structured_logging import (
    get_logger,
    log_execution_time,
    log_call,
    handle_errors,
    with_context,
    LogContext,
    log_database_query,
    log_metrics
)

from .context import ETLContext

logger = get_logger(__name__)

# Default schema path
DEFAULT_SCHEMA_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
    "schemas",
    "database_schema.sql"
)

# Default indexes for optimization
DEFAULT_INDEXES = [
    """
    CREATE INDEX IF NOT EXISTS idx_messages_conversation_timestamp
    ON public.skype_messages(conversation_id, timestamp)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_messages_sender
    ON public.skype_messages(sender_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_messages_type
    ON public.skype_messages(message_type)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_conversations_export_id
    ON public.skype_conversations(export_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_messages_content_gin
    ON public.skype_messages USING gin(to_tsvector('english', content))
    """
]

# Fallback schema definitions if schema file is not available
RAW_EXPORTS_TABLE = """
CREATE TABLE IF NOT EXISTS public.skype_raw_exports (
    export_id SERIAL PRIMARY KEY,
    user_id TEXT NOT NULL,
    export_date TIMESTAMP NOT NULL,
    raw_data JSONB NOT NULL,
    file_source TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CONVERSATIONS_TABLE = """
CREATE TABLE IF NOT EXISTS public.skype_conversations (
    conversation_id TEXT PRIMARY KEY,
    display_name TEXT,
    export_id INTEGER REFERENCES public.skype_raw_exports(export_id),
    first_message_time TIMESTAMP,
    last_message_time TIMESTAMP,
    message_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

MESSAGES_TABLE = """
CREATE TABLE IF NOT EXISTS public.skype_messages (
    message_id SERIAL PRIMARY KEY,
    conversation_id TEXT REFERENCES public.skype_conversations(conversation_id),
    timestamp TIMESTAMP NOT NULL,
    sender_id TEXT NOT NULL,
    sender_name TEXT,
    content TEXT,
    html_content TEXT,
    message_type TEXT NOT NULL,
    is_edited BOOLEAN DEFAULT FALSE,
    is_deleted BOOLEAN DEFAULT FALSE,
    reactions JSONB,
    attachments JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""


class Loader(LoaderProtocol):
    """Handles loading transformed Skype data into the database."""

    def __init__(
        self,
        context: ETLContext = None,
        db_config: Optional[Dict[str, Any]] = None,
        batch_size: int = 100,
        db_connection: Optional[DatabaseConnectionProtocol] = None,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        schema_file: Optional[str] = None,
    ):
        """
        Initialize the loader.

        Args:
            context: ETL context
            db_config: Database configuration
            batch_size: Batch size for database operations
            db_connection: Database connection
            max_retries: Maximum number of retries for database operations
            retry_delay: Delay between retries in seconds
            schema_file: Path to database schema file
        """
        # Initialize metrics
        self._metrics = {
            "start_time": None,
            "end_time": None,
            "rows_inserted": 0,
            "conversations_inserted": 0,
            "messages_inserted": 0,
            "batch_sizes": [],
            "query_times": [],
        }

        # Set context
        self.context = context

        # Set database configuration
        if db_config is None and context is not None:
            db_config = context.db_config
        self.db_config = db_config

        # Set batch size
        if batch_size is None and context is not None:
            batch_size = context.batch_size
        self.batch_size = batch_size

        # Set database connection
        self.db_connection = db_connection
        if self.db_connection is None:
            # Get database connection from service registry if available
            try:
                self.db_connection = get_service("db_connection")
            except (ImportError, KeyError):
                self.db_connection = None

        # Set retry parameters
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        # Set schema file
        self.schema_file = schema_file or DEFAULT_SCHEMA_PATH

        # Log initialization
        logger.info(
            "Initialized Loader",
            extra={
                "batch_size": self.batch_size,
                "max_retries": self.max_retries,
                "retry_delay": self.retry_delay,
                "schema_file": self.schema_file,
            }
        )

    @handle_errors(log_level="ERROR", default_message="Error connecting to database")
    def connect_db(self) -> None:
        """Connect to the database."""
        # Skip if already connected
        if self.db_connection is not None and hasattr(self.db_connection, "closed") and not self.db_connection.closed:
            logger.debug("Already connected to database")
            return

        # Validate database configuration
        if self.db_config is None:
            raise ValueError("Database configuration is required")

        # Log connection attempt
        logger.info(
            "Connecting to database",
            extra={
                "host": self.db_config.get("host", "localhost"),
                "port": self.db_config.get("port", 5432),
                "database": self.db_config.get("database", "postgres"),
                "user": self.db_config.get("user", "postgres"),
            }
        )

        # Connect to database
        start_time = time.time()
        try:
            self.db_connection = psycopg2.connect(**self.db_config)
            self.db_connection.set_isolation_level(ISOLATION_LEVEL_READ_COMMITTED)

            # Log successful connection
            duration_ms = (time.time() - start_time) * 1000
            logger.info(
                "Connected to database",
                extra={
                    "metrics": {
                        "duration_ms": duration_ms,
                        "host": self.db_config.get("host", "localhost"),
                        "database": self.db_config.get("database", "postgres"),
                    }
                }
            )
        except Exception as e:
            # Log connection error
            duration_ms = (time.time() - start_time) * 1000
            logger.error(
                f"Error connecting to database: {e}",
                exc_info=True,
                extra={
                    "metrics": {
                        "duration_ms": duration_ms,
                        "host": self.db_config.get("host", "localhost"),
                        "database": self.db_config.get("database", "postgres"),
                    },
                    "error": str(e),
                }
            )
            raise

    @handle_errors(log_level="ERROR", default_message="Error closing database connection")
    def close_db(self) -> None:
        """Close the database connection."""
        if self.db_connection is not None and hasattr(self.db_connection, "closed") and not self.db_connection.closed:
            logger.debug("Closing database connection")
            self.db_connection.close()
            logger.info("Database connection closed")

    @handle_errors(log_level="ERROR", default_message="Error checking database health")
    def _check_db_health(self) -> None:
        """Check database health."""
        if self.db_connection is None:
            raise ValueError("Database connection is not initialized")

        # Execute a simple query to check database health
        start_time = time.time()
        with self.db_connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()

        # Log health check
        duration_ms = (time.time() - start_time) * 1000
        logger.debug(
            "Database health check successful",
            extra={
                "metrics": {
                    "duration_ms": duration_ms,
                    "result": result[0] if result else None,
                }
            }
        )

    @handle_errors(log_level="ERROR", default_message="Error creating database tables")
    def _create_tables(self) -> None:
        """Create database tables."""
        if self.db_connection is None:
            raise ValueError("Database connection is not initialized")

        # Read schema file
        with open(self.schema_file, "r") as f:
            schema_sql = f.read()

        # Execute schema SQL
        start_time = time.time()
        with self.db_connection.cursor() as cursor:
            cursor.execute(schema_sql)
        self.db_connection.commit()

        # Log table creation
        duration_ms = (time.time() - start_time) * 1000
        logger.info(
            "Created database tables",
            extra={
                "metrics": {
                    "duration_ms": duration_ms,
                    "schema_file": self.schema_file,
                }
            }
        )

    @handle_errors(log_level="ERROR", default_message="Error creating database indexes")
    def _create_indexes(self) -> None:
        """Create database indexes."""
        if self.db_connection is None:
            raise ValueError("Database connection is not initialized")

        # Execute index creation SQL
        start_time = time.time()
        with self.db_connection.cursor() as cursor:
            for index_sql in DEFAULT_INDEXES:
                cursor.execute(index_sql)
        self.db_connection.commit()

        # Log index creation
        duration_ms = (time.time() - start_time) * 1000
        logger.info(
            "Created database indexes",
            extra={
                "metrics": {
                    "duration_ms": duration_ms,
                    "index_count": len(DEFAULT_INDEXES),
                }
            }
        )

    @log_execution_time(level=logging.INFO)
    @with_context(operation="load")
    def load(
        self,
        raw_data: Dict[str, Any],
        transformed_data: Dict[str, Any],
        file_source: Optional[str] = None,
    ) -> int:
        """
        Load transformed data into the database.

        Args:
            raw_data: Raw data from the extractor
            transformed_data: Transformed data from the transformer
            file_source: Source file path

        Returns:
            Export ID
        """
        # Start timer
        self._metrics["start_time"] = time.time()

        try:
            # Validate input data
            self._validate_input_data(raw_data, transformed_data)

            # Connect to database if not already connected
            self._validate_database_connection()

            # Create tables and indexes if they don't exist
            self._create_tables()
            self._create_indexes()

            # Begin transaction
            self._begin_transaction()

            try:
                # Store raw export
                export_id = self._store_raw_export(raw_data, file_source)

                # Store conversations and messages
                with LogContext(export_id=export_id):
                    self._store_conversations(transformed_data, export_id)
                    self._store_messages(transformed_data)

                # Commit transaction
                self._commit_transaction()

                # Update context if available
                if self.context is not None:
                    self.context.export_id = export_id

                # End timer
                self._metrics["end_time"] = time.time()

                # Calculate comprehensive metrics
                duration_ms = (self._metrics["end_time"] - self._metrics["start_time"]) * 1000
                avg_batch_size = sum(self._metrics["batch_sizes"]) / len(self._metrics["batch_sizes"]) if self._metrics["batch_sizes"] else 0
                avg_query_time_ms = sum(self._metrics["query_times"]) / len(self._metrics["query_times"]) if self._metrics["query_times"] else 0
                total_queries = len(self._metrics["query_times"])

                # Calculate throughput metrics
                messages_per_second = (self._metrics["messages_inserted"] / duration_ms) * 1000 if duration_ms > 0 else 0
                conversations_per_second = (self._metrics["conversations_inserted"] / duration_ms) * 1000 if duration_ms > 0 else 0

                # Log success with basic info
                logger.info(
                    f"Data loaded successfully with export ID: {export_id}",
                    extra={
                        "export_id": export_id,
                        "file_source": file_source
                    }
                )

                # Log detailed metrics separately for better analysis
                log_metrics(
                    logger,
                    {
                        "duration_ms": duration_ms,
                        "rows_inserted": self._metrics["rows_inserted"],
                        "conversations_inserted": self._metrics["conversations_inserted"],
                        "messages_inserted": self._metrics["messages_inserted"],
                        "avg_batch_size": avg_batch_size,
                        "avg_query_time_ms": avg_query_time_ms,
                        "total_queries": total_queries,
                        "messages_per_second": messages_per_second,
                        "conversations_per_second": conversations_per_second,
                        "max_batch_size": max(self._metrics["batch_sizes"]) if self._metrics["batch_sizes"] else 0,
                        "min_batch_size": min(self._metrics["batch_sizes"]) if self._metrics["batch_sizes"] else 0,
                        "max_query_time_ms": max(self._metrics["query_times"]) if self._metrics["query_times"] else 0,
                        "min_query_time_ms": min(self._metrics["query_times"]) if self._metrics["query_times"] else 0,
                        "export_id": export_id
                    },
                    level=logging.INFO,
                    message=f"Load operation metrics for export ID: {export_id}"
                )

                return export_id

            except Exception as e:
                # Rollback transaction on error
                self._rollback_transaction()
                raise

        except Exception as e:
            # Calculate partial metrics for error analysis
            end_time = time.time()
            duration_ms = (end_time - (self._metrics["start_time"] or end_time)) * 1000

            # Log error with detailed context
            logger.error(
                f"Error loading data: {e}",
                exc_info=True,
                extra={
                    "error": str(e),
                    "error_type": type(e).__name__,
                    "file_source": file_source,
                    "phase": "load",
                    "metrics": {
                        "duration_ms": duration_ms,
                        "rows_inserted": self._metrics["rows_inserted"],
                        "conversations_inserted": self._metrics["conversations_inserted"],
                        "messages_inserted": self._metrics["messages_inserted"],
                        "progress_percentage": (
                            (self._metrics["conversations_inserted"] / len(transformed_data.get("conversations", {}))) * 100
                            if transformed_data and "conversations" in transformed_data and len(transformed_data["conversations"]) > 0
                            else 0
                        )
                    }
                }
            )

            # Record error in context if available
            if self.context is not None:
                self.context.record_error(
                    "load",
                    f"Error loading data: {e}",
                    {
                        "error_type": type(e).__name__,
                        "file_source": file_source,
                        "metrics": {
                            "duration_ms": duration_ms,
                            "rows_inserted": self._metrics["rows_inserted"],
                            "conversations_inserted": self._metrics["conversations_inserted"],
                            "messages_inserted": self._metrics["messages_inserted"]
                        }
                    }
                )

            raise

    @handle_errors(log_level="ERROR", default_message="Error validating input data")
    def _validate_input_data(
        self, raw_data: Dict[str, Any], transformed_data: Dict[str, Any]
    ) -> None:
        """
        Validate input data.

        Args:
            raw_data: Raw data from the extractor
            transformed_data: Transformed data from the transformer

        Raises:
            ValueError: If input data is invalid
        """
        # Validate raw data
        if not isinstance(raw_data, dict):
            raise ValueError("Raw data must be a dictionary")
        if "messages" not in raw_data:
            raise ValueError("Raw data must contain 'messages' key")

        # Validate transformed data
        if not isinstance(transformed_data, dict):
            raise ValueError("Transformed data must be a dictionary")
        if "conversations" not in transformed_data:
            raise ValueError("Transformed data must contain 'conversations' key")
        if "messages" not in transformed_data:
            raise ValueError("Transformed data must contain 'messages' key")

        # Log validation success
        logger.debug(
            "Input data validated",
            extra={
                "raw_data_keys": list(raw_data.keys()),
                "transformed_data_keys": list(transformed_data.keys()),
                "conversation_count": len(transformed_data["conversations"]),
                "message_count": sum(len(msgs) for msgs in transformed_data["messages"].values()),
            }
        )

    @handle_errors(log_level="ERROR", default_message="Error validating database connection")
    def _validate_database_connection(self) -> None:
        """
        Validate database connection.

        Raises:
            ValueError: If database connection is invalid
        """
        # Connect to database if not already connected
        if self.db_connection is None:
            self.connect_db()

        # Check database health
        self._check_db_health()

    @with_context(operation="store_raw_export")
    @handle_errors(log_level="ERROR", default_message="Error storing raw export")
    def _store_raw_export(
        self, raw_data: Dict[str, Any], file_source: Optional[str] = None
    ) -> int:
        """
        Store raw export data.

        Args:
            raw_data: Raw data from the extractor
            file_source: Source file path

        Returns:
            Export ID
        """
        # Prepare data
        export_data = {
            "raw_data": json.dumps(raw_data),
            "file_source": file_source,
            "import_date": datetime.datetime.now().isoformat(),
            "user_id": self.context.user_id if self.context else None,
            "user_display_name": self.context.user_display_name if self.context else None,
            "export_date": self.context.export_date if self.context else None,
        }

        # Insert export
        query = """
        INSERT INTO public.skype_exports
        (raw_data, file_source, import_date, user_id, user_display_name, export_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """
        params = (
            export_data["raw_data"],
            export_data["file_source"],
            export_data["import_date"],
            export_data["user_id"],
            export_data["user_display_name"],
            export_data["export_date"],
        )

        # Execute query
        start_time = time.time()
        with self.db_connection.cursor() as cursor:
            cursor.execute(query, params)
            export_id = cursor.fetchone()[0]

        # Update metrics
        duration_ms = (time.time() - start_time) * 1000
        self._metrics["rows_inserted"] += 1
        self._metrics["query_times"].append(duration_ms)

        # Log query
        log_database_query(
            logger,
            query,
            {
                "raw_data": "<truncated>",
                "file_source": export_data["file_source"],
                "import_date": export_data["import_date"],
                "user_id": export_data["user_id"],
                "user_display_name": export_data["user_display_name"],
                "export_date": export_data["export_date"],
            },
            duration_ms,
            1,
            logging.DEBUG
        )

        # Log export storage
        logger.info(
            f"Stored raw export with ID: {export_id}",
            extra={
                "export_id": export_id,
                "file_source": file_source,
                "metrics": {
                    "duration_ms": duration_ms,
                    "raw_data_size_bytes": len(export_data["raw_data"]),
                }
            }
        )

        return export_id

    @with_context(operation="store_conversations")
    @handle_errors(log_level="ERROR", default_message="Error storing conversations")
    def _store_conversations(
        self, transformed_data: Dict[str, Any], export_id: int
    ) -> None:
        """
        Store conversations.

        Args:
            transformed_data: Transformed data from the transformer
            export_id: Export ID
        """
        # Get conversations
        conversations = transformed_data.get("conversations", {})

        # Track metrics
        start_time = time.time()
        conversation_sizes = []
        conversation_types = {}

        # Log start
        logger.info(
            f"Storing {len(conversations)} conversations for export ID: {export_id}",
            extra={
                "export_id": export_id,
                "conversation_count": len(conversations),
            }
        )

        # Store each conversation
        for conv_id, conv_data in conversations.items():
            # Track conversation size and type for metrics
            conv_size = len(json.dumps(conv_data))
            conversation_sizes.append(conv_size)

            # Track conversation type distribution
            conv_type = conv_data.get("type", "unknown")
            conversation_types[conv_type] = conversation_types.get(conv_type, 0) + 1

            # Insert the conversation
            self._insert_conversation(conv_id, conv_data, export_id)

            # Update progress in context if available
            if self.context is not None:
                self.context.update_progress(
                    "store_conversations",
                    list(conversations.keys()).index(conv_id) + 1,
                    len(conversations),
                    "conversations"
                )

        # Calculate metrics
        duration_ms = (time.time() - start_time) * 1000
        avg_conv_size = sum(conversation_sizes) / len(conversation_sizes) if conversation_sizes else 0

        # Log completion with basic info
        logger.info(
            f"Stored {len(conversations)} conversations for export ID: {export_id}",
            extra={
                "export_id": export_id,
                "conversation_count": len(conversations),
            }
        )

        # Log detailed metrics separately
        log_metrics(
            logger,
            {
                "conversations_inserted": len(conversations),
                "duration_ms": duration_ms,
                "conversations_per_second": (len(conversations) / duration_ms) * 1000 if duration_ms > 0 else 0,
                "avg_conversation_size_bytes": avg_conv_size,
                "min_conversation_size_bytes": min(conversation_sizes) if conversation_sizes else 0,
                "max_conversation_size_bytes": max(conversation_sizes) if conversation_sizes else 0,
                "total_conversation_data_kb": sum(conversation_sizes) / 1024 if conversation_sizes else 0,
                "conversation_type_distribution": conversation_types,
                "export_id": export_id
            },
            level=logging.INFO,
            message=f"Conversation storage metrics for export ID: {export_id}"
        )

    @handle_errors(log_level="ERROR", default_message="Error inserting conversation")
    def _insert_conversation(
        self, conv_id: str, conv_data: Dict[str, Any], export_id: int
    ) -> None:
        """
        Insert a conversation.

        Args:
            conv_id: Conversation ID
            conv_data: Conversation data
            export_id: Export ID
        """
        # Prepare data
        conversation = {
            "id": conv_id,
            "export_id": export_id,
            "display_name": conv_data.get("displayName", ""),
            "type": conv_data.get("type", ""),
            "version": conv_data.get("version", 0),
            "properties": json.dumps(conv_data.get("properties", {})),
            "thread_properties": json.dumps(conv_data.get("threadProperties", {})),
            "members": json.dumps(conv_data.get("members", [])),
        }

        # Insert conversation
        query = """
        INSERT INTO public.skype_conversations
        (id, export_id, display_name, type, version, properties, thread_properties, members)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
        export_id = EXCLUDED.export_id,
        display_name = EXCLUDED.display_name,
        type = EXCLUDED.type,
        version = EXCLUDED.version,
        properties = EXCLUDED.properties,
        thread_properties = EXCLUDED.thread_properties,
        members = EXCLUDED.members
        """
        params = (
            conversation["id"],
            conversation["export_id"],
            conversation["display_name"],
            conversation["type"],
            conversation["version"],
            conversation["properties"],
            conversation["thread_properties"],
            conversation["members"],
        )

        # Execute query
        start_time = time.time()
        with self.db_connection.cursor() as cursor:
            cursor.execute(query, params)

        # Update metrics
        duration_ms = (time.time() - start_time) * 1000
        self._metrics["rows_inserted"] += 1
        self._metrics["conversations_inserted"] += 1
        self._metrics["query_times"].append(duration_ms)

        # Log query
        log_database_query(
            logger,
            query,
            {
                "id": conversation["id"],
                "export_id": conversation["export_id"],
                "display_name": conversation["display_name"],
                "type": conversation["type"],
                "version": conversation["version"],
                "properties": "<truncated>",
                "thread_properties": "<truncated>",
                "members": "<truncated>",
            },
            duration_ms,
            1,
            logging.DEBUG
        )

    @with_context(operation="store_messages")
    @handle_errors(log_level="ERROR", default_message="Error storing messages")
    def _store_messages(self, transformed_data: Dict[str, Any]) -> None:
        """
        Store messages.

        Args:
            transformed_data: Transformed data from the transformer
        """
        # Get messages
        messages_by_conversation = transformed_data.get("messages", {})
        total_message_count = sum(len(msgs) for msgs in messages_by_conversation.values())

        # Log start
        logger.info(
            f"Storing {total_message_count} messages for {len(messages_by_conversation)} conversations",
            extra={
                "conversation_count": len(messages_by_conversation),
                "message_count": total_message_count,
            }
        )

        # Store messages for each conversation
        for conv_id, messages in messages_by_conversation.items():
            with LogContext(conversation_id=conv_id):
                self._insert_messages(conv_id, messages)

        # Log completion
        logger.info(
            f"Stored {total_message_count} messages for {len(messages_by_conversation)} conversations",
            extra={
                "conversation_count": len(messages_by_conversation),
                "message_count": total_message_count,
                "metrics": {
                    "messages_inserted": total_message_count,
                }
            }
        )

    @handle_errors(log_level="ERROR", default_message="Error inserting messages")
    def _insert_messages(self, conv_id: str, messages: List[Dict[str, Any]]) -> None:
        """
        Insert messages for a conversation.

        Args:
            conv_id: Conversation ID
            messages: List of messages
        """
        # Log start
        logger.debug(
            f"Inserting {len(messages)} messages for conversation: {conv_id}",
            extra={
                "conversation_id": conv_id,
                "message_count": len(messages),
            }
        )

        # Calculate optimal batch size
        batch_size = self._calculate_optimal_batch_size(messages)

        # Insert messages in batches
        for i in range(0, len(messages), batch_size):
            batch = messages[i:i+batch_size]
            self._insert_message_batch(conv_id, batch)

            # Log batch progress
            logger.debug(
                f"Inserted batch of {len(batch)} messages for conversation: {conv_id}",
                extra={
                    "conversation_id": conv_id,
                    "batch_size": len(batch),
                    "progress": f"{min(i+batch_size, len(messages))}/{len(messages)}",
                }
            )

    @handle_errors(log_level="ERROR", default_message="Error calculating optimal batch size")
    def _calculate_optimal_batch_size(self, messages: List[Dict[str, Any]]) -> int:
        """
        Calculate optimal batch size based on message size.

        Args:
            messages: List of messages

        Returns:
            Optimal batch size
        """
        # Use default batch size if no messages
        if not messages:
            logger.debug(
                "No messages to calculate batch size, using default",
                extra={"default_batch_size": self.batch_size}
            )
            return self.batch_size

        # Calculate average message size
        avg_message_size = sum(len(json.dumps(msg)) for msg in messages) / len(messages)

        # Calculate optimal batch size (target ~1MB per batch)
        target_batch_size_bytes = 1024 * 1024  # 1MB
        optimal_batch_size = max(1, min(self.batch_size, int(target_batch_size_bytes / avg_message_size)))

        # Log calculation with metrics
        log_metrics(
            logger,
            {
                "avg_message_size_bytes": avg_message_size,
                "default_batch_size": self.batch_size,
                "optimal_batch_size": optimal_batch_size,
                "target_batch_size_bytes": target_batch_size_bytes,
                "message_count": len(messages),
                "size_reduction_percent": ((self.batch_size - optimal_batch_size) / self.batch_size) * 100 if optimal_batch_size < self.batch_size else 0,
                "estimated_batch_size_kb": (optimal_batch_size * avg_message_size) / 1024
            },
            level=logging.DEBUG,
            message=f"Calculated optimal batch size: {optimal_batch_size}"
        )

        return optimal_batch_size

    @handle_errors(log_level="ERROR", default_message="Error inserting message batch")
    def _insert_message_batch(self, conv_id: str, messages: List[Dict[str, Any]]) -> None:
        """
        Insert a batch of messages.

        Args:
            conv_id: Conversation ID
            messages: List of messages
        """
        if not messages:
            return

        # Prepare query
        query = """
        INSERT INTO public.skype_messages
        (id, conversation_id, sender_id, sender_display_name, timestamp, content, message_type, properties)
        VALUES %s
        ON CONFLICT (id) DO UPDATE SET
        conversation_id = EXCLUDED.conversation_id,
        sender_id = EXCLUDED.sender_id,
        sender_display_name = EXCLUDED.sender_display_name,
        timestamp = EXCLUDED.timestamp,
        content = EXCLUDED.content,
        message_type = EXCLUDED.message_type,
        properties = EXCLUDED.properties
        """

        # Prepare values
        values = []
        for msg in messages:
            values.append((
                msg.get("id", ""),
                conv_id,
                msg.get("senderId", ""),
                msg.get("senderDisplayName", ""),
                msg.get("timestamp", ""),
                msg.get("content", ""),
                msg.get("messageType", ""),
                json.dumps(msg.get("properties", {})),
            ))

        # Execute query
        start_time = time.time()
        with self.db_connection.cursor() as cursor:
            # Use execute_values for efficient batch insert
            from psycopg2.extras import execute_values
            execute_values(cursor, query, values)

        # Calculate metrics
        duration_ms = (time.time() - start_time) * 1000
        self._metrics["rows_inserted"] += len(messages)
        self._metrics["messages_inserted"] += len(messages)
        self._metrics["batch_sizes"].append(len(messages))
        self._metrics["query_times"].append(duration_ms)

        # Log query with detailed metrics
        log_database_query(
            logger,
            query,
            {"values_count": len(values), "conversation_id": conv_id},
            duration_ms,
            len(messages),
            logging.DEBUG
        )

        # Log batch metrics separately for better analysis
        log_metrics(
            logger,
            {
                "batch_size": len(messages),
                "query_duration_ms": duration_ms,
                "avg_message_size_bytes": sum(len(json.dumps(msg)) for msg in messages) / max(1, len(messages)),
                "total_batch_size_kb": sum(len(json.dumps(msg)) for msg in messages) / 1024,
                "messages_per_second": (len(messages) / duration_ms) * 1000 if duration_ms > 0 else 0
            },
            level=logging.DEBUG,
            message=f"Message batch metrics for conversation {conv_id}"
        )

    @with_context(operation="begin_transaction")
    @handle_errors(log_level="ERROR", default_message="Error beginning transaction")
    def _begin_transaction(self) -> None:
        """Begin a database transaction."""
        logger.debug("Beginning database transaction")
        try:
            self.db_connection.autocommit = False
        except Exception as e:
            logger.error(
                f"Error beginning transaction: {e}",
                exc_info=True,
                extra={"error": str(e)}
            )
            raise

    @with_context(operation="commit_transaction")
    @handle_errors(log_level="ERROR", default_message="Error committing transaction")
    def _commit_transaction(self) -> None:
        """Commit the current database transaction."""
        logger.debug("Committing database transaction")
        try:
            self.db_connection.commit()
            logger.info("Transaction committed successfully")
        except Exception as e:
            logger.error(
                f"Error committing transaction: {e}",
                exc_info=True,
                extra={"error": str(e)}
            )
            raise

    @with_context(operation="rollback_transaction")
    @handle_errors(log_level="ERROR", default_message="Error rolling back transaction")
    def _rollback_transaction(self) -> None:
        """Rollback the current database transaction."""
        logger.debug("Rolling back database transaction")
        try:
            self.db_connection.rollback()
            logger.info("Transaction rolled back successfully")
        except Exception as e:
            logger.error(
                f"Error rolling back transaction: {e}",
                exc_info=True,
                extra={"error": str(e)}
            )
            # Don't raise here as we're already in an error handler
