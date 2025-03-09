"""
Core_Utils package for SkypeParser.

This package contains core_utils-related components.
"""

from .visualization import SkypeDataVisualizer
from .queries import SkypeQueryExamples
from .reporting import generate_report, SkypeReportGenerator
from .interfaces import ContentExtractorProtocol, StructuredDataExtractorProtocol, MessageHandlerProtocol, MessageHandlerFactoryProtocol, FileHandlerProtocol, DatabaseConnectionProtocol, ConnectionPoolProtocol, RepositoryProtocol, ExtractorProtocol, TransformerProtocol, LoaderProtocol, ValidationServiceProtocol
from .test_utils import is_test_environment, get_fast_test_mode
from .config import load_config, get_db_config, get_message_type_description, setup_logging
from .extractors import IExtractor, DefaultExtractor, CallableExtractor, ObjectExtractor
from .di import ServiceProvider, get_service_provider, get_service
from .initialize_error_handling import test_structured_logging, function_with_error, test_error_handling, test_schema_initialization, main
from .service_registry import register_core_services, register_database_connection, register_etl_services, register_all_services
from .structured_data_extractor import StructuredDataExtractor
from .db_connection import DatabaseConnection, create_database_connection
from .serialization import DateTimeEncoder, to_serializable, serialize_to_json, deserialize_from_json
from .dependencies import get_beautifulsoup, get_psycopg2, check_dependency, require_dependency
from .transformer_builder import TransformerBuilder
from .content_extractor import ContentExtractor, extract_content_data, format_content_with_markup, format_content_with_regex
# from .skype_parser import ...
from .exceptions import SkypeParserError, TimestampParsingError, ContentParsingError, FileOperationError, DataExtractionError, InvalidInputError, DatabaseOperationError, ExportError
from .parser_module import safe_filename, timestamp_parser, content_parser, tag_stripper, pretty_quotes
from .core_parser import timestamp_parser, content_parser, enhanced_tag_stripper, pretty_quotes, type_parser, banner_constructor, id_selector, parse_skype_data, parse_skype_data_streaming, stream_conversations
from .di_example import parse_arguments, create_db_config, main
from .skype_to_postgres import create_tables, import_skype_data, get_commandline_args, main
from .connection_factory import DatabaseConnectionFactory, get_connection_factory, create_db_connection, create_connection_pool, register_with_di
from .database_manager import DatabaseManager
from .etl_pipeline_compat import SkypeETLPipeline
from .connection import DatabaseConnection
from .etl_pipeline import SOLIDSkypeETLPipeline, create_solid_skype_etl_pipeline
from .transaction_manager import TransactionManager
from .schema_manager import SchemaManager
from .database_factory import DatabaseConnectionFactory
from .testable_etl_pipeline import MockFileHandler, MockValidationService, ImprovedTestableETLPipeline, create_testable_etl_pipeline, TestableETLPipeline, MockTransformer
from .store_skype_export import load_config, validate_data, clean_skype_data, get_commandline_args, main
from .connection_pool import PostgresConnectionPool, PooledDatabaseConnection
from .individual_insertion import IndividualInsertionStrategy
from .strategy_factory import StrategyType, StrategyFactory
from .bulk_insertion import BulkInsertionStrategy
from .insertion_strategy import InsertionStrategy
from .streaming_processor import StreamingProcessor
# from .pipeline_factory import ...
from .extractor import Extractor
# from .modular_pipeline import ...
from .context import DateTimeEncoder, ETLContext
from .utils import ProgressTracker, MemoryMonitor
from .loader import Loader
from .transformer import Transformer
from .pipeline_manager import MemoryMonitor, ETLPipeline
# from .models import ...
from .storage import SkypeDataStorage
from .tasks import process_skype_export, on_task_success, on_task_failure, submit_task
from .run_api import parse_args, run_worker, create_user, list_users, main
from .skype_api import SkypeParserAPI, create_app
from .user_management import UserManager, get_user_manager
