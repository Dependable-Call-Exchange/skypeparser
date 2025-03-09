"""
Test fixtures package for ETL pipeline tests.

This package contains fixtures for unit and integration tests, including:
- Skype data fixtures
- Database fixtures
- Mock fixtures
- Test helpers
- ETL component fixtures
"""

# Import factory classes
from tests.fixtures.factories.data_factories import (
    DatabaseRecordFactory,
    ExpectedApiResponseFactory,
    ExpectedTransformedConversationFactory,
    ExpectedTransformedMessageFactory,
    MockBuilderFactory,
    MockServiceFactory,
    SkypeConversationFactory,
    SkypeDataFactory,
    SkypeMessageFactory,
)

from .db_fixtures import get_test_db_config, is_db_available, test_db_connection

# Import all fixtures from etl_fixtures
from .etl_fixtures import (
    configured_extractor,
    configured_loader,
    configured_transformer,
    etl_context,
    etl_context_with_error,
    etl_context_with_phases,
    extraction_error_scenario,
    loading_error_scenario,
    mock_content_extractor,
    mock_file_handler,
    mock_message_handler_factory,
    mock_structured_data_extractor,
    mock_validation_service,
    pipeline_test_environment,
    temp_invalid_json_file,
    temp_json_file,
    transformation_error_scenario,
)

# Import all mocks from the mocks package
from .mocks import (
    MockContentExtractor,
    MockDatabase,
    MockExtractor,
    MockFileHandler,
    MockLoader,
    MockMessageHandler,
    MockMessageHandlerFactory,
    MockMessageProcessor,
    MockProgressTracker,
    MockStructuredDataExtractor,
    MockTransformer,
    MockValidationService,
    mock_message_handler_factory,
    mock_structured_data_extractor,
)

from .mock_fixtures import (
    MockFileReader,
    create_mock_file_environment,
    create_mock_functions,
)
from .skype_data import (
    BASIC_SKYPE_DATA,
    COMPLEX_SKYPE_DATA,
    CONVERSATION_SKIP_TEST_DATA,
    INVALID_SKYPE_DATA,
    MINIMAL_SKYPE_DATA,
)
from .test_helpers import (
    TestBase,
    create_test_file,
    create_test_json_file,
    create_test_tar_file,
    mock_sys_exit,
    patch_validation,
)

# Import expected data
from tests.fixtures.expected_data import (
    BASIC_TRANSFORMED_CONVERSATION,
    BASIC_TRANSFORMED_DATA,
    BASIC_TRANSFORMED_MESSAGE,
    ERROR_MESSAGES,
    EXPECTED_DB_QUERIES,
    MESSAGE_TYPE_DESCRIPTIONS,
    API_RESPONSES,
    get_expected_error_message,
    get_expected_transformed_conversation,
    get_expected_transformed_message,
)

__all__ = [
    # Skype data fixtures
    "BASIC_SKYPE_DATA",
    "COMPLEX_SKYPE_DATA",
    "INVALID_SKYPE_DATA",
    "MINIMAL_SKYPE_DATA",
    "CONVERSATION_SKIP_TEST_DATA",

    # Database fixtures
    "get_test_db_config",
    "test_db_connection",
    "is_db_available",

    # Mock fixtures
    "MockFileReader",
    "create_mock_file_environment",
    "create_mock_functions",

    # Test helpers
    "TestBase",
    "create_test_file",
    "create_test_json_file",
    "create_test_tar_file",
    "patch_validation",
    "mock_sys_exit",

    # ETL component fixtures
    "etl_context",
    "etl_context_with_phases",
    "etl_context_with_error",
    "mock_file_handler",
    "mock_validation_service",
    "mock_content_extractor",
    "mock_structured_data_extractor",
    "mock_message_handler_factory",
    "configured_extractor",
    "configured_transformer",
    "configured_loader",
    "temp_json_file",
    "temp_invalid_json_file",
    "extraction_error_scenario",
    "transformation_error_scenario",
    "loading_error_scenario",
    "pipeline_test_environment",

    # Factory classes
    "SkypeMessageFactory",
    "SkypeConversationFactory",
    "SkypeDataFactory",
    "DatabaseRecordFactory",
    "MockServiceFactory",
    "MockBuilderFactory",

    # Consolidated mocks
    "MockContentExtractor",
    "MockDatabase",
    "MockExtractor",
    "MockFileHandler",
    "MockLoader",
    "MockMessageHandler",
    "MockMessageHandlerFactory",
    "MockMessageProcessor",
    "MockProgressTracker",
    "MockStructuredDataExtractor",
    "MockTransformer",
    "MockValidationService",
    "mock_message_handler_factory",
    "mock_structured_data_extractor",

    # Factory classes for expected outputs
    "ExpectedTransformedMessageFactory",
    "ExpectedTransformedConversationFactory",
    "ExpectedApiResponseFactory",

    # Expected data constants
    "BASIC_TRANSFORMED_MESSAGE",
    "BASIC_TRANSFORMED_CONVERSATION",
    "BASIC_TRANSFORMED_DATA",
    "ERROR_MESSAGES",
    "EXPECTED_DB_QUERIES",
    "MESSAGE_TYPE_DESCRIPTIONS",
    "API_RESPONSES",

    # Expected data helpers
    "get_expected_transformed_message",
    "get_expected_transformed_conversation",
    "get_expected_error_message",
]
