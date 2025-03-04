"""
Test fixtures package for ETL pipeline tests.

This package contains fixtures for unit and integration tests, including:
- Skype data fixtures
- Database fixtures
- Mock fixtures
- Test helpers
"""

from .skype_data import (
    BASIC_SKYPE_DATA,
    COMPLEX_SKYPE_DATA,
    INVALID_SKYPE_DATA,
    MINIMAL_SKYPE_DATA,
    CONVERSATION_SKIP_TEST_DATA
)

from .db_fixtures import (
    get_test_db_config,
    test_db_connection,
    is_db_available
)

from .mock_fixtures import (
    MockFileReader,
    MockDatabase,
    create_mock_file_environment
)

from .test_helpers import (
    TestBase,
    create_test_file,
    create_test_json_file,
    create_test_tar_file,
    patch_validation,
    mock_sys_exit
)

__all__ = [
    # Skype data fixtures
    'BASIC_SKYPE_DATA',
    'COMPLEX_SKYPE_DATA',
    'INVALID_SKYPE_DATA',
    'MINIMAL_SKYPE_DATA',
    'CONVERSATION_SKIP_TEST_DATA',

    # Database fixtures
    'get_test_db_config',
    'test_db_connection',
    'is_db_available',

    # Mock fixtures
    'MockFileReader',
    'MockDatabase',
    'create_mock_file_environment',

    # Test helpers
    'TestBase',
    'create_test_file',
    'create_test_json_file',
    'create_test_tar_file',
    'patch_validation',
    'mock_sys_exit'
]