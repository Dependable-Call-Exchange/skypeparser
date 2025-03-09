# Test Suite Migration Tracker

This document tracks the progress of migrating the test suite to the new structure.

## Migration Status

| Test File | Status | Replaced By | Notes |
|-----------|--------|------------|-------|
| `tests/unit/test_etl_pipeline.py` | ❌ Deprecated | `test_etl_pipeline_pytest.py` | Ready for deletion after verification |
| `tests/unit/test_etl_transformer.py` | ❌ Deprecated | `test_etl_transformer_pytest.py` | Ready for deletion after verification |
| `tests/unit/test_message_type_handlers.py` | ✅ Migrated | `test_message_type_handlers_pytest.py` | Both can be kept temporarily for reference |
| `tests/unit/test_skype_parser.py` | ✅ Migrated | `test_skype_parser_pytest.py` | Deprecated, ready for deletion after verification |
| `tests/unit/test_content_extractor.py` | ✅ Migrated | `test_content_extractor_pytest.py` | Deprecated, ready for deletion after verification |
| `tests/integration/test_etl_pipeline_integration.py` | ✅ Migrated | `test_etl_pipeline_integration_pytest.py` | Successfully migrated with proper fixtures and dependency injection |
| `tests/unit/test_structured_logging.py` | ✅ Migrated | `test_structured_logging_pytest.py` | Migrated with proper fixtures, old file marked as deprecated |

## Files for Removal

| File | Reason for Removal | Replacement |
|------|-------------------|-------------|
| `tests/mocks.py` | Old centralized mocks | `tests/fixtures/mocks/` |
| `tests/factories.py` | Some factories are unused | Move needed factories to fixtures |
| `tests/helpers.py` | Outdated helper functions | Move needed helpers to test_helpers.py |
| `scripts/test_logging.py` | Conflicts with utils/test_logging.py | N/A |

## Migration Steps Completed

- ✅ Created centralized expectations (`expected_data.py`)
- ✅ Created shared pytest fixtures (`conftest.py`)
- ✅ Established consolidated mock implementations
- ✅ Migrated key test files to pytest style
- ✅ Created enhanced test runner (`run_pytest_tests.py`)
- ✅ Created migration helper scripts (`migrate_test.py` and `verify_test_migration.py`)
- ✅ Migrated `test_skype_parser.py` to pytest style with dependency injection
- ✅ Migrated `test_content_extractor.py` to pytest style with dependency injection
- ✅ Migrated `test_etl_pipeline_integration.py` to pytest style with dependency injection
- ✅ Migrated `test_structured_logging.py` to pytest style with proper fixtures
- ✅ Fixed issues with migrated tests to ensure they all pass

## Prioritized Migration Plan

### Priority 1: High-Impact Files
1. ✅ `tests/unit/test_skype_parser.py` - Core functionality test
   - ✅ Use dependency injection instead of patching
   - ✅ Leverage typed fixtures from conftest.py
   - ✅ Use expected_data.py for assertions

2. ✅ `tests/unit/test_content_extractor.py` - Critical component test
   - ✅ Replace patching with fixture injection
   - ✅ Use mocks from fixtures/mocks/content_extractor.py
   - ✅ Follow Single Responsibility Principle in test organization

### Priority 2: Integration Tests
1. ✅ `tests/integration/test_etl_pipeline_integration.py`
   - ✅ Fixed challenges with validation patching
   - ✅ Using ImprovedTestableETLPipeline instead of deprecated TestableETLPipeline
   - ✅ Implemented proper patching strategy for validation chain
   - ✅ Using centralized expectations from expected_data.py
   - ✅ Replaced hardcoded test data with fixtures
   - ✅ Implemented proper dependency injection

### Priority 3: Utility Tests
1. ✅ `tests/unit/test_structured_logging.py`
   - ✅ Convert to pytest style
   - ✅ Use appropriate fixtures for context
   - ✅ Mark old file as deprecated

## Migration Checklist for Each File

### `test_skype_parser.py` → `test_skype_parser_pytest.py`
- [x] Run initial migration with migrate_test.py
- [x] Replace unittest assertions with pytest assertions
- [x] Replace patches with fixture injection
- [x] Use typed fixtures from conftest.py
- [x] Use expected_data.py for centralized expectations
- [x] Verify coverage with verify_test_migration.py
- [x] Update MIGRATION_TRACKER.md
- [x] Mark old file as deprecated

### `test_content_extractor.py` → `test_content_extractor_pytest.py`
- [x] Run initial migration with migrate_test.py
- [x] Replace patches with mock implementations from fixtures/mocks
- [x] Implement dependency injection pattern
- [x] Verify coverage with verify_test_migration.py
- [x] Update MIGRATION_TRACKER.md
- [x] Mark old file as deprecated

### `test_etl_pipeline_integration.py` → `test_etl_pipeline_integration_pytest.py`
- [x] Run initial migration with migrate_test.py
- [x] Replace hardcoded expectations with centralized ones
- [x] Use factory patterns for test data
- [x] Create proper mock implementations for validation
- [x] Fix API mismatches in pipeline methods
- [x] Implement proper dependency injection
- [x] Run tests to ensure they pass
- [x] Update MIGRATION_TRACKER.md
- [x] Mark old file as deprecated

### `test_structured_logging.py` → `test_structured_logging_pytest.py`
- [x] Run initial migration with migrate_test.py
- [x] Replace unittest assertions with pytest assertions
- [x] Create proper fixtures for context and logging
- [x] Fix issues with decorated test functions
- [x] Run tests to ensure they pass
- [x] Update MIGRATION_TRACKER.md
- [x] Mark old file as deprecated

## Next Steps

1. ✅ Fix integration test issues:
   - ✅ Using ImprovedTestableETLPipeline instead of TestableETLPipeline
   - ✅ Properly handling the validation chain
   - ✅ Creating appropriate fixtures for test data and dependencies
2. ✅ Migrate remaining test files:
   - ✅ Migrate `tests/unit/test_structured_logging.py`
3. ✅ Fix issues with migrated tests:
   - ✅ Fixed import errors in test_message_type_handlers_pytest.py
   - ✅ Fixed assertion errors in test_message_type_handlers_pytest.py
   - ✅ Fixed fixture issues in conftest.py
4. ✅ Remove deprecated/duplicate files:
   - ✅ Remove `tests/unit/test_etl_pipeline.py`
   - ✅ Remove `tests/unit/test_etl_transformer.py`
   - ✅ Remove `tests/mocks.py` (functionality moved to fixtures/mocks directory)
   - ✅ Remove `tests/factories.py` (functionality moved to fixtures directory)
   - ✅ Move needed code from `tests/helpers.py` to test_helpers.py (Note: helpers.py not found in tests directory)
   - ✅ Remove `scripts/test_centralized_logging.py`
5. ✅ Final verification:
   - ✅ Run full test suite with coverage
   - ✅ Generate final migration report
   - ✅ Update documentation