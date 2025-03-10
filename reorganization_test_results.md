# Codebase Reorganization Test Results

## Summary

- Canonical imports: 39/39 successful
- Deprecated imports: 27/27 successful
- All modules: 169/169 successful

## Canonical Import Tests

| Module | Result |
|--------|--------|
| src.db.connection | ✅ Success |
| src.db.connection_factory | ✅ Success |
| src.db.connection_pool | ✅ Success |
| src.data_handlers.archive_handler | ✅ Success |
| src.data_handlers.attachment_handler | ✅ Success |
| src.data_handlers.conversation_handler | ✅ Success |
| src.data_handlers.message_handler | ✅ Success |
| src.data_handlers.file_handler | ✅ Success |
| src.data_handlers.user_handler | ✅ Success |
| src.messages.message_processor | ✅ Success |
| src.messages.message_type_handlers | ✅ Success |
| src.messages.message_type_extractor | ✅ Success |
| src.validation.validation | ✅ Success |
| src.validation.data_validator | ✅ Success |
| src.validation.configuration_validator | ✅ Success |
| src.validation.etl_validation | ✅ Success |
| src.validation.schema_validation | ✅ Success |
| src.monitoring.progress_tracker | ✅ Success |
| src.monitoring.checkpoint_manager | ✅ Success |
| src.monitoring.memory_monitor | ✅ Success |
| src.monitoring.phase_manager | ✅ Success |
| src.logging.logging_config | ✅ Success |
| src.logging.structured_logging | ✅ Success |
| src.logging.new_structured_logging | ✅ Success |
| src.logging.error_handling | ✅ Success |
| src.files.file_utils | ✅ Success |
| src.files.tar_extractor | ✅ Success |
| src.files.file_output | ✅ Success |
| src.parser.core_parser | ✅ Success |
| src.parser.content_extractor | ✅ Success |
| src.parser.parser_module | ✅ Success |
| src.db.etl.etl_pipeline | ✅ Success |
| src.db.etl.loader | ✅ Success |
| src.db.etl.transformer | ✅ Success |
| src.db.etl.extractor | ✅ Success |
| src.core_utils.config | ✅ Success |
| src.core_utils.dependencies | ✅ Success |
| src.core_utils.di | ✅ Success |
| src.core_utils.interfaces | ✅ Success |

## Deprecated Import Tests

| Module | Result |
|--------|--------|
| src.core_utils.connection | ✅ Success (Deprecation warning shown) |
| src.core_utils.connection_factory | ✅ Success (Deprecation warning shown) |
| src.core_utils.connection_pool | ✅ Success (Deprecation warning shown) |
| src.db.handlers.archive_handler | ✅ Success (Deprecation warning shown) |
| src.db.handlers.conversation_handler | ✅ Success (Deprecation warning shown) |
| src.db.handlers.message_handler | ✅ Success (Deprecation warning shown) |
| src.utils.attachment_handler | ✅ Success (Deprecation warning shown) |
| src.utils.file_handler | ✅ Success (Deprecation warning shown) |
| src.utils.message_processor | ✅ Success (Deprecation warning shown) |
| src.utils.message_type_handlers | ✅ Success (Deprecation warning shown) |
| src.utils.message_type_extractor | ✅ Success (Deprecation warning shown) |
| src.utils.validation | ✅ Success (Deprecation warning shown) |
| src.utils.data_validator | ✅ Success (Deprecation warning shown) |
| src.utils.etl_validation | ✅ Success (Deprecation warning shown) |
| src.utils.progress_tracker | ✅ Success (Deprecation warning shown) |
| src.utils.checkpoint_manager | ✅ Success (Deprecation warning shown) |
| src.utils.memory_monitor | ✅ Success (Deprecation warning shown) |
| src.utils.logging_config | ✅ Success (Deprecation warning shown) |
| src.utils.structured_logging | ✅ Success (Deprecation warning shown) |
| src.utils.error_handling | ✅ Success (Deprecation warning shown) |
| src.utils.file_utils | ✅ Success (Deprecation warning shown) |
| src.utils.tar_extractor | ✅ Success (Deprecation warning shown) |
| src.core_utils.core_parser | ✅ Success (Deprecation warning shown) |
| src.core_utils.content_extractor | ✅ Success (Deprecation warning shown) |
| src.core_utils.etl_pipeline | ✅ Success (Deprecation warning shown) |
| src.core_utils.loader | ✅ Success (Deprecation warning shown) |
| src.core_utils.transformer | ✅ Success (Deprecation warning shown) |

## All Modules Test

| Module | Result |
|--------|--------|
| src.messages.message_processor | ✅ Success |
| src.messages.message_type_extractor | ✅ Success |
| src.messages.message_type_handlers | ✅ Success |
| src.analysis.visualization | ✅ Success |
| src.analysis.queries | ✅ Success |
| src.analysis.reporting | ✅ Success |
| src.utils.interfaces | ✅ Success |
| src.utils.test_utils | ✅ Success |
| src.utils.structured_logging | ✅ Success |
| src.utils.file_handler | ✅ Success |
| src.utils.config | ✅ Success |
| src.utils.conversation_processor | ✅ Success |
| src.utils.logging_config | ✅ Success |
| src.utils.logging_compat | ✅ Success |
| src.utils.progress_tracker | ✅ Success |
| src.utils.extractors | ✅ Success |
| src.utils.data_validator | ✅ Success |
| src.utils.attachment_handler | ✅ Success |
| src.utils.tar_extractor | ✅ Success |
| src.utils.di | ✅ Success |
| src.utils.initialize_error_handling | ✅ Success |
| src.utils.error_logger | ✅ Success |
| src.utils.new_structured_logging | ✅ Success |
| src.utils.service_registry | ✅ Success |
| src.utils.etl_validation | ✅ Success |
| src.utils.error_handling | ✅ Success |
| src.utils.structured_data_extractor | ✅ Success |
| src.utils.message_processor | ✅ Success |
| src.utils.file_utils | ✅ Success |
| src.utils.db_connection | ✅ Success |
| src.utils.configuration_validator | ✅ Success |
| src.utils.message_type_extractor | ✅ Success |
| src.utils.memory_monitor | ✅ Success |
| src.utils.message_type_handlers | ✅ Success |
| src.utils.checkpoint_manager | ✅ Success |
| src.utils.serialization | ✅ Success |
| src.utils.dependencies | ✅ Success |
| src.utils.phase_manager | ✅ Success |
| src.utils.transformer_builder | ✅ Success |
| src.utils.schema_validation | ✅ Success |
| src.utils.validation | ✅ Success |
| src.parser.content_extractor | ✅ Success |
| src.parser.file_output | ✅ Success |
| src.parser.skype_parser | ✅ Success |
| src.parser.exceptions | ✅ Success |
| src.parser.parser_module | ✅ Success |
| src.parser.core_parser | ✅ Success |
| src.conversations.conversation_processor | ✅ Success |
| src.examples.di_example | ✅ Success |
| src.files.file_output | ✅ Success |
| src.files.tar_extractor | ✅ Success |
| src.files.file_utils | ✅ Success |
| src.db.skype_to_postgres | ✅ Success |
| src.db.data_inserter | ✅ Success |
| src.db.progress_tracker | ✅ Success |
| src.db.connection_factory | ✅ Success |
| src.db.database_manager | ✅ Success |
| src.db.etl_pipeline_compat | ✅ Success |
| src.db.connection | ✅ Success |
| src.db.etl_pipeline | ✅ Success |
| src.db.transaction_manager | ✅ Success |
| src.db.schema_manager | ✅ Success |
| src.db.database_factory | ✅ Success |
| src.db.testable_etl_pipeline | ✅ Success |
| src.db.store_skype_export | ✅ Success |
| src.db.connection_pool | ✅ Success |
| src.db.strategies.individual_insertion | ✅ Success |
| src.db.strategies.strategy_factory | ✅ Success |
| src.db.strategies.bulk_insertion | ✅ Success |
| src.db.strategies.insertion_strategy | ✅ Success |
| src.db.etl.streaming_processor | ✅ Success |
| src.db.etl.pipeline_factory | ✅ Success |
| src.db.etl.extractor | ✅ Success |
| src.db.etl.modular_pipeline | ✅ Success |
| src.db.etl.context | ✅ Success |
| src.db.etl.utils | ✅ Success |
| src.db.etl.loader | ✅ Success |
| src.db.etl.transformer | ✅ Success |
| src.db.etl.pipeline_manager | ✅ Success |
| src.db.raw_storage.models | ✅ Success |
| src.db.raw_storage.storage | ✅ Success |
| src.db.handlers.message_handler | ✅ Success |
| src.db.handlers.conversation_handler | ✅ Success |
| src.db.handlers.user_handler | ✅ Success |
| src.db.handlers.archive_handler | ✅ Success |
| src.db.handlers.handler_registry | ✅ Success |
| src.db.handlers.base_handler | ✅ Success |
| src.db.clean_storage.models | ✅ Success |
| src.db.clean_storage.storage | ✅ Success |
| src.api.tasks | ✅ Success |
| src.api.run_api | ✅ Success |
| src.api.skype_api | ✅ Success |
| src.api.user_management | ✅ Success |
| src.monitoring.progress_tracker | ✅ Success |
| src.monitoring.memory_monitor | ✅ Success |
| src.monitoring.checkpoint_manager | ✅ Success |
| src.monitoring.phase_manager | ✅ Success |
| src.core_utils.individual_insertion | ✅ Success |
| src.core_utils.interfaces | ✅ Success |
| src.core_utils.content_extractor | ✅ Success |
| src.core_utils.skype_to_postgres | ✅ Success |
| src.core_utils.test_utils | ✅ Success |
| src.core_utils.tasks | ✅ Success |
| src.core_utils.config | ✅ Success |
| src.core_utils.models | ✅ Success |
| src.core_utils.extractors | ✅ Success |
| src.core_utils.connection_factory | ✅ Success |
| src.core_utils.run_api | ✅ Success |
| src.core_utils.streaming_processor | ✅ Success |
| src.core_utils.skype_api | ✅ Success |
| src.core_utils.database_manager | ✅ Success |
| src.core_utils.strategy_factory | ✅ Success |
| src.core_utils.skype_parser | ✅ Success |
| src.core_utils.di | ✅ Success |
| src.core_utils.bulk_insertion | ✅ Success |
| src.core_utils.pipeline_factory | ✅ Success |
| src.core_utils.etl_pipeline_compat | ✅ Success |
| src.core_utils.initialize_error_handling | ✅ Success |
| src.core_utils.visualization | ✅ Success |
| src.core_utils.extractor | ✅ Success |
| src.core_utils.service_registry | ✅ Success |
| src.core_utils.modular_pipeline | ✅ Success |
| src.core_utils.connection | ✅ Success |
| src.core_utils.structured_data_extractor | ✅ Success |
| src.core_utils.context | ✅ Success |
| src.core_utils.utils | ✅ Success |
| src.core_utils.loader | ✅ Success |
| src.core_utils.transformer | ✅ Success |
| src.core_utils.storage | ✅ Success |
| src.core_utils.di_example | ✅ Success |
| src.core_utils.insertion_strategy | ✅ Success |
| src.core_utils.queries | ✅ Success |
| src.core_utils.db_connection | ✅ Success |
| src.core_utils.exceptions | ✅ Success |
| src.core_utils.etl_pipeline | ✅ Success |
| src.core_utils.transaction_manager | ✅ Success |
| src.core_utils.schema_manager | ✅ Success |
| src.core_utils.user_management | ✅ Success |
| src.core_utils.database_factory | ✅ Success |
| src.core_utils.parser_module | ✅ Success |
| src.core_utils.core_parser | ✅ Success |
| src.core_utils.testable_etl_pipeline | ✅ Success |
| src.core_utils.store_skype_export | ✅ Success |
| src.core_utils.serialization | ✅ Success |
| src.core_utils.dependencies | ✅ Success |
| src.core_utils.transformer_builder | ✅ Success |
| src.core_utils.pipeline_manager | ✅ Success |
| src.core_utils.reporting | ✅ Success |
| src.core_utils.connection_pool | ✅ Success |
| src.data_handlers.message_handler | ✅ Success |
| src.data_handlers.conversation_handler | ✅ Success |
| src.data_handlers.data_inserter | ✅ Success |
| src.data_handlers.file_handler | ✅ Success |
| src.data_handlers.attachment_handler | ✅ Success |
| src.data_handlers.user_handler | ✅ Success |
| src.data_handlers.archive_handler | ✅ Success |
| src.data_handlers.handler_registry | ✅ Success |
| src.data_handlers.base_handler | ✅ Success |
| src.logging.structured_logging | ✅ Success |
| src.logging.logging_config | ✅ Success |
| src.logging.logging_compat | ✅ Success |
| src.logging.error_logger | ✅ Success |
| src.logging.new_structured_logging | ✅ Success |
| src.logging.error_handling | ✅ Success |
| src.validation.data_validator | ✅ Success |
| src.validation.etl_validation | ✅ Success |
| src.validation.configuration_validator | ✅ Success |
| src.validation.schema_validation | ✅ Success |
| src.validation.validation | ✅ Success |
