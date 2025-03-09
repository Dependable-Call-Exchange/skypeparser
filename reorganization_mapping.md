# File Reorganization Mapping

## Purpose
This document maps duplicated files to their canonical locations as part of our codebase reorganization effort. We'll follow SOLID principles in determining where each module should live.

## Principles Applied
- **Single Responsibility Principle (SRP)**: Each module should have one reason to change
- **Open/Closed Principle (OCP)**: Entities should be open for extension but closed for modification
- **Liskov Substitution Principle (LSP)**: Objects should be replaceable with instances of their subtypes
- **Interface Segregation Principle (ISP)**: Many specific interfaces are better than one general-purpose interface
- **Dependency Inversion Principle (DIP)**: Depend on abstractions, not concretions

## Canonical File Locations

### Database Connection Files
| Duplicate File | Canonical Location | Status |
|----------------|-------------------|--------|
| src/core_utils/connection.py | src/db/connection.py | ⬆️ To be kept |
| src/core_utils/connection_factory.py | src/db/connection_factory.py | ⬆️ To be kept |
| src/core_utils/connection_pool.py | src/db/connection_pool.py | ⬆️ To be kept |

### Data Handlers
| Duplicate File | Canonical Location | Status |
|----------------|-------------------|--------|
| src/db/handlers/archive_handler.py | src/data_handlers/archive_handler.py | ⬆️ To be kept |
| src/db/handlers/base_handler.py | src/data_handlers/base_handler.py | ⬆️ To be kept |
| src/db/handlers/conversation_handler.py | src/data_handlers/conversation_handler.py | ⬆️ To be kept |
| src/db/handlers/message_handler.py | src/data_handlers/message_handler.py | ⬆️ To be kept |
| src/db/handlers/user_handler.py | src/data_handlers/user_handler.py | ⬆️ To be kept |
| src/utils/attachment_handler.py | src/data_handlers/attachment_handler.py | ⬆️ To be kept |
| src/utils/file_handler.py | src/data_handlers/file_handler.py | ⬆️ To be kept |

### Message Processing
| Duplicate File | Canonical Location | Status |
|----------------|-------------------|--------|
| src/utils/message_processor.py | src/messages/message_processor.py | ⬆️ To be kept |
| src/utils/message_type_handlers.py | src/messages/message_type_handlers.py | ⬆️ To be kept |
| src/utils/message_type_extractor.py | src/messages/message_type_extractor.py | ⬆️ To be kept |

### Validation
| Duplicate File | Canonical Location | Status |
|----------------|-------------------|--------|
| src/utils/validation.py | src/validation/validation.py | ⬆️ To be kept |
| src/utils/data_validator.py | src/validation/data_validator.py | ⬆️ To be kept |
| src/utils/configuration_validator.py | src/validation/configuration_validator.py | ⬆️ To be kept |
| src/utils/etl_validation.py | src/validation/etl_validation.py | ⬆️ To be kept |
| src/utils/schema_validation.py | src/validation/schema_validation.py | ⬆️ To be kept |

### Monitoring
| Duplicate File | Canonical Location | Status |
|----------------|-------------------|--------|
| src/utils/progress_tracker.py | src/monitoring/progress_tracker.py | ⬆️ To be kept |
| src/db/progress_tracker.py | src/monitoring/progress_tracker.py | ⬆️ To be kept |
| src/utils/checkpoint_manager.py | src/monitoring/checkpoint_manager.py | ⬆️ To be kept |
| src/utils/memory_monitor.py | src/monitoring/memory_monitor.py | ⬆️ To be kept |
| src/utils/phase_manager.py | src/monitoring/phase_manager.py | ⬆️ To be kept |

### Logging and Error Handling
| Duplicate File | Canonical Location | Status |
|----------------|-------------------|--------|
| src/utils/logging_config.py | src/logging/logging_config.py | ⬆️ To be kept |
| src/utils/structured_logging.py | src/logging/structured_logging.py | ⬆️ To be kept |
| src/utils/new_structured_logging.py | src/logging/new_structured_logging.py | ⬆️ To be kept |
| src/utils/logging_compat.py | src/logging/logging_compat.py | ⬆️ To be kept |
| src/utils/error_handling.py | src/logging/error_handling.py | ⬆️ To be kept |
| src/utils/error_logger.py | src/logging/error_logger.py | ⬆️ To be kept |

### File-Related Utils
| Duplicate File | Canonical Location | Status |
|----------------|-------------------|--------|
| src/utils/file_utils.py | src/files/file_utils.py | ⬆️ To be kept |
| src/utils/tar_extractor.py | src/files/tar_extractor.py | ⬆️ To be kept |
| src/parser/file_output.py | src/files/file_output.py | ⬆️ To be kept |

### Parsing
| Duplicate File | Canonical Location | Status |
|----------------|-------------------|--------|
| src/core_utils/core_parser.py | src/parser/core_parser.py | ⬆️ To be kept |
| src/core_utils/content_extractor.py | src/parser/content_extractor.py | ⬆️ To be kept |
| src/core_utils/parser_module.py | src/parser/parser_module.py | ⬆️ To be kept |
| src/core_utils/skype_parser.py | src/parser/skype_parser.py | ⬆️ To be kept |
| src/core_utils/structured_data_extractor.py | src/parser/structured_data_extractor.py | ⬆️ To be kept |

### ETL Pipeline
| Duplicate File | Canonical Location | Status |
|----------------|-------------------|--------|
| src/core_utils/etl_pipeline.py | src/db/etl/etl_pipeline.py | ⬆️ To be kept |
| src/core_utils/etl_pipeline_compat.py | src/db/etl/etl_pipeline_compat.py | ⬆️ To be kept |
| src/core_utils/loader.py | src/db/etl/loader.py | ⬆️ To be kept |
| src/core_utils/transformer.py | src/db/etl/transformer.py | ⬆️ To be kept |
| src/core_utils/extractor.py | src/db/etl/extractor.py | ⬆️ To be kept |
| src/core_utils/modular_pipeline.py | src/db/etl/modular_pipeline.py | ⬆️ To be kept |
| src/core_utils/pipeline_factory.py | src/db/etl/pipeline_factory.py | ⬆️ To be kept |
| src/core_utils/pipeline_manager.py | src/db/etl/pipeline_manager.py | ⬆️ To be kept |
| src/core_utils/streaming_processor.py | src/db/etl/streaming_processor.py | ⬆️ To be kept |
| src/core_utils/context.py | src/db/etl/context.py | ⬆️ To be kept |
| src/core_utils/testable_etl_pipeline.py | src/db/etl/testable_etl_pipeline.py | ⬆️ To be kept |

### API
| Duplicate File | Canonical Location | Status |
|----------------|-------------------|--------|
| src/core_utils/run_api.py | src/api/run_api.py | ⬆️ To be kept |
| src/core_utils/tasks.py | src/api/tasks.py | ⬆️ To be kept |
| src/core_utils/skype_api.py | src/api/skype_api.py | ⬆️ To be kept |
| src/core_utils/user_management.py | src/api/user_management.py | ⬆️ To be kept |

### Analysis
| Duplicate File | Canonical Location | Status |
|----------------|-------------------|--------|
| src/core_utils/queries.py | src/analysis/queries.py | ⬆️ To be kept |
| src/core_utils/reporting.py | src/analysis/reporting.py | ⬆️ To be kept |
| src/core_utils/visualization.py | src/analysis/visualization.py | ⬆️ To be kept |

### Core Utilities (To be kept in core_utils)
| File | Reason for Keeping in core_utils | Status |
|------|----------------------------------|--------|
| src/core_utils/config.py | Core configuration used across multiple domains | ✅ |
| src/core_utils/dependencies.py | Core dependency management | ✅ |
| src/core_utils/di.py | Dependency injection framework | ✅ |
| src/core_utils/di_example.py | Examples for DI usage | ✅ |
| src/core_utils/exceptions.py | Core exception types | ✅ |
| src/core_utils/extractors.py | Generic extraction interfaces | ✅ |
| src/core_utils/interfaces.py | Core interfaces for the application | ✅ |
| src/core_utils/models.py | Core data models | ✅ |
| src/core_utils/serialization.py | General serialization utilities | ✅ |
| src/core_utils/service_registry.py | Service registry for DI | ✅ |
| src/core_utils/test_utils.py | Testing utilities | ✅ |
| src/core_utils/utils.py | Genuinely generic utilities | ✅ |

## Implementation Strategy
1. Create symbolic links for duplicated files to maintain backward compatibility
2. Update imports in affected files
3. Add deprecation warnings to duplicated files
4. Document the canonical locations in relevant READMEs
5. Update the project structure documentation