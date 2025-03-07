# Transition Plan from Deprecated Files

This document outlines the plan for transitioning from deprecated files to their modern replacements in the SkypeParser project. This plan has been updated to reflect the completion of all stages of the SkypeParser Improvement Plan.

## Current State

The codebase has undergone significant architectural improvements beyond the original transition plan:

1. **Modular ETL Pipeline**: Implemented a fully modular ETL pipeline with clean separation of concerns
2. **Factory Pattern**: Added pipeline factory for better dependency management
3. **Enhanced Error Handling**: Implemented standardized error handling and reporting
4. **Structured Logging**: Added structured logging with context and performance tracking
5. **Validation System**: Created schema validation for configuration and inputs
6. **Connection Pooling**: Implemented database connection pooling for better performance

## Deprecated Files and Their Replacements

| Deprecated File | Replacement | Status |
|-----------------|-------------|--------|
| `src/parser/parser_module.py` | `src/parser/core_parser.py` | Ready for removal after v2.0.0 |
| `src/db/etl_pipeline.py` | `src/db/etl/modular_pipeline.py` & `src/db/etl/pipeline_manager.py` | Ready for removal after v2.0.0 |
| `src/db/skype_to_postgres.py` | `scripts/run_etl_pipeline_enhanced.py` or `scripts/run_modular_etl.py` | Ready for removal after v2.0.0 |
| `src/db/store_skype_export.py` | `scripts/run_etl_pipeline_enhanced.py` or `scripts/run_modular_etl.py` | Ready for removal after v2.0.0 |

## Transition Tools

To help with the transition, we've created the following tools:

1. **Import Migration Script**: `scripts/migrate_from_deprecated.py`
   - Scans Python files for imports of deprecated modules and suggests replacements
   - Can automatically apply the suggested changes with the `--apply` flag

2. **CLI Command Migration Script**: `scripts/migrate_cli_commands.py`
   - Helps migrate command-line commands from deprecated scripts to the new unified CLI
   - Provides a comparison of old and new commands

3. **Enhanced ETL Pipeline Script**: `scripts/run_etl_pipeline_enhanced.py`
   - Provides a unified command-line interface that replaces the functionality of the deprecated scripts
   - Supports all the features of the deprecated scripts with improved options

4. **Modular ETL Migration Script**: `scripts/migrate_to_modular_etl.py`
   - Helps migrate code using `SkypeETLPipeline` to the new modular architecture
   - Scans Python files for usage of the old ETL pipeline and suggests replacements

5. **Modular ETL Pipeline Script**: `scripts/run_modular_etl.py`
   - Provides a command-line interface for the new modular ETL pipeline
   - Offers improved configuration options and better error handling

## Transition Steps

### 1. Update Imports

Use the `migrate_from_deprecated.py` script to update imports in your code:

```bash
# Scan all Python files in the current directory
python scripts/migrate_from_deprecated.py

# Scan a specific directory
python scripts/migrate_from_deprecated.py --path src/your_module

# Apply suggested changes
python scripts/migrate_from_deprecated.py --apply
```

### 2. Update CLI Commands

Use the `migrate_cli_commands.py` script to update command-line commands:

```bash
# Migrate a command
python scripts/migrate_cli_commands.py "python src/db/skype_to_postgres.py -f export.tar -u 'John Doe' -d skype_db"
```

### 3. Update Code That Uses Deprecated Modules

After updating imports, you'll need to update code that uses the deprecated modules. Here are the key changes:

#### From `parser_module.py` to `core_parser.py`

- `timestamp_parser` → `timestamp_parser` (same name, improved implementation)
- `content_parser` → `content_parser` (same name, improved implementation)
- `tag_stripper` → `enhanced_tag_stripper` (renamed)
- `pretty_quotes` → `pretty_quotes` (same name, improved implementation)
- `read_file` and `read_tarfile` → Import from `src.utils.file_handler` directly

#### From `etl_pipeline.py` to Modular Architecture

There are two migration paths available:

**Option 1: Enhanced Pipeline (intermediate step)**
```python
from src.db.etl import ETLPipeline
from src.db.etl import ETLContext

context = ETLContext(
    db_config={"dbname": "skype_db", "user": "postgres"},
    parallel_processing=True
)
pipeline = ETLPipeline(context=context)
result = pipeline.run_pipeline(
    file_path="export.tar",
    user_display_name="John Doe"
)
```

**Option 2: Modular Pipeline (recommended)**
```python
from src.db.etl.modular_pipeline import ModularETLPipeline
from src.db.etl.pipeline_factory import PipelineFactory

# Create pipeline using factory
factory = PipelineFactory()
pipeline = factory.create_pipeline(
    db_config={"dbname": "skype_db", "user": "postgres"},
    parallel_processing=True
)

# Run pipeline
result = pipeline.process(
    file_path="export.tar",
    user_display_name="John Doe"
)
```

#### From Command-Line Scripts to Unified CLI

**Old Command**:
```bash
python src/db/skype_to_postgres.py -f export.tar -u "John Doe" -d skype_db
```

**New Command (Enhanced Pipeline)**:
```bash
python scripts/run_etl_pipeline_enhanced.py -f export.tar -u "John Doe" -d skype_db
```

**New Command (Modular Pipeline)**:
```bash
python scripts/run_modular_etl.py -f export.tar -u "John Doe" -d skype_db
```

### 4. Run Tests

After updating your code, run the tests to ensure everything works correctly:

```bash
python scripts/run_tests.py
```

Our improved testing infrastructure now includes:
- Specialized test fixtures for different components
- Test factory patterns for easier test data generation
- Improved mock objects for better test isolation

### 5. Remove Deprecated Files

The deprecated files will be removed in version 2.0.0. Until then, they will continue to work with deprecation warnings.

## Compatibility Layer

To ease the transition, we've provided compatibility layers:

- `src/db/etl_pipeline_compat.py`: Implements the old `SkypeETLPipeline` interface using the new architecture
- Deprecation warnings in all deprecated files to encourage migration

The compatibility layer will be maintained until version 2.0.0, at which point it will be removed along with the deprecated files.

## Timeline

- **Current Status**: All stages of the SkypeParser Improvement Plan are now complete
- **Current Version**: Deprecated files are marked with warnings but still functional
- **Next Minor Version**: Deprecated files will emit more prominent warnings
- **Version 2.0.0**: Deprecated files and compatibility layers will be removed

## Recommended Migration Strategy

For optimal results, we recommend:

1. Use the migration tools to identify and update imports and CLI commands
2. For new code, adopt the modular ETL pipeline architecture directly
3. For existing code, consider migrating directly to the modular pipeline rather than the enhanced pipeline
4. Take advantage of the new error handling, logging, and validation systems

## Need Help?

If you encounter any issues during the transition, please:

1. Check the documentation in the `docs/implementation` directory
2. Run the migration scripts with the `--help` flag for more information
3. Explore the examples in the `src/examples` directory
4. Open an issue on the project's issue tracker

## References

- [SkypeParser Improvement Plan](../SkypeParser_Improvement_Plan.md)
- [ETL Migration Decisions](ETL_MIGRATION_DECISIONS.md)
- [Migration Guide](MIGRATION.md)
