# Transition Plan from Deprecated Files

This document outlines the plan for transitioning from deprecated files to their replacements in the SkypeParser project.

## Deprecated Files and Their Replacements

| Deprecated File | Replacement | Status |
|-----------------|-------------|--------|
| `src/parser/parser_module.py` | `src/parser/core_parser.py` | Ready for transition |
| `src/db/etl_pipeline.py` | `src/db/etl/pipeline_manager.py` | Ready for transition |
| `src/db/skype_to_postgres.py` | `src/db/etl/pipeline_manager.py` | Ready for transition |
| `src/db/store_skype_export.py` | `src/db/etl/pipeline_manager.py` | Ready for transition |

## Transition Tools

To help with the transition, we've created the following tools:

1. **Import Migration Script**: `scripts/migrate_from_deprecated.py`
   - Scans Python files for imports of deprecated modules and suggests replacements
   - Can automatically apply the suggested changes with the `--apply` flag

2. **CLI Command Migration Script**: `scripts/migrate_cli_commands.py`
   - Helps migrate command-line commands from deprecated scripts to the new `run_etl_pipeline_enhanced.py` script
   - Provides a comparison of old and new commands

3. **Enhanced ETL Pipeline Script**: `scripts/run_etl_pipeline_enhanced.py`
   - Provides a unified command-line interface that replaces the functionality of the deprecated scripts
   - Supports all the features of the deprecated scripts with improved options

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

#### From `etl_pipeline.py` to `etl/pipeline_manager.py`

- `SkypeETLPipeline` → `ETLPipeline`
- The new `ETLPipeline` uses a context object for configuration
- The new `ETLPipeline` has a more modular design with separate extractor, transformer, and loader components

#### From `skype_to_postgres.py` and `store_skype_export.py` to `etl/pipeline_manager.py`

- Use the `run_etl_pipeline_enhanced.py` script instead of these scripts
- The new script provides a unified interface with all the features of the deprecated scripts

### 4. Run Tests

After updating your code, run the tests to ensure everything works correctly:

```bash
python scripts/run_tests.py
```

### 5. Remove Deprecated Files

Once all code has been migrated and tests pass, the deprecated files can be removed:

```bash
rm src/parser/parser_module.py
rm src/db/etl_pipeline.py
rm src/db/skype_to_postgres.py
rm src/db/store_skype_export.py
```

## Compatibility Layer

To ease the transition, we've provided a compatibility layer in `src/db/etl_pipeline_compat.py` that redirects calls from the old API to the new one. This layer is automatically used when importing `SkypeETLPipeline` from `src.db`.

The compatibility layer will be maintained until version 2.0.0, at which point it will be removed along with the deprecated files.

## Timeline

- **Current Version**: Deprecated files are marked with warnings but still functional
- **Next Minor Version**: Deprecated files will emit more prominent warnings
- **Version 2.0.0**: Deprecated files and compatibility layer will be removed

## Need Help?

If you encounter any issues during the transition, please:

1. Check the documentation in the `docs/implementation` directory
2. Run the migration scripts with the `--help` flag for more information
3. Open an issue on the project's issue tracker

## References

- [ETL Migration Plan](ETL_MIGRATION_PLAN.md)
- [ETL Migration Decisions](ETL_MIGRATION_DECISIONS.md)
- [Migration Guide](MIGRATION.md)