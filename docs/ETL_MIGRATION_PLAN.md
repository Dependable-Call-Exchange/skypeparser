# ETL Migration Plan

This document outlines the plan for migrating from the old `SkypeETLPipeline` to the new modular ETL pipeline.

## Overview

The Skype Parser ETL pipeline has been refactored into a more modular and maintainable architecture. The new architecture separates the ETL process into distinct components:

- **ETLContext**: Manages state and configuration across ETL components
- **Extractor**: Handles file reading and validation
- **Transformer**: Processes raw data into structured format
- **Loader**: Manages database operations
- **ETLPipeline**: Orchestrates the ETL process

This refactoring provides several benefits:

- **Improved maintainability**: Each component has a single responsibility
- **Better testability**: Components can be tested in isolation
- **Enhanced flexibility**: Components can be used independently or together
- **Reduced complexity**: Each component is simpler and more focused
- **Better error handling**: Errors are handled at the appropriate level

## Migration Timeline

The migration will follow this timeline:

1. **Phase 1 (Current)**: Both implementations coexist
   - The old `SkypeETLPipeline` is still available but deprecated
   - The new modular ETL pipeline is available and recommended
   - A compatibility layer redirects calls from the old API to the new one

2. **Phase 2 (Next Release)**: API components updated
   - All API components will be updated to use the new modular ETL pipeline
   - The old `SkypeETLPipeline` will remain available but with stronger deprecation warnings

3. **Phase 3 (Version 2.0.0)**: Old implementation removed
   - The old `SkypeETLPipeline` will be removed
   - Only the new modular ETL pipeline will be available

## Migration Steps for Users

### Step 1: Update Imports

Replace imports of the old ETL pipeline with imports of the new one:

```python
# Old
from src.db.etl_pipeline import SkypeETLPipeline

# New
from src.db import ETLPipeline
```

### Step 2: Update Initialization

Update the initialization of the ETL pipeline:

```python
# Old
pipeline = SkypeETLPipeline(
    db_name='skype_logs',
    db_user='postgres',
    db_password='your_password',
    db_host='localhost',
    db_port=5432,
    output_dir='output'
)

# New
pipeline = ETLPipeline(
    db_config={
        'dbname': 'skype_logs',
        'user': 'postgres',
        'password': 'your_password',
        'host': 'localhost',
        'port': 5432
    },
    output_dir='output'
)
```

### Step 3: Update Method Calls

The new ETL pipeline has a slightly different API:

```python
# Old
results = pipeline.run_pipeline(
    file_path='skype_export.tar',
    is_tar=True,  # No longer needed, detected automatically
    user_display_name='Your Name'
)

# New
results = pipeline.run_pipeline(
    file_path='skype_export.tar',
    user_display_name='Your Name'
)
```

### Step 4: Update Result Handling

The results structure has changed:

```python
# Old
print(f"Processed {results['message_count']} messages in {len(results['conversations'])} conversations")

# New
print(f"Processed {results['phases']['transform']['processed_messages']} messages")
```

## Advanced Usage

For advanced use cases, you can use the individual components directly:

```python
from src.db import ETLContext, Extractor, Transformer, Loader

# Create context
context = ETLContext(
    db_config={
        'dbname': 'skype_logs',
        'user': 'postgres',
        'password': 'your_password',
        'host': 'localhost',
        'port': 5432
    },
    output_dir='output',
    memory_limit_mb=2048,
    parallel_processing=True,
    chunk_size=2000
)

# Create components
extractor = Extractor(context=context)
transformer = Transformer(context=context)
loader = Loader(context=context)

# Extract data
raw_data = extractor.extract(file_path='skype_export.tar')

# Transform data
transformed_data = transformer.transform(raw_data, user_display_name='Your Name')

# Load data
loader.connect_db()
try:
    export_id = loader.load(raw_data, transformed_data, 'skype_export.tar')
    print(f"Data loaded with export ID: {export_id}")
finally:
    loader.close_db()
```

## Migration Assistance

A migration script is available to help identify code that needs to be updated:

```bash
python scripts/migrate_to_modular_etl.py /path/to/your/code --output migration_report.txt
```

This script will scan your code for imports and usage of the old ETL pipeline and generate a report with suggested replacements.

## Compatibility Layer

For backward compatibility, a compatibility layer is provided that redirects calls from the old API to the new one. This layer is implemented in `src.db.etl_pipeline_compat` and is automatically used when importing `SkypeETLPipeline` from `src.db`.

While this compatibility layer helps with the transition, it's recommended to migrate to the new API directly to take full advantage of the new features and improvements.

## Support and Assistance

If you encounter any issues during migration, please:

1. Check the migration guide in `docs/MIGRATION.md`
2. Run the migration script to identify code that needs to be updated
3. Refer to the examples in the `examples/` directory
4. Open an issue on the project's issue tracker

## Conclusion

The migration to the new modular ETL pipeline is an important step in improving the maintainability and flexibility of the Skype Parser. While it requires some changes to existing code, the benefits in terms of maintainability, testability, and flexibility make it worthwhile.

We appreciate your patience and cooperation during this transition period.