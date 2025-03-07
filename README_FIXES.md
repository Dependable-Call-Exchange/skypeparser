# TestableETLPipeline Fixes

This directory contains scripts to fix issues in the `TestableETLPipeline` class and related files. These scripts address the following issues:

1. Correcting import paths
2. Renaming service classes
3. Fixing the validation override in TestableETLPipeline
4. Adjusting the DI registration
5. Improving test implementation

## Scripts

- `fix_all.py`: Master script that runs all the fix scripts
- `fix_etl_pipeline.py`: Script to fix common issues across multiple files
- `fix_testable_etl_pipeline.py`: Script to specifically fix the `TestableETLPipeline` class
- `fix_test_example.py`: Script to fix the test file

## Usage

### Running All Fixes

To run all the fixes at once, execute the master script:

```bash
python fix_all.py [options]
```

Available options for the master script:
- `--dry-run`: Show changes without modifying files
- `--verbose`: Show verbose output
- `--dir PATH`: Directory to search for files (default: src and tests directories)
- `--file PATH`: Specific file to fix
- `--subprocess`: Run scripts as subprocesses
- `--skip {etl,testable,test}`: Skip specific scripts

### Running Individual Scripts

To run individual fix scripts with advanced options:

```bash
python fix_etl_pipeline.py [options]
python fix_testable_etl_pipeline.py [options]
python fix_test_example.py [options]
```

Available options for individual scripts vary, but generally include:
- `--dry-run`: Show changes without modifying files
- `--verbose`: Show verbose output
- `--file PATH`: Specific file to fix (for scripts that operate on specific files)
- `--dir PATH`: Directory to search for files (for scripts that search for files)

## What the Scripts Do

### fix_etl_pipeline.py

This script provides the following improvements:

- **AST-based transformations**: Uses LibCST for precise code modifications (if installed)
- **Regex fallback**: Falls back to regex-based replacements if LibCST is not available
- **Surgical edits**: Makes targeted changes instead of replacing entire files
- **Syntax validation**: Validates that modifications result in valid Python code
- **Diff display**: Shows diffs of changes before applying them
- **Recursive file discovery**: Can find and fix Python files throughout the project

Key fixes:
- Corrects import paths (e.g., using `from src.db.etl.pipeline_manager import ETLPipeline` instead of `from src.db.etl import ETLPipeline`)
- Renames service classes (e.g., using `SkypeMessageHandlerFactory` instead of `MessageHandlerFactory`)
- Modifies the validation method in `run_pipeline` to use `patch` for OS path functions

### fix_testable_etl_pipeline.py

This script makes targeted improvements to the `TestableETLPipeline` class:

- **Constructor parameters**: Adds parameters for additional dependencies
- **Constructor initialization**: Updates initialization to properly use injected dependencies
- **Validation override**: Replaces the custom validation with `patch` decorators
- **DI registration**: Improves dependency injection for injected dependencies
- **Transformer initialization**: Updates the transformer initialization in non-DI case

### fix_test_example.py

This script makes targeted improvements to the test file:

- **Import fixes**: Updates imports to use correct paths and adds missing imports
- **setUp/tearDown**: Adds or updates test setup and teardown methods
- **Test file creation**: Ensures test files are created and cleaned up properly
- **Pipeline creation**: Updates the pipeline creation to include all necessary dependencies

## Features

All scripts include the following features:

- **Dry run mode**: Show changes without modifying files
- **Backup creation**: Create backups of files before modifying them
- **Syntax validation**: Validate that modifications result in valid Python code
- **Diff display**: Show diffs of changes before applying them
- **Error handling**: Proper error handling and reporting

## Backup

Each script creates a backup of the original file before making changes. The backup files are saved with a `.bak` extension. If something goes wrong, you can restore the original files from these backups.

## Next Steps

After running the scripts:

1. Run the tests to verify the fixes:
   ```bash
   python -m pytest tests/examples/refactored_test_example.py -vv --log-cli-level=DEBUG
   ```

2. Review the changes and make any necessary adjustments

3. Commit the changes to version control

## Dependencies

- **Required**: Python 3.6+
- **Optional**: LibCST (`pip install libcst`) for AST-based transformations

## Architecture Improvements

These scripts address immediate issues with a surgical approach, but for long-term architectural improvements, consider:

1. **Consistent Dependency Injection**: Refactor the codebase to consistently use dependency injection throughout by:
   - Creating interfaces for all external dependencies
   - Using constructor injection for all dependencies
   - Registering dependencies in a centralized service provider

2. **File System Abstraction**: Create a file system abstraction layer for easier testing:
   ```python
   class FileSystem:
       def exists(self, path: str) -> bool: ...
       def read_file(self, path: str) -> str: ...
       def write_file(self, path: str, content: str) -> None: ...
   ```

3. **Improved Test Fixtures**: Create more comprehensive test fixtures that cover all dependencies:
   ```python
   @pytest.fixture
   def mock_environment():
       return {
           'file_system': MockFileSystem(),
           'content_extractor': MockContentExtractor(),
           'message_handler_factory': MockMessageHandlerFactory(),
           'db_connection': MockDBConnection()
       }
   ```

4. **Simplified TestableETLPipeline**: Simplify the `TestableETLPipeline` class by:
   - Using environment variables or configuration files for test settings
   - Making the test class a thin wrapper around the real implementation
   - Moving test-specific logic to test fixtures

## Troubleshooting

If you encounter issues after running the scripts:

1. Restore the backup files (`.bak` extension)
2. Run the scripts in dry-run mode to see the changes without applying them
3. Try running individual scripts with verbose output for more detailed information
4. Check specific error messages and make manual adjustments
5. If a script fails, you can skip it with the `--skip` option in the master script

## Contributing

If you find bugs or have suggestions for improvements:

1. Create an issue describing the problem or suggestion
2. Submit a pull request with the fix
3. Include tests to verify the fix works as expected