# Test Fixes and Best Practices

This document outlines the improvements made to the test suite and provides best practices for writing tests in the SkypeParser project.

## Recent Test Fixes

### Path Validation Issues

One of the main issues encountered in the test suite was related to path validation. The validation functions in the project are designed to prevent path traversal attacks by rejecting absolute paths. However, this created challenges for testing, as many tests needed to use absolute paths to properly test functionality.

The following fixes were implemented:

1. **Patching Validation Functions**: Created a `patch_validation` decorator in `tests/test_helpers.py` that patches the `validate_path_safety` function to allow absolute paths during testing.

2. **Using Relative Paths**: Modified tests to use relative paths where possible, particularly by creating test files within a dedicated test directory.

3. **Temporary Directories**: Implemented the use of temporary directories for test files to avoid path validation issues and ensure proper cleanup after tests.

### Test Data Management

To improve test data management and avoid issues with file creation and cleanup:

1. **Standardized Test Data Creation**: Created helper functions in `tests/test_helpers.py` for creating test files, JSON files, and TAR files.

2. **Proper Teardown**: Ensured all tests properly clean up after themselves by implementing tearDown methods that remove temporary files and directories.

3. **Test Base Class**: Created a `TestBase` class that provides common setup and teardown methods for all tests.

### Import Path Issues in Modular Components

When testing modular components, we encountered issues with import paths:

1. **Relative vs. Absolute Imports**: The ETL module was using relative imports (`..utils`) that assumed a `utils` package in the `src.db` directory, but the utilities were actually in the `src/utils` directory.

2. **Import Path Fixes**: Changed relative imports to absolute imports in the ETL module components:
   - `src/db/etl/extractor.py`
   - `src/db/etl/transformer.py`
   - `src/db/etl/loader.py`

### Test-Implementation Mismatch

When implementing new components like `ETLContext`, we encountered issues where the tests were written based on an expected implementation that didn't match the actual implementation:

1. **Test Updates**: Updated tests to match the actual implementation:
   - Fixed attribute names and structure
   - Updated method signatures and parameters
   - Adjusted assertions to match the actual behavior

2. **Missing Dependencies**: Installed required dependencies like `psutil` for memory monitoring

## Best Practices for Writing Tests

### 1. Use the Test Helper Module

The `tests/test_helpers.py` module provides several utilities to make testing easier:

```python
from tests.test_helpers import TestBase, patch_validation, create_test_file

class TestMyModule(TestBase):
    @patch_validation
    def test_my_function(self, mock_validate):
        # Test code here
        pass
```

### 2. Avoid Hardcoded Absolute Paths

Instead of hardcoding absolute paths, use the helper functions to create test files:

```python
# Instead of this:
test_file = "/absolute/path/to/file.txt"

# Do this:
test_file = create_test_file(self.test_dir, "file.txt", "content")
```

### 3. Mock External Dependencies

Use unittest.mock to mock external dependencies:

```python
@patch('module.function')
def test_with_mock(self, mock_function):
    mock_function.return_value = "mocked result"
    # Test code here
```

### 4. Test for Exceptions

When testing functions that should raise exceptions, use `assertRaises`:

```python
with self.assertRaises(ValueError):
    function_that_should_raise()
```

### 5. Test Edge Cases

Make sure to test edge cases, such as:
- Empty inputs
- Invalid inputs
- Boundary conditions
- Error conditions

### 6. Keep Tests Independent

Each test should be independent of others. Don't rely on state from previous tests.

### 7. Use Descriptive Test Names

Test names should describe what they're testing:

```python
def test_extract_tar_with_valid_file_should_extract_contents(self):
    # Test code here
```

### 8. Use Consistent Import Strategies

When organizing a Python project, it's important to have a clear and consistent import strategy:

- **Prefer Absolute Imports**: Absolute imports are often more reliable than relative imports, especially in larger projects.
- **Document Import Conventions**: Make sure the team follows consistent import conventions.

### 9. Test-Driven Development Best Practices

When using TDD, it's important to update tests as the implementation evolves:

- **Update Tests with Implementation**: Tests should reflect the actual behavior of the code.
- **Refactor Tests After Implementation**: After implementing a feature, refactor the tests to match the actual implementation.
- **Test One Thing at a Time**: Each test should focus on testing one specific aspect of the code.

### 10. Use the Right Testing Framework

Different testing frameworks (unittest vs. pytest) have different features:

- **unittest**: Standard library testing framework
- **pytest**: More powerful testing framework with additional features
- **Use Appropriate Flags**: Use the appropriate flags for the chosen framework:
  - unittest: `-v` for verbose output
  - pytest: `-vv --log-cli-level=DEBUG` for detailed output and logging

## Migrating Existing Tests

When migrating existing tests to use the new helpers:

1. Inherit from `TestBase` instead of `unittest.TestCase`
2. Use the `patch_validation` decorator for tests that involve path validation
3. Replace file creation code with calls to the helper functions
4. Update teardown code to use the base class teardown

Example:

```python
# Before
class TestModule(unittest.TestCase):
    def setUp(self):
        self.test_file = "/tmp/test.txt"
        with open(self.test_file, "w") as f:
            f.write("test content")

    def tearDown(self):
        os.remove(self.test_file)

    def test_function(self):
        result = module.function(self.test_file)
        self.assertEqual(result, "expected")

# After
class TestModule(TestBase):
    @patch_validation
    def test_function(self, mock_validate):
        test_file = create_test_file(self.test_dir, "test.txt", "test content")
        result = module.function(test_file)
        self.assertEqual(result, "expected")
```

## Testing Modular Components

When testing modular components like the ETL pipeline:

1. **Test Each Component Individually**: Write unit tests for each component (Extractor, Transformer, Loader, Context).
2. **Test Component Interactions**: Write integration tests for how components interact.
3. **Use Shared Context**: Test how components share state through the `ETLContext`.
4. **Mock Dependencies**: Mock external dependencies like database connections.

Example of testing with `ETLContext`:

```python
def test_etl_with_context(self):
    # Create a context with test configuration
    context = ETLContext(
        db_config={'dbname': 'test_db', 'user': 'test_user'},
        output_dir=self.test_dir,
        task_id="test-task"
    )

    # Create components with the shared context
    extractor = Extractor(context=context)
    transformer = Transformer(context=context)

    # Test the extraction phase
    context.start_phase("extract")
    raw_data = extractor.extract(file_path=test_file)
    context.end_phase()

    # Verify the context was updated correctly
    self.assertIsNotNone(context.raw_data)
    self.assertIn("extract", context.phase_results)
```

## Running Tests

### Running with unittest

```bash
python -m unittest tests/unit/test_module.py
```

### Running with pytest

```bash
python -m pytest tests/unit/test_module.py -vv --log-cli-level=DEBUG
```

See the README.md file for more instructions on running tests.