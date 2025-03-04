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

## Running Tests

See the README.md file for instructions on running tests.