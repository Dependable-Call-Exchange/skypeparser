# Dependency Injection Refactoring Guide

This guide provides steps and examples for refactoring components in the Skype Parser project to use proper dependency injection, following SOLID principles.

## Detected Dependency Injection Issues

Our linting tools have identified several components that violate the Dependency Inversion Principle by directly instantiating services in constructors:

```
src/db/testable_etl_pipeline.py
src/db/etl/pipeline_manager.py
tests/integration/test_transformer_integration.py
src/db/etl_pipeline_compat.py
```

## Step-by-Step Refactoring Guide

Below is a step-by-step guide to refactor components to use proper dependency injection.

### 1. Identify Direct Instantiations

Use our custom linter to identify direct instantiations of service classes:

```bash
python scripts/check_di_compliance.py path/to/your/file.py
```

For example, running this on `src/db/testable_etl_pipeline.py` identified multiple violations:

```
Line 79: Direct instantiation of service class 'ETLContext' in constructor
Line 90: Direct instantiation of service class 'ETLPipeline' in constructor
Line 97: Direct instantiation of service class 'FileHandler' in constructor
Line 98: Direct instantiation of service class 'ContentExtractor' in constructor
Line 100: Direct instantiation of service class 'StructuredDataExtractor' in constructor
Line 101: Direct instantiation of service class 'DatabaseConnection' in constructor
...
```

### 2. Import Required Interfaces and DI Utilities

Replace direct imports of concrete classes with their interfaces:

```python
# Before
from ..utils.file_handler import FileHandler
from ..parser.content_extractor import ContentExtractor

# After
from ..utils.interfaces import FileHandlerProtocol, ContentExtractorProtocol
from ..utils.di import get_service, get_service_provider
from ..utils.service_registry import register_all_services
```

### 3. Use the Service Registry for Service Registration

Instead of creating services directly, use the service registry:

```python
# Before
self.file_handler = FileHandler()
self.content_extractor = ContentExtractor()

# After
# Register services with DI container
provider = get_service_provider()
register_all_services(db_config=db_config, output_dir=output_dir, provider=provider)

# Get dependencies from the container
self.file_handler = get_service(FileHandlerProtocol)
self.content_extractor = get_service(ContentExtractorProtocol)
```

### 4. Allow for Testing Overrides

Maintain the ability to inject mock dependencies for testing:

```python
# Accept dependencies through constructor
def __init__(
    self,
    db_config: Optional[Dict[str, Any]] = None,
    file_handler: Optional[FileHandlerProtocol] = None,
    content_extractor: Optional[ContentExtractorProtocol] = None
):
    # Get from container or use provided ones for testing
    self.file_handler = file_handler or get_service(FileHandlerProtocol)
    self.content_extractor = content_extractor or get_service(ContentExtractorProtocol)
```

### 5. Modify File Validation Logic

Replace direct OS calls with injected validators:

```python
# Before
if file_path and not os.path.exists(file_path):
    raise ValueError(f"File does not exist: {file_path}")

# After
if file_path and not getattr(self, "validate_file_exists", lambda f: os.path.exists(f))(file_path):
    raise ValueError(f"File does not exist: {file_path}")
```

## Example: Refactoring PipelineManager

Here's an example of how to refactor the `pipeline_manager.py` file:

```python
# Before
def __init__(self, db_config: Dict[str, Any], output_dir: Optional[str] = None):
    self.context = ETLContext(db_config=db_config, output_dir=output_dir)
    self.extractor = None
    self.transformer = None
    self.loader = None

# After
def __init__(
    self,
    db_config: Dict[str, Any],
    output_dir: Optional[str] = None,
    context: Optional[ETLContext] = None
):
    # Register services
    provider = get_service_provider()
    register_all_services(db_config=db_config, output_dir=output_dir, provider=provider)

    # Get context from DI container or use provided one
    self.context = context or get_service(ETLContext)
    self.extractor = None
    self.transformer = None
    self.loader = None
```

## Special Case: Test Files

For test files, you can either:

1. Use the DI container with mock services:

```python
# Create mock dependencies
mock_file_handler = MagicMock(spec=FileHandlerProtocol)
mock_file_handler.read_file.return_value = test_data

# Register with DI container
provider = ServiceProvider()
provider.register_singleton(FileHandlerProtocol, mock_file_handler)

# Test class will use the registered mock
test_instance = ClassUnderTest()
```

2. Or inject mocks directly:

```python
# Create mock dependencies
mock_file_handler = MagicMock(spec=FileHandlerProtocol)
mock_file_handler.read_file.return_value = test_data

# Inject directly
test_instance = ClassUnderTest(file_handler=mock_file_handler)
```

## Compatibility Layer Considerations

For compatibility layers like `etl_pipeline_compat.py`, follow these guidelines:

1. Clearly mark the file as deprecated
2. Try to use DI principles even in the compatibility layer
3. If you must instantiate directly, add a comment explaining why
4. Add a timeline for when this compatibility layer will be removed

## Checklist for Refactoring

- [ ] Identify direct instantiations with the linter
- [ ] Import proper interfaces and DI utilities
- [ ] Replace direct instantiations with DI container usage
- [ ] Allow for testing overrides in constructors
- [ ] Update file validation logic to use injected validators
- [ ] Add proper error handling for DI container failures
- [ ] Update unit tests to work with the refactored class
- [ ] Verify all functionality still works

## Benefits of the Refactoring

- **Improved Testability**: Components are now easier to test in isolation
- **Reduced Coupling**: Components depend on abstractions, not concrete implementations
- **Clearer Dependencies**: Dependencies are explicitly declared in constructors
- **Consistent Architecture**: All components follow the same pattern
- **Better Maintainability**: Changes to implementations don't require changes to consumers

## Common Issues and Solutions

### Issue 1: "No registration found for X"

If you get a KeyError with this message, it means the service isn't registered with the DI container.

**Solution**: Make sure you call `register_all_services()` or the specific registration function for that service.

### Issue 2: Circular Dependencies

If service A depends on service B, and service B depends on service A, you'll get an infinite recursion.

**Solution**: Extract the common functionality into a service C that both A and B can depend on.

### Issue 3: Services Not Being Instantiated Correctly

If a service isn't being created with the right parameters, it might be due to how it's registered.

**Solution**: Use a factory function to control exactly how the service is created:

```python
def create_custom_service():
    return CustomService(special_param="value")

provider.register_factory(CustomServiceProtocol, create_custom_service)
```

## Conclusion

Following this guide will help you refactor components to use proper dependency injection, improving the overall architecture and maintainability of the Skype Parser project. Remember that the goal is to make components more testable, loosely coupled, and adherent to SOLID principles.

If you encounter any issues during refactoring, please refer to the examples in this guide or consult the documentation in `docs/development/DEPENDENCY_HANDLING.md`.