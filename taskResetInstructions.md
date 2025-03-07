# Skype Parser Development Context

## Task Overview & Current Status

### Core Problem/Feature
Implementing a standardized dependency injection (DI) framework for the Skype Parser project to improve testability, maintainability, and modularity of the ETL pipeline components.

### Current Status
✅ Completed:
- DI framework implementation with protocol-based interfaces
- Service provider and registry implementation
- Message handler refactoring to use protocols
- Unit tests for all components
- Documentation updates

### Key Architectural Decisions
1. **Protocol-Based Interfaces**: Using Python's Protocol classes for defining interfaces
   - Rationale: Provides type safety and explicit contracts without inheritance constraints
   - Enables easier mocking in tests and cleaner dependency management

2. **Service Registry Pattern**: Centralized service registration
   - Rationale: Single source of truth for dependency configuration
   - Makes service lifecycle management more maintainable

3. **Constructor Injection**: Primary DI method
   - Rationale: Makes dependencies explicit and supports testing
   - Allows optional dependencies with sensible defaults

### Critical Constraints
- Must maintain backward compatibility with existing code
- Must support both DI and non-DI usage modes
- Must handle both JSON and TAR file formats
- Must support parallel processing for large datasets

## Codebase Navigation

### Key Files (Ranked by Importance)

1. `src/utils/di.py`
   - Core DI framework implementation
   - Contains ServiceProvider class and global service access functions
   - Modified to support singleton, transient, and factory services

2. `src/utils/interfaces.py`
   - Protocol definitions for all major components
   - Defines contracts for ETL components and message handlers
   - Recently added message handler protocols

3. `src/utils/service_registry.py`
   - Centralized service registration
   - Handles core services and ETL component registration
   - Updated to support new message handler factory

4. `src/utils/message_type_handlers.py`
   - Message handler implementations
   - Refactored to use protocol-based approach
   - Supports various message types (text, media, calls, etc.)

5. `src/db/etl/pipeline_manager.py`
   - Main ETL pipeline orchestrator
   - Updated to support both DI and non-DI modes
   - Handles pipeline state management

### Dependencies
- Python 3.8+ (for Protocol support)
- psycopg2 for PostgreSQL interaction
- BeautifulSoup4 for HTML content parsing
- pytest for testing framework

## Technical Context

### Technical Assumptions
- ETL components can be instantiated independently
- Message handlers are stateless
- Database connections are managed by the loader
- File operations are abstracted through the file handler

### External Services
- PostgreSQL database for data storage
- File system for temporary storage and checkpoints

### Performance Considerations
- Parallel processing for message transformation
- Batch processing for database operations
- Memory management for large exports

### Security Considerations
- Database credentials handled through configuration
- No hardcoded secrets
- File path validation to prevent directory traversal

## Development Progress

### Last Completed Tasks
1. Implemented protocol-based message handlers
2. Updated ETL component tests for DI
3. Created comprehensive test suite for DI framework
4. Updated documentation

### Immediate Next Steps
1. Review and optimize parallel processing implementation
2. Add more specialized message handlers
3. Implement caching for frequently used services
4. Add integration tests for the complete pipeline

### Known Issues
- Some legacy code still uses old message handler pattern
- Parallel processing could be more efficient
- Need better error handling for database connection failures

### Failed Approaches
- Tried using metaclasses for service registration (too complex)
- Attempted automatic dependency resolution (led to circular dependencies)
- Considered using decorators for service registration (less explicit)

## Developer Notes

### Codebase Insights
- The ETL pipeline is designed to be resumable
- Context objects carry state between components
- Message handlers are designed for extensibility

### Temporary Solutions
- Legacy functions for backward compatibility
- Some direct service access in tests
- Manual service registration (could be automated)

### Areas Needing Attention
1. Error handling in parallel processing
2. Memory management for large exports
3. Database connection pooling
4. Service lifecycle management

### Best Practices
1. Always use protocols for new interfaces
2. Inject dependencies through constructors
3. Use the service registry for registration
4. Write tests for both DI and non-DI modes
5. Document all protocol contracts

### Future Improvements
1. Implement dependency validation at startup
2. Add service lifecycle hooks
3. Improve error reporting
4. Add performance monitoring
5. Implement service scopes