# Loader Component Optimization

This document outlines the optimizations made to the Load (L) component of the ETL pipeline for the Skype Parser project. These improvements enhance performance, reliability, and maintainability when loading data into PostgreSQL databases, particularly on Supabase.

## Table of Contents

1. [Overview](#overview)
2. [Key Optimizations](#key-optimizations)
3. [Implementation Details](#implementation-details)
4. [Performance Metrics](#performance-metrics)
5. [Usage Examples](#usage-examples)
6. [Best Practices](#best-practices)

## Overview

The Loader component is responsible for loading transformed Skype data into a PostgreSQL database. The optimizations focus on improving efficiency, reliability, and maintainability, particularly for large datasets.

## Key Optimizations

### 1. Connection Pooling

- **Implementation**: Added a dedicated `PostgresConnectionPool` class that manages a pool of database connections.
- **Benefits**:
  - Reduces connection overhead by reusing existing connections
  - Improves throughput by maintaining multiple concurrent connections
  - Handles connection lifecycle management automatically
  - Implements connection health checks and recycling

### 2. Dynamic Batch Sizing

- **Implementation**: Added intelligent batch size calculation based on message characteristics.
- **Benefits**:
  - Optimizes memory usage by adjusting batch sizes based on message size
  - Prevents out-of-memory errors for large messages
  - Improves performance by using larger batches for small messages
  - Adapts to different data characteristics automatically

### 3. Proper Transaction Management

- **Implementation**: Added explicit transaction management with proper begin/commit/rollback operations.
- **Benefits**:
  - Ensures data consistency
  - Improves error recovery
  - Makes transaction boundaries clear and explicit

### 4. Retry Logic

- **Implementation**: Added comprehensive retry logic with exponential backoff for transient errors.
- **Benefits**:
  - Improves resilience against network issues
  - Handles temporary database unavailability
  - Implements smart backoff strategy to prevent overwhelming the database
  - Distinguishes between transient and permanent errors

### 5. Externalized Schema Definition

- **Implementation**: Moved database schema to an external SQL file.
- **Benefits**:
  - Improves maintainability by separating schema from code
  - Makes schema changes easier to track
  - Allows for environment-specific schema variations
  - Enables better version control of schema changes

### 6. Optimized Indexing Strategy

- **Implementation**: Added a comprehensive indexing strategy based on query patterns.
- **Benefits**:
  - Improves query performance
  - Optimizes common access patterns
  - Includes specialized indexes like GIN for text search

### 7. Performance Metrics

- **Implementation**: Added detailed performance metrics collection and reporting.
- **Benefits**:
  - Provides visibility into loader performance
  - Helps identify bottlenecks
  - Tracks key metrics like duration, batch sizes, and retry counts
  - Enables data-driven optimization

### 8. Enhanced Error Handling

- **Implementation**: Improved error handling with detailed logging and context.
- **Benefits**:
  - Makes debugging easier
  - Provides better error context
  - Implements proper cleanup on errors
  - Distinguishes between different error types

## Implementation Details

### Connection Pool Implementation

The connection pool manages a set of database connections that can be reused across operations:

```python
class PostgresConnectionPool(ConnectionPoolProtocol):
    def __init__(
        self,
        db_config: Dict[str, Any],
        min_connections: int = 1,
        max_connections: int = 10,
        connection_timeout: float = 30.0,
        idle_timeout: float = 600.0,
        max_age: float = 3600.0,
    ):
        # Initialize pool parameters

    def get_connection(self) -> Tuple[Any, DictCursor]:
        # Get a connection from the pool

    def release_connection(self, conn: Any, cursor: Any) -> None:
        # Return a connection to the pool
```

The pool includes features like:
- Connection health monitoring
- Automatic connection recycling
- Idle connection cleanup
- Connection timeout handling
- Thread safety

### Dynamic Batch Sizing

The loader dynamically calculates optimal batch sizes based on message characteristics:

```python
def _calculate_optimal_batch_size(self, messages: List[Dict[str, Any]]) -> int:
    # Sample messages to estimate size
    sample_size = min(10, len(messages))
    sample_messages = messages[:sample_size]

    # Estimate average message size
    avg_size = sum(sys.getsizeof(json.dumps(msg)) for msg in sample_messages) / sample_size

    # Calculate optimal batch size (target ~10MB per batch)
    target_batch_bytes = 10 * 1024 * 1024  # 10MB
    calculated_size = max(10, min(1000, int(target_batch_bytes / avg_size)))

    # Stay within reasonable limits
    min_size = max(10, int(self.batch_size * 0.5))
    max_size = min(1000, int(self.batch_size * 2))

    return max(min_size, min(calculated_size, max_size))
```

This approach ensures that:
1. Batch sizes are appropriate for the data being processed
2. Memory usage remains within reasonable limits
3. Database operations are efficient

### Retry Logic

The loader implements retry logic with exponential backoff for transient errors:

```python
retry_count = 0
while retry_count < self.max_retries:
    try:
        # Execute database operation
        return result
    except psycopg2.OperationalError as e:
        # Only retry on operational errors
        retry_count += 1
        self._metrics["retries"] += 1

        logger.warning(f"Database operation failed, retrying ({retry_count}/{self.max_retries}): {e}")

        if retry_count < self.max_retries:
            # Exponential backoff
            sleep_time = self.retry_delay * (2 ** (retry_count - 1))
            time.sleep(sleep_time)
```

This approach:
1. Only retries on transient errors (like connection issues)
2. Uses exponential backoff to prevent overwhelming the database
3. Tracks retry metrics for monitoring
4. Provides detailed logging for troubleshooting

## Performance Metrics

The optimized loader collects and reports detailed performance metrics:

```python
self._metrics = {
    "start_time": None,
    "end_time": None,
    "duration": None,
    "conversations_loaded": 0,
    "messages_loaded": 0,
    "batch_sizes": [],
    "retries": 0,
}
```

These metrics provide insights into:
- Overall loading duration
- Number of conversations and messages processed
- Batch sizes used for different message sets
- Number of retries due to transient errors

## Usage Examples

### Basic Usage

```python
# Create a loader with default settings
loader = Loader(context=context, db_connection=db_connection)

# Connect to the database
loader.connect_db()

try:
    # Load the data
    export_id = loader.load(raw_data, transformed_data, file_source)
    print(f"Data loaded successfully with export ID: {export_id}")
finally:
    # Close the database connection
    loader.close_db()
```

### Advanced Usage with Custom Configuration

```python
# Create a loader with custom settings
loader = Loader(
    context=context,
    batch_size=200,
    max_retries=5,
    retry_delay=2.0,
    schema_file="/path/to/custom_schema.sql",
    db_connection=db_connection
)

# Connect to the database
loader.connect_db()

try:
    # Load the data
    export_id = loader.load(raw_data, transformed_data, file_source)

    # Access performance metrics
    duration = loader._metrics["duration"]
    messages_loaded = loader._metrics["messages_loaded"]
    avg_batch_size = sum(loader._metrics["batch_sizes"]) / len(loader._metrics["batch_sizes"])

    print(f"Loaded {messages_loaded} messages in {duration:.2f} seconds")
    print(f"Average batch size: {avg_batch_size:.1f}")
finally:
    # Close the database connection
    loader.close_db()
```

## Best Practices

When using the optimized loader, follow these best practices:

1. **Use Connection Pooling**: Always use connection pooling for production environments to improve performance.

2. **Configure Batch Size Appropriately**: Set a reasonable base batch size (100-200 is often a good starting point).

3. **Implement Proper Error Handling**: Always wrap loader operations in try/finally blocks to ensure connections are properly closed.

4. **Monitor Performance Metrics**: Regularly review the performance metrics to identify optimization opportunities.

5. **Use Transactions Properly**: Ensure that related operations are grouped within a single transaction for data consistency.

6. **Customize for Your Environment**: Adjust connection pool settings based on your specific database environment.

7. **Regular Maintenance**: Periodically analyze and optimize database indexes based on query patterns.

8. **Schema Management**: Keep the schema file up-to-date with all database changes.

9. **Testing**: Test the loader with various data sizes and characteristics to ensure it performs well in all scenarios.

10. **Resource Cleanup**: Always close connections and release resources when done to prevent leaks.