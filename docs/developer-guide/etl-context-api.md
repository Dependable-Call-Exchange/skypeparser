# ETLContext API in SkypeParser

This document provides a comprehensive overview of the `ETLContext` class in the SkypeParser project, focusing on its role in state management, phase tracking, and cross-component communication within the ETL pipeline.

## Overview

The `ETLContext` class is a critical component of the SkypeParser ETL (Extract, Transform, Load) architecture, providing:

1. **Centralized State Management**: Maintains configuration values and shared state
2. **Phase Tracking**: Monitors progress of ETL phases (extract, transform, load)
3. **Cross-Component Communication**: Enables components to share information
4. **Resource Management**: Handles database connections and resources
5. **Performance Monitoring**: Tracks memory usage and execution time

```
┌─────────────────────────────────────────────────────────────┐
│                      ETL Pipeline                           │
└───────────────────────────┬─────────────────────────────────┘
                            │ uses
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                       ETLContext                            │
├─────────────────────┬─────────────────────┬─────────────────┤
│   Phase Management  │    State Tracking   │ Configuration   │
├─────────────────────┼─────────────────────┼─────────────────┤
│ - start_phase()     │ - progress_tracker  │ - db_config     │
│ - end_phase()       │ - memory_monitor    │ - batch_size    │
│ - update_progress() │ - statistics        │ - options       │
└─────────────────────┴─────────────────────┴─────────────────┘
                            │
                            │ shared between
                            ▼
┌─────────────────┐   ┌─────────────────┐   ┌─────────────────┐
│    Extractor    │◄─►│   Transformer   │◄─►│     Loader      │
└─────────────────┘   └─────────────────┘   └─────────────────┘
```

## ETLContext Class Architecture

The `ETLContext` class is implemented in `src/db/etl/context.py`. It maintains state and provides methods for all ETL components.

### Core Properties

```python
class ETLContext:
    """
    Context object for the ETL pipeline, providing shared state and configuration.

    This class maintains state across ETL phases and provides methods for
    tracking progress, managing resources, and handling configuration.
    """

    def __init__(self, db_config, batch_size=100,
                 memory_limit_mb=None, parallel_processing=False,
                 output_dir=None):
        """
        Initialize the ETL context.

        Args:
            db_config (dict): Database configuration
            batch_size (int): Batch size for database operations
            memory_limit_mb (int): Memory limit in MB (None for no limit)
            parallel_processing (bool): Whether to use parallel processing
            output_dir (str): Output directory for files
        """
        # Configuration
        self.db_config = db_config
        self.batch_size = batch_size
        self.memory_limit_mb = memory_limit_mb
        self.parallel_processing = parallel_processing
        self.output_dir = output_dir

        # State tracking
        self.current_phase = None
        self.phase_status = {
            'extract': 'pending',
            'transform': 'pending',
            'load': 'pending'
        }

        # Progress tracking
        self.progress_tracker = ProgressTracker()
        self.memory_monitor = MemoryMonitor(memory_limit_mb)

        # Results storage
        self.statistics = {
            'phases': {},
            'total': {
                'start_time': datetime.now(),
                'end_time': None,
                'duration_seconds': 0,
                'conversations_processed': 0,
                'messages_processed': 0
            }
        }

        # Resources
        self._db_connection = None
        self._resources = {}
```

## Phase Management API

The ETLContext provides a comprehensive API for managing ETL phases.

### Phase Lifecycle Methods

#### `start_phase(phase_name, total_conversations=0, total_messages=0)`

Initiates a new phase in the ETL pipeline process.

```python
def start_phase(self, phase_name, total_conversations=0, total_messages=0):
    """
    Start a new phase in the ETL pipeline.

    Args:
        phase_name (str): The name of the phase (extract, transform, load)
        total_conversations (int): Expected number of conversations to process
        total_messages (int): Expected number of messages to process

    Raises:
        ValueError: If a phase is already in progress or if phase name is invalid
    """
    # Validate phase name
    if phase_name not in self.phase_status:
        valid_phases = ', '.join(self.phase_status.keys())
        raise ValueError(f"Invalid phase name: {phase_name}. "
                         f"Valid phases are: {valid_phases}")

    # Check if a phase is already in progress
    if self.current_phase:
        raise ValueError(f"Cannot start phase '{phase_name}': Phase "
                         f"'{self.current_phase}' is already in progress")

    # Set current phase and status
    self.current_phase = phase_name
    self.phase_status[phase_name] = 'running'

    # Initialize progress tracking
    self.progress_tracker.start_phase(
        phase_name, total_conversations, total_messages
    )

    # Log phase start
    logger.info(f"Started ETL phase: {phase_name}")
```

#### `end_phase(result=None)`

Completes the current phase and records results.

```python
def end_phase(self, result=None):
    """
    End the current phase and record results.

    Args:
        result (dict): Results from the phase (optional)

    Returns:
        dict: Phase statistics

    Raises:
        ValueError: If no phase is currently in progress or if result is invalid
    """
    # Check if a phase is in progress
    if not self.current_phase:
        raise ValueError("Cannot end phase: No phase is currently in progress")

    # Validate result
    if result is not None and not isinstance(result, dict):
        raise ValueError("Result must be a dictionary or None")

    phase_name = self.current_phase

    # Finalize phase statistics
    phase_stats = self.progress_tracker.finish_phase()

    # Update phase status
    status = result.get('status', 'completed') if result else 'completed'
    self.phase_status[phase_name] = status

    # Combine statistics with result
    combined_result = {**phase_stats}
    if result:
        combined_result.update(result)

    # Store the result
    self.statistics['phases'][phase_name] = combined_result

    # Update total statistics
    self.statistics['total']['conversations_processed'] += phase_stats['processed_conversations']
    self.statistics['total']['messages_processed'] += phase_stats['processed_messages']

    # Reset current phase
    self.current_phase = None

    # Log phase completion
    logger.info(f"Completed ETL phase: {phase_name} (Status: {status})")

    return combined_result
```

#### `update_progress(conversations=0, messages=0)`

Updates progress counters during a phase.

```python
def update_progress(self, conversations=0, messages=0):
    """
    Update progress counters for the current phase.

    Args:
        conversations (int): Number of additional conversations processed
        messages (int): Number of additional messages processed

    Raises:
        ValueError: If no phase is currently in progress
    """
    # Check if a phase is in progress
    if not self.current_phase:
        raise ValueError("Cannot update progress: No phase is currently in progress")

    # Update progress tracker
    self.progress_tracker.update(conversations, messages)

    # Check memory usage
    self.memory_monitor.check_memory()
```

### Phase Status Management

#### `get_phase_status(phase)`

Retrieves the status of a specific phase.

```python
def get_phase_status(self, phase):
    """
    Get the status of a specific phase.

    Args:
        phase (str): Phase name

    Returns:
        str: Phase status (pending, running, completed, or failed)

    Raises:
        ValueError: If phase name is invalid
    """
    if phase not in self.phase_status:
        valid_phases = ', '.join(self.phase_status.keys())
        raise ValueError(f"Invalid phase name: {phase}. "
                         f"Valid phases are: {valid_phases}")

    return self.phase_status[phase]
```

#### `set_phase_status(phase, status)`

Sets the status of a specific phase.

```python
def set_phase_status(self, phase, status):
    """
    Set the status of a specific phase.

    Args:
        phase (str): Phase name
        status (str): Phase status (pending, running, completed, failed)

    Raises:
        ValueError: If phase name or status is invalid
    """
    # Validate phase
    if phase not in self.phase_status:
        valid_phases = ', '.join(self.phase_status.keys())
        raise ValueError(f"Invalid phase name: {phase}. "
                         f"Valid phases are: {valid_phases}")

    # Validate status
    valid_statuses = ['pending', 'running', 'completed', 'failed']
    if status not in valid_statuses:
        valid_status_str = ', '.join(valid_statuses)
        raise ValueError(f"Invalid status: {status}. "
                         f"Valid statuses are: {valid_status_str}")

    # Update status
    self.phase_status[phase] = status

    # Log status change
    logger.info(f"ETL phase '{phase}' status changed to '{status}'")
```

### Error Handling Methods

#### `record_error(phase, error)`

Records an error that occurred during a phase.

```python
def record_error(self, phase, error):
    """
    Record an error that occurred during a phase.

    Args:
        phase (str): Phase name
        error (Exception): Error that occurred

    Returns:
        dict: Error details
    """
    # Validate phase
    if phase not in self.phase_status:
        valid_phases = ', '.join(self.phase_status.keys())
        raise ValueError(f"Invalid phase name: {phase}. "
                         f"Valid phases are: {valid_phases}")

    # Create error record
    error_details = {
        'error_type': type(error).__name__,
        'error_message': str(error),
        'timestamp': datetime.now().isoformat(),
        'phase': phase
    }

    # Add traceback
    import traceback
    error_details['traceback'] = traceback.format_exc()

    # Store error in statistics
    if 'errors' not in self.statistics:
        self.statistics['errors'] = []
    self.statistics['errors'].append(error_details)

    # Update phase status
    self.phase_status[phase] = 'failed'

    # Log error
    logger.error(f"Error in ETL phase '{phase}': {error}")

    return error_details
```

## Resource Management

The ETLContext also manages shared resources like database connections.

### Database Connection Management

```python
def get_db_connection(self):
    """
    Get the database connection, creating it if it doesn't exist.

    Returns:
        Connection: Database connection
    """
    if self._db_connection is None:
        import psycopg2
        self._db_connection = psycopg2.connect(**self.db_config)

    return self._db_connection

def close_db_connection(self):
    """Close the database connection if it exists."""
    if self._db_connection:
        self._db_connection.close()
        self._db_connection = None
```

### Generic Resource Management

```python
def set_resource(self, key, value):
    """
    Store a resource in the context.

    Args:
        key (str): Resource identifier
        value: Resource value
    """
    self._resources[key] = value

def get_resource(self, key, default=None):
    """
    Get a resource from the context.

    Args:
        key (str): Resource identifier
        default: Default value if resource doesn't exist

    Returns:
        The resource value or default
    """
    return self._resources.get(key, default)
```

## Supporting Classes

### ProgressTracker

The `ProgressTracker` class handles detailed progress monitoring within a phase.

```python
class ProgressTracker:
    """Tracks progress within an ETL phase."""

    def __init__(self):
        """Initialize the progress tracker."""
        self.reset()

    def reset(self):
        """Reset the progress tracker."""
        self.phase = None
        self.start_time = None
        self.end_time = None
        self.total_conversations = 0
        self.total_messages = 0
        self.processed_conversations = 0
        self.processed_messages = 0

    def start_phase(self, phase, total_conversations=0, total_messages=0):
        """
        Start tracking a new phase.

        Args:
            phase (str): Phase name
            total_conversations (int): Expected conversations to process
            total_messages (int): Expected messages to process
        """
        self.reset()
        self.phase = phase
        self.start_time = datetime.now()
        self.total_conversations = total_conversations
        self.total_messages = total_messages

    def update(self, conversations=0, messages=0):
        """
        Update progress counters.

        Args:
            conversations (int): Additional conversations processed
            messages (int): Additional messages processed
        """
        self.processed_conversations += conversations
        self.processed_messages += messages

    def finish_phase(self):
        """
        Finish the current phase and generate statistics.

        Returns:
            dict: Phase statistics
        """
        self.end_time = datetime.now()
        duration = (self.end_time - self.start_time).total_seconds()

        # Calculate progress percentages
        conversation_percent = (
            (self.processed_conversations / self.total_conversations * 100)
            if self.total_conversations else 0
        )
        message_percent = (
            (self.processed_messages / self.total_messages * 100)
            if self.total_messages else 0
        )

        # Calculate processing rates
        conversations_per_second = (
            self.processed_conversations / duration if duration > 0 else 0
        )
        messages_per_second = (
            self.processed_messages / duration if duration > 0 else 0
        )

        # Generate statistics
        stats = {
            'phase': self.phase,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'duration_seconds': duration,
            'total_conversations': self.total_conversations,
            'processed_conversations': self.processed_conversations,
            'conversation_percent': conversation_percent,
            'total_messages': self.total_messages,
            'processed_messages': self.processed_messages,
            'message_percent': message_percent,
            'conversations_per_second': conversations_per_second,
            'messages_per_second': messages_per_second
        }

        return stats
```

### MemoryMonitor

The `MemoryMonitor` class tracks memory usage to prevent out-of-memory errors.

```python
class MemoryMonitor:
    """Monitors memory usage during ETL processing."""

    def __init__(self, memory_limit_mb=None):
        """
        Initialize the memory monitor.

        Args:
            memory_limit_mb (int): Memory limit in MB (None for no limit)
        """
        self.memory_limit_mb = memory_limit_mb
        self.peak_memory_mb = 0

    def get_current_memory_mb(self):
        """
        Get current memory usage in MB.

        Returns:
            float: Current memory usage in MB
        """
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        return memory_info.rss / (1024 * 1024)  # Convert to MB

    def check_memory(self):
        """
        Check current memory usage against the limit.

        Raises:
            MemoryError: If memory usage exceeds the limit
        """
        if not self.memory_limit_mb:
            return

        current_memory_mb = self.get_current_memory_mb()
        self.peak_memory_mb = max(self.peak_memory_mb, current_memory_mb)

        if current_memory_mb > self.memory_limit_mb:
            raise MemoryError(
                f"Memory usage ({current_memory_mb:.2f} MB) "
                f"exceeds limit ({self.memory_limit_mb} MB)"
            )
```

## ETLContext Initialization

The ETLContext is typically initialized at the start of the ETL process:

```python
# Initialize the context with database configuration
context = ETLContext(
    db_config={
        'host': 'localhost',
        'dbname': 'skype_db',
        'user': 'user',
        'password': 'password'
    },
    batch_size=200,
    memory_limit_mb=1024,
    parallel_processing=True
)
```

## Complete ETL Process Flow

Below is a complete example of using the ETLContext throughout an ETL process:

```python
def run_etl_pipeline(file_path, user_display_name):
    """
    Run the complete ETL pipeline.

    Args:
        file_path (str): Path to the input file
        user_display_name (str): User display name

    Returns:
        dict: Pipeline results
    """
    # Initialize context
    context = ETLContext(
        db_config={'host': 'localhost', 'dbname': 'skype_db'},
        batch_size=200
    )

    # Initialize components
    extractor = Extractor()
    transformer = Transformer()
    loader = Loader()

    # Initialize results
    results = {
        'success': False,
        'phases': {}
    }

    try:
        # EXTRACT PHASE
    context.start_phase('extract')

        # Extract data
        raw_data = extractor.extract(file_path, context)

        # Track extraction metadata
        extraction_result = {
        'status': 'completed',
            'file_path': file_path,
            'file_size_bytes': os.path.getsize(file_path)
        }

        # End extract phase
        extract_stats = context.end_phase(extraction_result)
        results['phases']['extract'] = extract_stats

        # TRANSFORM PHASE
        context.start_phase(
            'transform',
            total_conversations=len(raw_data.get('conversations', {})),
            total_messages=sum(
                len(c.get('MessageList', []))
                for c in raw_data.get('conversations', {}).values()
            )
        )

        # Transform data
        transformed_data = transformer.transform(raw_data, context, user_display_name)

        # End transform phase
        transform_result = {
        'status': 'completed',
            'conversations_count': len(transformed_data['conversations']),
            'messages_count': transformed_data['message_count']
        }
        transform_stats = context.end_phase(transform_result)
        results['phases']['transform'] = transform_stats

        # LOAD PHASE
        context.start_phase(
            'load',
            total_conversations=len(transformed_data['conversations']),
            total_messages=transformed_data['message_count']
        )

        # Load data
        load_result = loader.load(transformed_data, context)

        # End load phase
        load_stats = context.end_phase(load_result)
        results['phases']['load'] = load_stats

        # Set overall success
        results['success'] = True
        results['export_id'] = load_result.get('export_id')

    except Exception as e:
        # Record error
        if context.current_phase:
            error_details = context.record_error(context.current_phase, e)
            results['error'] = error_details
        else:
            results['error'] = {
                'error_type': type(e).__name__,
                'error_message': str(e)
            }
    finally:
        # Clean up resources
        context.close_db_connection()

        # Update final statistics
        context.statistics['total']['end_time'] = datetime.now()
        context.statistics['total']['duration_seconds'] = (
            context.statistics['total']['end_time'] -
            context.statistics['total']['start_time']
        ).total_seconds()

        # Add statistics to results
        results['statistics'] = context.statistics

    return results
```

## Best Practices for Using ETLContext

### 1. Proper Phase Management

Always start a phase before ending it and ensure phases are properly completed:

```python
# Start the phase
context.start_phase('extract')

try:
    # Phase logic...

    # End the phase with success
    context.end_phase({'status': 'completed'})
except Exception as e:
    # Record the error
    context.record_error('extract', e)
    # End the phase with failure
    context.end_phase({'status': 'failed'})
    raise
```

### 2. Regular Progress Updates

Update progress frequently for accurate monitoring:

```python
# Process in batches
for i, batch in enumerate(batches):
    process_batch(batch)

    # Update progress after each batch
    context.update_progress(
        conversations=len(batch),
        messages=count_messages(batch)
    )

    # Log progress every 10 batches
    if i % 10 == 0:
        logger.info(f"Processed {i}/{len(batches)} batches")
```

### 3. Resource Management

Properly manage resources to prevent leaks:

```python
try:
    # Get a database connection from the context
    connection = context.get_db_connection()

    # Use the connection...

finally:
    # Ensure the connection is closed
    context.close_db_connection()
```

### 4. Error Handling

Handle errors appropriately:

```python
try:
    # Process data...
except MemoryError as e:
    # Handle memory errors
    logger.error(f"Memory limit exceeded: {e}")
    context.record_error('transform', e)
    raise
except Exception as e:
    # Handle other errors
    logger.error(f"Error processing data: {e}")
    context.record_error('transform', e)
    raise
```

### 5. Dependency Management

Use the context to share dependencies between components:

```python
# Share a data parser between components
parser = CustomDataParser()
context.set_resource('data_parser', parser)

# Later, in another component
parser = context.get_resource('data_parser')
if parser:
    parsed_data = parser.parse(data)
else:
    # Fallback if resource isn't available
    parsed_data = default_parse(data)
```

## Common Pitfalls and Solutions

### Forgetting to End a Phase

**Problem**: If a phase isn't properly ended, the next phase can't be started.

**Solution**: Use try/finally to ensure phases are ended:

```python
try:
    context.start_phase('extract')
    # Extract logic...
finally:
    if context.current_phase == 'extract':
        context.end_phase()
```

### Memory Management Issues

**Problem**: Large data structures can cause memory issues.

**Solution**: Process data in batches and use the memory monitor:

```python
# Set a memory limit
context = ETLContext(memory_limit_mb=1024)

# Process in batches
for batch in get_batches(data, batch_size=context.batch_size):
    process_batch(batch)

    # Check memory usage
    context.memory_monitor.check_memory()
```

### Incorrect Progress Tracking

**Problem**: Progress tracking is inaccurate because initial totals are unknown.

**Solution**: Update totals once known:

```python
# Start with default totals
context.start_phase('extract')

# Extract data and determine actual totals
data = extract_initial_data()
total_conversations = len(data['conversations'])
total_messages = count_messages(data)

# Update progress tracker with actual totals
context.progress_tracker.total_conversations = total_conversations
context.progress_tracker.total_messages = total_messages
```

### Database Connection Management

**Problem**: Database connections aren't properly closed, leading to connection leaks.

**Solution**: Use a context manager pattern:

```python
class DatabaseOperation:
    def __init__(self, context):
        self.context = context

    def __enter__(self):
        self.connection = self.context.get_db_connection()
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Connection is maintained by the context
        pass

# Usage
with DatabaseOperation(context) as connection:
    # Use the connection...
# No need to explicitly close
```

## Conclusion

The `ETLContext` class provides a comprehensive framework for managing the ETL process in SkypeParser. By centralizing state management, progress tracking, and resource handling, it enables clean separation of concerns between ETL components while facilitating communication and coordination.

Through proper use of the phase management API, components can report progress, handle errors consistently, and ensure proper resource cleanup. This results in a more maintainable, robust ETL pipeline that can handle large datasets efficiently.
