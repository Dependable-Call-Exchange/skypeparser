# ETLContext Phase Management API

## Overview

The `ETLContext` class provides a centralized mechanism for managing the state and phases of the ETL (Extract, Transform, Load) pipeline. This document focuses specifically on the phase management API, which allows ETL components to track, update, and report on the progress of each phase in the pipeline.

## Example Implementation

For a complete working example of the ETLContext phase management API, see the `examples/etl_context_phase_management_example.py` file. This example demonstrates:

- Basic phase management (start_phase, update_progress, end_phase)
- Phase status management (get_phase_status, set_phase_status)
- Error handling during phases
- A complete ETL flow with all phases
- Phase dependencies and conditional execution

Running this example will show the API in action with detailed logging of each step.

## Core Phase Management Methods

### `start_phase(phase_name: str, total_conversations: int = 0, total_messages: int = 0) -> None`

Starts a new phase in the ETL pipeline.

**Parameters:**
- `phase_name` (str): The name of the phase to start (e.g., "extract", "transform", "load")
- `total_conversations` (int, optional): The total number of conversations expected to be processed in this phase. Defaults to 0.
- `total_messages` (int, optional): The total number of messages expected to be processed in this phase. Defaults to 0.

**Returns:**
- None

**Raises:**
- `ValueError`: If the phase name is invalid or if a phase is already in progress

**Example:**
```python
# Start the extract phase
context.start_phase('extract')

# Start the transform phase with expected counts
context.start_phase('transform', total_conversations=10, total_messages=500)
```

### `end_phase(result: Optional[Dict[str, Any]] = None) -> Dict[str, Any]`

Ends the current phase and records results.

**Parameters:**
- `result` (Dict[str, Any], optional): A dictionary containing results from the phase. Defaults to None.

**Returns:**
- Dict[str, Any]: A dictionary containing statistics about the completed phase, including:
  - `phase`: The name of the completed phase
  - `status`: The status of the phase (e.g., "completed", "failed")
  - `duration_seconds`: The duration of the phase in seconds
  - `conversations_processed`: The number of conversations processed
  - `messages_processed`: The number of messages processed
  - Any additional keys provided in the `result` parameter

**Raises:**
- `ValueError`: If no phase is currently in progress or if the result is not a dictionary
- `TypeError`: If the result contains non-serializable objects

**Example:**
```python
# End the extract phase with results
result = context.end_phase({
    'status': 'completed',
    'conversations_processed': 10,
    'messages_processed': 500,
    'additional_info': 'Extraction completed successfully'
})
```

### `get_phase_status(phase: str) -> str`

Gets the status of a specific phase.

**Parameters:**
- `phase` (str): The name of the phase to check

**Returns:**
- str: The status of the phase ("pending", "running", "completed", or "failed")

**Example:**
```python
# Check if the extract phase is completed
if context.get_phase_status('extract') == 'completed':
    # Proceed with transform phase
    context.start_phase('transform')
```

### `set_phase_status(phase: str, status: str) -> None`

Sets the status of a specific phase.

**Parameters:**
- `phase` (str): The name of the phase to update
- `status` (str): The new status for the phase. Should be one of: "pending", "running", "completed", or "failed"

**Returns:**
- None

**Raises:**
- `ValueError`: If the phase name or status is invalid

**Example:**
```python
# Mark the extract phase as failed
context.set_phase_status('extract', 'failed')
```

### `update_progress(conversations: int = 0, messages: int = 0) -> None`

Updates the progress counters for the current phase.

**Parameters:**
- `conversations` (int, optional): The number of additional conversations processed. Defaults to 0.
- `messages` (int, optional): The number of additional messages processed. Defaults to 0.

**Returns:**
- None

**Example:**
```python
# Update progress after processing a batch
context.update_progress(conversations=5, messages=250)
```

## Phase Result Structure

When a phase is completed, the result dictionary should follow this structure:

```python
{
    'status': str,  # 'completed' or 'failed'
    'conversations_processed': int,  # Number of conversations processed
    'messages_processed': int,  # Number of messages processed
    # Additional phase-specific information
}
```

## Complete Usage Example

```python
from src.db.etl.context import ETLContext

# Create the context
context = ETLContext(db_config={'host': 'localhost', 'dbname': 'skype'})

try:
    # Start the extract phase
    context.start_phase('extract')

    # Process data and update progress
    # ...
    context.update_progress(conversations=5, messages=100)
    # ...

    # End the extract phase
    extract_result = context.end_phase({
        'status': 'completed',
        'conversations_processed': 10,
        'messages_processed': 500
    })

    # Start the transform phase
    context.start_phase('transform',
                       total_conversations=extract_result['conversations_processed'],
                       total_messages=extract_result['messages_processed'])

    # Process data and update progress
    # ...
    context.update_progress(conversations=5, messages=250)
    # ...

    # End the transform phase
    transform_result = context.end_phase({
        'status': 'completed',
        'conversations_processed': 10,
        'messages_processed': 500
    })

    # Start the load phase
    context.start_phase('load')

    # Process data and update progress
    # ...
    context.update_progress(conversations=10, messages=500)
    # ...

    # End the load phase
    load_result = context.end_phase({
        'status': 'completed',
        'conversations_processed': 10,
        'messages_processed': 500,
        'export_id': 12345
    })

except Exception as e:
    # Handle errors
    if context.current_phase:
        context.set_phase_status(context.current_phase, 'failed')
        context.record_error(context.current_phase, e)
    raise
```

## Best Practices

1. **Always start a phase before ending it**: Call `start_phase()` before calling `end_phase()`.

2. **Handle errors properly**: If an error occurs during a phase, set the phase status to 'failed' and record the error.

3. **Provide complete results**: When ending a phase, include all relevant information in the result dictionary.

4. **Update progress regularly**: For long-running phases, call `update_progress()` regularly to provide feedback.

5. **Check phase status before proceeding**: Before starting a new phase, check that the previous phase completed successfully.

6. **Use consistent phase names**: Stick to the standard phase names ("extract", "transform", "load") for consistency.

7. **Avoid modifying the context directly**: Use the provided methods to update the context state.

## Common Errors and Solutions

### "No phase is currently in progress"

This error occurs when calling `end_phase()` without first calling `start_phase()`.

**Solution**: Always call `start_phase()` before calling `end_phase()`.

### "A phase is already in progress"

This error occurs when calling `start_phase()` while another phase is still in progress.

**Solution**: Call `end_phase()` to end the current phase before starting a new one.

### "Result must be a dictionary"

This error occurs when passing a non-dictionary value to `end_phase()`.

**Solution**: Ensure that the result parameter is a dictionary or None.

### "Phase name is invalid"

This error occurs when providing an invalid phase name to `start_phase()` or `set_phase_status()`.

**Solution**: Use one of the standard phase names ("extract", "transform", "load") or a custom phase name that follows the naming conventions.

### "Status is invalid"

This error occurs when providing an invalid status to `set_phase_status()`.

**Solution**: Use one of the standard status values: "pending", "running", "completed", or "failed".
