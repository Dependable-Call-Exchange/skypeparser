# Processing Very Large Datasets

This document provides information about processing very large Skype export datasets (millions of messages) using the Skype Parser ETL pipeline.

## Overview

The standard ETL pipeline is designed to handle moderate-sized Skype export files efficiently. However, when dealing with very large datasets containing millions of messages, memory usage can become a concern. To address this, the Skype Parser includes a streaming processor that can handle very large datasets with minimal memory footprint.

## Streaming Processor

The streaming processor uses a different approach to process Skype export data:

1. **Streaming Extraction**: Instead of loading the entire export file into memory, it reads the file in chunks, yielding individual conversations.
2. **Batch Processing**: It processes messages in batches, transforming and loading them incrementally.
3. **Memory Optimization**: It actively monitors and manages memory usage, triggering garbage collection when needed.

This approach allows the streaming processor to handle datasets that would otherwise exceed available memory.

## When to Use the Streaming Processor

Consider using the streaming processor in the following scenarios:

- Your Skype export file is very large (hundreds of MB or more)
- Your dataset contains millions of messages
- You're experiencing memory issues with the standard ETL pipeline
- You're running on a system with limited memory

## Usage

### Command Line Interface

The streaming processor can be accessed through the `scripts/run_streaming_etl.py` script:

```bash
python scripts/run_streaming_etl.py --file path/to/skype_export.tar --user "Your Name"
```

### Options

The streaming processor supports the following options:

- `--file`: Path to the Skype export file (required)
- `--user`: User display name for the export
- `--output-dir`: Directory to save intermediate files (default: `output`)
- `--config`: Path to configuration file (default: `config/config.json`)
- `--memory`: Memory limit in MB (default: 1024)
- `--batch-size`: Batch size for processing and database operations (default: 1000)
- `--checkpoint-interval`: Number of messages to process before creating a checkpoint (default: 10000)
- `--verbose`: Enable verbose logging
- `--debug`: Enable debug logging

### Example

Process a very large Skype export file with a 2GB memory limit and a batch size of 500:

```bash
python scripts/run_streaming_etl.py --file path/to/large_skype_export.tar --user "Your Name" --memory 2048 --batch-size 500
```

## Performance Considerations

### Memory Usage

The streaming processor is designed to use minimal memory, regardless of the size of the input file. Memory usage is primarily determined by the batch size, not the total dataset size.

### Processing Speed

While the streaming processor uses less memory, it may be slightly slower than the standard ETL pipeline for small to medium-sized datasets. This is because it trades some processing speed for memory efficiency. However, for very large datasets that would cause memory issues with the standard pipeline, the streaming processor is the only viable option.

### Batch Size

The batch size parameter controls the number of messages processed in each batch. A larger batch size can improve performance but will use more memory. The default value of 1000 is a good balance for most systems, but you can adjust it based on your specific requirements:

- Decrease batch size if you're experiencing memory issues
- Increase batch size if you have ample memory and want to improve performance

### Checkpointing

The streaming processor creates checkpoints at regular intervals (controlled by the `--checkpoint-interval` parameter). These checkpoints allow you to resume processing if an error occurs, without having to start from the beginning.

## Comparison with Standard ETL Pipeline

| Feature | Standard ETL Pipeline | Streaming Processor |
|---------|----------------------|---------------------|
| Memory Usage | Loads entire dataset into memory | Processes data in chunks with minimal memory footprint |
| Processing Speed | Faster for small to medium datasets | Optimized for very large datasets |
| Checkpointing | Creates checkpoints at phase boundaries | Creates checkpoints at regular intervals |
| Parallel Processing | Supports parallel processing | Sequential processing with batch optimization |
| Use Case | Small to medium datasets | Very large datasets (millions of messages) |

## Troubleshooting

### Memory Issues

If you're still experiencing memory issues with the streaming processor:

1. Decrease the batch size (`--batch-size`)
2. Increase the memory limit (`--memory`)
3. Ensure no other memory-intensive applications are running
4. Consider splitting the export file into smaller chunks

### Performance Issues

If processing is too slow:

1. Increase the batch size (`--batch-size`) if memory allows
2. Ensure your database is properly indexed
3. Consider using a more powerful machine for processing

### Database Issues

The streaming processor makes frequent database connections. If you're experiencing database issues:

1. Ensure your database can handle the connection load
2. Check that your database has sufficient resources
3. Consider optimizing your database configuration for bulk inserts

## Conclusion

The streaming processor is a powerful tool for handling very large Skype export datasets. By processing data in chunks and actively managing memory usage, it allows you to process datasets that would otherwise be impossible to handle with the standard ETL pipeline.