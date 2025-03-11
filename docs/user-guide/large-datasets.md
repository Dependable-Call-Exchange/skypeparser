# Processing Skype Export Files at Scale

## Overview
Skype exports can range from a few megabytes to multiple gigabytes, depending on the user’s chat history. Handling these files efficiently requires different strategies based on dataset size. This guide provides optimized approaches for processing Skype export files, categorized by scale.

---

## Processing Medium-Sized Datasets (<500 MB, <100,000 Messages)

For medium-sized datasets, the standard ETL pipeline is typically sufficient. This approach loads the export file into memory and processes it in bulk, making it the fastest option for datasets of this size.

### Recommended Method: Standard ETL Pipeline

#### Command Line Usage
```bash
python scripts/run_etl.py --file path/to/skype_export.tar --user "Your Name"
```

#### Advantages:
- Fast processing speed
- Works well on systems with sufficient memory

#### Limitations:
- Memory-intensive, not suitable for very large datasets

---

## Processing Large Datasets (500 MB - 2 GB, 100,000 - 1,000,000 Messages)

For larger exports, memory efficiency becomes a concern. The streaming processor is recommended to reduce memory consumption by processing data in chunks.

### Recommended Method: Streaming ETL Processor

#### Features:
- **Streaming Extraction**: Reads data incrementally instead of loading everything at once.
- **Batch Processing**: Processes messages in batches to optimize performance.
- **Memory Optimization**: Monitors memory usage and triggers garbage collection as needed.

#### Command Line Usage
```bash
python scripts/run_streaming_etl.py --file path/to/skype_export.tar --user "Your Name"
```

#### Additional Options:
- `--memory`: Set memory limit in MB (default: 1024)
- `--batch-size`: Adjust batch processing size (default: 1000)
- `--checkpoint-interval`: Set checkpoint interval (default: 10,000 messages)
- `--verbose`: Enable detailed logging

#### Advantages:
- Lower memory footprint compared to standard ETL
- Can handle larger datasets without requiring high RAM

#### Limitations:
- Slightly slower than the standard ETL pipeline due to incremental processing

---

## Processing Very Large Datasets (>2 GB, >1,000,000 Messages)

For extremely large datasets, the standard ETL and streaming ETL methods may not be sufficient. Instead, using fully streamed processing methods is recommended.

### Recommended Method: Fully Streamed Processing

#### Available Streaming Methods:

##### 1. `parse_skype_data_streaming()`
Processes Skype exports in a memory-efficient manner without loading the entire file.

```python
from src.parser.core_parser import parse_skype_data_streaming

stats = parse_skype_data_streaming(
    file_path='path/to/large_export.tar',
    user_display_name='Your Name',
    callback=lambda key, value: print(f"Progress: {key}={value}")
)
```

##### 2. `stream_conversations()`
Processes conversations one at a time to avoid memory overload.

```python
from src.parser.core_parser import stream_conversations

for conversation in stream_conversations('path/to/large_export.tar'):
    print(f"Processing conversation {conversation.get('id')} with {len(conversation.get('MessageList', []))} messages")
```

#### Command Line Usage
```bash
python scripts/stream_skype_data.py -f path/to/large_export.tar -u "Your Name" -v
```

#### Advantages:
- Minimal memory usage, regardless of dataset size
- Supports custom filtering and analysis
- Resilient to system limitations

#### Limitations:
- Can be slower than batch processing methods

---

## Performance Considerations

| Factor | Standard ETL | Streaming ETL | Fully Streamed |
|--------|-------------|---------------|---------------|
| Max File Size | <500 MB | <2 GB | >2 GB |
| Memory Usage | High | Moderate | Minimal |
| Speed | Fast | Moderate | Slower |
| Checkpointing | No | Yes | Yes |
| Custom Filtering | Limited | Somewhat | Highly Flexible |

### Optimizing Performance
- **For Memory Constraints**: Reduce batch size (`--batch-size`) and increase checkpoints (`--checkpoint-interval`)
- **For Speed**: Increase batch size if memory allows
- **For Large Datasets**: Use streamed processing to avoid memory overload

---

## Troubleshooting

### Memory Issues
- Lower the batch size (`--batch-size`)
- Increase memory allocation (`--memory`)
- Close unnecessary applications
- Split large export files into smaller chunks

### Slow Processing
- Increase batch size if memory allows
- Ensure disk I/O performance is not a bottleneck
- Run on an SSD for faster read/write speeds

### Database Issues
- Optimize database configuration for bulk inserts
- Ensure sufficient database resources are available

---

## Conclusion

Choosing the right processing method depends on dataset size and available system resources. The standard ETL pipeline works well for medium-sized exports, while streaming ETL is suited for large datasets. For very large exports, fully streamed processing is the most efficient solution, ensuring scalable and memory-efficient handling of Skype data.

