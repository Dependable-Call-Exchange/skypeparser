# Processing Large Skype Export Files

## Overview

Skype exports can vary significantly in size, from a few megabytes to several gigabytes, depending on the user's chat history. Large exports can contain millions of messages across thousands of conversations. This guide explains efficient approaches for handling these large files.

## Challenges with Large Exports

When working with large Skype export files, you may encounter several challenges:

1. **Memory Limitations**: Loading the entire export file into memory can cause Out-Of-Memory (OOM) errors
2. **Processing Time**: Parsing millions of messages can take a long time
3. **System Resources**: High CPU and disk I/O usage during processing
4. **Temporary Storage**: Extracting large TAR files requires sufficient disk space

## Streaming Processing Approach

The SkypeParser now includes a streaming processing approach that addresses these challenges by:

1. Reading data incrementally instead of loading everything at once
2. Processing one conversation at a time
3. Minimizing memory usage through efficient data handling
4. Using garbage collection to free memory during processing

## Available Streaming Methods

### 1. parse_skype_data_streaming()

This method provides a memory-efficient way to analyze Skype exports without loading the entire file into memory.

```python
from src.parser.core_parser import parse_skype_data_streaming

# Process a large export file
stats = parse_skype_data_streaming(
    file_path='path/to/large_export.tar',
    user_display_name='Your Name',
    callback=lambda key, value: print(f"Progress: {key}={value}")
)

# Access the statistics
print(f"Conversations: {stats['conversation_count']}")
print(f"Messages: {stats['message_count']}")
print(f"Processing time: {stats['duration_seconds']} seconds")
```

The method returns statistics about the export without storing all conversations and messages in memory.

### 2. stream_conversations()

For more granular processing, you can stream individual conversations and process them one at a time:

```python
from src.parser.core_parser import stream_conversations

# Process conversations one by one
for conversation in stream_conversations('path/to/large_export.tar'):
    # Process each conversation individually
    conversation_id = conversation.get('id')
    messages = conversation.get('MessageList', [])

    # Do something with the conversation data
    print(f"Processing conversation {conversation_id} with {len(messages)} messages")

    # You can also filter conversations based on criteria
    if len(messages) > 100:
        # Process only conversations with more than 100 messages
        # ...
```

This approach allows you to implement custom filtering and processing logic while maintaining memory efficiency.

## Command Line Usage

The `stream_skype_data.py` script provides a command-line interface for processing large exports:

```bash
python scripts/stream_skype_data.py -f your_export.tar -u "Your Name" -v
```

Arguments:
- `-f, --file`: Path to the Skype export file (TAR or JSON)
- `-u, --user`: Your display name as it appears in Skype
- `-v, --verbose`: Enable verbose logging

## Performance Considerations

### Memory Optimization

- The streaming process periodically triggers garbage collection to free memory
- Progress updates are logged at regular intervals (every 100 conversations, 10,000 messages)
- Temporary files are cleaned up after processing

### When to Use Streaming vs. Standard Processing

| Factor | Standard Processing | Streaming Processing |
|--------|---------------------|---------------------|
| Export Size | < 500 MB | > 500 MB |
| Memory Available | High | Limited |
| Processing Needs | Full data analysis | Statistics/Filtering |
| Conversations | < 1,000 | > 1,000 |
| Messages | < 100,000 | > 100,000 |

## Requirements

To use the streaming functionality, you need to install the `ijson` library:

```bash
pip install ijson
```

## Troubleshooting

### Common Issues

1. **"ijson library is required"**: Install the ijson library using pip
2. **Slow processing**: Check disk I/O performance; consider processing on an SSD
3. **Temporary directory errors**: Ensure write permissions in the directory
4. **JSON parsing errors**: Skype export format may have changed; check for updates

## Advanced Usage: Custom Processing Pipelines

For advanced use cases, you can create custom processing pipelines that analyze data while streaming:

```python
from src.parser.core_parser import stream_conversations
from collections import Counter

# Create analyzers
word_counter = Counter()
user_message_count = Counter()

# Stream and analyze
for conversation in stream_conversations('path/to/large_export.tar'):
    for message in conversation.get('MessageList', []):
        # Count messages per user
        sender = message.get('from', {}).get('username', 'unknown')
        user_message_count[sender] += 1

        # Count words (simplified example)
        content = message.get('content', '')
        if isinstance(content, str):
            words = content.lower().split()
            word_counter.update(words)

# Output results
print("Top 10 most active users:")
for user, count in user_message_count.most_common(10):
    print(f"{user}: {count} messages")

print("\nTop 20 most common words:")
for word, count in word_counter.most_common(20):
    print(f"{word}: {count}")
```

This approach allows you to perform sophisticated analysis on large datasets without memory constraints.
