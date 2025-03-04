# Skype to PostgreSQL Integration

This guide explains how to import your Skype conversation data into a PostgreSQL database using the `skype_to_postgres.py` script.

## Prerequisites

1. Python 3.6 or higher
2. PostgreSQL installed and running
3. The following Python packages:
   - psycopg2-binary
   - beautifulsoup4 (optional, for better HTML parsing)

You can install the required packages using pip:

```bash
pip install psycopg2-binary beautifulsoup4
```

## Database Setup

Before running the script, you need to create a PostgreSQL database:

```bash
# Connect to PostgreSQL
sudo -u postgres psql

# Create a database
CREATE DATABASE skype_logs;

# Create a user (optional)
CREATE USER skype_user WITH PASSWORD 'your_password';

# Grant privileges
GRANT ALL PRIVILEGES ON DATABASE skype_logs TO skype_user;

# Exit psql
\q
```

## Usage

The script can be run with the following command:

```bash
python skype_to_postgres.py -f <skype_export_file> -u <your_display_name> -d <database_name> [options]
```

### Required Arguments

- `-f, --filename`: Path to the Skype export file (JSON or TAR)
- `-d, --dbname`: PostgreSQL database name
- `-u, --user-display-name`: Your display name in the Skype logs

### Optional Arguments

- `-t, --tar`: Specify if the input file is a TAR archive
- `--select-json`: Select a specific JSON file from the TAR archive
- `-H, --host`: PostgreSQL host (default: localhost)
- `-P, --port`: PostgreSQL port (default: 5432)
- `-U, --username`: PostgreSQL username
- `-W, --password`: PostgreSQL password
- `--create-tables`: Create database tables if they do not exist

### Example

```bash
# Import from a TAR file and create tables
python skype_to_postgres.py -f 8_live_dave.leathers113_export.tar -t -u "David Leathers" -d skype_logs -U postgres --create-tables

# Import from a JSON file
python skype_to_postgres.py -f messages.json -u "David Leathers" -d skype_logs -U postgres
```

## Database Schema

The script creates the following tables in the PostgreSQL database:

### conversations

Stores metadata about each conversation (group chat or 1:1).

| Column          | Type      | Description                       |
|-----------------|-----------|-----------------------------------|
| conversation_id | VARCHAR   | Primary key, Skype thread ID      |
| display_name    | VARCHAR   | Human-readable name               |
| start_time      | TIMESTAMP | Earliest message timestamp        |
| end_time        | TIMESTAMP | Latest message timestamp          |

### participants

Stores user information.

| Column         | Type    | Description                  |
|----------------|---------|------------------------------|
| participant_id | VARCHAR | Primary key, Skype user ID   |
| display_name   | VARCHAR | Human-readable name          |

### conversation_participants

Links participants to conversations (many-to-many relationship).

| Column          | Type    | Description                                |
|-----------------|---------|-------------------------------------------|
| conversation_id | VARCHAR | Foreign key to conversations.conversation_id |
| participant_id  | VARCHAR | Foreign key to participants.participant_id   |

### messages

Stores the actual chat messages.

| Column          | Type      | Description                                |
|-----------------|-----------|-------------------------------------------|
| message_id      | BIGSERIAL | Primary key, auto-incrementing             |
| conversation_id | VARCHAR   | Foreign key to conversations.conversation_id |
| participant_id  | VARCHAR   | Foreign key to participants.participant_id   |
| timestamp_utc   | TIMESTAMP | Message timestamp in UTC                   |
| message_type    | VARCHAR   | Type of message (e.g., RichText, Event)    |
| is_edited       | BOOLEAN   | Whether the message was edited             |
| raw_content     | TEXT      | Original message content with HTML         |
| cleaned_content | TEXT      | Message content with HTML tags removed     |
| content_tsv     | TSVECTOR  | Text search vector for full-text search    |

## Example Queries

Once your data is imported, you can run various queries:

### List all conversations

```sql
SELECT conversation_id, display_name, start_time, end_time
FROM conversations
ORDER BY start_time DESC;
```

### List all messages in a conversation

```sql
SELECT m.timestamp_utc, p.display_name AS sender, m.cleaned_content
FROM messages m
JOIN participants p ON m.participant_id = p.participant_id
WHERE m.conversation_id = '19:some-thread-id@thread.skype'
ORDER BY m.timestamp_utc;
```

### Search for messages containing specific text

```sql
SELECT m.timestamp_utc, p.display_name AS sender, m.cleaned_content
FROM messages m
JOIN participants p ON m.participant_id = p.participant_id
WHERE m.content_tsv @@ to_tsquery('english', 'important & meeting')
ORDER BY m.timestamp_utc DESC;
```

### Count messages per conversation

```sql
SELECT c.display_name, COUNT(*) AS message_count
FROM messages m
JOIN conversations c ON m.conversation_id = c.conversation_id
GROUP BY c.display_name
ORDER BY message_count DESC;
```

### Count messages per participant

```sql
SELECT p.display_name, COUNT(*) AS message_count
FROM messages m
JOIN participants p ON m.participant_id = p.participant_id
GROUP BY p.display_name
ORDER BY message_count DESC;
```

## Maintenance

For optimal performance, periodically run:

```sql
VACUUM ANALYZE;
```

To back up your database:

```bash
pg_dump -U your_username -d skype_logs > skype_logs_backup.sql
```

## Known Limitations and Future Improvements

### Current Limitations

1. **Multiple JSON Files in TAR Archives**: The script currently processes only the first JSON file found in a TAR archive unless `--select-json` is specified. It doesn't support processing multiple JSON files in a single run.

2. **Edited Messages**: The script identifies edited messages by checking for the presence of 'skypeeditedid' in the message data, but doesn't track or store the edit history.

3. **Large Datasets**: While the script now processes messages in batches, extremely large datasets (millions of messages) might still require additional optimization.

4. **Security**: Passing database passwords on the command line can be insecure in shared environments. Consider using environment variables or connection strings for sensitive information.

5. **Language Support**: Full-text search is configured for English by default. For multilingual conversations, you might need to adjust the text search configuration.

### Planned Improvements

1. **Migration System**: Implement a proper database migration system for schema changes.

2. **Multiple JSON Processing**: Add support for processing multiple JSON files in a single TAR archive.

3. **Edit History**: Store edit history for messages that have been modified.

4. **Attachment Handling**: Improve support for message attachments and media.

5. **Performance Optimizations**: Further optimize for very large datasets.

6. **Testing Framework**: Add comprehensive tests for parsing and database operations.

7. **User Interface**: Develop a web interface for browsing and searching imported messages.

## Contributing

Contributions to improve the script are welcome! Please feel free to submit pull requests or open issues for bugs and feature requests.

## Troubleshooting

### Connection Issues

If you encounter connection issues, check:
- PostgreSQL service is running
- Database name, username, and password are correct
- PostgreSQL is configured to accept connections from your IP address (check pg_hba.conf)

### Import Errors

If you encounter errors during import:
- Check that the Skype export file is valid
- Ensure you have sufficient disk space
- Check PostgreSQL logs for detailed error messages

## License

This script is provided under the same license as the skype-parser project.