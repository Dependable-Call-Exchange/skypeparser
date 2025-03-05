# Skype Parser API Usage Guide

This document provides detailed information on how to use the Skype Parser API for integrating with front-end applications.

## Overview

The Skype Parser API provides a RESTful interface for processing Skype export files. It allows you to upload Skype export files, process them through the ETL pipeline, and retrieve the results.

## API Server Setup

### Installation

1. Install the required dependencies:

```bash
pip install -r src/api/requirements.txt
```

2. Set up environment variables:

```bash
export API_KEY=your_secure_api_key
export DB_NAME=skype
export DB_USER=postgres
export DB_PASSWORD=your_password
export CELERY_BROKER_URL=redis://localhost:6379/0
export CELERY_RESULT_BACKEND=redis://localhost:6379/0
```

3. Start Redis (required for asynchronous processing):

```bash
# Install Redis if not already installed
# On Ubuntu/Debian:
# sudo apt-get install redis-server

# On macOS with Homebrew:
# brew install redis

# Start Redis server
redis-server
```

4. Run the API server:

```bash
python -m src.api.run_api --host 0.0.0.0 --port 5000
```

5. Run a Celery worker for asynchronous processing:

```bash
python -m src.api.run_api --worker --worker-concurrency 2
```

### Command-line Options

The API server supports the following command-line options:

- `--host`: Host to bind to (default: 0.0.0.0)
- `--port`: Port to bind to (default: 5000)
- `--debug`: Run in debug mode
- `--upload-folder`: Directory for temporary file uploads
- `--output-folder`: Directory for ETL pipeline output
- `--api-key`: API key for authentication (default: from API_KEY environment variable)
- `--db-host`: Database host (default: localhost)
- `--db-port`: Database port (default: 5432)
- `--db-name`: Database name (default: from DB_NAME environment variable)
- `--db-user`: Database user (default: from DB_USER environment variable)
- `--db-password`: Database password (default: from DB_PASSWORD environment variable)
- `--log-level`: Logging level (default: INFO)
- `--async-threshold`: File size threshold for asynchronous processing in bytes (default: 50MB)
- `--redis-url`: Redis URL for Celery (default: redis://localhost:6379/0)
- `--worker`: Run a Celery worker instead of the API server
- `--worker-concurrency`: Number of worker processes (default: 2)

## API Endpoints

### Health Check

```
GET /api/health
```

Returns the current status of the API server.

#### Response

```json
{
  "status": "ok",
  "timestamp": "2023-03-05T12:34:56.789012"
}
```

### Upload and Process

```
POST /api/upload
```

Uploads a Skype export file and processes it through the ETL pipeline.

#### Request Headers

- `X-API-Key`: API key for authentication

#### Request Body

The request body should be a `multipart/form-data` request with the following fields:

- `file`: The Skype export file (TAR or JSON)
- `user_display_name` (optional): The display name of the user

#### Response

The response is a JSON object containing the results of the ETL pipeline:

```json
{
  "extraction": {
    "user_id": "live:user123",
    "export_date": "2023-03-05",
    "conversation_count": 42
  },
  "transformation": {
    "conversation_count": 42,
    "message_count": 1234
  },
  "loading": {
    "export_id": 123
  },
  "task_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

For large files (by default, files larger than 50MB), the API will process the file asynchronously. In this case, the response will be:

```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "processing",
  "message": "File is being processed asynchronously",
  "async": true
}
```

The `task_id` field is included in the response and can be used to subscribe to progress updates via WebSocket or to check the task status via the Task Status endpoint.

### Task Status

```
GET /api/task/<task_id>
```

Returns the current status of a task.

#### Request Headers

- `X-API-Key`: API key for authentication

#### Response

```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "completed",
  "message": "ETL pipeline completed successfully",
  "current_step": 100,
  "total_steps": 100,
  "percent_complete": 100.0,
  "elapsed_time": 42.5,
  "timestamp": 1646476800.0
}
```

## WebSocket API

The API also provides a WebSocket interface for real-time progress updates. This is implemented using Socket.IO.

### Connecting to the WebSocket Server

```javascript
// Connect to the Socket.IO server
const socket = io();

// Handle connection events
socket.on('connect', () => {
  console.log('Connected to server');
});

socket.on('disconnect', () => {
  console.log('Disconnected from server');
});

socket.on('connect_error', (error) => {
  console.error('Connection error:', error);
});
```

### Subscribing to Task Updates

```javascript
// Subscribe to task updates
socket.emit('subscribe', { task_id: taskId }, (response) => {
  if (response && response.error) {
    console.error('Error subscribing to task:', response.error);
    return;
  }

  // Handle initial progress data
  updateProgressUI(response);
});

// Listen for task progress updates
socket.on(`task_progress_${taskId}`, (data) => {
  // Update UI with progress data
  updateProgressUI(data);

  // If task is completed or failed, handle accordingly
  if (data.status === 'completed') {
    showResults();
  } else if (data.status === 'failed') {
    showError(data.message);
  }
});
```

### Progress Data Format

The progress data sent via WebSocket has the following format:

```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "transforming",
  "message": "Transformed 42 conversations",
  "current_step": 75,
  "total_steps": 100,
  "percent_complete": 75.0,
  "elapsed_time": 30.2,
  "timestamp": 1646476800.0
}
```

The `status` field can have the following values:
- `initializing`: The task is being initialized
- `starting`: The task is starting
- `extracting`: Data is being extracted from the Skype export file
- `transforming`: Data is being transformed
- `loading`: Data is being loaded into the database
- `completed`: The task has completed successfully
- `failed`: The task has failed

## Client Integration

### JavaScript Example

```javascript
async function uploadSkypeExport(file, userDisplayName, apiKey) {
  const formData = new FormData();
  formData.append('file', file);
  formData.append('user_display_name', userDisplayName);

  try {
    const response = await fetch('/api/upload', {
      method: 'POST',
      headers: {
        'X-API-Key': apiKey
      },
      body: formData
    });

    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(errorData.error || 'Upload failed');
    }

    return await response.json();
  } catch (error) {
    console.error('Error uploading file:', error);
    throw error;
  }
}
```

### Python Example

```python
import requests

def upload_skype_export(file_path, user_display_name, api_key, api_url):
    """
    Upload a Skype export file to the API.

    Args:
        file_path: Path to the Skype export file
        user_display_name: Display name of the user
        api_key: API key for authentication
        api_url: URL of the API server

    Returns:
        dict: Results of the ETL pipeline
    """
    with open(file_path, 'rb') as f:
        files = {'file': f}
        data = {'user_display_name': user_display_name}
        headers = {'X-API-Key': api_key}

        response = requests.post(
            f"{api_url}/api/upload",
            files=files,
            data=data,
            headers=headers
        )

        response.raise_for_status()
        return response.json()
```

## Front-end Example

A complete front-end example is provided in the `examples/frontend_example` directory. It includes:

- `index.html`: A simple HTML interface for uploading Skype export files
- `serve.py`: A server script that serves the HTML interface and proxies API requests

To run the example:

```bash
cd examples/frontend_example
python serve.py --host localhost --port 8080 --api-url http://localhost:5000
```

Then open a web browser and navigate to `http://localhost:8080`.

## Security Considerations

- Use HTTPS in production to protect API keys and data
- Generate a strong, random API key
- Store API keys securely
- Implement rate limiting to prevent abuse
- Validate and sanitize all inputs
- Use proper error handling to avoid leaking sensitive information

## Troubleshooting

### Common Issues

- **File too large**: The default maximum file size is 500MB. You can increase this by modifying the `MAX_CONTENT_LENGTH` constant in `src/api/skype_api.py`.
- **Database connection error**: Ensure that the database is running and the credentials are correct.
- **API key error**: Ensure that the API key is set correctly in both the server and client.

### Logging

The API server logs information to the console. You can increase the log level by setting the `LOGLEVEL` environment variable:

```bash
export LOGLEVEL=DEBUG
```

## Further Reading

- [OpenAPI Specification](../docs/API.md)
- [ETL Pipeline Documentation](../docs/ETL_PIPELINE.md)
- [Content Extraction Documentation](../docs/CONTENT_EXTRACTION.md)