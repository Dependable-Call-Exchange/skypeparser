# Skype Parser API Usage Guide

This document provides detailed information on how to use the Skype Parser API for integrating with front-end applications.

## Overview

The Skype Parser API provides a RESTful interface for processing Skype export files. It allows you to upload Skype export files, process them through the ETL pipeline, and retrieve the results. The API supports both synchronous and asynchronous processing, depending on the file size.

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
export SECRET_KEY=your_secure_secret_key
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
- `--user-file`: Path to the user data file (default: users.json)
- `--secret-key`: Secret key for session encryption (default: from SECRET_KEY environment variable or random)
- `--create-user`: Create a new user
- `--username`: Username for the new user
- `--password`: Password for the new user
- `--email`: Email for the new user
- `--display-name`: Display name for the new user
- `--list-users`: List all users

## Authentication

The API supports two authentication methods:

1. **API Key Authentication**: For programmatic access, use the `X-API-Key` header with a valid API key.
2. **Session Authentication**: For web browser access, use the session cookie obtained after logging in through the `/api/login` endpoint.

## API Endpoints

### System Endpoints

#### Health Check

```
GET /api/health
```

Returns the current status of the API server.

##### Response

```json
{
  "status": "ok",
  "timestamp": "2023-03-05T12:34:56.789012",
  "version": "1.0.0"
}
```

#### Version

```
GET /api/version
```

Returns the current version of the API.

##### Response

```json
{
  "version": "1.0.0"
}
```

### User Management

#### Register

```
POST /api/register
```

Registers a new user.

##### Request Body

```json
{
  "username": "johndoe",
  "password": "securepassword",
  "email": "john.doe@example.com",
  "display_name": "John Doe"
}
```

##### Response

```json
{
  "username": "johndoe",
  "email": "john.doe@example.com",
  "display_name": "John Doe",
  "api_key": "550e8400e29b41d4a716446655440000"
}
```

#### Login

```
POST /api/login
```

Logs in a user and creates a session.

##### Request Body

```json
{
  "username": "johndoe",
  "password": "securepassword"
}
```

##### Response

```json
{
  "username": "johndoe",
  "email": "john.doe@example.com",
  "display_name": "John Doe",
  "api_key": "550e8400e29b41d4a716446655440000"
}
```

#### Logout

```
POST /api/logout
```

Logs out the current user by clearing the session.

##### Response

```json
{
  "message": "Logged out successfully"
}
```

#### Profile

```
GET /api/profile
```

Returns the profile of the authenticated user.

##### Request Headers

- `X-API-Key`: API key for authentication

##### Response

```json
{
  "username": "johndoe",
  "email": "john.doe@example.com",
  "display_name": "John Doe",
  "api_key": "550e8400e29b41d4a716446655440000"
}
```

#### Regenerate API Key

```
POST /api/regenerate-api-key
```

Regenerates the API key for the authenticated user.

##### Request Headers

- `X-API-Key`: API key for authentication

##### Response

```json
{
  "api_key": "550e8400e29b41d4a716446655440000"
}
```

### ETL Pipeline

#### Upload and Process

```
POST /api/upload
```

Uploads a Skype export file and processes it through the ETL pipeline. For files larger than the async threshold (default: 50MB), processing will be done asynchronously.

##### Request Headers

- `X-API-Key`: API key for authentication

##### Request Body

The request body should be a `multipart/form-data` request with the following fields:

- `file`: The Skype export file (TAR or JSON)
- `user_display_name` (optional): The display name of the user

##### Response (Synchronous Processing)

For smaller files, the response is a JSON object containing the results of the ETL pipeline:

```json
{
  "success": true,
  "export_id": 123,
  "conversations": 42,
  "message_count": 1234,
  "phases": {
    "extract": {
      "status": "completed",
      "processed_conversations": 42,
      "processed_messages": 1234,
      "duration_seconds": 1.5
    },
    "transform": {
      "status": "completed",
      "processed_conversations": 42,
      "processed_messages": 1234,
      "duration_seconds": 2.3
    },
    "load": {
      "status": "completed",
      "processed_conversations": 42,
      "processed_messages": 1234,
      "duration_seconds": 3.1,
      "export_id": 123
    }
  }
}
```

##### Response (Asynchronous Processing)

For larger files, the response contains a task ID that can be used to check the status of the processing:

```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000"
}
```

#### Task Status

```
GET /api/status/{task_id}
```

Returns the current status of an asynchronous task.

##### Request Headers

- `X-API-Key`: API key for authentication

##### Response

```json
{
  "status": "running",
  "progress": 50,
  "message": "Processing conversations..."
}
```

When the task is completed:

```json
{
  "status": "completed",
  "progress": 100,
  "message": "Processing completed successfully",
  "export_id": 123
}
```

If the task fails:

```json
{
  "status": "failed",
  "progress": 75,
  "message": "Processing failed",
  "error": "Error message"
}
```

#### List Exports

```
GET /api/exports
```

Returns a list of all exports for the authenticated user.

##### Request Headers

- `X-API-Key`: API key for authentication

##### Response

```json
[
  {
    "export_id": 123,
    "user_id": "live:user123",
    "export_date": "2023-03-05T12:34:56.789012",
    "conversation_count": 42,
    "message_count": 1234
  },
  {
    "export_id": 124,
    "user_id": "live:user123",
    "export_date": "2023-03-06T12:34:56.789012",
    "conversation_count": 43,
    "message_count": 1235
  }
]
```

### Analysis

#### Get Analysis Data

```
GET /api/analysis/{export_id}
```

Returns analysis data for a processed Skype export.

##### Request Headers

- `X-API-Key`: API key for authentication

##### Response

```json
{
  "message_count": 1234,
  "conversation_count": 42,
  "date_range": {
    "start": "2022-01-01T00:00:00.000000",
    "end": "2023-03-05T12:34:56.789012"
  },
  "top_contacts": [
    {
      "name": "Jane Doe",
      "message_count": 567
    },
    {
      "name": "John Smith",
      "message_count": 456
    },
    {
      "name": "Bob Johnson",
      "message_count": 345
    }
  ]
}
```

#### Get HTML Report

```
GET /api/report/{export_id}
```

Returns an HTML report for a processed Skype export.

##### Request Headers

- `X-API-Key`: API key for authentication

##### Response

The response is an HTML document containing a report of the Skype export data.

## Real-time Updates with Socket.IO

The API also supports real-time updates using Socket.IO. This allows clients to receive progress updates for asynchronous tasks without polling the API.

### Connecting to the Socket.IO Server

```javascript
const socket = io('http://localhost:5000');

socket.on('connect', () => {
  console.log('Connected to Socket.IO server');
});

socket.on('disconnect', () => {
  console.log('Disconnected from Socket.IO server');
});
```

### Subscribing to Task Updates

```javascript
// Subscribe to updates for a specific task
socket.emit('subscribe', { task_id: '550e8400-e29b-41d4-a716-446655440000' });

// Listen for task progress updates
socket.on('task_progress_550e8400-e29b-41d4-a716-446655440000', (data) => {
  console.log('Task progress:', data);
});
```

## Error Handling

All errors are returned with a JSON object containing an `error` field with a descriptive message. The HTTP status code indicates the type of error:

- 400: Bad Request (invalid input)
- 401: Unauthorized (invalid credentials)
- 404: Not Found (resource not found)
- 413: Payload Too Large (file too large)
- 429: Too Many Requests (rate limit exceeded)
- 500: Internal Server Error (server-side error)

Example error response:

```json
{
  "error": "Invalid API key"
}
```

## Rate Limiting

The API implements rate limiting to prevent abuse. If you exceed the rate limit, you will receive a 429 response with a `Retry-After` header indicating when you can try again.

## File Size Limits

The API enforces a file size limit of 50MB for uploaded files. If you attempt to upload a larger file, you will receive a 413 response.

## Example Usage

### Using cURL

```bash
# Register a new user
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"username":"johndoe","password":"securepassword","email":"john.doe@example.com","display_name":"John Doe"}' \
  http://localhost:5000/api/register

# Log in
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"username":"johndoe","password":"securepassword"}' \
  http://localhost:5000/api/login

# Upload a file with API key authentication
curl -X POST \
  -H "X-API-Key: your_api_key" \
  -F "file=@path/to/skype_export.tar" \
  -F "user_display_name=Your Name" \
  http://localhost:5000/api/upload

# Check task status
curl -X GET \
  -H "X-API-Key: your_api_key" \
  http://localhost:5000/api/status/task_id_from_upload_response

# Get analysis data
curl -X GET \
  -H "X-API-Key: your_api_key" \
  http://localhost:5000/api/analysis/export_id_from_task_status

# Get HTML report
curl -X GET \
  -H "X-API-Key: your_api_key" \
  http://localhost:5000/api/report/export_id_from_task_status

# List all exports
curl -X GET \
  -H "X-API-Key: your_api_key" \
  http://localhost:5000/api/exports

# Get user profile
curl -X GET \
  -H "X-API-Key: your_api_key" \
  http://localhost:5000/api/profile

# Regenerate API key
curl -X POST \
  -H "X-API-Key: your_api_key" \
  http://localhost:5000/api/regenerate-api-key

# Log out
curl -X POST \
  -b "session=your_session_cookie" \
  http://localhost:5000/api/logout
```

### Using JavaScript (Browser)

```javascript
// Register a new user
async function registerUser(username, password, email, displayName) {
  const response = await fetch('http://localhost:5000/api/register', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      username,
      password,
      email,
      display_name: displayName
    })
  });
  return await response.json();
}

// Log in
async function login(username, password) {
  const response = await fetch('http://localhost:5000/api/login', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({
      username,
      password
    }),
    credentials: 'include'
  });
  return await response.json();
}

// Upload a file
async function uploadFile(file, userDisplayName, apiKey) {
  const formData = new FormData();
  formData.append('file', file);
  if (userDisplayName) {
    formData.append('user_display_name', userDisplayName);
  }

  const response = await fetch('http://localhost:5000/api/upload', {
    method: 'POST',
    headers: {
      'X-API-Key': apiKey
    },
    body: formData
  });
  return await response.json();
}

// Check task status
async function checkTaskStatus(taskId, apiKey) {
  const response = await fetch(`http://localhost:5000/api/status/${taskId}`, {
    method: 'GET',
    headers: {
      'X-API-Key': apiKey
    }
  });
  return await response.json();
}

// Get analysis data
async function getAnalysis(exportId, apiKey) {
  const response = await fetch(`http://localhost:5000/api/analysis/${exportId}`, {
    method: 'GET',
    headers: {
      'X-API-Key': apiKey
    }
  });
  return await response.json();
}

// Get HTML report
async function getReport(exportId, apiKey) {
  const response = await fetch(`http://localhost:5000/api/report/${exportId}`, {
    method: 'GET',
    headers: {
      'X-API-Key': apiKey
    }
  });
  return await response.text();
}

// List all exports
async function listExports(apiKey) {
  const response = await fetch('http://localhost:5000/api/exports', {
    method: 'GET',
    headers: {
      'X-API-Key': apiKey
    }
  });
  return await response.json();
}

// Get user profile
async function getProfile(apiKey) {
  const response = await fetch('http://localhost:5000/api/profile', {
    method: 'GET',
    headers: {
      'X-API-Key': apiKey
    },
    credentials: 'include'
  });
  return await response.json();
}

// Regenerate API key
async function regenerateApiKey(apiKey) {
  const response = await fetch('http://localhost:5000/api/regenerate-api-key', {
    method: 'POST',
    headers: {
      'X-API-Key': apiKey
    }
  });
  return await response.json();
}

// Log out
async function logout() {
  const response = await fetch('http://localhost:5000/api/logout', {
    method: 'POST',
    credentials: 'include'
  });
  return await response.json();
}
```

### Using Python

```python
import requests

# Register a new user
def register_user(username, password, email, display_name):
    response = requests.post(
        'http://localhost:5000/api/register',
        json={
            'username': username,
            'password': password,
            'email': email,
            'display_name': display_name
        }
    )
    return response.json()

# Log in
def login(username, password):
    response = requests.post(
        'http://localhost:5000/api/login',
        json={
            'username': username,
            'password': password
        }
    )
    return response.json(), response.cookies

# Upload a file with API key authentication
def upload_file(file_path, user_display_name, api_key):
    with open(file_path, 'rb') as file:
        response = requests.post(
            'http://localhost:5000/api/upload',
            headers={'X-API-Key': api_key},
            files={'file': file},
            data={'user_display_name': user_display_name}
        )
    return response.json()

# Check task status
def check_task_status(task_id, api_key):
    response = requests.get(
        f'http://localhost:5000/api/status/{task_id}',
        headers={'X-API-Key': api_key}
    )
    return response.json()

# Get analysis data
def get_analysis(export_id, api_key):
    response = requests.get(
        f'http://localhost:5000/api/analysis/{export_id}',
        headers={'X-API-Key': api_key}
    )
    return response.json()

# Get HTML report
def get_report(export_id, api_key):
    response = requests.get(
        f'http://localhost:5000/api/report/{export_id}',
        headers={'X-API-Key': api_key}
    )
    return response.text

# List all exports
def list_exports(api_key):
    response = requests.get(
        'http://localhost:5000/api/exports',
        headers={'X-API-Key': api_key}
    )
    return response.json()

# Get user profile
def get_profile(api_key):
    response = requests.get(
        'http://localhost:5000/api/profile',
        headers={'X-API-Key': api_key}
    )
    return response.json()

# Regenerate API key
def regenerate_api_key(api_key):
    response = requests.post(
        'http://localhost:5000/api/regenerate-api-key',
        headers={'X-API-Key': api_key}
    )
    return response.json()

# Log out
def logout(session_cookie):
    response = requests.post(
        'http://localhost:5000/api/logout',
        cookies={'session': session_cookie}
    )
    return response.json()

# Example usage
if __name__ == '__main__':
    # Register a new user
    user_data = register_user('johndoe', 'securepassword', 'john.doe@example.com', 'John Doe')
    api_key = user_data['api_key']

    # Upload a file
    result = upload_file('path/to/skype_export.tar', 'John Doe', api_key)

    # If asynchronous processing
    if 'task_id' in result:
        task_id = result['task_id']

        # Check task status until completed
        import time
        while True:
            status = check_task_status(task_id, api_key)
            print(f"Status: {status['status']}, Progress: {status.get('progress', 0)}%")

            if status['status'] in ('completed', 'failed'):
                break

            time.sleep(1)

        if status['status'] == 'completed':
            export_id = status['export_id']

            # Get analysis data
            analysis = get_analysis(export_id, api_key)
            print(f"Analysis: {analysis}")

            # Get HTML report
            report = get_report(export_id, api_key)
            with open('report.html', 'w') as f:
                f.write(report)

    # If synchronous processing
    elif 'export_id' in result:
        export_id = result['export_id']

        # Get analysis data
        analysis = get_analysis(export_id, api_key)
        print(f"Analysis: {analysis}")

        # Get HTML report
        report = get_report(export_id, api_key)
        with open('report.html', 'w') as f:
            f.write(report)
