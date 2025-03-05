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
```

3. Run the API server:

```bash
python -m src.api.run_api --host 0.0.0.0 --port 5000
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
  }
}
```

#### Error Responses

- `400 Bad Request`: Invalid request or file
- `401 Unauthorized`: Invalid API key
- `413 Request Entity Too Large`: File too large
- `500 Internal Server Error`: Server error

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