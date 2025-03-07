# Skype Parser API Documentation

This document provides the API specification for the Skype Parser web service.

## OpenAPI Specification

```yaml
openapi: 3.0.3
info:
  title: Skype Parser API
  description: API for parsing and storing Skype export data
  version: 1.0.0
  contact:
    name: David Leathers
    url: https://github.com/yourusername/skype-parser
  license:
    name: MIT
    url: https://opensource.org/licenses/MIT

servers:
  - url: http://localhost:5000
    description: Local development server

security:
  - ApiKeyAuth: []
  - SessionAuth: []

components:
  securitySchemes:
    ApiKeyAuth:
      type: apiKey
      in: header
      name: X-API-Key
    SessionAuth:
      type: apiKey
      in: cookie
      name: session

  schemas:
    Error:
      type: object
      properties:
        error:
          type: string
          description: Error message
      required:
        - error

    ProcessingResult:
      type: object
      properties:
        extraction:
          type: object
          properties:
            userId:
              type: string
              description: User ID from the Skype export
            exportDate:
              type: string
              description: Export date from the Skype export
            conversationCount:
              type: integer
              description: Number of conversations in the export
        transformation:
          type: object
          properties:
            conversationCount:
              type: integer
              description: Number of conversations after transformation
            messageCount:
              type: integer
              description: Total number of messages across all conversations
        loading:
          type: object
          properties:
            exportId:
              type: integer
              description: ID of the export in the database
        export_id:
          type: integer
          description: ID of the export in the database
        conversations:
          type: array
          description: List of conversation IDs
        message_count:
          type: integer
          description: Total number of messages

paths:
  /api/upload:
    post:
      summary: Upload and process a Skype export file
      description: |
        Upload a Skype export file (TAR or JSON) and process it through the ETL pipeline.
        The file will be extracted, transformed, and loaded into the database.
      operationId: uploadFile
      security:
        - ApiKeyAuth: []
      tags:
        - ETL
      requestBody:
        content:
          multipart/form-data:
            schema:
              type: object
              properties:
                file:
                  type: string
                  format: binary
                  description: Skype export file (TAR or JSON)
                user_display_name:
                  type: string
                  description: Display name of the user
              required:
                - file
      responses:
        '200':
          description: File processed successfully
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/ProcessingResult'
        '400':
          description: Bad request
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
        '401':
          description: Unauthorized
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
        '413':
          description: File too large
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
        '429':
          description: Too many requests
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'
        '500':
          description: Internal server error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'

  /login:
    post:
      summary: Log in to the application
      description: |
        Log in to the application using a username and password.
        This will create a session cookie that can be used for authentication.
      operationId: login
      tags:
        - Authentication
      requestBody:
        content:
          application/x-www-form-urlencoded:
            schema:
              type: object
              properties:
                username:
                  type: string
                  description: Username
                password:
                  type: string
                  description: Password
              required:
                - username
                - password
      responses:
        '302':
          description: Redirect to the main page on success or back to login on failure
          headers:
            Set-Cookie:
              schema:
                type: string
                description: Session cookie
            Location:
              schema:
                type: string
                description: Redirect URL

  /logout:
    get:
      summary: Log out of the application
      description: |
        Log out of the application by clearing the session cookie.
      operationId: logout
      tags:
        - Authentication
      responses:
        '302':
          description: Redirect to the login page
          headers:
            Set-Cookie:
              schema:
                type: string
                description: Cleared session cookie
            Location:
              schema:
                type: string
                description: Redirect URL
```

## Authentication

The API supports two authentication methods:

1. **API Key Authentication**: For programmatic access, use the `X-API-Key` header with a valid API key.
2. **Session Authentication**: For web browser access, use the session cookie obtained after logging in.

## Rate Limiting

The API implements rate limiting to prevent abuse. If you exceed the rate limit, you will receive a 429 response with a `Retry-After` header indicating when you can try again.

## File Size Limits

The API enforces a file size limit of 50MB for uploaded files. If you attempt to upload a larger file, you will receive a 413 response.

## Error Handling

All errors are returned with a JSON object containing an `error` field with a descriptive message. The HTTP status code indicates the type of error:

- 400: Bad Request (invalid input)
- 401: Unauthorized (invalid credentials)
- 413: Payload Too Large (file too large)
- 429: Too Many Requests (rate limit exceeded)
- 500: Internal Server Error (server-side error)

## Example Usage

### Using cURL

```bash
# Upload a file with API key authentication
curl -X POST \
  -H "X-API-Key: your_api_key" \
  -F "file=@path/to/skype_export.tar" \
  -F "user_display_name=Your Name" \
  http://localhost:5000/api/upload

# Log in
curl -X POST \
  -F "username=admin" \
  -F "password=admin" \
  -c cookies.txt \
  http://localhost:5000/login

# Upload a file with session authentication
curl -X POST \
  -b cookies.txt \
  -F "file=@path/to/skype_export.tar" \
  -F "user_display_name=Your Name" \
  http://localhost:5000/api/upload

# Log out
curl -X GET \
  -b cookies.txt \
  http://localhost:5000/logout
```

### Using Python

```python
import requests

# Upload a file with API key authentication
url = "http://localhost:5000/api/upload"
headers = {"X-API-Key": "your_api_key"}
files = {"file": open("path/to/skype_export.tar", "rb")}
data = {"user_display_name": "Your Name"}

response = requests.post(url, headers=headers, files=files, data=data)
print(response.json())

# Log in
session = requests.Session()
login_url = "http://localhost:5000/login"
login_data = {"username": "admin", "password": "admin"}

response = session.post(login_url, data=login_data)

# Upload a file with session authentication
response = session.post(url, files=files, data=data)
print(response.json())

# Log out
session.get("http://localhost:5000/logout")
```