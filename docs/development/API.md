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

    TaskResponse:
      type: object
      properties:
        task_id:
          type: string
          description: ID of the asynchronous task
      required:
        - task_id

    TaskStatus:
      type: object
      properties:
        status:
          type: string
          enum: [pending, running, completed, failed]
          description: Current status of the task
        progress:
          type: integer
          description: Progress percentage (0-100)
        message:
          type: string
          description: Status message
        export_id:
          type: integer
          description: ID of the export in the database (when completed)
        error:
          type: string
          description: Error message (when failed)
      required:
        - status

    ProcessingResult:
      type: object
      properties:
        success:
          type: boolean
          description: Whether the processing was successful
        export_id:
          type: integer
          description: ID of the export in the database
        conversations:
          type: integer
          description: Number of conversations processed
        message_count:
          type: integer
          description: Total number of messages processed
        phases:
          type: object
          properties:
            extract:
              type: object
              properties:
                status:
                  type: string
                  enum: [pending, running, completed, failed]
                processed_conversations:
                  type: integer
                processed_messages:
                  type: integer
                duration_seconds:
                  type: number
            transform:
              type: object
              properties:
                status:
                  type: string
                  enum: [pending, running, completed, failed]
                processed_conversations:
                  type: integer
                processed_messages:
                  type: integer
                duration_seconds:
                  type: number
            load:
              type: object
              properties:
                status:
                  type: string
                  enum: [pending, running, completed, failed]
                processed_conversations:
                  type: integer
                processed_messages:
                  type: integer
                duration_seconds:
                  type: number
                export_id:
                  type: integer
      required:
        - success

    UserProfile:
      type: object
      properties:
        username:
          type: string
          description: Username
        email:
          type: string
          description: Email address
        display_name:
          type: string
          description: Display name
        api_key:
          type: string
          description: API key for authentication
      required:
        - username
        - email
        - display_name
        - api_key

    HealthCheck:
      type: object
      properties:
        status:
          type: string
          description: Status of the API
        timestamp:
          type: string
          format: date-time
          description: Current timestamp
        version:
          type: string
          description: API version
      required:
        - status
        - timestamp

paths:
  /api/health:
    get:
      summary: Check the health of the API
      description: |
        Returns the current status of the API server.
      operationId: healthCheck
      tags:
        - System
      responses:
        '200':
          description: API is healthy
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/HealthCheck'

  /api/version:
    get:
      summary: Get API version
      description: |
        Returns the current version of the API.
      operationId: getVersion
      tags:
        - System
      responses:
        '200':
          description: Version information
          content:
            application/json:
              schema:
                type: object
                properties:
                  version:
                    type: string
                    description: API version

  /api/upload:
    post:
      summary: Upload and process a Skype export file
      description: |
        Upload a Skype export file (TAR or JSON) and process it through the ETL pipeline.
        For files larger than the async threshold (default 50MB), processing will be done asynchronously.
      operationId: uploadFile
      security:
        - ApiKeyAuth: []
        - SessionAuth: []
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
          description: File accepted for processing
          content:
            application/json:
              schema:
                oneOf:
                  - $ref: '#/components/schemas/TaskResponse'
                  - $ref: '#/components/schemas/ProcessingResult'
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

  /api/status/{task_id}:
    get:
      summary: Get the status of an asynchronous task
      description: |
        Returns the current status of an asynchronous task.
      operationId: getTaskStatus
      security:
        - ApiKeyAuth: []
        - SessionAuth: []
      tags:
        - ETL
      parameters:
        - name: task_id
          in: path
          required: true
          schema:
            type: string
          description: ID of the task to check
      responses:
        '200':
          description: Task status
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/TaskStatus'
        '404':
          description: Task not found
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
        '500':
          description: Internal server error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'

  /api/analysis/{export_id}:
    get:
      summary: Get analysis data for an export
      description: |
        Returns analysis data for a processed Skype export.
      operationId: getAnalysis
      security:
        - ApiKeyAuth: []
        - SessionAuth: []
      tags:
        - Analysis
      parameters:
        - name: export_id
          in: path
          required: true
          schema:
            type: integer
          description: ID of the export to analyze
      responses:
        '200':
          description: Analysis data
          content:
            application/json:
              schema:
                type: object
                properties:
                  message_count:
                    type: integer
                    description: Total number of messages
                  conversation_count:
                    type: integer
                    description: Number of conversations
                  date_range:
                    type: object
                    properties:
                      start:
                        type: string
                        format: date-time
                      end:
                        type: string
                        format: date-time
                  top_contacts:
                    type: array
                    items:
                      type: object
                      properties:
                        name:
                          type: string
                        message_count:
                          type: integer
        '404':
          description: Export not found
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
        '500':
          description: Internal server error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'

  /api/report/{export_id}:
    get:
      summary: Get HTML report for an export
      description: |
        Returns an HTML report for a processed Skype export.
      operationId: getReport
      security:
        - ApiKeyAuth: []
        - SessionAuth: []
      tags:
        - Analysis
      parameters:
        - name: export_id
          in: path
          required: true
          schema:
            type: integer
          description: ID of the export to report on
      responses:
        '200':
          description: HTML report
          content:
            text/html:
              schema:
                type: string
        '404':
          description: Export not found
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
        '500':
          description: Internal server error
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'

  /api/exports:
    get:
      summary: List all exports
      description: |
        Returns a list of all exports for the authenticated user.
      operationId: listExports
      security:
        - ApiKeyAuth: []
        - SessionAuth: []
      tags:
        - ETL
      responses:
        '200':
          description: List of exports
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    export_id:
                      type: integer
                    user_id:
                      type: string
                    export_date:
                      type: string
                      format: date-time
                    conversation_count:
                      type: integer
                    message_count:
                      type: integer
        '401':
          description: Unauthorized
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

  /api/register:
    post:
      summary: Register a new user
      description: |
        Register a new user with username, password, email, and display name.
      operationId: registerUser
      tags:
        - Authentication
      requestBody:
        content:
          application/json:
            schema:
              type: object
              properties:
                username:
                  type: string
                  description: Username
                password:
                  type: string
                  description: Password
                email:
                  type: string
                  description: Email address
                display_name:
                  type: string
                  description: Display name
              required:
                - username
                - password
                - email
                - display_name
      responses:
        '200':
          description: User registered successfully
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/UserProfile'
        '400':
          description: Bad request
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

  /api/login:
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
          application/json:
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
        '200':
          description: Login successful
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/UserProfile'
        '401':
          description: Invalid username or password
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

  /api/logout:
    post:
      summary: Log out of the application
      description: |
        Log out of the application by clearing the session cookie.
      operationId: logout
      security:
        - SessionAuth: []
      tags:
        - Authentication
      responses:
        '200':
          description: Logout successful
          content:
            application/json:
              schema:
                type: object
                properties:
                  message:
                    type: string
                    example: "Logged out successfully"
        '401':
          description: Not logged in
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Error'

  /api/profile:
    get:
      summary: Get user profile
      description: |
        Returns the profile of the authenticated user.
      operationId: getProfile
      security:
        - ApiKeyAuth: []
        - SessionAuth: []
      tags:
        - Authentication
      responses:
        '200':
          description: User profile
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/UserProfile'
        '401':
          description: Unauthorized
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

  /api/regenerate-api-key:
    post:
      summary: Regenerate API key
      description: |
        Regenerates the API key for the authenticated user.
      operationId: regenerateApiKey
      security:
        - ApiKeyAuth: []
        - SessionAuth: []
      tags:
        - Authentication
      responses:
        '200':
          description: API key regenerated
          content:
            application/json:
              schema:
                type: object
                properties:
                  api_key:
                    type: string
                    description: New API key
        '401':
          description: Unauthorized
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
```

## Authentication

The API supports two authentication methods:

1. **API Key Authentication**: For programmatic access, use the `X-API-Key` header with a valid API key.
2. **Session Authentication**: For web browser access, use the session cookie obtained after logging in through the `/api/login` endpoint.

## Rate Limiting

The API implements rate limiting to prevent abuse. If you exceed the rate limit, you will receive a 429 response with a `Retry-After` header indicating when you can try again.

## File Size Limits

The API enforces a file size limit of 50MB for uploaded files. If you attempt to upload a larger file, you will receive a 413 response.

## Asynchronous Processing

Files larger than the async threshold (default: 50MB) will be processed asynchronously. In this case, the `/api/upload` endpoint will return a task ID that can be used to check the status of the processing with the `/api/status/{task_id}` endpoint.

## Error Handling

All errors are returned with a JSON object containing an `error` field with a descriptive message. The HTTP status code indicates the type of error:

- 400: Bad Request (invalid input)
- 401: Unauthorized (invalid credentials)
- 404: Not Found (resource not found)
- 413: Payload Too Large (file too large)
- 429: Too Many Requests (rate limit exceeded)
- 500: Internal Server Error (server-side error)

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
