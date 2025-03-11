# Skype Parser API Reference

This document provides a comprehensive reference for the Skype Parser web service. It details available endpoints, authentication methods, error handling, and usage examples, along with structured sections for clarity and consistency.

---

## Table of Contents

- [Skype Parser API Reference](#skype-parser-api-reference)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [API Overview](#api-overview)
  - [OpenAPI Specification](#openapi-specification)
  - [Authentication](#authentication)
    - [API Key Authentication](#api-key-authentication)
    - [Session Authentication](#session-authentication)
  - [Rate Limiting](#rate-limiting)
  - [File Size Limits](#file-size-limits)
  - [Asynchronous Processing](#asynchronous-processing)
  - [Error Handling](#error-handling)
  - [Endpoints](#endpoints)
    - [1. `POST /api/register` – Register a new user](#1-post-apiregister--register-a-new-user)
    - [2. `POST /api/login` – Log in](#2-post-apilogin--log-in)
    - [3. `POST /api/upload` – Upload a file (API key authentication)](#3-post-apiupload--upload-a-file-api-key-authentication)
    - [4. `GET /api/status/{task_id}` – Check task status](#4-get-apistatustask_id--check-task-status)
    - [5. `GET /api/analysis/{export_id}` – Get analysis data](#5-get-apianalysisexport_id--get-analysis-data)
    - [6. `GET /api/report/{export_id}` – Get HTML report](#6-get-apireportexport_id--get-html-report)
    - [7. `GET /api/exports` – List all exports](#7-get-apiexports--list-all-exports)
    - [8. `GET /api/profile` – Get user profile](#8-get-apiprofile--get-user-profile)
    - [9. `POST /api/regenerate-api-key` – Regenerate API key](#9-post-apiregenerate-api-key--regenerate-api-key)
    - [10. `POST /api/logout` – Log out](#10-post-apilogout--log-out)
  - [Example Usage](#example-usage)
    - [Using cURL](#using-curl)
    - [Using JavaScript (Browser)](#using-javascript-browser)
    - [Using Python](#using-python)
  - [Best Practices](#best-practices)

---

## Introduction

The **Skype Parser API** is a RESTful service for processing Skype export files, extracting conversation data, and analyzing communication patterns. It supports both **synchronous** and **asynchronous** modes, secure authentication, and real-time progress tracking.

**Key Features**:

- **Secure**: Supports API key and session-based authentication.
- **Scalable**: Handles large files through asynchronous processing.
- **Reliable**: Provides detailed error handling and reporting.
- **Developer-friendly**: Offers comprehensive documentation and examples.

---

## API Overview

The Skype Parser API follows REST principles and uses standard HTTP methods. All responses are in **JSON** format (except for the HTML report endpoint).

| Resource Type    | Endpoints                                                     | Description                            |
|------------------|---------------------------------------------------------------|----------------------------------------|
| **System**       | `/api/health`, `/api/version`                                | Check status/version info              |
| **Authentication** | `/api/register`, `/api/login`, `/api/logout`, `/api/profile`, `/api/regenerate-api-key` | User management & authentication |
| **ETL Pipeline** | `/api/upload`, `/api/status/{task_id}`, `/api/exports`       | File upload & processing               |
| **Analysis**     | `/api/analysis/{export_id}`, `/api/report/{export_id}`        | Data analysis & reporting              |

---

## OpenAPI Specification

Below is the condensed OpenAPI 3.0.3 specification for the Skype Parser API.

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
  # System endpoints
  /api/health:
    get:
      summary: Check the health of the API
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

  # ETL endpoints
  /api/upload:
    post:
      summary: Upload and process a Skype export file
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

  /api/exports:
    get:
      summary: List all exports
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

  # Analysis endpoints
  /api/analysis/{export_id}:
    get:
      summary: Get analysis data for an export
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

  # Authentication endpoints
  /api/register:
    post:
      summary: Register a new user
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
                password:
                  type: string
                email:
                  type: string
                display_name:
                  type: string
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
                password:
                  type: string
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

---

## Authentication

### API Key Authentication

For programmatic access, **attach the API key** to each request using an HTTP header:

```
X-API-Key: <your_api_key>
```

**Example**:

```http
GET /api/profile HTTP/1.1
Host: localhost:5000
X-API-Key: your_api_key
```

**Regenerating an API key**: Use the `/api/regenerate-api-key` endpoint.

> **Security Note**: Store your API keys securely (e.g., in environment variables or a secrets manager). **Never** commit them to version control.

### Session Authentication

For web-browser-based access, **log in** using `/api/login`. A session cookie is sent back and is managed automatically by the browser.

```http
POST /api/login HTTP/1.1
Host: localhost:5000
Content-Type: application/json

{
  "username": "johndoe",
  "password": "securepassword"
}
```

> **Security Note**: Always use **HTTPS** in production environments to protect both API keys and session cookies.

---

## Rate Limiting

The API enforces rate limits to prevent abuse. If you exceed the limit, you’ll get a **429** response with a `Retry-After` header indicating when you can try again.

---

## File Size Limits

The API enforces a file size limit of **50 MB**. Larger files cause a **413** (Payload Too Large) error.

---

## Asynchronous Processing

Files exceeding the **50 MB** threshold are processed asynchronously. The `/api/upload` response will include a `task_id` you can use with `/api/status/{task_id}` to track processing progress.

---

## Error Handling

All errors are returned as a JSON object with an `error` field:

- **400**: Bad Request (invalid input)
- **401**: Unauthorized (invalid credentials)
- **404**: Not Found (resource not found)
- **413**: Payload Too Large (file exceeds limit)
- **429**: Too Many Requests (rate limit exceeded)
- **500**: Internal Server Error (unexpected server error)

---

## Endpoints

Below is a structured overview of each primary endpoint.

### 1. `POST /api/register` – Register a new user

**Description**
Create a new user account by supplying `username`, `password`, `email`, and `display_name`.

**Request Body**
```json
{
  "username": "johndoe",
  "password": "securepassword",
  "email": "john.doe@example.com",
  "display_name": "John Doe"
}
```

**Response**
Returns a `200` status with a `UserProfile` JSON if successful.

### 2. `POST /api/login` – Log in

**Description**
Logs in an existing user with `username` and `password`. On success, returns a `UserProfile` object along with a session cookie.

**Request Body**
```json
{
  "username": "johndoe",
  "password": "securepassword"
}
```

**Response**
Returns a `200` status with user profile and sets a session cookie (`session=<cookie_value>`).

### 3. `POST /api/upload` – Upload a file (API key authentication)

**Description**
Uploads a Skype export file (TAR or JSON) and processes it through the ETL pipeline. Large files (>50MB) are handled asynchronously.

**Request (multipart/form-data)**
- **file**: The actual file (`tar` or `json`).
- **user_display_name**: (optional) A friendly name for the user associated with this data.

**Response**
- **Synchronous** (`ProcessingResult`) if file is small enough.
- **Asynchronous** (`TaskResponse` with `task_id`) if file is large.

### 4. `GET /api/status/{task_id}` – Check task status

**Description**
Retrieves the current status of an asynchronous processing task.

**Path Parameter**
- **task_id**: String identifier returned from `/api/upload`.

**Response**
A `TaskStatus` object with `status`, `progress`, `export_id` (if completed), or `error` (if failed).

### 5. `GET /api/analysis/{export_id}` – Get analysis data

**Description**
Retrieves analysis data for a completed Skype export.

**Path Parameter**
- **export_id**: Numeric ID for the processed export.

**Response**
JSON object containing:
- **message_count**: total messages
- **conversation_count**: total conversations
- **date_range**: start and end timestamps
- **top_contacts**: array of contacts with corresponding message counts

### 6. `GET /api/report/{export_id}` – Get HTML report

**Description**
Retrieves an HTML report for a completed Skype export.

**Path Parameter**
- **export_id**: Numeric ID for the processed export.

**Response**
HTML content (MIME type: `text/html`).

### 7. `GET /api/exports` – List all exports

**Description**
Lists all exports processed by the authenticated user.

**Response**
An array of export objects:
```json
[
  {
    "export_id": 123,
    "user_id": "johndoe",
    "export_date": "2025-03-10T12:00:00Z",
    "conversation_count": 10,
    "message_count": 100
  }
]
```

### 8. `GET /api/profile` – Get user profile

**Description**
Retrieves the user’s profile (including `username`, `email`, `display_name`, and `api_key`).

**Response**
A `UserProfile` JSON object.

### 9. `POST /api/regenerate-api-key` – Regenerate API key

**Description**
Generates a new API key for the currently authenticated user.

**Response**
JSON object with `api_key`.

### 10. `POST /api/logout` – Log out

**Description**
Logs out an authenticated session-based user by clearing the session cookie.

**Response**
JSON object with a message confirming logout.

---

## Example Usage

Below are sample code snippets demonstrating how to interact with the Skype Parser API using:

- [cURL](#using-curl)
- [JavaScript (Browser)](#using-javascript-browser)
- [Python](#using-python)

### Using cURL

```bash
# 1) Register a new user
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"username":"johndoe","password":"securepassword","email":"john.doe@example.com","display_name":"John Doe"}' \
  http://localhost:5000/api/register

# 2) Log in
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{"username":"johndoe","password":"securepassword"}' \
  http://localhost:5000/api/login

# 3) Upload a file (using API Key)
curl -X POST \
  -H "X-API-Key: your_api_key" \
  -F "file=@path/to/skype_export.tar" \
  -F "user_display_name=John Doe" \
  http://localhost:5000/api/upload

# 4) Check task status (asynchronous tasks)
curl -X GET \
  -H "X-API-Key: your_api_key" \
  http://localhost:5000/api/status/TASK_ID_HERE

# 5) Get analysis data
curl -X GET \
  -H "X-API-Key: your_api_key" \
  http://localhost:5000/api/analysis/EXPORT_ID_HERE

# 6) Get HTML report
curl -X GET \
  -H "X-API-Key: your_api_key" \
  http://localhost:5000/api/report/EXPORT_ID_HERE

# 7) List all exports
curl -X GET \
  -H "X-API-Key: your_api_key" \
  http://localhost:5000/api/exports

# 8) Get user profile
curl -X GET \
  -H "X-API-Key: your_api_key" \
  http://localhost:5000/api/profile

# 9) Regenerate API key
curl -X POST \
  -H "X-API-Key: your_api_key" \
  http://localhost:5000/api/regenerate-api-key

# 10) Log out
curl -X POST \
  -b "session=YOUR_SESSION_COOKIE" \
  http://localhost:5000/api/logout
```

### Using JavaScript (Browser)

```javascript
// 1) Register a new user
async function registerUser(username, password, email, displayName) {
  const response = await fetch('http://localhost:5000/api/register', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password, email, display_name: displayName })
  });
  return await response.json();
}

// 2) Log in
async function login(username, password) {
  const response = await fetch('http://localhost:5000/api/login', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ username, password }),
    credentials: 'include'
  });
  return await response.json();
}

// 3) Upload a file (API key)
async function uploadFile(file, userDisplayName, apiKey) {
  const formData = new FormData();
  formData.append('file', file);
  if (userDisplayName) {
    formData.append('user_display_name', userDisplayName);
  }

  const response = await fetch('http://localhost:5000/api/upload', {
    method: 'POST',
    headers: { 'X-API-Key': apiKey },
    body: formData
  });
  return await response.json();
}

// 4) Check task status
async function checkTaskStatus(taskId, apiKey) {
  const response = await fetch(`http://localhost:5000/api/status/${taskId}`, {
    method: 'GET',
    headers: { 'X-API-Key': apiKey }
  });
  return await response.json();
}

// 5) Get analysis data
async function getAnalysis(exportId, apiKey) {
  const response = await fetch(`http://localhost:5000/api/analysis/${exportId}`, {
    method: 'GET',
    headers: { 'X-API-Key': apiKey }
  });
  return await response.json();
}

// 6) Get HTML report
async function getReport(exportId, apiKey) {
  const response = await fetch(`http://localhost:5000/api/report/${exportId}`, {
    method: 'GET',
    headers: { 'X-API-Key': apiKey }
  });
  return await response.text();
}

// 7) List all exports
async function listExports(apiKey) {
  const response = await fetch('http://localhost:5000/api/exports', {
    method: 'GET',
    headers: { 'X-API-Key': apiKey }
  });
  return await response.json();
}

// 8) Get user profile
async function getProfile(apiKey) {
  const response = await fetch('http://localhost:5000/api/profile', {
    method: 'GET',
    headers: { 'X-API-Key': apiKey },
    credentials: 'include'
  });
  return await response.json();
}

// 9) Regenerate API key
async function regenerateApiKey(apiKey) {
  const response = await fetch('http://localhost:5000/api/regenerate-api-key', {
    method: 'POST',
    headers: { 'X-API-Key': apiKey }
  });
  return await response.json();
}

// 10) Log out
async function logout() {
  const response = await fetch('http://localhost:5000/api/logout', {
    method: 'POST',
    credentials: 'include'
  });
  return await response.json();
}
```

### Using Python

Below is an example Python client. You can integrate these functions into your script, web application, or CLI tool.

```python
import requests
import time

BASE_URL = 'http://localhost:5000'


# 1) Register a new user
def register_user(username, password, email, display_name):
    """
    Registers a new user on the Skype Parser service.

    :param username: (str) Desired username
    :param password: (str) Account password
    :param email: (str) User's email address
    :param display_name: (str) Display name
    :return: dict containing user profile info (including api_key)
    """
    url = f'{BASE_URL}/api/register'
    payload = {
        'username': username,
        'password': password,
        'email': email,
        'display_name': display_name
    }
    response = requests.post(url, json=payload)
    return response.json()


# 2) Log in (returns user profile and session cookie)
def login(username, password):
    """
    Logs in with provided credentials and returns user profile + cookies.

    :param username: (str) Username
    :param password: (str) Password
    :return: (user_profile_dict, cookies)
    """
    url = f'{BASE_URL}/api/login'
    response = requests.post(url, json={'username': username, 'password': password})
    return response.json(), response.cookies


# 3) Upload a file (with API key authentication)
def upload_file(file_path, user_display_name, api_key):
    """
    Uploads a Skype export file for ETL processing.

    :param file_path: (str) Path to the TAR or JSON file
    :param user_display_name: (str) (Optional) Display name of the user
    :param api_key: (str) API Key for authentication
    :return: dict (either TaskResponse if async or ProcessingResult if sync)
    """
    url = f'{BASE_URL}/api/upload'
    with open(file_path, 'rb') as file:
        files = {'file': file}
        data = {'user_display_name': user_display_name}
        headers = {'X-API-Key': api_key}
        response = requests.post(url, headers=headers, files=files, data=data)
    return response.json()


# 4) Check task status
def check_task_status(task_id, api_key):
    """
    Checks the status of an asynchronous ETL task.

    :param task_id: (str) The task_id returned by /api/upload
    :param api_key: (str) API Key for authentication
    :return: dict containing task status
    """
    url = f'{BASE_URL}/api/status/{task_id}'
    response = requests.get(url, headers={'X-API-Key': api_key})
    return response.json()


# 5) Get analysis data
def get_analysis(export_id, api_key):
    """
    Fetches analysis data for a completed Skype export.

    :param export_id: (int) The export_id indicating a completed export
    :param api_key: (str) API Key for authentication
    :return: dict containing the analysis results
    """
    url = f'{BASE_URL}/api/analysis/{export_id}'
    response = requests.get(url, headers={'X-API-Key': api_key})
    return response.json()


# 6) Get HTML report
def get_report(export_id, api_key):
    """
    Retrieves the HTML report for a completed Skype export.

    :param export_id: (int) The ID of the export
    :param api_key: (str) API Key for authentication
    :return: str containing HTML content
    """
    url = f'{BASE_URL}/api/report/{export_id}'
    response = requests.get(url, headers={'X-API-Key': api_key})
    return response.text


# 7) List all exports
def list_exports(api_key):
    """
    Lists all exports available to the authenticated user.

    :param api_key: (str) API Key for authentication
    :return: list of dict objects describing exports
    """
    url = f'{BASE_URL}/api/exports'
    response = requests.get(url, headers={'X-API-Key': api_key})
    return response.json()


# 8) Get user profile
def get_profile(api_key):
    """
    Retrieves the user profile (username, email, display_name, api_key).

    :param api_key: (str) API Key for authentication
    :return: dict containing profile information
    """
    url = f'{BASE_URL}/api/profile'
    response = requests.get(url, headers={'X-API-Key': api_key})
    return response.json()


# 9) Regenerate API key
def regenerate_api_key(api_key):
    """
    Regenerates the API key for the authenticated user.

    :param api_key: (str) Current API Key
    :return: dict containing the new api_key
    """
    url = f'{BASE_URL}/api/regenerate-api-key'
    response = requests.post(url, headers={'X-API-Key': api_key})
    return response.json()


# 10) Log out (session-based authentication)
def logout(session_cookie):
    """
    Logs out the user by clearing the session cookie.

    :param session_cookie: (str) The session cookie from /api/login
    :return: dict containing a logout confirmation message
    """
    url = f'{BASE_URL}/api/logout'
    response = requests.post(url, cookies={'session': session_cookie})
    return response.json()


# Example "main" usage
if __name__ == '__main__':
    # 1) Register a new user
    user_data = register_user('johndoe', 'securepassword', 'john.doe@example.com', 'John Doe')
    print("User registration response:", user_data)

    # Extract the API key
    api_key = user_data.get('api_key')

    # 3) Upload a file (could be small or large)
    result = upload_file('path/to/skype_export.tar', 'John Doe', api_key)

    # Handle asynchronous vs. synchronous response
    if 'task_id' in result:
        task_id = result['task_id']
        print(f"Processing asynchronously. Task ID = {task_id}")

        # Check status until completed
        while True:
            status = check_task_status(task_id, api_key)
            print(f"Status: {status['status']}, Progress: {status.get('progress', 0)}%")
            if status['status'] in ('completed', 'failed'):
                break
            time.sleep(1)

        if status['status'] == 'completed':
            export_id = status['export_id']
            # Now get analysis
            analysis = get_analysis(export_id, api_key)
            print("Analysis:", analysis)
            # Get the report
            report_html = get_report(export_id, api_key)
            with open('report.html', 'w') as f:
                f.write(report_html)

    elif 'export_id' in result:
        # Synchronous
        export_id = result['export_id']
        analysis = get_analysis(export_id, api_key)
        print("Analysis:", analysis)
        report_html = get_report(export_id, api_key)
        with open('report.html', 'w') as f:
            f.write(report_html)
```

---

## Best Practices

1. **Use HTTPS**
   Always run your services over HTTPS in production to protect API keys and session cookies.

2. **Store Secrets Safely**
   Store your API keys in environment variables or secret managers. **Never** hard-code them in source code.

3. **Check Upload Size**
   Before uploading, ensure your file meets the 50 MB limit. If not, split or compress it if possible.

4. **Handle Async Gracefully**
   For large files, the `/api/upload` call might return `task_id`. Poll `/api/status/{task_id}` periodically until completion or failure.

5. **Monitor Rate Limits**
   If you receive a **429** response, respect the `Retry-After` header to avoid being blocked.

6. **Log Out Securely**
   If using session auth, always log out after performing critical operations, especially in multi-user or shared environments.

---

**Document Version**: 1.0.0

_Last updated: 2025-03-10_