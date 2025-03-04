#!/usr/bin/env python3
"""
Web Integration Example for Skype ETL Pipeline

This example demonstrates how to integrate the Skype ETL pipeline into a web application
using Flask. It provides a simple web interface for uploading Skype export files and
processing them through the ETL pipeline.

Features:
- User authentication with session management
- File upload with validation
- ETL pipeline integration
- Error handling and user feedback
- Rate limiting to prevent abuse
- CSRF protection
"""

import os
import sys
import logging
import secrets
import time
from functools import wraps
from pathlib import Path
from typing import Callable
from werkzeug.utils import secure_filename
from werkzeug.security import generate_password_hash, check_password_hash
from flask import (
    Flask, request, session, redirect, url_for, render_template_string,
    flash, jsonify
)

# Add the parent directory to the path so we can import the package
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# Import the ETL pipeline and validation functions
from src.db.etl_pipeline import SkypeETLPipeline
from src.utils.validation import (
    ValidationError,
    validate_file_object,
    validate_user_display_name,
    validate_db_config,
    validate_directory
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Configuration
UPLOAD_FOLDER = 'uploads'
OUTPUT_FOLDER = 'output'
ALLOWED_EXTENSIONS = {'.json', '.tar'}
MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50MB

# Create upload and output directories
try:
    validate_directory(UPLOAD_FOLDER, create_if_missing=True)
    validate_directory(OUTPUT_FOLDER, create_if_missing=True)
except ValidationError as e:
    logger.error(f"Directory validation error: {e}")
    sys.exit(1)

# Database configuration
db_config = {
    'dbname': os.environ.get('DB_NAME', 'skype'),
    'user': os.environ.get('DB_USER', 'postgres'),
    'password': os.environ.get('DB_PASSWORD', 'postgres'),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': int(os.environ.get('DB_PORT', 5432))
}

# Validate database configuration
try:
    validate_db_config(db_config)
except ValidationError as e:
    logger.warning(f"Database configuration validation error: {e}")
    logger.warning("Database operations will be disabled")
    db_config = None

# Create Flask app
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH
app.secret_key = os.environ.get('SECRET_KEY', secrets.token_hex(16))

# Simple user database for authentication
# In a real application, this would be stored in a database
users = {
    'admin': {
        'password': generate_password_hash('admin'),
        'rate_limit': {
            'last_request_time': 0,
            'request_count': 0
        }
    },
    'user': {
        'password': generate_password_hash('password'),
        'rate_limit': {
            'last_request_time': 0,
            'request_count': 0
        }
    }
}

# Rate limiting configuration
RATE_LIMIT_WINDOW = 60  # seconds
RATE_LIMIT_MAX_REQUESTS = 10  # requests per window

# Login required decorator
def login_required(f: Callable) -> Callable:
    """
    Decorator to require login for a route.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            flash('Please log in to access this page')
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

# Rate limiting decorator
def rate_limit(f: Callable) -> Callable:
    """
    Decorator to apply rate limiting to a route.
    """
    @wraps(f)
    def decorated_function(*args, **kwargs):
        username = session.get('username')
        if not username or username not in users:
            return redirect(url_for('login'))

        user = users[username]
        current_time = time.time()

        # Reset count if window has passed
        if current_time - user['rate_limit']['last_request_time'] > RATE_LIMIT_WINDOW:
            user['rate_limit']['request_count'] = 0
            user['rate_limit']['last_request_time'] = current_time

        # Check if rate limit exceeded
        if user['rate_limit']['request_count'] >= RATE_LIMIT_MAX_REQUESTS:
            return jsonify({
                'error': f'Rate limit exceeded. Please try again in {int(RATE_LIMIT_WINDOW - (current_time - user["rate_limit"]["last_request_time"]))} seconds'
            }), 429

        # Update rate limit
        user['rate_limit']['request_count'] += 1

        return f(*args, **kwargs)
    return decorated_function

# CSRF token generation
def generate_csrf_token() -> str:
    """
    Generate a CSRF token for form protection.
    """
    if 'csrf_token' not in session:
        session['csrf_token'] = secrets.token_hex(16)
    return session['csrf_token']

# Check if a file has an allowed extension
def allowed_file(filename: str) -> bool:
    """
    Check if a file has an allowed extension.

    Args:
        filename (str): The filename to check

    Returns:
        bool: True if the file has an allowed extension
    """
    if not filename:
        return False

    _, ext = os.path.splitext(filename.lower())
    return ext in ALLOWED_EXTENSIONS

# Login page
@app.route('/login', methods=['GET', 'POST'])
def login():
    """
    Handle user login.
    """
    error = None

    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        if not username or not password:
            error = 'Please provide both username and password'
        elif username not in users:
            error = 'Invalid username or password'
        elif not check_password_hash(users[username]['password'], password):
            error = 'Invalid username or password'
        else:
            session['username'] = username
            session['csrf_token'] = secrets.token_hex(16)
            flash('You were successfully logged in')
            return redirect(url_for('index'))

    return render_template_string(
        LOGIN_TEMPLATE,
        error=error
    )

# Logout route
@app.route('/logout')
def logout():
    """
    Handle user logout.
    """
    session.pop('username', None)
    session.pop('csrf_token', None)
    flash('You were successfully logged out')
    return redirect(url_for('login'))

# Main page
@app.route('/')
@login_required
def index():
    """
    Display the main page with file upload form.
    """
    return render_template_string(
        UPLOAD_TEMPLATE,
        csrf_token=session['csrf_token'],
        username=session.get('username')
    )

# File upload handler
@app.route('/upload', methods=['POST'])
@login_required
@rate_limit
def upload_file():
    """
    Handle file upload and process it through the ETL pipeline.
    """
    result = {'error': None}

    try:
        # Verify CSRF token
        token = request.form.get('csrf_token')
        if not token or token != session.get('csrf_token'):
            return jsonify({'error': 'Invalid CSRF token'}), 403

        # Check if a file was uploaded
        if 'file' not in request.files:
            result['error'] = 'No file part'
            return render_template_string(UPLOAD_TEMPLATE, result=result, csrf_token=session['csrf_token'])

        file = request.files['file']

        # Check if the file is empty
        if file.filename == '':
            result['error'] = 'No file selected'
            return render_template_string(UPLOAD_TEMPLATE, result=result, csrf_token=session['csrf_token'])

        # Check if the file type is allowed
        if not allowed_file(file.filename):
            result['error'] = 'File type not allowed. Please upload a .tar or .json file.'
            return render_template_string(UPLOAD_TEMPLATE, result=result, csrf_token=session['csrf_token'])

        # Secure the filename
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)

        # Save the file temporarily
        file.save(file_path)

        # Get user display name
        user_display_name = request.form.get('user_display_name', session.get('username', ''))

        # Validate user display name
        try:
            user_display_name = validate_user_display_name(user_display_name)
        except ValidationError as e:
            logger.warning(f"User display name validation error: {e}")
            user_display_name = session.get('username', '')

        # Process the file through the ETL pipeline
        pipeline = SkypeETLPipeline(
            db_config=db_config,
            output_dir=OUTPUT_FOLDER
        )

        # Run the pipeline with the uploaded file
        results = pipeline.run_pipeline(
            file_path=file_path,
            user_display_name=user_display_name
        )

        # Clean up the temporary file
        try:
            os.remove(file_path)
        except Exception as e:
            logger.warning(f"Failed to remove temporary file {file_path}: {e}")

        # Return the results
        return render_template_string(
            UPLOAD_TEMPLATE,
            result=results,
            csrf_token=session['csrf_token'],
            username=session.get('username')
        )

    except ValidationError as e:
        logger.error(f"Validation error: {e}", exc_info=True)
        result['error'] = str(e)
        return render_template_string(
            UPLOAD_TEMPLATE,
            result=result,
            csrf_token=session['csrf_token'],
            username=session.get('username')
        )
    except Exception as e:
        logger.error(f"Error processing uploaded file: {e}", exc_info=True)
        result['error'] = str(e)
        return render_template_string(
            UPLOAD_TEMPLATE,
            result=result,
            csrf_token=session['csrf_token'],
            username=session.get('username')
        )

# API endpoint for file upload
@app.route('/api/upload', methods=['POST'])
@login_required
@rate_limit
def api_upload_file():
    """
    API endpoint for file upload and processing.
    """
    try:
        # API key authentication for programmatic access
        api_key = request.headers.get('X-API-Key')
        if not api_key or api_key != os.environ.get('API_KEY'):
            return jsonify({'error': 'Invalid API key'}), 401

        # Check if a file was uploaded
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'}), 400

        file = request.files['file']

        # Check if the file is empty
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400

        # Check if the file type is allowed
        if not allowed_file(file.filename):
            return jsonify({'error': 'File type not allowed'}), 400

        # Validate file object
        try:
            validate_file_object(file, allowed_extensions=ALLOWED_EXTENSIONS)
        except ValidationError as e:
            logger.error(f"File validation error: {e}")
            return jsonify({'error': str(e)}), 400

        # Secure the filename
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)

        # Save the file temporarily
        file.save(file_path)

        # Get user display name
        user_display_name = request.form.get('user_display_name', '')

        # Validate user display name if provided
        if user_display_name:
            try:
                user_display_name = validate_user_display_name(user_display_name)
            except ValidationError as e:
                logger.warning(f"User display name validation error: {e}")
                user_display_name = ''

        # Process the file through the ETL pipeline
        pipeline = SkypeETLPipeline(
            db_config=db_config,
            output_dir=OUTPUT_FOLDER
        )

        # Run the pipeline with the uploaded file
        results = pipeline.run_pipeline(
            file_path=file_path,
            user_display_name=user_display_name
        )

        # Clean up the temporary file
        try:
            os.remove(file_path)
        except Exception as e:
            logger.warning(f"Failed to remove temporary file {file_path}: {e}")

        # Return the results
        return jsonify(results)

    except ValidationError as e:
        logger.error(f"Validation error: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 400
    except Exception as e:
        logger.error(f"Error processing uploaded file: {e}", exc_info=True)
        return jsonify({'error': str(e)}), 500

# Error handlers
@app.errorhandler(404)
def page_not_found(e):
    return render_template_string(ERROR_TEMPLATE, error="Page not found"), 404

@app.errorhandler(500)
def server_error(e):
    return render_template_string(ERROR_TEMPLATE, error="Internal server error"), 500

@app.errorhandler(413)
def request_entity_too_large(e):
    return render_template_string(ERROR_TEMPLATE, error=f"File too large. Maximum size is {MAX_CONTENT_LENGTH/(1024*1024)}MB"), 413

# HTML Templates
LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Skype ETL Pipeline - Login</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            font-family: Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 600px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
            margin-bottom: 20px;
        }
        .form-group {
            margin-bottom: 15px;
        }
        label {
            display: block;
            margin-bottom: 5px;
            font-weight: bold;
        }
        input[type="text"],
        input[type="password"] {
            width: 100%;
            padding: 8px;
            border: 1px solid #ddd;
            border-radius: 4px;
            box-sizing: border-box;
        }
        button {
            background-color: #4CAF50;
            color: white;
            padding: 10px 15px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
        }
        button:hover {
            background-color: #45a049;
        }
        .error {
            color: red;
            margin-bottom: 15px;
        }
        .flash {
            padding: 10px;
            margin-bottom: 15px;
            background-color: #d4edda;
            border-color: #c3e6cb;
            color: #155724;
            border-radius: 4px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Skype ETL Pipeline - Login</h1>

        {% if error %}
        <div class="error">{{ error }}</div>
        {% endif %}

        {% with messages = get_flashed_messages() %}
        {% if messages %}
        {% for message in messages %}
        <div class="flash">{{ message }}</div>
        {% endfor %}
        {% endif %}
        {% endwith %}

        <form method="post">
            <div class="form-group">
                <label for="username">Username:</label>
                <input type="text" id="username" name="username" required>
            </div>

            <div class="form-group">
                <label for="password">Password:</label>
                <input type="password" id="password" name="password" required>
            </div>

            <button type="submit">Login</button>
        </form>
    </div>
</body>
</html>
"""

UPLOAD_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Skype ETL Pipeline</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            font-family: Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 800px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        }
        h1 {
            color: #333;
            margin-bottom: 20px;
        }
        .form-group {
            margin-bottom: 15px;
        }
        label {
            display: block;
            margin-bottom: 5px;
            font-weight: bold;
        }
        input[type="text"],
        input[type="file"] {
            width: 100%;
            padding: 8px;
            border: 1px solid #ddd;
            border-radius: 4px;
            box-sizing: border-box;
        }
        button {
            background-color: #4CAF50;
            color: white;
            padding: 10px 15px;
            border: none;
            border-radius: 4px;
            cursor: pointer;
        }
        button:hover {
            background-color: #45a049;
        }
        .error {
            color: red;
            margin-bottom: 15px;
        }
        .success {
            color: green;
            margin-bottom: 15px;
        }
        .result {
            margin-top: 20px;
            padding: 15px;
            background-color: #f8f9fa;
            border-radius: 4px;
            border: 1px solid #ddd;
        }
        .result pre {
            white-space: pre-wrap;
            word-wrap: break-word;
        }
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }
        .user-info {
            text-align: right;
        }
        .logout {
            color: #dc3545;
            text-decoration: none;
            margin-left: 10px;
        }
        .flash {
            padding: 10px;
            margin-bottom: 15px;
            background-color: #d4edda;
            border-color: #c3e6cb;
            color: #155724;
            border-radius: 4px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Skype ETL Pipeline</h1>
            <div class="user-info">
                {% if username %}
                Logged in as: <strong>{{ username }}</strong>
                <a href="/logout" class="logout">Logout</a>
                {% endif %}
            </div>
        </div>

        {% with messages = get_flashed_messages() %}
        {% if messages %}
        {% for message in messages %}
        <div class="flash">{{ message }}</div>
        {% endfor %}
        {% endif %}
        {% endwith %}

        <form method="post" action="/upload" enctype="multipart/form-data">
            <input type="hidden" name="csrf_token" value="{{ csrf_token }}">

            <div class="form-group">
                <label for="file">Select Skype export file (.json or .tar):</label>
                <input type="file" id="file" name="file" accept=".json,.tar" required>
            </div>

            <div class="form-group">
                <label for="user_display_name">Your display name (optional):</label>
                <input type="text" id="user_display_name" name="user_display_name" value="{{ username }}">
            </div>

            <button type="submit">Upload and Process</button>
        </form>

        {% if result %}
        <div class="result">
            <h2>Processing Results</h2>

            {% if result.error %}
            <div class="error">Error: {{ result.error }}</div>
            {% else %}
            <div class="success">File processed successfully!</div>

            <h3>Extraction</h3>
            <pre>{{ result.extraction }}</pre>

            <h3>Transformation</h3>
            <pre>{{ result.transformation }}</pre>

            {% if result.loading %}
            <h3>Loading</h3>
            <pre>{{ result.loading }}</pre>
            {% endif %}
            {% endif %}
        </div>
        {% endif %}
    </div>
</body>
</html>
"""

ERROR_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Error - Skype ETL Pipeline</title>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        body {
            font-family: Arial, sans-serif;
            line-height: 1.6;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 600px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            text-align: center;
        }
        h1 {
            color: #dc3545;
            margin-bottom: 20px;
        }
        .error-message {
            margin-bottom: 20px;
        }
        .back-link {
            display: inline-block;
            margin-top: 20px;
            color: #007bff;
            text-decoration: none;
        }
        .back-link:hover {
            text-decoration: underline;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Error</h1>
        <div class="error-message">{{ error }}</div>
        <a href="/" class="back-link">Back to Home</a>
    </div>
</body>
</html>
"""

if __name__ == '__main__':
    app.run(debug=True)