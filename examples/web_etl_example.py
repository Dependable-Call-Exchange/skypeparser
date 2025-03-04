#!/usr/bin/env python3
"""
Web ETL Example

This example demonstrates how to use the Skype ETL pipeline in a web application context.
It provides a Flask application that allows users to upload Skype export files
and processes them using the ETL pipeline.

Features:
- Basic authentication
- Secure file handling
- Comprehensive error handling
- Rate limiting
- CSRF protection
"""

import os
import sys
import json
import logging
import secrets
import datetime
from typing import Dict, Any, Optional, Tuple
from functools import wraps
from pathlib import Path

from flask import (
    Flask, request, jsonify, render_template_string, session,
    redirect, url_for, flash, Response, abort
)
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the ETL pipeline and configuration utilities
from src.db.etl_pipeline import SkypeETLPipeline
from src.utils.config import load_config, get_db_config, setup_logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('skype-web-etl')

# Create Flask application
app = Flask(__name__)
app.secret_key = os.environ.get('FLASK_SECRET_KEY', secrets.token_hex(16))

# Load configuration
config_file = os.environ.get('CONFIG_FILE', 'config/config.json')
config = load_config(config_file)
db_config = get_db_config(config)

# Configure upload folder
UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER', os.path.join(os.path.dirname(__file__), 'uploads'))
OUTPUT_FOLDER = os.environ.get('OUTPUT_FOLDER', os.path.join(os.path.dirname(__file__), 'output'))
ALLOWED_EXTENSIONS = {'tar', 'json'}
MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50MB limit

# Create necessary directories
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Configure the app
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# Simple user database (in a real app, use a proper database)
USERS = {
    'admin': {
        'password': generate_password_hash('admin'),
        'role': 'admin'
    }
}

# Rate limiting
RATE_LIMIT = {
    'window_seconds': 60,
    'max_requests': 10,
    'clients': {}
}

# Authentication decorator
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            flash('Please log in to access this page', 'error')
            return redirect(url_for('login', next=request.url))
        return f(*args, **kwargs)
    return decorated_function

# Rate limiting decorator
def rate_limit(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        client_ip = request.remote_addr
        current_time = datetime.datetime.now()

        # Initialize client data if not exists
        if client_ip not in RATE_LIMIT['clients']:
            RATE_LIMIT['clients'][client_ip] = {
                'requests': [],
                'blocked_until': None
            }

        client = RATE_LIMIT['clients'][client_ip]

        # Check if client is blocked
        if client['blocked_until'] and client['blocked_until'] > current_time:
            return jsonify({
                'error': 'Too many requests',
                'retry_after': (client['blocked_until'] - current_time).seconds
            }), 429

        # Clean up old requests
        window_start = current_time - datetime.timedelta(seconds=RATE_LIMIT['window_seconds'])
        client['requests'] = [r for r in client['requests'] if r > window_start]

        # Check rate limit
        if len(client['requests']) >= RATE_LIMIT['max_requests']:
            client['blocked_until'] = current_time + datetime.timedelta(minutes=1)
            return jsonify({
                'error': 'Too many requests',
                'retry_after': 60
            }), 429

        # Add current request
        client['requests'].append(current_time)

        return f(*args, **kwargs)
    return decorated_function

def allowed_file(filename):
    """Check if a filename has an allowed extension."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# Login page
@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')

        if username in USERS and check_password_hash(USERS[username]['password'], password):
            session['username'] = username
            session['role'] = USERS[username]['role']

            next_page = request.args.get('next')
            if next_page and next_page.startswith('/'):
                return redirect(next_page)
            return redirect(url_for('index'))
        else:
            error = 'Invalid username or password'

    return render_template_string(LOGIN_TEMPLATE, error=error)

# Logout route
@app.route('/logout')
def logout():
    session.pop('username', None)
    session.pop('role', None)
    flash('You have been logged out', 'info')
    return redirect(url_for('login'))

# Main page
@app.route('/')
@login_required
def index():
    """Render the upload form."""
    # Generate CSRF token if not present
    if 'csrf_token' not in session:
        session['csrf_token'] = secrets.token_hex(16)

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

        # Secure the filename
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)

        # Save the file temporarily
        file.save(file_path)

        # Get user display name
        user_display_name = request.form.get('user_display_name', '')

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
    <title>Login - Skype Export ETL Pipeline</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
        }
        h1 {
            color: #0078d7;
        }
        .form-group {
            margin-bottom: 15px;
        }
        label {
            display: block;
            margin-bottom: 5px;
            font-weight: bold;
        }
        input[type="text"], input[type="password"] {
            width: 100%;
            padding: 8px;
            box-sizing: border-box;
        }
        button {
            background-color: #0078d7;
            color: white;
            border: none;
            padding: 10px 15px;
            cursor: pointer;
        }
        .error {
            color: red;
            margin-bottom: 15px;
        }
    </style>
</head>
<body>
    <h1>Skype Export ETL Pipeline</h1>
    <h2>Login</h2>

    {% if error %}
    <div class="error">{{ error }}</div>
    {% endif %}

    <form action="/login" method="post">
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
</body>
</html>
"""

UPLOAD_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Skype Export ETL Pipeline</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
        }
        h1 {
            color: #0078d7;
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
        .form-group {
            margin-bottom: 15px;
        }
        label {
            display: block;
            margin-bottom: 5px;
            font-weight: bold;
        }
        input[type="text"], input[type="file"] {
            width: 100%;
            padding: 8px;
            box-sizing: border-box;
        }
        button {
            background-color: #0078d7;
            color: white;
            border: none;
            padding: 10px 15px;
            cursor: pointer;
        }
        .result {
            margin-top: 20px;
            padding: 15px;
            background-color: #f5f5f5;
            border-radius: 5px;
        }
        .error {
            color: red;
        }
        .success {
            color: green;
        }
        .logout {
            margin-left: 10px;
            font-size: 0.8em;
        }
    </style>
</head>
<body>
    <div class="header">
        <h1>Skype Export ETL Pipeline</h1>
        <div class="user-info">
            {% if username %}
            Logged in as: <strong>{{ username }}</strong>
            <a href="/logout" class="logout">Logout</a>
            {% endif %}
        </div>
    </div>

    <p>Upload your Skype export file (TAR or JSON) to process it through the ETL pipeline.</p>

    <form action="/upload" method="post" enctype="multipart/form-data">
        <input type="hidden" name="csrf_token" value="{{ csrf_token }}">

        <div class="form-group">
            <label for="file">Skype Export File:</label>
            <input type="file" id="file" name="file" accept=".tar,.json" required>
        </div>

        <div class="form-group">
            <label for="user_display_name">Your Display Name:</label>
            <input type="text" id="user_display_name" name="user_display_name" placeholder="How your name should appear in the logs" value="{{ username }}">
        </div>

        <button type="submit">Process File</button>
    </form>

    {% if result %}
    <div class="result">
        <h2>Processing Results</h2>
        {% if result.error %}
            <p class="error">{{ result.error }}</p>
        {% else %}
            <p class="success">File processed successfully!</p>
            <h3>Extraction</h3>
            <p>User ID: {{ result.extraction.userId }}</p>
            <p>Export Date: {{ result.extraction.exportDate }}</p>
            <p>Conversations: {{ result.extraction.conversationCount }}</p>

            <h3>Transformation</h3>
            <p>Conversations Transformed: {{ result.transformation.conversationCount }}</p>

            {% if result.loading %}
            <h3>Loading</h3>
            <p>Export ID: {{ result.loading.exportId }}</p>
            {% else %}
            <p>Loading phase skipped (no database connection)</p>
            {% endif %}
        {% endif %}
    </div>
    {% endif %}
</body>
</html>
"""

ERROR_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Error - Skype Export ETL Pipeline</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            max-width: 800px;
            margin: 0 auto;
            padding: 20px;
        }
        h1 {
            color: #0078d7;
        }
        .error {
            color: red;
            margin: 20px 0;
            padding: 15px;
            background-color: #ffeeee;
            border-radius: 5px;
        }
        .back {
            margin-top: 20px;
        }
    </style>
</head>
<body>
    <h1>Skype Export ETL Pipeline</h1>
    <div class="error">
        <h2>Error</h2>
        <p>{{ error }}</p>
    </div>
    <div class="back">
        <a href="/">Back to Home</a>
    </div>
</body>
</html>
"""

def main():
    """
    Main entry point for the web application.
    """
    # Check if Flask is installed
    try:
        import flask
    except ImportError:
        print("Flask is not installed. Please install it with: pip install flask")
        sys.exit(1)

    # Run the Flask application
    host = os.environ.get('HOST', '0.0.0.0')
    port = int(os.environ.get('PORT', '5000'))
    debug = os.environ.get('DEBUG', 'False').lower() in ('true', 'yes', '1')

    app.run(debug=debug, host=host, port=port)

if __name__ == "__main__":
    main()