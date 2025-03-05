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
from src.db.etl import ETLPipeline  # Import the modular ETL pipeline
from src.db.etl_pipeline_compat import SkypeETLPipeline  # Import the compatibility layer
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
USE_MODULAR_PIPELINE = os.environ.get('USE_MODULAR_PIPELINE', 'true').lower() == 'true'

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

# Create ETL pipeline based on configuration
def create_etl_pipeline():
    """
    Create an ETL pipeline based on configuration.

    Returns:
        ETLPipeline or SkypeETLPipeline: The ETL pipeline instance
    """
    if USE_MODULAR_PIPELINE:
        logger.info("Using modular ETL pipeline")
        return ETLPipeline(
            db_config=db_config,
            output_dir=OUTPUT_FOLDER
        )
    else:
        logger.info("Using legacy ETL pipeline")
        return SkypeETLPipeline(
            db_config=db_config,
            output_dir=OUTPUT_FOLDER
        )

# Process file through ETL pipeline
def process_file(file, user_display_name):
    """
    Process a file through the ETL pipeline.

    Args:
        file: The file to process
        user_display_name: The user display name

    Returns:
        dict: The results of the ETL pipeline
    """
    # Create ETL pipeline
    pipeline = create_etl_pipeline()

    # Run the pipeline
    result = pipeline.run_pipeline(
        file_obj=file,
        user_display_name=user_display_name
    )

    # Format the result for display
    if USE_MODULAR_PIPELINE:
        # Extract conversation and message counts from the modular pipeline result
        conversation_count = 0
        message_count = 0

        for phase, stats in result['phases'].items():
            if phase == 'transform' and stats:
                conversation_count = stats.get('processed_conversations', 0)
                message_count = stats.get('processed_messages', 0)

        # Return in the legacy format for compatibility
        return {
            'success': result['success'],
            'export_id': result.get('export_id'),
            'conversations': conversation_count,
            'message_count': message_count,
            'error': result.get('error'),
            'phases': result.get('phases')  # Include phase details for advanced usage
        }
    else:
        # Legacy pipeline already returns the expected format
        return result

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
        username=session.get('username'),
        use_modular_pipeline=USE_MODULAR_PIPELINE
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
            result['error'] = 'Invalid CSRF token'
            return render_template_string(
                RESULT_TEMPLATE,
                result=result,
                username=session.get('username')
            )

        # Check if a file was uploaded
        if 'file' not in request.files:
            result['error'] = 'No file part'
            return render_template_string(
                RESULT_TEMPLATE,
                result=result,
                username=session.get('username')
            )

        file = request.files['file']

        # Check if the file is empty
        if file.filename == '':
            result['error'] = 'No file selected'
            return render_template_string(
                RESULT_TEMPLATE,
                result=result,
                username=session.get('username')
            )

        # Check if the file has an allowed extension
        if not allowed_file(file.filename):
            result['error'] = 'File type not allowed'
            return render_template_string(
                RESULT_TEMPLATE,
                result=result,
                username=session.get('username')
            )

        # Get user display name
        user_display_name = request.form.get('user_display_name', '')
        try:
            validate_user_display_name(user_display_name)
        except ValidationError as e:
            result['error'] = str(e)
            return render_template_string(
                RESULT_TEMPLATE,
                result=result,
                username=session.get('username')
            )

        # Save the file
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)

        # Process the file through the ETL pipeline
        file.seek(0)  # Reset file pointer to beginning
        result = process_file(file, user_display_name)

        return render_template_string(
            RESULT_TEMPLATE,
            result=result,
            username=session.get('username'),
            use_modular_pipeline=USE_MODULAR_PIPELINE
        )

    except Exception as e:
        logger.exception(f"Error processing file: {e}")
        result['error'] = str(e)
        return render_template_string(
            RESULT_TEMPLATE,
            result=result,
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
        # Check if a file was uploaded
        if 'file' not in request.files:
            return jsonify({'error': 'No file part'}), 400

        file = request.files['file']

        # Check if the file is empty
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400

        # Check if the file has an allowed extension
        if not allowed_file(file.filename):
            return jsonify({'error': 'File type not allowed'}), 400

        # Get user display name
        user_display_name = request.form.get('user_display_name', '')
        try:
            validate_user_display_name(user_display_name)
        except ValidationError as e:
            return jsonify({'error': str(e)}), 400

        # Save the file
        filename = secure_filename(file.filename)
        file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(file_path)

        # Process the file through the ETL pipeline
        file.seek(0)  # Reset file pointer to beginning
        result = process_file(file, user_display_name)

        return jsonify(result)

    except Exception as e:
        logger.exception(f"API error processing file: {e}")
        return jsonify({'error': str(e)}), 500

# Error handlers
@app.errorhandler(404)
def page_not_found(e):
    """
    Handle 404 errors.
    """
    return render_template_string(
        ERROR_TEMPLATE,
        error='Page not found',
        username=session.get('username')
    ), 404

@app.errorhandler(500)
def server_error(e):
    """
    Handle 500 errors.
    """
    return render_template_string(
        ERROR_TEMPLATE,
        error='Server error',
        username=session.get('username')
    ), 500

@app.errorhandler(413)
def request_entity_too_large(e):
    """
    Handle 413 errors (file too large).
    """
    return render_template_string(
        ERROR_TEMPLATE,
        error=f'File too large. Maximum size is {MAX_CONTENT_LENGTH / (1024 * 1024)}MB',
        username=session.get('username')
    ), 413

# HTML Templates
LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Skype ETL Pipeline - Login</title>
    <style>
        body {
            font-family: Arial, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }
        .container {
            max-width: 500px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 5px;
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
        }
        h1 {
            color: #0078d7;
            text-align: center;
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
            background-color: #0078d7;
            color: white;
            border: none;
            padding: 10px 15px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 16px;
        }
        button:hover {
            background-color: #005a9e;
        }
        .error {
            color: red;
            margin-bottom: 15px;
        }
    </style>
</head>
<body>
    <div class="container">
        <h1>Skype ETL Pipeline</h1>
        <h2>Login</h2>

        {% if error %}
        <div class="error">{{ error }}</div>
        {% endif %}

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
    <title>Skype ETL Pipeline - Upload</title>
    <style>
        body {
            font-family: Arial, sans-serif;
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
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
        }
        h1 {
            color: #0078d7;
            text-align: center;
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
            background-color: #0078d7;
            color: white;
            border: none;
            padding: 10px 15px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 16px;
        }
        button:hover {
            background-color: #005a9e;
        }
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }
        .logout {
            background-color: #d9534f;
        }
        .logout:hover {
            background-color: #c9302c;
        }
        .info {
            margin-top: 20px;
            padding: 10px;
            background-color: #f0f7fd;
            border-left: 5px solid #0078d7;
        }
        .pipeline-info {
            margin-top: 20px;
            padding: 10px;
            background-color: #dff0d8;
            border-left: 5px solid #3c763d;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Skype ETL Pipeline</h1>
            <div>
                <span>Welcome, {{ username }}</span>
                <a href="{{ url_for('logout') }}"><button class="logout">Logout</button></a>
            </div>
        </div>

        <form method="post" action="{{ url_for('upload_file') }}" enctype="multipart/form-data">
            <input type="hidden" name="csrf_token" value="{{ csrf_token }}">

            <div class="form-group">
                <label for="file">Skype Export File:</label>
                <input type="file" id="file" name="file" required>
                <small>Allowed file types: .json, .tar</small>
            </div>

            <div class="form-group">
                <label for="user_display_name">User Display Name:</label>
                <input type="text" id="user_display_name" name="user_display_name" placeholder="Your name in Skype">
                <small>Optional: Your display name in Skype</small>
            </div>

            <button type="submit">Upload and Process</button>
        </form>

        <div class="info">
            <h3>Instructions</h3>
            <p>Upload your Skype export file to process it through the ETL pipeline. The file will be validated, processed, and loaded into the database.</p>
            <p>You can optionally provide your display name in Skype to help identify your messages in the processed data.</p>
        </div>

        <div class="pipeline-info">
            <h3>Pipeline Information</h3>
            <p>Currently using: <strong>{% if use_modular_pipeline %}Modular ETL Pipeline{% else %}Legacy ETL Pipeline{% endif %}</strong></p>
            <p>The modular pipeline provides improved performance, better error handling, and more detailed progress tracking.</p>
        </div>
    </div>
</body>
</html>
"""

RESULT_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Skype ETL Pipeline - Result</title>
    <style>
        body {
            font-family: Arial, sans-serif;
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
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
        }
        h1 {
            color: #0078d7;
            text-align: center;
        }
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }
        .result {
            margin-top: 20px;
            padding: 15px;
            border-radius: 4px;
        }
        .success {
            background-color: #dff0d8;
            border-left: 5px solid #3c763d;
            color: #3c763d;
        }
        .error {
            background-color: #f2dede;
            border-left: 5px solid #a94442;
            color: #a94442;
        }
        .details {
            margin-top: 20px;
            background-color: #f8f9fa;
            padding: 15px;
            border-radius: 4px;
        }
        .button {
            display: inline-block;
            background-color: #0078d7;
            color: white;
            border: none;
            padding: 10px 15px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 16px;
            text-decoration: none;
            margin-top: 20px;
        }
        .button:hover {
            background-color: #005a9e;
        }
        .logout {
            background-color: #d9534f;
        }
        .logout:hover {
            background-color: #c9302c;
        }
        .phase {
            margin-top: 15px;
            padding: 10px;
            background-color: #f0f7fd;
            border-left: 3px solid #0078d7;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Skype ETL Pipeline</h1>
            <div>
                <span>Welcome, {{ username }}</span>
                <a href="{{ url_for('logout') }}"><button class="logout">Logout</button></a>
            </div>
        </div>

        <h2>Processing Result</h2>

        {% if result.error %}
        <div class="result error">
            <h3>Error</h3>
            <p>{{ result.error }}</p>
        </div>
        {% else %}
        <div class="result success">
            <h3>Success</h3>
            <p>The file was processed successfully.</p>
        </div>

        <div class="details">
            <h3>Details</h3>
            <p><strong>Export ID:</strong> {{ result.export_id }}</p>
            <p><strong>Conversations:</strong> {{ result.conversations }}</p>
            <p><strong>Messages:</strong> {{ result.message_count }}</p>

            {% if use_modular_pipeline and result.phases %}
            <h3>Phase Statistics</h3>
            {% for phase_name, phase_stats in result.phases.items() %}
            {% if phase_stats %}
            <div class="phase">
                <h4>{{ phase_name|capitalize }} Phase</h4>
                <p><strong>Duration:</strong> {{ phase_stats.duration_seconds|round(2) }} seconds</p>
                <p><strong>Messages:</strong> {{ phase_stats.processed_messages }}</p>
                <p><strong>Conversations:</strong> {{ phase_stats.processed_conversations }}</p>
                <p><strong>Messages per second:</strong> {{ phase_stats.messages_per_second|round(2) }}</p>
            </div>
            {% endif %}
            {% endfor %}
            {% endif %}
        </div>
        {% endif %}

        <a href="{{ url_for('index') }}" class="button">Upload Another File</a>
    </div>
</body>
</html>
"""

ERROR_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Skype ETL Pipeline - Error</title>
    <style>
        body {
            font-family: Arial, sans-serif;
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
            box-shadow: 0 2px 5px rgba(0, 0, 0, 0.1);
        }
        h1 {
            color: #0078d7;
            text-align: center;
        }
        .header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
        }
        .error {
            margin-top: 20px;
            padding: 15px;
            background-color: #f2dede;
            border-left: 5px solid #a94442;
            color: #a94442;
            border-radius: 4px;
        }
        .button {
            display: inline-block;
            background-color: #0078d7;
            color: white;
            border: none;
            padding: 10px 15px;
            border-radius: 4px;
            cursor: pointer;
            font-size: 16px;
            text-decoration: none;
            margin-top: 20px;
        }
        .button:hover {
            background-color: #005a9e;
        }
        .logout {
            background-color: #d9534f;
        }
        .logout:hover {
            background-color: #c9302c;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>Skype ETL Pipeline</h1>
            {% if username %}
            <div>
                <span>Welcome, {{ username }}</span>
                <a href="{{ url_for('logout') }}"><button class="logout">Logout</button></a>
            </div>
            {% endif %}
        </div>

        <h2>Error</h2>

        <div class="error">
            <p>{{ error }}</p>
        </div>

        <a href="{{ url_for('index') }}" class="button">Return to Upload</a>
    </div>
</body>
</html>
"""

if __name__ == '__main__':
    app.run(debug=True)