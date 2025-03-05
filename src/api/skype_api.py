"""
Skype Parser API Module

This module provides a Flask-based API for processing Skype export files.
It includes endpoints for file upload, authentication, and processing status.
"""

import os
import logging
import tempfile
import uuid
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, Union

from flask import Flask, request, jsonify, g, Response, session
from flask_cors import CORS
from flask_socketio import SocketIO
from werkzeug.utils import secure_filename
from werkzeug.exceptions import RequestEntityTooLarge

# Import the new ETL pipeline
from src.db import ETLPipeline, ETLContext
from src.db.progress_tracker import get_tracker
from src.parser.exceptions import ValidationError
from src.api.tasks import submit_task
from src.api.user_management import get_user_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Constants
ALLOWED_EXTENSIONS = {'tar', 'json'}
MAX_CONTENT_LENGTH = 500 * 1024 * 1024  # 500 MB
ASYNC_THRESHOLD = 50 * 1024 * 1024  # 50 MB - Files larger than this will be processed asynchronously


class SkypeParserAPI:
    """
    API class for the Skype Parser.

    This class provides a Flask application with endpoints for processing
    Skype export files and retrieving results.
    """

    def __init__(
        self,
        upload_folder: Optional[str] = None,
        output_folder: Optional[str] = None,
        db_config: Optional[Dict[str, Any]] = None,
        api_key: Optional[str] = None,
        enable_cors: bool = True,
        async_threshold: int = ASYNC_THRESHOLD,
        user_file: Optional[str] = None,
        secret_key: Optional[str] = None
    ):
        """
        Initialize the Skype Parser API.

        Args:
            upload_folder: Directory for temporary file uploads
            output_folder: Directory for ETL pipeline output
            db_config: Database configuration for the ETL pipeline
            api_key: API key for authentication
            enable_cors: Whether to enable CORS for the API
            async_threshold: File size threshold for asynchronous processing (in bytes)
            user_file: Path to the user data file
            secret_key: Secret key for session encryption
        """
        self.app = Flask(__name__)

        # Configure CORS if enabled
        if enable_cors:
            CORS(self.app)

        # Set up SocketIO
        self.socketio = SocketIO(self.app, cors_allowed_origins="*")

        # Set up file size limit
        self.app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

        # Set up session
        self.app.config['SECRET_KEY'] = secret_key or os.environ.get('SECRET_KEY', os.urandom(24).hex())
        self.app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=7)

        # Set up folders
        self.upload_folder = upload_folder or tempfile.mkdtemp()
        self.output_folder = output_folder or tempfile.mkdtemp()

        # Ensure folders exist
        os.makedirs(self.upload_folder, exist_ok=True)
        os.makedirs(self.output_folder, exist_ok=True)

        # Store configuration
        self.db_config = db_config
        self.api_key = api_key or os.environ.get('API_KEY')
        self.async_threshold = async_threshold

        # Initialize user manager
        self.user_manager = get_user_manager(user_file)

        # Set up routes
        self._setup_routes()

        # Set up error handlers
        self._setup_error_handlers()

        # Set up SocketIO events
        self._setup_socketio_events()

    def _setup_routes(self) -> None:
        """Set up the API routes."""

        # API key authentication decorator
        def require_api_key(f):
            @wraps(f)
            def decorated_function(*args, **kwargs):
                # Check for API key in header
                api_key = request.headers.get('X-API-Key')

                # If API key is provided, check if it's valid
                if api_key:
                    # Check if API key is valid
                    user = self.user_manager.get_user_by_api_key(api_key)
                    if user:
                        # Store user in g for later use
                        g.user = user
                        return f(*args, **kwargs)

                # If no API key is provided or it's invalid, check for session authentication
                if 'username' in session:
                    # Get user from session
                    user = self.user_manager.get_user(session['username'])
                    if user:
                        # Store user in g for later use
                        g.user = user
                        return f(*args, **kwargs)

                # If no valid authentication is found, return 401
                return jsonify({'error': 'Unauthorized'}), 401
            return decorated_function

        # Health check endpoint
        @self.app.route('/api/health', methods=['GET'])
        def health_check():
            """Health check endpoint."""
            return jsonify({
                'status': 'ok',
                'timestamp': datetime.now().isoformat()
            })

        # User registration endpoint
        @self.app.route('/api/register', methods=['POST'])
        def register():
            """
            API endpoint for user registration.
            """
            try:
                # Get registration data
                data = request.get_json()

                # Check if required fields are present
                required_fields = ['username', 'password', 'email', 'display_name']
                for field in required_fields:
                    if field not in data:
                        return jsonify({'error': f'Missing required field: {field}'}), 400

                # Register user
                success = self.user_manager.register_user(
                    username=data['username'],
                    password=data['password'],
                    email=data['email'],
                    display_name=data['display_name']
                )

                if not success:
                    return jsonify({'error': 'Registration failed'}), 400

                # Get user
                user = self.user_manager.get_user(data['username'])

                # Return user data
                return jsonify({
                    'username': user['username'],
                    'email': user['email'],
                    'display_name': user['display_name'],
                    'api_key': user['api_key']
                })

            except Exception as e:
                logger.error(f"Error registering user: {e}", exc_info=True)
                return jsonify({'error': str(e)}), 500

        # User login endpoint
        @self.app.route('/api/login', methods=['POST'])
        def login():
            """
            API endpoint for user login.
            """
            try:
                # Get login data
                data = request.get_json()

                # Check if required fields are present
                required_fields = ['username', 'password']
                for field in required_fields:
                    if field not in data:
                        return jsonify({'error': f'Missing required field: {field}'}), 400

                # Authenticate user
                success = self.user_manager.authenticate_user(
                    username=data['username'],
                    password=data['password']
                )

                if not success:
                    return jsonify({'error': 'Invalid username or password'}), 401

                # Get user
                user = self.user_manager.get_user(data['username'])

                # Set session
                session.permanent = True
                session['username'] = user['username']

                # Return user data
                return jsonify({
                    'username': user['username'],
                    'email': user['email'],
                    'display_name': user['display_name'],
                    'api_key': user['api_key']
                })

            except Exception as e:
                logger.error(f"Error logging in user: {e}", exc_info=True)
                return jsonify({'error': str(e)}), 500

        # User logout endpoint
        @self.app.route('/api/logout', methods=['POST'])
        def logout():
            """
            API endpoint for user logout.
            """
            try:
                # Clear session
                session.clear()

                return jsonify({'message': 'Logged out successfully'})

            except Exception as e:
                logger.error(f"Error logging out user: {e}", exc_info=True)
                return jsonify({'error': str(e)}), 500

        # User profile endpoint
        @self.app.route('/api/profile', methods=['GET'])
        @require_api_key
        def profile():
            """
            API endpoint for getting user profile.
            """
            try:
                # Get user from g
                user = g.user

                # Return user data
                return jsonify({
                    'username': user['username'],
                    'email': user['email'],
                    'display_name': user['display_name'],
                    'api_key': user['api_key']
                })

            except Exception as e:
                logger.error(f"Error getting user profile: {e}", exc_info=True)
                return jsonify({'error': str(e)}), 500

        # API key regeneration endpoint
        @self.app.route('/api/regenerate-api-key', methods=['POST'])
        @require_api_key
        def regenerate_api_key():
            """
            API endpoint for regenerating API key.
            """
            try:
                # Get user from g
                user = g.user

                # Regenerate API key
                api_key = self.user_manager.regenerate_api_key(user['username'])

                if not api_key:
                    return jsonify({'error': 'Failed to regenerate API key'}), 400

                # Return new API key
                return jsonify({'api_key': api_key})

            except Exception as e:
                logger.error(f"Error regenerating API key: {e}", exc_info=True)
                return jsonify({'error': str(e)}), 500

        # Upload endpoint
        @self.app.route('/api/upload', methods=['POST'])
        @require_api_key
        def upload_file():
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

                # Check if the file type is allowed
                if not self._allowed_file(file.filename):
                    return jsonify({'error': 'File type not allowed'}), 400

                # Secure the filename
                filename = secure_filename(file.filename)
                unique_filename = f"{uuid.uuid4()}_{filename}"
                file_path = os.path.join(self.upload_folder, unique_filename)

                # Save the file temporarily
                file.save(file_path)

                # Get user display name
                user_display_name = request.form.get('user_display_name', '')

                # If no user display name is provided, use the user's display name
                if not user_display_name and hasattr(g, 'user'):
                    user_display_name = g.user.get('display_name', '')

                # Get file size
                file_size = os.path.getsize(file_path)

                # Determine if the file should be processed asynchronously
                process_async = file_size > self.async_threshold

                if process_async:
                    # Process the file asynchronously
                    logger.info(f"Processing file asynchronously (size: {file_size} bytes)")

                    # Submit the task to the task queue
                    task_id = submit_task(
                        file_path=file_path,
                        user_display_name=user_display_name,
                        db_config=self.db_config,
                        output_dir=self.output_folder,
                        cleanup=True
                    )

                    # Return the task ID
                    return jsonify({
                        'task_id': task_id,
                        'status': 'processing',
                        'message': 'File is being processed asynchronously',
                        'async': True
                    })
                else:
                    # Process the file synchronously
                    logger.info(f"Processing file synchronously (size: {file_size} bytes)")

                    # Create an ETL context
                    context = ETLContext(
                        db_config=self.db_config,
                        output_dir=self.output_folder
                    )

                    # Process the file through the ETL pipeline
                    pipeline = ETLPipeline(
                        context=context
                    )

                    # Run the pipeline with the uploaded file
                    results = pipeline.run_pipeline(
                        file_path=file_path,
                        user_display_name=user_display_name
                    )

                    # Convert results to the format expected by the API
                    # This maintains backward compatibility with code that expects the old format
                    if results.get('success', False):
                        # Extract conversation and message counts
                        conversation_count = 0
                        message_count = 0

                        for phase, stats in results.get('phases', {}).items():
                            if phase == 'transform' and stats:
                                conversation_count = stats.get('processed_conversations', 0)
                                message_count = stats.get('processed_messages', 0)

                        # Add backward compatibility fields
                        results['conversations'] = conversation_count
                        results['message_count'] = message_count

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

        # Task status endpoint
        @self.app.route('/api/task/<task_id>', methods=['GET'])
        @require_api_key
        def get_task_status(task_id):
            """
            API endpoint for checking task status.
            """
            tracker = get_tracker(task_id)
            if not tracker:
                return jsonify({'error': 'Task not found'}), 404

            # Get progress data
            progress_data = tracker._get_progress_data()

            return jsonify(progress_data)

    def _setup_error_handlers(self) -> None:
        """Set up error handlers for the API."""

        @self.app.errorhandler(404)
        def page_not_found(e):
            return jsonify({'error': 'Not found'}), 404

        @self.app.errorhandler(500)
        def server_error(e):
            return jsonify({'error': 'Internal server error'}), 500

        @self.app.errorhandler(RequestEntityTooLarge)
        def request_entity_too_large(e):
            return jsonify({
                'error': f'File too large. Maximum size is {MAX_CONTENT_LENGTH/(1024*1024)}MB'
            }), 413

    def _setup_socketio_events(self) -> None:
        """Set up SocketIO events."""

        @self.socketio.on('connect')
        def handle_connect():
            logger.info(f"Client connected: {request.sid}")

        @self.socketio.on('disconnect')
        def handle_disconnect():
            logger.info(f"Client disconnected: {request.sid}")

        @self.socketio.on('subscribe')
        def handle_subscribe(data):
            """
            Handle client subscription to task updates.

            Args:
                data: Dictionary containing task_id
            """
            task_id = data.get('task_id')
            if not task_id:
                return {'error': 'No task_id provided'}

            logger.info(f"Client {request.sid} subscribed to task {task_id}")

            # Get the tracker for this task
            tracker = get_tracker(task_id)
            if not tracker:
                return {'error': 'Task not found'}

            # Add a listener that emits progress updates to this client
            def emit_progress(progress_data):
                self.socketio.emit(f'task_progress_{task_id}', progress_data, room=request.sid)

            tracker.add_listener(emit_progress)

            # Return initial progress data
            return tracker._get_progress_data()

    def _allowed_file(self, filename: str) -> bool:
        """
        Check if a file has an allowed extension.

        Args:
            filename: The filename to check

        Returns:
            bool: True if the file has an allowed extension
        """
        return '.' in filename and \
               filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

    def run(self, host: str = '0.0.0.0', port: int = 5000, debug: bool = False) -> None:
        """
        Run the API server.

        Args:
            host: Host to bind to
            port: Port to bind to
            debug: Whether to run in debug mode
        """
        self.socketio.run(self.app, host=host, port=port, debug=debug)

    def get_app(self) -> Flask:
        """
        Get the Flask application.

        Returns:
            Flask: The Flask application
        """
        return self.app