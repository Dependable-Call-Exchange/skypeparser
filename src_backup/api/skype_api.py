"""
Skype Parser API Module

This module provides a Flask-based API for processing Skype export files.
It includes endpoints for file upload, authentication, and processing status.
"""

import logging
import os
import tempfile
import uuid
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, Union

from flask import Flask, Response, g, jsonify, request, session
from flask_cors import CORS
from flask_socketio import SocketIO
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.utils import secure_filename

from src.api.tasks import submit_task
from src.api.user_management import get_user_manager
from src.db.etl.context import ETLContext

# Import the new ETL pipeline
from src.db.etl.pipeline_manager import ETLPipeline
from src.db.progress_tracker import get_tracker
from src.utils.validation import ValidationError

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Constants
ALLOWED_EXTENSIONS = {"tar", "json"}
MAX_CONTENT_LENGTH = 500 * 1024 * 1024  # 500 MB
ASYNC_THRESHOLD = (
    50 * 1024 * 1024
)  # 50 MB - Files larger than this will be processed asynchronously
API_VERSION = "1.0.0"  # Current API version


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
        secret_key: Optional[str] = None,
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
        self.app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH

        # Set up session
        self.app.config["SECRET_KEY"] = secret_key or os.environ.get(
            "SECRET_KEY", os.urandom(24).hex()
        )
        self.app.config["PERMANENT_SESSION_LIFETIME"] = timedelta(days=7)

        # Set up folders
        self.upload_folder = upload_folder or tempfile.mkdtemp()
        self.output_folder = output_folder or tempfile.mkdtemp()

        # Ensure folders exist
        os.makedirs(self.upload_folder, exist_ok=True)
        os.makedirs(self.output_folder, exist_ok=True)

        # Store configuration
        self.db_config = db_config
        self.api_key = api_key or os.environ.get("API_KEY")
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
                api_key = request.headers.get("X-API-Key")

                # If API key is provided, check if it's valid
                if api_key:
                    # Check if API key is valid
                    user = self.user_manager.get_user_by_api_key(api_key)
                    if user:
                        # Store user in g for later use
                        g.user = user
                        return f(*args, **kwargs)

                # If no API key is provided or it's invalid, check for session authentication
                if "username" in session:
                    # Get user from session
                    user = self.user_manager.get_user(session["username"])
                    if user:
                        # Store user in g for later use
                        g.user = user
                        return f(*args, **kwargs)

                # If no valid authentication is found, return 401
                return jsonify({"error": "Unauthorized"}), 401

            return decorated_function

        # Health check endpoint
        @self.app.route("/api/health", methods=["GET"])
        def health_check():
            """Health check endpoint."""
            return jsonify(
                {
                    "status": "ok",
                    "timestamp": datetime.now().isoformat(),
                    "version": API_VERSION,
                }
            )

        # Versioned health check endpoint
        @self.app.route("/api/v1/health", methods=["GET"])
        def health_check_v1():
            """Health check endpoint (v1)."""
            return jsonify(
                {
                    "status": "ok",
                    "timestamp": datetime.now().isoformat(),
                    "version": API_VERSION,
                }
            )

        # Version endpoint
        @self.app.route("/api/version", methods=["GET"])
        def get_version():
            """Get API version endpoint."""
            return jsonify({"version": API_VERSION})

        # Versioned version endpoint
        @self.app.route("/api/v1/version", methods=["GET"])
        def get_version_v1():
            """Get API version endpoint (v1)."""
            return jsonify({"version": API_VERSION})

        # User registration endpoint
        @self.app.route("/api/register", methods=["POST"])
        def register():
            """
            API endpoint for user registration.
            """
            try:
                # Get registration data
                data = request.get_json()

                # Check if required fields are present
                required_fields = ["username", "password", "email", "display_name"]
                for field in required_fields:
                    if field not in data:
                        return (
                            jsonify({"error": f"Missing required field: {field}"}),
                            400,
                        )

                # Register user
                success = self.user_manager.register_user(
                    username=data["username"],
                    password=data["password"],
                    email=data["email"],
                    display_name=data["display_name"],
                )

                if not success:
                    return jsonify({"error": "Registration failed"}), 400

                # Get user
                user = self.user_manager.get_user(data["username"])

                # Return user data
                return jsonify(
                    {
                        "username": user["username"],
                        "email": user["email"],
                        "display_name": user["display_name"],
                        "api_key": user["api_key"],
                    }
                )

            except Exception as e:
                logger.error(f"Error registering user: {e}", exc_info=True)
                return jsonify({"error": str(e)}), 500

        # Versioned user registration endpoint
        @self.app.route("/api/v1/register", methods=["POST"])
        def register_v1():
            """
            API endpoint for user registration (v1).
            """
            return register()

        # User login endpoint
        @self.app.route("/api/login", methods=["POST"])
        def login():
            """
            API endpoint for user login.
            """
            try:
                # Get login data
                data = request.get_json()

                # Check if required fields are present
                required_fields = ["username", "password"]
                for field in required_fields:
                    if field not in data:
                        return (
                            jsonify({"error": f"Missing required field: {field}"}),
                            400,
                        )

                # Authenticate user
                success = self.user_manager.authenticate_user(
                    username=data["username"], password=data["password"]
                )

                if not success:
                    return jsonify({"error": "Invalid username or password"}), 401

                # Get user
                user = self.user_manager.get_user(data["username"])

                # Set session
                session.permanent = True
                session["username"] = user["username"]

                # Return user data
                return jsonify(
                    {
                        "username": user["username"],
                        "email": user["email"],
                        "display_name": user["display_name"],
                        "api_key": user["api_key"],
                    }
                )

            except Exception as e:
                logger.error(f"Error logging in user: {e}", exc_info=True)
                return jsonify({"error": str(e)}), 500

        # Versioned user login endpoint
        @self.app.route("/api/v1/login", methods=["POST"])
        def login_v1():
            """
            API endpoint for user login (v1).
            """
            return login()

        # User logout endpoint
        @self.app.route("/api/logout", methods=["POST"])
        def logout():
            """
            API endpoint for user logout.
            """
            try:
                # Clear session
                session.clear()
                return jsonify({"message": "Logged out successfully"})
            except Exception as e:
                logger.error(f"Error logging out user: {e}", exc_info=True)
                return jsonify({"error": str(e)}), 500

        # Versioned user logout endpoint
        @self.app.route("/api/v1/logout", methods=["POST"])
        def logout_v1():
            """
            API endpoint for user logout (v1).
            """
            return logout()

        # User profile endpoint
        @self.app.route("/api/profile", methods=["GET"])
        @require_api_key
        def profile():
            """
            API endpoint for getting user profile.
            """
            try:
                # Get user from g
                user = g.user

                # Return user data
                return jsonify(
                    {
                        "username": user["username"],
                        "email": user["email"],
                        "display_name": user["display_name"],
                        "api_key": user["api_key"],
                    }
                )
            except Exception as e:
                logger.error(f"Error getting user profile: {e}", exc_info=True)
                return jsonify({"error": str(e)}), 500

        # Versioned user profile endpoint
        @self.app.route("/api/v1/profile", methods=["GET"])
        @require_api_key
        def profile_v1():
            """
            API endpoint for getting user profile (v1).
            """
            return profile()

        # Regenerate API key endpoint
        @self.app.route("/api/regenerate-api-key", methods=["POST"])
        @require_api_key
        def regenerate_api_key():
            """
            API endpoint for regenerating API key.
            """
            try:
                # Get user from g
                user = g.user

                # Generate new API key
                new_api_key = self.user_manager.regenerate_api_key(user["username"])

                # Return new API key
                return jsonify({"api_key": new_api_key})
            except Exception as e:
                logger.error(f"Error regenerating API key: {e}", exc_info=True)
                return jsonify({"error": str(e)}), 500

        # Versioned regenerate API key endpoint
        @self.app.route("/api/v1/regenerate-api-key", methods=["POST"])
        @require_api_key
        def regenerate_api_key_v1():
            """
            API endpoint for regenerating API key (v1).
            """
            return regenerate_api_key()

        # Upload endpoint
        @self.app.route("/api/upload", methods=["POST"])
        @require_api_key
        def upload():
            """
            API endpoint for uploading and processing a Skype export file.
            """
            try:
                # Check if file is present
                if "file" not in request.files:
                    return jsonify({"error": "No file provided"}), 400

                file = request.files["file"]

                # Check if file is empty
                if file.filename == "":
                    return jsonify({"error": "No file selected"}), 400

                # Check if file has allowed extension
                if not self._allowed_file(file.filename):
                    return (
                        jsonify(
                            {
                                "error": f'File type not allowed. Allowed types: {", ".join(ALLOWED_EXTENSIONS)}'
                            }
                        ),
                        400,
                    )

                # Get user display name
                user_display_name = request.form.get(
                    "user_display_name", g.user.get("display_name", "")
                )

                # Save file to temporary location
                filename = secure_filename(file.filename)
                file_path = os.path.join(
                    self.upload_folder, f"{uuid.uuid4()}_{filename}"
                )
                file.save(file_path)

                # Check if file should be processed asynchronously
                if os.path.getsize(file_path) > self.async_threshold:
                    # Submit task for asynchronous processing
                    task_id = submit_task(
                        file_path=file_path,
                        user_display_name=user_display_name,
                        db_config=self.db_config,
                        output_dir=self.output_folder,
                    )

                    # Return task ID
                    return jsonify({"task_id": task_id})
                else:
                    # Process file synchronously
                    pipeline = ETLPipeline(
                        context=ETLContext(
                            db_config=self.db_config, output_dir=self.output_folder
                        )
                    )

                    # Run the pipeline
                    results = pipeline.run_pipeline(
                        file_path=file_path, user_display_name=user_display_name
                    )

                    # Clean up the file
                    try:
                        os.remove(file_path)
                    except Exception as e:
                        logger.warning(
                            f"Failed to clean up temporary file {file_path}: {e}"
                        )

                    # Return results
                    return jsonify(results)

            except RequestEntityTooLarge:
                return jsonify({"error": "File too large"}), 413
            except ValidationError as e:
                return jsonify({"error": str(e)}), 400
            except Exception as e:
                logger.error(f"Error processing upload: {e}", exc_info=True)
                return jsonify({"error": str(e)}), 500

        # Versioned upload endpoint
        @self.app.route("/api/v1/upload", methods=["POST"])
        @require_api_key
        def upload_v1():
            """
            API endpoint for uploading and processing a Skype export file (v1).
            """
            return upload()

        # Task status endpoint
        @self.app.route("/api/status/<task_id>", methods=["GET"])
        @require_api_key
        def task_status(task_id):
            """
            API endpoint for checking the status of a task.
            """
            try:
                # Get tracker for task
                tracker = get_tracker(task_id)

                # Check if tracker exists
                if not tracker:
                    return jsonify({"error": f"Task {task_id} not found"}), 404

                # Return status
                return jsonify(tracker.get_status())
            except Exception as e:
                logger.error(f"Error getting task status: {e}", exc_info=True)
                return jsonify({"error": str(e)}), 500

        # Versioned task status endpoint
        @self.app.route("/api/v1/status/<task_id>", methods=["GET"])
        @require_api_key
        def task_status_v1(task_id):
            """
            API endpoint for checking the status of a task (v1).
            """
            return task_status(task_id)

        # List exports endpoint
        @self.app.route("/api/exports", methods=["GET"])
        @require_api_key
        def list_exports():
            """
            API endpoint for listing all exports.
            """
            try:
                # Get user from g
                user = g.user

                # TODO: Implement listing exports from database
                # This is a placeholder implementation
                return jsonify(
                    [
                        {
                            "export_id": 123,
                            "user_id": user["username"],
                            "export_date": datetime.now().isoformat(),
                            "conversation_count": 42,
                            "message_count": 1234,
                        }
                    ]
                )
            except Exception as e:
                logger.error(f"Error listing exports: {e}", exc_info=True)
                return jsonify({"error": str(e)}), 500

        # Versioned list exports endpoint
        @self.app.route("/api/v1/exports", methods=["GET"])
        @require_api_key
        def list_exports_v1():
            """
            API endpoint for listing all exports (v1).
            """
            return list_exports()

        # Analysis endpoint
        @self.app.route("/api/analysis/<int:export_id>", methods=["GET"])
        @require_api_key
        def analysis(export_id):
            """
            API endpoint for getting analysis data for an export.
            """
            try:
                # TODO: Implement getting analysis data from database
                # This is a placeholder implementation
                return jsonify(
                    {
                        "message_count": 1234,
                        "conversation_count": 42,
                        "date_range": {
                            "start": "2022-01-01T00:00:00.000000",
                            "end": datetime.now().isoformat(),
                        },
                        "top_contacts": [
                            {"name": "Jane Doe", "message_count": 567},
                            {"name": "John Smith", "message_count": 456},
                            {"name": "Bob Johnson", "message_count": 345},
                        ],
                    }
                )
            except Exception as e:
                logger.error(f"Error getting analysis data: {e}", exc_info=True)
                return jsonify({"error": str(e)}), 500

        # Versioned analysis endpoint
        @self.app.route("/api/v1/analysis/<int:export_id>", methods=["GET"])
        @require_api_key
        def analysis_v1(export_id):
            """
            API endpoint for getting analysis data for an export (v1).
            """
            return analysis(export_id)

        # Report endpoint
        @self.app.route("/api/report/<int:export_id>", methods=["GET"])
        @require_api_key
        def report(export_id):
            """
            API endpoint for getting an HTML report for an export.
            """
            try:
                # TODO: Implement generating HTML report
                # This is a placeholder implementation
                html = f"""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>Skype Export Report</title>
                </head>
                <body>
                    <h1>Skype Export Report</h1>
                    <p>Export ID: {export_id}</p>
                    <p>Generated at: {datetime.now().isoformat()}</p>
                    <p>This is a placeholder report.</p>
                </body>
                </html>
                """
                return Response(html, mimetype="text/html")
            except Exception as e:
                logger.error(f"Error generating report: {e}", exc_info=True)
                return jsonify({"error": str(e)}), 500

        # Versioned report endpoint
        @self.app.route("/api/v1/report/<int:export_id>", methods=["GET"])
        @require_api_key
        def report_v1(export_id):
            """
            API endpoint for getting an HTML report for an export (v1).
            """
            return report(export_id)

    def _setup_error_handlers(self) -> None:
        """Set up error handlers."""

        @self.app.errorhandler(400)
        def bad_request(e):
            """Handle 400 Bad Request errors."""
            return jsonify({"error": str(e)}), 400

        @self.app.errorhandler(401)
        def unauthorized(e):
            """Handle 401 Unauthorized errors."""
            return jsonify({"error": "Unauthorized"}), 401

        @self.app.errorhandler(404)
        def not_found(e):
            """Handle 404 Not Found errors."""
            return jsonify({"error": "Not found"}), 404

        @self.app.errorhandler(413)
        def request_entity_too_large(e):
            """Handle 413 Request Entity Too Large errors."""
            return jsonify({"error": "File too large"}), 413

        @self.app.errorhandler(429)
        def too_many_requests(e):
            """Handle 429 Too Many Requests errors."""
            return jsonify({"error": "Too many requests"}), 429

        @self.app.errorhandler(500)
        def internal_server_error(e):
            """Handle 500 Internal Server Error errors."""
            logger.error(f"Internal server error: {e}", exc_info=True)
            return jsonify({"error": "Internal server error"}), 500

    def _setup_socketio_events(self) -> None:
        """Set up Socket.IO events."""

        @self.socketio.on("connect")
        def handle_connect():
            """Handle client connection."""
            logger.info(f"Client connected: {request.sid}")

        @self.socketio.on("disconnect")
        def handle_disconnect():
            """Handle client disconnection."""
            logger.info(f"Client disconnected: {request.sid}")

        @self.socketio.on("subscribe")
        def handle_subscribe(data):
            """
            Handle client subscription to task updates.

            Args:
                data: Dictionary containing task_id
            """
            task_id = data.get("task_id")
            if not task_id:
                return {"error": "No task_id provided"}

            logger.info(f"Client {request.sid} subscribed to task {task_id}")

            # Get tracker for task
            tracker = get_tracker(task_id)
            if not tracker:
                return {"error": f"Task {task_id} not found"}

            # Return current status
            return tracker.get_status()

    def _allowed_file(self, filename: str) -> bool:
        """
        Check if a file has an allowed extension.

        Args:
            filename: The filename to check

        Returns:
            bool: True if the file has an allowed extension
        """
        return (
            "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS
        )

    def run(self, host: str = "0.0.0.0", port: int = 5000, debug: bool = False) -> None:
        """
        Run the API server.

        Args:
            host: Host to bind to
            port: Port to bind to
            debug: Whether to run in debug mode
        """
        self.socketio.run(self.app, host=host, port=port, debug=debug)


def create_app(
    db_config: Optional[Dict[str, Any]] = None,
    upload_dir: Optional[str] = None,
    output_dir: Optional[str] = None,
    enable_auth: bool = True,
    api_key: Optional[str] = None,
    require_auth: bool = True,
    user_file: Optional[str] = None,
    secret_key: Optional[str] = None,
) -> Flask:
    """
    Create a Flask application for the Skype Parser API.

    Args:
        db_config: Database configuration for the ETL pipeline
        upload_dir: Directory for temporary file uploads
        output_dir: Directory for ETL pipeline output
        enable_auth: Whether to enable authentication
        api_key: API key for authentication
        require_auth: Whether to require authentication for all endpoints
        user_file: Path to the user data file
        secret_key: Secret key for session encryption

    Returns:
        Flask: Flask application
    """
    api = SkypeParserAPI(
        upload_folder=upload_dir,
        output_folder=output_dir,
        db_config=db_config,
        api_key=api_key,
        enable_cors=True,
        user_file=user_file,
        secret_key=secret_key,
    )
    return api.app
