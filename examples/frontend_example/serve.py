#!/usr/bin/env python3
"""
Serve the front-end example for the SkypeParser UI.

This script serves the front-end example and proxies API requests to the Skype Parser API.
It provides a development server for testing the frontend application with real-time updates
via Socket.IO for ETL task progress monitoring.

Key features:
1. Static file serving for the frontend application
2. API request proxying to the backend service
3. WebSocket support for real-time updates
4. Automatic reconnection to the API server
5. Security headers and CORS configuration

Compatibility:
- SkypeParser version: 2.0.0+
- Python version: 3.7+
- Works with any modern browser (Chrome, Firefox, Safari, Edge)

Usage:
    python serve.py [--host HOST] [--port PORT] [--api-url API_URL] [--debug]
"""

import os
import sys
import time
import argparse
import logging
import json
import threading
from pathlib import Path
from typing import Dict, Set, Any, Optional, Union

try:
    import requests
    import socketio
    from flask import Flask, request, send_from_directory, Response, jsonify, abort
    from flask_cors import CORS
    from flask_socketio import SocketIO
except ImportError as e:
    print(f"Error importing required modules: {e}")
    print("Please install required packages with: pip install flask flask-socketio flask-cors python-socketio requests")
    sys.exit(1)

# Configure logging with more structured format
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'
LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO').upper()

# Create logs directory if it doesn't exist
log_dir = Path('logs')
log_dir.mkdir(exist_ok=True)

# Setup logging to both console and file
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(log_dir / f'frontend_server_{time.strftime("%Y%m%d_%H%M%S")}.log')
    ]
)
logger = logging.getLogger('frontend-server')

# Create Flask app with enhanced security
app = Flask(__name__, static_folder='static')
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', os.urandom(24).hex())

# Rate limiting data structure
rate_limits: Dict[str, Dict[str, Union[int, float]]] = {}
RATE_LIMIT_MAX_REQUESTS = int(os.environ.get('RATE_LIMIT_MAX_REQUESTS', 100))
RATE_LIMIT_WINDOW = int(os.environ.get('RATE_LIMIT_WINDOW', 60))  # seconds

# Setup CORS with more restrictive defaults
cors_origins = os.environ.get('CORS_ORIGINS', 'http://localhost:8080,http://127.0.0.1:8080').split(',')
CORS(app, resources={r"/api/*": {"origins": cors_origins}})

# Create SocketIO server with proper CORS configuration
socketio_cors_origins = os.environ.get('SOCKETIO_CORS_ORIGINS', '*')
socketio = SocketIO(app, cors_allowed_origins=socketio_cors_origins)

# Create SocketIO client for connecting to the API server
sio_client = socketio.Client(reconnection=True, reconnection_attempts=5, reconnection_delay=1, reconnection_delay_max=5)

# API endpoint
API_URL: Optional[str] = None

# Client and task mappings
client_to_task: Dict[str, str] = {}
task_to_clients: Dict[str, Set[str]] = {}

# Reconnection thread
reconnect_thread = None
reconnect_active = False

def apply_rate_limit(client_ip: str) -> bool:
    """
    Apply rate limiting to requests.

    Args:
        client_ip: The client's IP address

    Returns:
        True if the request is allowed, False if rate limited
    """
    current_time = time.time()

    # Initialize rate limit data for new clients
    if client_ip not in rate_limits:
        rate_limits[client_ip] = {
            'count': 0,
            'reset_time': current_time + RATE_LIMIT_WINDOW
        }

    # Reset count if window has expired
    if current_time > rate_limits[client_ip]['reset_time']:
        rate_limits[client_ip] = {
            'count': 0,
            'reset_time': current_time + RATE_LIMIT_WINDOW
        }

    # Increment count and check limit
    rate_limits[client_ip]['count'] += 1

    # Clean up old entries periodically
    if len(rate_limits) > 1000:  # prevent memory leak
        clean_rate_limits()

    return rate_limits[client_ip]['count'] <= RATE_LIMIT_MAX_REQUESTS

def clean_rate_limits() -> None:
    """Clean up expired rate limit entries to prevent memory leaks."""
    current_time = time.time()
    expired_ips = [ip for ip, data in rate_limits.items()
                  if current_time > data['reset_time']]

    for ip in expired_ips:
        del rate_limits[ip]

@app.before_request
def before_request() -> Optional[Response]:
    """
    Process request before handling.

    Implements:
    1. Rate limiting
    2. Basic request validation

    Returns:
        None if request should proceed, or an error response
    """
    # Skip rate limiting for static files
    if request.endpoint == 'static':
        return None

    # Get client IP (with proxy support)
    client_ip = request.headers.get('X-Forwarded-For', request.remote_addr)
    if client_ip and ',' in client_ip:
        client_ip = client_ip.split(',')[0].strip()

    # Apply rate limiting
    if not apply_rate_limit(client_ip):
        logger.warning(f"Rate limit exceeded for IP: {client_ip}")
        response = jsonify({'error': 'Rate limit exceeded. Please try again later.'})
        response.status_code = 429
        return response

    return None

@app.after_request
def after_request(response: Response) -> Response:
    """
    Add security headers to all responses.

    Args:
        response: The Flask response object

    Returns:
        Modified response with security headers
    """
    # Add security headers
    response.headers['X-Content-Type-Options'] = 'nosniff'
    response.headers['X-Frame-Options'] = 'DENY'
    response.headers['X-XSS-Protection'] = '1; mode=block'
    response.headers['Content-Security-Policy'] = "default-src 'self'; connect-src 'self' ws: wss:; script-src 'self'; style-src 'self' 'unsafe-inline'"

    return response

@app.route('/')
def index() -> Response:
    """
    Serve the index.html file from the static directory.

    Returns:
        HTML content of the index page
    """
    if not Path('static/index.html').exists():
        logger.error("Frontend files not found. Please build the frontend first.")
        return jsonify({
            'error': 'Frontend files not found',
            'help': 'Please build the frontend or check the static directory path'
        }), 500

    return send_from_directory('static', 'index.html')

@app.route('/status')
def status() -> Dict[str, Any]:
    """
    Return server status information.

    Returns:
        Status information as JSON
    """
    return {
        'status': 'online',
        'version': '2.0.0',
        'api_connected': sio_client.connected,
        'api_url': API_URL,
        'active_clients': len(client_to_task),
        'active_tasks': len(task_to_clients)
    }

@app.route('/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def proxy_api(path: str) -> Union[Response, tuple]:
    """
    Proxy API requests to the Skype Parser API.

    Args:
        path: The API path to forward the request to

    Returns:
        Proxied response from the API server
    """
    if not API_URL:
        logger.error("API URL not configured, cannot proxy request")
        return jsonify({'error': 'API URL not configured'}), 500

    # Forward the request to the API
    url = f"{API_URL}/api/{path}"

    # Get request data
    headers = {key: value for key, value in request.headers if key.lower() != 'host'}
    data = request.get_data()

    # Forward the request
    try:
        logger.debug(f"Proxying {request.method} request to {url}")
        resp = requests.request(
            method=request.method,
            url=url,
            headers=headers,
            data=data,
            cookies=request.cookies,
            stream=True,
            timeout=30  # Add timeout
        )

        # Create response
        response = Response(
            resp.iter_content(chunk_size=10*1024),
            status=resp.status_code,
            content_type=resp.headers.get('Content-Type')
        )

        # Add headers
        for key, value in resp.headers.items():
            if key.lower() not in ('content-length', 'connection', 'content-encoding'):
                response.headers[key] = value

        return response
    except requests.Timeout:
        logger.error(f"Timeout proxying request to API: {url}")
        return jsonify({'error': 'API request timed out'}), 504
    except requests.RequestException as e:
        logger.error(f"Error proxying request to API: {e}")
        return jsonify({'error': 'Error connecting to API', 'details': str(e)}), 502

@app.errorhandler(404)
def not_found(error) -> Response:
    """Handle 404 errors by serving the index for SPA routing."""
    # Check if the request is for an API path
    if request.path.startswith('/api/'):
        return jsonify({'error': 'API endpoint not found'}), 404

    # For other paths, serve the index.html for SPA routing
    return send_from_directory('static', 'index.html')

# Socket.IO event handlers
@socketio.on('connect')
def handle_connect() -> None:
    """Handle client connection."""
    logger.info(f"Client connected: {request.sid}")

@socketio.on('disconnect')
def handle_disconnect() -> None:
    """Handle client disconnection."""
    logger.info(f"Client disconnected: {request.sid}")

    # Clean up mappings when a client disconnects
    if request.sid in client_to_task:
        task_id = client_to_task[request.sid]
        del client_to_task[request.sid]

        if task_id in task_to_clients:
            task_to_clients[task_id].discard(request.sid)
            if not task_to_clients[task_id]:
                del task_to_clients[task_id]

@socketio.on('subscribe')
def handle_subscribe(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Handle client subscription to task updates.

    Args:
        data: Dictionary containing task_id

    Returns:
        Response dictionary with success or error information
    """
    # Validate input
    if not isinstance(data, dict):
        return {'error': 'Invalid data format'}

    task_id = data.get('task_id')
    if not task_id:
        return {'error': 'No task_id provided'}

    if not isinstance(task_id, str):
        return {'error': 'task_id must be a string'}

    logger.info(f"Client {request.sid} subscribed to task {task_id}")

    # Create a mapping from client SID to task ID
    client_to_task[request.sid] = task_id

    # Create a mapping from task ID to client SIDs
    if task_id not in task_to_clients:
        task_to_clients[task_id] = set()
    task_to_clients[task_id].add(request.sid)

    # Ensure we have a handler for this task
    register_task_progress_handler(task_id)

    # Forward the subscription to the API server
    if sio_client.connected:
        try:
            response = sio_client.call('subscribe', {'task_id': task_id}, timeout=5)
            return response
        except socketio.exceptions.TimeoutError:
            logger.error(f"Timeout subscribing to task {task_id}")
            return {'error': 'Subscription request timed out'}
        except Exception as e:
            logger.error(f"Error subscribing to task {task_id}: {e}")
            return {'error': f'Error subscribing to task: {str(e)}'}
    else:
        # Schedule reconnection attempt if not already trying
        schedule_api_reconnection()
        return {'error': 'Not connected to API server, reconnection scheduled', 'status': 'pending'}

# Socket.IO client event handlers
@sio_client.event
def connect() -> None:
    """Handle connection to API server."""
    logger.info("Connected to API server")

    # Resubscribe to all tasks after reconnection
    resubscribe_to_tasks()

@sio_client.event
def disconnect() -> None:
    """Handle disconnection from API server."""
    logger.warning("Disconnected from API server")

    # Schedule reconnection
    schedule_api_reconnection()

@sio_client.event
def connect_error(data) -> None:
    """
    Handle connection error to API server.

    Args:
        data: Error data from Socket.IO
    """
    logger.error(f"Connection error to API server: {data}")

def resubscribe_to_tasks() -> None:
    """Resubscribe to all tasks after reconnection."""
    if not sio_client.connected:
        return

    # Get unique task IDs
    task_ids = set(task_to_clients.keys())

    if not task_ids:
        return

    logger.info(f"Resubscribing to {len(task_ids)} tasks")

    # Resubscribe to each task
    for task_id in task_ids:
        try:
            response = sio_client.call('subscribe', {'task_id': task_id}, timeout=5)
            logger.debug(f"Resubscribed to task {task_id}: {response}")
        except Exception as e:
            logger.error(f"Error resubscribing to task {task_id}: {e}")

# Dynamically register event handlers for task progress events
def register_task_progress_handler(task_id: str) -> None:
    """
    Register a handler for task progress events.

    Args:
        task_id: Task ID to register handler for
    """
    event_name = f'task_progress_{task_id}'

    # Only register if not already registered
    if event_name not in sio_client.handlers:
        @sio_client.on(event_name)
        def handle_task_progress(data):
            """
            Handle task progress event.

            Args:
                data: Progress data
            """
            logger.debug(f"Received progress for task {task_id}: {data}")

            # Forward the progress to all clients subscribed to this task
            if task_id in task_to_clients:
                for client_sid in task_to_clients[task_id]:
                    socketio.emit(event_name, data, room=client_sid)

def schedule_api_reconnection() -> None:
    """Schedule reconnection to API server in a separate thread."""
    global reconnect_thread, reconnect_active

    if reconnect_active:
        # Already trying to reconnect
        return

    reconnect_active = True
    reconnect_thread = threading.Thread(target=reconnect_to_api_server)
    reconnect_thread.daemon = True
    reconnect_thread.start()

def reconnect_to_api_server() -> None:
    """Repeatedly try to reconnect to API server."""
    global reconnect_active

    try:
        reconnect_attempt = 0
        max_attempts = 5

        while reconnect_attempt < max_attempts and not sio_client.connected:
            reconnect_attempt += 1
            backoff = min(reconnect_attempt * 2, 30)  # Exponential backoff, max 30 seconds

            logger.info(f"Attempting to reconnect to API server (attempt {reconnect_attempt}/{max_attempts})")

            try:
                if not API_URL:
                    logger.error("Cannot reconnect: API URL not configured")
                    break

                # Try to connect
                sio_client.connect(API_URL)

                if sio_client.connected:
                    logger.info(f"Successfully reconnected to API server at {API_URL}")
                    break
            except Exception as e:
                logger.error(f"Failed to reconnect to API server: {e}")

            logger.info(f"Waiting {backoff} seconds before next reconnection attempt")
            time.sleep(backoff)
    finally:
        reconnect_active = False

# Connect to API server
def connect_to_api_server() -> bool:
    """
    Connect to the API server.

    Returns:
        True if connection successful, False otherwise
    """
    if not API_URL:
        logger.error("API URL not configured")
        return False

    # Connect to the API server
    try:
        sio_client.connect(API_URL)
        logger.info(f"Connected to API server at {API_URL}")
        return True
    except Exception as e:
        logger.error(f"Error connecting to API server: {e}")
        return False

def check_static_directory() -> bool:
    """
    Check if the static directory exists and contains necessary files.

    Returns:
        True if static directory is valid, False otherwise
    """
    static_dir = Path('static')

    if not static_dir.exists():
        logger.warning("Static directory not found, creating it")
        static_dir.mkdir(exist_ok=True)

        # Create a placeholder index.html if it doesn't exist
        index_file = static_dir / 'index.html'
        if not index_file.exists():
            with open(index_file, 'w') as f:
                f.write("""
                <!DOCTYPE html>
                <html>
                <head>
                    <title>SkypeParser Frontend</title>
                    <meta charset="UTF-8">
                    <meta name="viewport" content="width=device-width, initial-scale=1.0">
                </head>
                <body>
                    <h1>SkypeParser Frontend Example</h1>
                    <p>This is a placeholder page. Please build the frontend first.</p>
                    <p>Server status: <a href="/status">Check status</a></p>
                </body>
                </html>
                """)
            logger.info("Created placeholder index.html")

        return False

    # Check for index.html
    if not (static_dir / 'index.html').exists():
        logger.warning("index.html not found in static directory")
        return False

    logger.info("Static directory check passed")
    return True

def parse_args():
    """
    Parse command-line arguments.

    Returns:
        Parsed arguments namespace
    """
    parser = argparse.ArgumentParser(
        description='Serve the front-end example for SkypeParser.',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Server options
    server_group = parser.add_argument_group('Server Configuration')
    server_group.add_argument(
        '--host',
        type=str,
        default=os.environ.get('SERVER_HOST', 'localhost'),
        help='Host to bind to'
    )

    server_group.add_argument(
        '--port',
        type=int,
        default=int(os.environ.get('SERVER_PORT', 8080)),
        help='Port to bind to'
    )

    server_group.add_argument(
        '--api-url',
        type=str,
        default=os.environ.get('API_URL', 'http://localhost:5000'),
        help='URL of the Skype Parser API'
    )

    # Security options
    security_group = parser.add_argument_group('Security Options')
    security_group.add_argument(
        '--cors-origins',
        type=str,
        default=os.environ.get('CORS_ORIGINS', '*'),
        help='Comma-separated list of allowed CORS origins'
    )

    security_group.add_argument(
        '--rate-limit',
        type=int,
        default=int(os.environ.get('RATE_LIMIT_MAX_REQUESTS', 100)),
        help='Maximum number of requests per minute per IP'
    )

    # Debugging options
    debug_group = parser.add_argument_group('Debugging')
    debug_group.add_argument(
        '--debug',
        action='store_true',
        default=os.environ.get('DEBUG', '').lower() in ('true', '1', 'yes'),
        help='Run in debug mode'
    )

    debug_group.add_argument(
        '--log-level',
        type=str,
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'],
        default=os.environ.get('LOG_LEVEL', 'INFO').upper(),
        help='Set the logging level'
    )

    args = parser.parse_args()

    # Apply any CORS origin changes
    if args.cors_origins != '*':
        global cors_origins
        cors_origins = args.cors_origins.split(',')

    # Apply rate limit changes
    if args.rate_limit != RATE_LIMIT_MAX_REQUESTS:
        global RATE_LIMIT_MAX_REQUESTS
        RATE_LIMIT_MAX_REQUESTS = args.rate_limit

    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    return args

def main():
    """Run the server."""
    try:
        args = parse_args()

        # Set API URL
        global API_URL
        API_URL = args.api_url.rstrip('/')  # Remove trailing slash if present

        # Initialize client-to-task and task-to-clients mappings
        global client_to_task, task_to_clients
        client_to_task = {}
        task_to_clients = {}

        # Check static directory
        check_static_directory()

        # Connect to API server
        api_connected = connect_to_api_server()
        if not api_connected:
            logger.warning("Could not connect to API server. Some features may not work correctly.")
            logger.info("Server will still start and will try to reconnect automatically.")

        # Set host and port for server
        host = args.host
        port = args.port

        # Log startup information
        startup_info = {
            'server': f"{host}:{port}",
            'api_url': API_URL,
            'api_connected': api_connected,
            'debug_mode': args.debug,
            'log_level': args.log_level,
            'cors_origins': cors_origins if isinstance(cors_origins, list) else socketio_cors_origins,
            'rate_limit': f"{RATE_LIMIT_MAX_REQUESTS} requests per {RATE_LIMIT_WINDOW} seconds"
        }

        logger.info(f"Starting server with configuration: {json.dumps(startup_info, indent=2)}")

        # Run the server
        socketio.run(app, host=host, port=port, debug=args.debug)
    except KeyboardInterrupt:
        logger.info("Server shutdown requested by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        import traceback
        logger.error(traceback.format_exc())
        sys.exit(1)
    finally:
        # Clean shutdown
        if sio_client.connected:
            sio_client.disconnect()
        logger.info("Server shutdown complete")

if __name__ == "__main__":
    main()