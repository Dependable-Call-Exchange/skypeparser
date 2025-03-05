#!/usr/bin/env python
"""
Serve the front-end example.

This script serves the front-end example and proxies API requests to the Skype Parser API.
"""

import os
import sys
import argparse
import logging
from pathlib import Path
import requests
import socketio
from flask import Flask, request, send_from_directory, Response
from flask_socketio import SocketIO

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)

# Create SocketIO server
socketio = SocketIO(app, cors_allowed_origins="*")

# Create SocketIO client for connecting to the API server
sio_client = socketio.Client()

# API endpoint
API_URL = None


@app.route('/')
def index():
    """Serve the index.html file."""
    return send_from_directory('.', 'index.html')


@app.route('/api/<path:path>', methods=['GET', 'POST', 'PUT', 'DELETE'])
def proxy_api(path):
    """Proxy API requests to the Skype Parser API."""
    if not API_URL:
        return {'error': 'API URL not configured'}, 500

    # Forward the request to the API
    url = f"{API_URL}/api/{path}"

    # Get request data
    headers = {key: value for key, value in request.headers if key != 'Host'}
    data = request.get_data()

    # Forward the request
    try:
        resp = requests.request(
            method=request.method,
            url=url,
            headers=headers,
            data=data,
            cookies=request.cookies,
            stream=True
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
    except requests.RequestException as e:
        logger.error(f"Error proxying request to API: {e}")
        return {'error': 'Error connecting to API'}, 500


# Socket.IO event handlers
@socketio.on('connect')
def handle_connect():
    """Handle client connection."""
    logger.info(f"Client connected: {request.sid}")


@socketio.on('disconnect')
def handle_disconnect():
    """Handle client disconnection."""
    logger.info(f"Client disconnected: {request.sid}")


@socketio.on('subscribe')
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

    # Create a mapping from client SID to task ID
    client_to_task[request.sid] = task_id

    # Create a mapping from task ID to client SIDs
    if task_id not in task_to_clients:
        task_to_clients[task_id] = set()
    task_to_clients[task_id].add(request.sid)

    # Forward the subscription to the API server
    if sio_client.connected:
        try:
            response = sio_client.call('subscribe', {'task_id': task_id})
            return response
        except Exception as e:
            logger.error(f"Error subscribing to task {task_id}: {e}")
            return {'error': f'Error subscribing to task: {str(e)}'}
    else:
        return {'error': 'Not connected to API server'}


# Socket.IO client event handlers
@sio_client.event
def connect():
    """Handle connection to API server."""
    logger.info("Connected to API server")


@sio_client.event
def disconnect():
    """Handle disconnection from API server."""
    logger.error("Disconnected from API server")
    # Try to reconnect
    connect_to_api_server()


@sio_client.event
def connect_error(data):
    """Handle connection error to API server."""
    logger.error(f"Connection error to API server: {data}")


# Dynamically register event handlers for task progress events
def register_task_progress_handler(task_id):
    """
    Register a handler for task progress events.

    Args:
        task_id: Task ID to register handler for
    """
    @sio_client.on(f'task_progress_{task_id}')
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
                socketio.emit(f'task_progress_{task_id}', data, room=client_sid)


# Connect to API server
def connect_to_api_server():
    """Connect to the API server."""
    if not API_URL:
        logger.error("API URL not configured")
        return

    # Extract the base URL from the API URL
    api_base_url = API_URL

    # Connect to the API server
    try:
        sio_client.connect(api_base_url)
        logger.info(f"Connected to API server at {api_base_url}")
    except Exception as e:
        logger.error(f"Error connecting to API server: {e}")


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description='Serve the front-end example.')

    parser.add_argument(
        '--host',
        type=str,
        default='localhost',
        help='Host to bind to (default: localhost)'
    )

    parser.add_argument(
        '--port',
        type=int,
        default=8080,
        help='Port to bind to (default: 8080)'
    )

    parser.add_argument(
        '--api-url',
        type=str,
        default='http://localhost:5000',
        help='URL of the Skype Parser API (default: http://localhost:5000)'
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        help='Run in debug mode'
    )

    return parser.parse_args()


def main():
    """Run the server."""
    args = parse_args()

    # Set API URL
    global API_URL
    API_URL = args.api_url

    # Initialize client-to-task and task-to-clients mappings
    global client_to_task, task_to_clients
    client_to_task = {}
    task_to_clients = {}

    # Connect to API server
    connect_to_api_server()

    # Run the server
    logger.info(f'Starting server on {args.host}:{args.port}')
    logger.info(f'Proxying API requests to {API_URL}')
    socketio.run(app, host=args.host, port=args.port, debug=args.debug)


if __name__ == '__main__':
    main()