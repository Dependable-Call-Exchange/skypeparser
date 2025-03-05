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
from flask import Flask, request, send_from_directory, Response

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Create Flask app
app = Flask(__name__)

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

    # Run the server
    logger.info(f'Starting server on {args.host}:{args.port}')
    logger.info(f'Proxying API requests to {API_URL}')
    app.run(host=args.host, port=args.port, debug=args.debug)


if __name__ == '__main__':
    main()