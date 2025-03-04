#!/usr/bin/env python3
"""
Web ETL Example

This example demonstrates how to use the Skype ETL pipeline in a web application context.
It provides a simple Flask application that allows users to upload Skype export files
and processes them using the ETL pipeline.

Note: This is a simplified example for demonstration purposes only.
In a production environment, you would need to add proper authentication,
error handling, and security measures.
"""

import os
import sys
import json
import logging
from typing import Dict, Any
from flask import Flask, request, jsonify, render_template_string

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the ETL pipeline
from src.db.etl_pipeline import SkypeETLPipeline

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Create Flask application
app = Flask(__name__)

# Configure upload folder
UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'uploads')
OUTPUT_FOLDER = os.path.join(os.path.dirname(__file__), 'output')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)

# Database configuration
DB_CONFIG = {
    'dbname': 'skype_archive',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432
}

# Simple HTML template for the upload form
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
    </style>
</head>
<body>
    <h1>Skype Export ETL Pipeline</h1>
    <p>Upload your Skype export file (TAR or JSON) to process it through the ETL pipeline.</p>

    <form action="/upload" method="post" enctype="multipart/form-data">
        <div class="form-group">
            <label for="file">Skype Export File:</label>
            <input type="file" id="file" name="file" accept=".tar,.json" required>
        </div>

        <div class="form-group">
            <label for="user_display_name">Your Display Name:</label>
            <input type="text" id="user_display_name" name="user_display_name" placeholder="How your name should appear in the logs">
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

@app.route('/')
def index():
    """Render the upload form."""
    return render_template_string(UPLOAD_TEMPLATE)

@app.route('/upload', methods=['POST'])
def upload_file():
    """
    Handle file upload and process it through the ETL pipeline.
    """
    try:
        # Check if a file was uploaded
        if 'file' not in request.files:
            return render_template_string(UPLOAD_TEMPLATE, result={'error': 'No file part'})

        file = request.files['file']

        # Check if the file is empty
        if file.filename == '':
            return render_template_string(UPLOAD_TEMPLATE, result={'error': 'No file selected'})

        # Get user display name
        user_display_name = request.form.get('user_display_name', '')

        # Process the file through the ETL pipeline
        pipeline = SkypeETLPipeline(
            db_config=DB_CONFIG,
            output_dir=OUTPUT_FOLDER
        )

        # Run the pipeline with the uploaded file
        results = pipeline.run_pipeline(
            file_obj=file,
            user_display_name=user_display_name
        )

        # Return the results
        return render_template_string(UPLOAD_TEMPLATE, result=results)

    except Exception as e:
        logger.error(f"Error processing uploaded file: {e}")
        return render_template_string(UPLOAD_TEMPLATE, result={'error': str(e)})

@app.route('/api/upload', methods=['POST'])
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

        # Get user display name
        user_display_name = request.form.get('user_display_name', '')

        # Process the file through the ETL pipeline
        pipeline = SkypeETLPipeline(
            db_config=DB_CONFIG,
            output_dir=OUTPUT_FOLDER
        )

        # Run the pipeline with the uploaded file
        results = pipeline.run_pipeline(
            file_obj=file,
            user_display_name=user_display_name
        )

        # Return the results
        return jsonify(results)

    except Exception as e:
        logger.error(f"Error processing uploaded file: {e}")
        return jsonify({'error': str(e)}), 500

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
    app.run(debug=True, host='0.0.0.0', port=5000)

if __name__ == "__main__":
    main()