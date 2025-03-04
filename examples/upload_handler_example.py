#!/usr/bin/env python3
"""
Upload Handler Example

This example demonstrates how to use the file_handler module with uploaded files
in a web application context. It shows how to process both JSON and TAR files
that have been uploaded by users.

Note: This is a simplified example and doesn't include actual web framework code.
In a real application, you would integrate this with Flask, Django, FastAPI, etc.
"""

import io
import os
import sys
import logging
from typing import BinaryIO, Dict, Any, List

# Add the parent directory to the path so we can import the src module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import the file handler functions
from src.utils.file_handler import (
    read_file_object,
    read_tarfile_object,
    extract_tar_object,
    list_tar_object
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def process_uploaded_json(file_obj: BinaryIO) -> Dict[str, Any]:
    """
    Process an uploaded JSON file.

    In a real web application, this would be called with the file object
    from the uploaded file.

    Args:
        file_obj (BinaryIO): The uploaded file object

    Returns:
        dict: The parsed JSON data
    """
    try:
        # Parse the JSON data from the uploaded file
        data = read_file_object(file_obj)

        # Log some basic information about the data
        if isinstance(data, dict):
            logger.info(f"Successfully parsed JSON with {len(data)} top-level keys")
            if 'userId' in data and 'exportDate' in data:
                logger.info(f"Skype export for user {data['userId']} from {data['exportDate']}")

        return data
    except Exception as e:
        logger.error(f"Error processing uploaded JSON: {e}")
        raise

def process_uploaded_tar(file_obj: BinaryIO, extract_dir: str = None) -> Dict[str, Any]:
    """
    Process an uploaded TAR file containing Skype export data.

    In a real web application, this would be called with the file object
    from the uploaded file.

    Args:
        file_obj (BinaryIO): The uploaded file object
        extract_dir (str, optional): Directory to extract files to

    Returns:
        dict: The parsed JSON data from the TAR file
    """
    try:
        # First, list the contents to see what's in the archive
        contents = list_tar_object(file_obj)
        logger.info(f"TAR file contains {len(contents)} files")

        # Look for JSON files
        json_files = [f for f in contents if f.endswith('.json')]
        logger.info(f"Found {len(json_files)} JSON files in the archive")

        # If extract_dir is provided, extract all files
        if extract_dir:
            extracted_files = extract_tar_object(file_obj, extract_dir)
            logger.info(f"Extracted {len(extracted_files)} files to {extract_dir}")

        # Extract and parse the JSON data
        data = read_tarfile_object(file_obj, auto_select=True)

        # Log some basic information about the data
        if isinstance(data, dict):
            logger.info(f"Successfully parsed JSON with {len(data)} top-level keys")
            if 'userId' in data and 'exportDate' in data:
                logger.info(f"Skype export for user {data['userId']} from {data['exportDate']}")

        return data
    except Exception as e:
        logger.error(f"Error processing uploaded TAR: {e}")
        raise

def simulate_file_upload(file_path: str) -> BinaryIO:
    """
    Simulate a file upload by creating a file-like object from a local file.

    This is just for demonstration purposes. In a real web application,
    you would get the file object directly from the upload.

    Args:
        file_path (str): Path to a local file

    Returns:
        BinaryIO: A file-like object containing the file's contents
    """
    with open(file_path, 'rb') as f:
        content = f.read()

    # Create a file-like object
    file_obj = io.BytesIO(content)
    file_obj.name = os.path.basename(file_path)

    return file_obj

def main():
    """
    Main function to demonstrate the usage of the file handler with uploaded files.
    """
    # Check if a file path was provided
    if len(sys.argv) < 2:
        logger.error("Please provide a path to a JSON or TAR file")
        sys.exit(1)

    file_path = sys.argv[1]

    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        sys.exit(1)

    # Simulate a file upload
    uploaded_file = simulate_file_upload(file_path)

    # Process the file based on its extension
    if file_path.endswith('.json'):
        logger.info("Processing as JSON file")
        data = process_uploaded_json(uploaded_file)
    elif file_path.endswith('.tar'):
        logger.info("Processing as TAR file")
        # Create a temporary directory for extraction
        extract_dir = os.path.join(os.path.dirname(__file__), 'extracted')
        os.makedirs(extract_dir, exist_ok=True)

        data = process_uploaded_tar(uploaded_file, extract_dir)
    else:
        logger.error(f"Unsupported file type: {file_path}")
        sys.exit(1)

    # Print some information about the data
    if isinstance(data, dict):
        print("\nData Summary:")
        print(f"- User ID: {data.get('userId', 'Unknown')}")
        print(f"- Export Date: {data.get('exportDate', 'Unknown')}")

        if 'conversations' in data:
            conversations = data['conversations']
            print(f"- Number of conversations: {len(conversations)}")

            # Print information about the first few conversations
            for i, conv in enumerate(conversations[:3]):
                print(f"\nConversation {i+1}:")
                print(f"  - ID: {conv.get('id', 'Unknown')}")
                print(f"  - Display Name: {conv.get('displayName', 'Unknown')}")

                if 'MessageList' in conv:
                    messages = conv['MessageList']
                    print(f"  - Number of messages: {len(messages)}")

            if len(conversations) > 3:
                print(f"\n... and {len(conversations) - 3} more conversations")

if __name__ == "__main__":
    main()