#!/usr/bin/env python3
"""
Integration tests for the Loader component.

This test suite provides integration testing for the Loader component,
verifying its functionality with database operations and dependency injection.
"""

import os
import sys
import logging
import json
import pytest
import tempfile
from typing import Dict, Any, Optional

# Add the src directory to the path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from src.db.etl.loader import Loader
from src.db.etl.context import ETLContext
from src.utils.di import get_service_provider, get_service
from src.utils.interfaces import LoaderProtocol, DatabaseConnectionProtocol
from src.utils.service_registry import register_all_services
from tests.fixtures import is_db_available, test_db_connection, get_test_db_config

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def adapt_transformed_data(transformed_data):
    """Adapt transformed data to the format expected by the loader."""
    logger.info("Adapting transformed data to the expected format")

    # Create a copy of the data to avoid modifying the original
    adapted_data = transformed_data.copy()

    # Add required keys if they don't exist
    if 'user' in adapted_data and 'id' in adapted_data['user']:
        adapted_data['user_id'] = adapted_data['user']['id']
    else:
        adapted_data['user_id'] = "test_user_id"

    # Add export_date if it doesn't exist
    if 'metadata' in adapted_data and 'transformed_at' in adapted_data['metadata']:
        adapted_data['export_date'] = adapted_data['metadata']['transformed_at']
    else:
        from datetime import datetime
        adapted_data['export_date'] = datetime.now().isoformat()

    logger.info("✅ Successfully adapted transformed data")
    return adapted_data


def load_transformed_data(file_path):
    """Load transformed data from a file."""
    logger.info(f"Loading transformed data from {file_path}")

    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

        logger.info("✅ Successfully loaded transformed data")
        return data
    except Exception as e:
        logger.error(f"❌ Error loading transformed data: {e}")
        return None


def load_raw_data(file_path):
    """Load raw data from a file."""
    logger.info(f"Loading raw data from {file_path}")

    try:
        with open(file_path, 'r') as f:
            data = json.load(f)

        logger.info("✅ Successfully loaded raw data")
        return data
    except Exception as e:
        logger.error(f"❌ Error loading raw data: {e}")
        return None


@pytest.mark.integration
class TestLoaderIntegration:
    """Integration tests for the Loader component."""

    def setup_method(self):
        """Set up the test environment."""
        # Skip integration tests unless database is available
        if not is_db_available():
            pytest.skip("Integration tests disabled. Database not available.")

        # Create temporary directories for test output
        self.temp_dir = tempfile.mkdtemp()
        self.output_dir = os.path.join(self.temp_dir, "loader_test")
        os.makedirs(self.output_dir, exist_ok=True)

        # Get database configuration
        self.db_config = get_test_db_config()

        # Create a context with the db_config
        self.context = ETLContext(db_config=self.db_config, output_dir=self.output_dir)

        # Load transformed data
        transformed_data_path = "output/transformer_test/transformed_data.json"
        if not os.path.exists(transformed_data_path):
            pytest.skip(f"Transformed data file not found: {transformed_data_path}")

        self.transformed_data = load_transformed_data(transformed_data_path)
        if not self.transformed_data:
            pytest.skip("Failed to load transformed data")

        # Adapt transformed data to the expected format
        self.adapted_data = adapt_transformed_data(self.transformed_data)

        # Set user_id and export_date in context
        self.context.user_id = self.adapted_data['user_id']
        self.context.export_date = self.adapted_data['export_date']

        # Load raw data
        raw_data_path = "output/raw_data.json"
        if os.path.exists(raw_data_path):
            self.raw_data = load_raw_data(raw_data_path)
        else:
            # Use transformed data as raw data for testing purposes
            logger.warning("Using transformed data as raw data for testing purposes")
            self.raw_data = self.transformed_data

    def teardown_method(self):
        """Clean up after the test."""
        # Clean up temporary directory
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_loader_with_direct_instantiation(self):
        """Test the loader by directly instantiating it."""
        logger.info("Testing loader with direct instantiation...")

        # Create a database connection using the test fixture
        with test_db_connection(self.db_config) as db_connection:
            # Create a loader instance with direct dependencies
            loader = Loader(
                context=self.context,
                db_connection=db_connection,
                batch_size=10  # Small batch size for testing
            )

            # Test database schema creation
            try:
                loader.connect_db()
                logger.info("✅ Successfully connected loader to database")

                # Test data loading
                try:
                    export_id = loader.load(self.raw_data, self.adapted_data, "test_file.json")
                    logger.info(f"✅ Successfully loaded data with export ID: {export_id}")

                    # Verify the export ID
                    assert export_id > 0, "Export ID should be a positive integer"
                except Exception as e:
                    logger.error(f"❌ Error loading data: {e}")
                    assert False, f"Error loading data: {e}"

                # Test database connection closing
                try:
                    loader.close_db()
                    logger.info("✅ Successfully closed database connection")
                except Exception as e:
                    logger.error(f"❌ Error closing database connection: {e}")
                    assert False, f"Error closing database connection: {e}"
            except Exception as e:
                logger.error(f"❌ Error creating database schema: {e}")
                assert False, f"Error creating database schema: {e}"

    def test_loader_with_dependency_injection(self):
        """Test the loader using dependency injection."""
        logger.info("Testing loader with dependency injection...")

        # Register all services with the test database configuration
        register_all_services(db_config=self.db_config)
        logger.info("Registered all services")

        # Get the loader from DI
        loader = get_service(LoaderProtocol)

        if not loader:
            pytest.fail("Failed to get loader from DI")

        # Set the context
        loader.context = self.context

        # Test database schema creation
        try:
            loader.connect_db()
            logger.info("✅ Successfully connected loader to database")

            # Test data loading
            try:
                export_id = loader.load(self.raw_data, self.adapted_data, "test_file.json")
                logger.info(f"✅ Successfully loaded data with export ID: {export_id}")

                # Verify the export ID
                assert export_id > 0, "Export ID should be a positive integer"
            except Exception as e:
                logger.error(f"❌ Error loading data: {e}")
                assert False, f"Error loading data: {e}"

            # Test database connection closing
            try:
                loader.close_db()
                logger.info("✅ Successfully closed database connection")
            except Exception as e:
                logger.error(f"❌ Error closing database connection: {e}")
                assert False, f"Error closing database connection: {e}"
        except Exception as e:
            logger.error(f"❌ Error creating database schema: {e}")
            assert False, f"Error creating database schema: {e}"
