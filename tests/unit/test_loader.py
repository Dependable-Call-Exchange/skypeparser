"""
Tests for the Loader class.
"""

import json
import logging
import unittest
from unittest.mock import MagicMock, patch

from src.db.etl.loader import Loader
from src.db.data_inserter import DataInserter
from src.db.schema_manager import SchemaManager


class TestLoader(unittest.TestCase):
    """Tests for the Loader class."""

    def setUp(self):
        """Set up test fixtures."""
        # Create mock database connection
        self.mock_db_connection = MagicMock()

        # Create mock schema manager
        self.mock_schema_manager = MagicMock()

        # Create mock data inserter
        self.mock_data_inserter = MagicMock()

        # Patch the SchemaManager and DataInserter classes
        self.schema_manager_patcher = patch('src.db.etl.loader.SchemaManager', return_value=self.mock_schema_manager)
        self.data_inserter_patcher = patch('src.db.etl.loader.DataInserter', return_value=self.mock_data_inserter)

        # Start the patchers
        self.mock_schema_manager_class = self.schema_manager_patcher.start()
        self.mock_data_inserter_class = self.data_inserter_patcher.start()

        # Create loader instance
        self.loader = Loader(
            db_connection=self.mock_db_connection,
            batch_size=100,
            create_schema=False
        )

    def tearDown(self):
        """Tear down test fixtures."""
        # Stop the patchers
        self.schema_manager_patcher.stop()
        self.data_inserter_patcher.stop()

    def test_init(self):
        """Test initialization of the Loader."""
        # Assert that the SchemaManager was created with the correct parameters
        self.mock_schema_manager_class.assert_called_once_with(self.mock_db_connection)

        # Assert that the DataInserter was created with the correct parameters
        self.mock_data_inserter_class.assert_called_once()

        # Assert that the schema was not created
        self.mock_schema_manager.create_schema.assert_not_called()

        # Assert that the loader has the correct attributes
        self.assertEqual(self.loader.db_connection, self.mock_db_connection)
        self.assertEqual(self.loader.schema_manager, self.mock_schema_manager)
        self.assertEqual(self.loader.data_inserter, self.mock_data_inserter)
        self.assertEqual(self.loader.batch_size, 100)

    def test_init_with_schema_creation(self):
        """Test initialization of the Loader with schema creation."""
        # Create loader instance with schema creation
        loader = Loader(
            db_connection=self.mock_db_connection,
            batch_size=100,
            create_schema=True
        )

        # Assert that the schema was created
        self.mock_schema_manager.create_schema.assert_called_once()

    def test_load(self):
        """Test loading data."""
        # Create test data
        transformed_data = {
            "conversations": {
                "conv1": {"id": "conv1", "display_name": "Conversation 1"},
                "conv2": {"id": "conv2", "display_name": "Conversation 2"}
            },
            "metadata": {
                "user_id": "user1",
                "user_display_name": "User 1"
            }
        }

        # Configure mock data inserter to return counts
        self.mock_data_inserter.insert.return_value = {
            "conversations": 2,
            "messages": 5,
            "users": 1
        }

        # Call the load method
        result = self.loader.load(transformed_data)

        # Assert that the data inserter was called with the correct parameters
        self.mock_data_inserter.insert.assert_called_once()

        # Get the actual argument passed to insert
        args, _ = self.mock_data_inserter.insert.call_args
        data_to_insert = args[0]

        # Assert that the data was prepared correctly
        self.assertEqual(len(data_to_insert["conversations"]), 2)
        self.assertEqual(data_to_insert["conversations"]["conv1"]["display_name"], "Conversation 1")
        self.assertEqual(data_to_insert["conversations"]["conv2"]["display_name"], "Conversation 2")
        self.assertEqual(data_to_insert["users"]["user1"]["display_name"], "User 1")

        # Assert that the result is correct
        self.assertEqual(result, {"conversations": 2, "messages": 5, "users": 1})

        # Assert that the metrics were updated
        self.assertEqual(self.loader._metrics["conversation_count"], 2)
        self.assertEqual(self.loader._metrics["message_count"], 5)
        self.assertEqual(self.loader._metrics["user_count"], 1)

    def test_load_with_messages(self):
        """Test loading data with messages."""
        # Create test data with messages
        transformed_data = {
            "conversations": {
                "conv1": {
                    "id": "conv1",
                    "display_name": "Conversation 1",
                    "messages": [
                        {"id": "msg1", "content": "Hello"},
                        {"id": "msg2", "content": "World"}
                    ]
                },
                "conv2": {
                    "id": "conv2",
                    "display_name": "Conversation 2",
                    "messages": [
                        {"id": "msg3", "content": "Test"},
                        {"id": "msg4", "content": "Message"},
                        {"id": "msg5", "content": "Content"}
                    ]
                }
            },
            "metadata": {
                "user_id": "user1",
                "user_display_name": "User 1"
            }
        }

        # Configure mock data inserter to return counts
        self.mock_data_inserter.insert.return_value = {
            "conversations": 2,
            "messages": 5,
            "users": 1
        }

        # Call the load method
        result = self.loader.load(transformed_data)

        # Assert that the data inserter was called with the correct parameters
        self.mock_data_inserter.insert.assert_called_once()

        # Get the actual argument passed to insert
        args, _ = self.mock_data_inserter.insert.call_args
        data_to_insert = args[0]

        # Assert that the data was prepared correctly
        self.assertEqual(len(data_to_insert["conversations"]), 2)
        self.assertEqual(data_to_insert["conversations"]["conv1"]["display_name"], "Conversation 1")
        self.assertEqual(data_to_insert["conversations"]["conv2"]["display_name"], "Conversation 2")

        # Assert that messages were extracted from conversations
        self.assertEqual(len(data_to_insert["messages"]["conv1"]), 2)
        self.assertEqual(len(data_to_insert["messages"]["conv2"]), 3)
        self.assertEqual(data_to_insert["messages"]["conv1"][0]["content"], "Hello")
        self.assertEqual(data_to_insert["messages"]["conv2"][2]["content"], "Content")

        # Assert that messages were removed from conversations
        self.assertNotIn("messages", data_to_insert["conversations"]["conv1"])
        self.assertNotIn("messages", data_to_insert["conversations"]["conv2"])

        # Assert that the result is correct
        self.assertEqual(result, {"conversations": 2, "messages": 5, "users": 1})

    def test_validate_input_data_valid(self):
        """Test validating valid input data."""
        # Create valid test data
        transformed_data = {
            "conversations": {
                "conv1": {"id": "conv1", "display_name": "Conversation 1"},
                "conv2": {"id": "conv2", "display_name": "Conversation 2"}
            }
        }

        # Call the validation method
        self.loader._validate_input_data(transformed_data)

        # No assertion needed - if no exception is raised, the test passes

    def test_validate_input_data_invalid_type(self):
        """Test validating input data with invalid type."""
        # Create invalid test data (not a dictionary)
        transformed_data = ["conv1", "conv2"]

        # Assert that validation raises ValueError
        with self.assertRaises(ValueError) as context:
            self.loader._validate_input_data(transformed_data)

        # Assert the error message
        self.assertEqual(str(context.exception), "Transformed data must be a dictionary")

    def test_validate_input_data_missing_conversations(self):
        """Test validating input data with missing conversations."""
        # Create invalid test data (missing conversations)
        transformed_data = {
            "metadata": {
                "user_id": "user1",
                "user_display_name": "User 1"
            }
        }

        # Assert that validation raises ValueError
        with self.assertRaises(ValueError) as context:
            self.loader._validate_input_data(transformed_data)

        # Assert the error message
        self.assertEqual(str(context.exception), "Transformed data must contain 'conversations' key")

    def test_validate_input_data_invalid_conversations(self):
        """Test validating input data with invalid conversations."""
        # Create invalid test data (conversations not a dictionary)
        transformed_data = {
            "conversations": ["conv1", "conv2"]
        }

        # Assert that validation raises ValueError
        with self.assertRaises(ValueError) as context:
            self.loader._validate_input_data(transformed_data)

        # Assert the error message
        self.assertEqual(str(context.exception), "Conversations must be a dictionary")

    def test_close(self):
        """Test closing the database connection."""
        # Call the close method
        self.loader.close()

        # Assert that the database connection was closed
        self.mock_db_connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()