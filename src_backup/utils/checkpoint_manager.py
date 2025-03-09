"""
Checkpoint manager for ETL pipeline.

This module provides the CheckpointManager class that handles
checkpoint creation, storage, and restoration for ETL processes.
"""

import base64
import datetime
import json
import logging
import os
import pickle
import uuid
from typing import Any, Dict, List, Optional, Set

from src.utils.new_structured_logging import get_logger, handle_errors, log_execution_time, with_context

logger = get_logger(__name__)


class DateTimeEncoder(json.JSONEncoder):
    """JSON encoder that handles datetime objects."""

    def default(self, obj):
        """Convert datetime objects to ISO format strings."""
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        return super().default(obj)


class CheckpointManager:
    """Handles checkpoint creation, storage, and restoration."""

    def __init__(
        self,
        output_dir: Optional[str] = None,
        serializable_attributes: Optional[List[str]] = None,
        data_attributes: Optional[List[str]] = None
    ):
        """
        Initialize the checkpoint manager.

        Args:
            output_dir: Directory to save checkpoint files
            serializable_attributes: List of attribute names that can be serialized
            data_attributes: List of attribute names that contain data to be serialized
        """
        self.output_dir = output_dir
        self.checkpoints: Dict[str, Dict[str, Any]] = {}
        self.serializable_attributes = serializable_attributes or []
        self.data_attributes = data_attributes or []

        # Create output directory if it doesn't exist
        if self.output_dir and not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir, exist_ok=True)
            logger.info(f"Created checkpoint directory: {self.output_dir}")

    @with_context(operation="create_checkpoint")
    @log_execution_time(level=logging.INFO)
    def create_checkpoint(
        self,
        context_state: Dict[str, Any],
        checkpoint_id: Optional[str] = None
    ) -> str:
        """
        Create a checkpoint of the current ETL state.

        Args:
            context_state: Dictionary containing the current state
            checkpoint_id: Optional ID for the checkpoint (generated if not provided)

        Returns:
            Checkpoint ID
        """
        # Generate checkpoint ID if not provided
        if checkpoint_id is None:
            checkpoint_id = str(uuid.uuid4())

        # Create checkpoint data
        checkpoint_data = {
            "id": checkpoint_id,
            "timestamp": datetime.datetime.now().isoformat(),
            "serialized_attributes": {},
            "serialized_data": {},
        }

        # Copy basic attributes
        for key, value in context_state.items():
            if key not in self.serializable_attributes and key not in self.data_attributes:
                if isinstance(value, (dict, list, str, int, float, bool)) or value is None:
                    checkpoint_data[key] = value

        # Serialize attributes
        for attr in self.serializable_attributes:
            if attr in context_state:
                value = context_state[attr]
                if isinstance(value, (dict, list, str, int, float, bool)) or value is None:
                    checkpoint_data["serialized_attributes"][attr] = value
                else:
                    try:
                        # Try to convert to JSON-serializable format
                        checkpoint_data["serialized_attributes"][attr] = json.loads(
                            json.dumps(value, cls=DateTimeEncoder)
                        )
                    except (TypeError, json.JSONDecodeError):
                        logger.warning(
                            f"Could not serialize attribute {attr} for checkpoint",
                            extra={
                                "checkpoint_id": checkpoint_id,
                            }
                        )

        # Serialize data attributes
        for attr in self.data_attributes:
            if attr in context_state and context_state[attr] is not None:
                try:
                    # Serialize data to base64-encoded pickle
                    data = context_state[attr]
                    serialized = base64.b64encode(pickle.dumps(data)).decode("utf-8")
                    checkpoint_data["serialized_data"][attr] = serialized
                except Exception as e:
                    logger.warning(
                        f"Could not serialize data attribute {attr} for checkpoint: {str(e)}",
                        extra={"checkpoint_id": checkpoint_id},
                    )

        # Store checkpoint
        self.checkpoints[checkpoint_id] = checkpoint_data

        # Save checkpoint to file if output directory is specified
        if self.output_dir:
            self._save_checkpoint_to_file(checkpoint_id, checkpoint_data)

        # Log checkpoint creation
        logger.info(
            f"Created ETL checkpoint: {checkpoint_id}",
            extra={
                "checkpoint_id": checkpoint_id,
            }
        )

        return checkpoint_id

    @handle_errors(log_level="ERROR", default_message="Error saving checkpoint to file")
    def _save_checkpoint_to_file(
        self, checkpoint_id: str, checkpoint_data: Dict[str, Any]
    ) -> None:
        """
        Save checkpoint to a file.

        Args:
            checkpoint_id: Checkpoint ID
            checkpoint_data: Checkpoint data
        """
        # Create checkpoint file path
        checkpoint_file = os.path.join(
            self.output_dir, f"checkpoint_{checkpoint_id}.json"
        )

        # Save checkpoint to file
        with open(checkpoint_file, "w") as f:
            json.dump(checkpoint_data, f, indent=2, cls=DateTimeEncoder)

        logger.debug(
            f"Saved checkpoint to file: {checkpoint_file}",
            extra={
                "checkpoint_id": checkpoint_id,
                "checkpoint_file": checkpoint_file,
            }
        )

    @with_context(operation="restore_checkpoint")
    @log_execution_time(level=logging.INFO)
    def restore_checkpoint(self, checkpoint_id: str) -> Dict[str, Any]:
        """
        Restore a checkpoint.

        Args:
            checkpoint_id: Checkpoint ID

        Returns:
            Restored checkpoint data

        Raises:
            ValueError: If the checkpoint does not exist
        """
        # Check if checkpoint exists in memory
        if checkpoint_id in self.checkpoints:
            checkpoint = self.checkpoints[checkpoint_id]
        else:
            # Try to load checkpoint from file
            checkpoint = self._load_checkpoint_from_file(checkpoint_id)

        if not checkpoint:
            raise ValueError(f"Checkpoint {checkpoint_id} does not exist")

        # Deserialize data attributes
        restored_data = {}
        for attr, serialized in checkpoint.get("serialized_data", {}).items():
            try:
                # Deserialize data from base64-encoded pickle
                data = pickle.loads(base64.b64decode(serialized.encode("utf-8")))
                restored_data[attr] = data
            except Exception as e:
                logger.warning(
                    f"Could not deserialize data attribute {attr} from checkpoint: {str(e)}",
                    extra={"checkpoint_id": checkpoint_id},
                )

        # Combine basic attributes, serialized attributes, and deserialized data
        restored_checkpoint = {
            **checkpoint,
            **checkpoint.get("serialized_attributes", {}),
            **restored_data,
        }

        # Remove internal checkpoint structure
        if "serialized_attributes" in restored_checkpoint:
            del restored_checkpoint["serialized_attributes"]
        if "serialized_data" in restored_checkpoint:
            del restored_checkpoint["serialized_data"]

        logger.info(
            f"Restored checkpoint: {checkpoint_id}",
            extra={
                "checkpoint_id": checkpoint_id,
                "restored_keys": list(restored_checkpoint.keys()),
            }
        )

        return restored_checkpoint

    @handle_errors(log_level="ERROR", default_message="Error loading checkpoint from file")
    def _load_checkpoint_from_file(self, checkpoint_id: str) -> Optional[Dict[str, Any]]:
        """
        Load checkpoint from a file.

        Args:
            checkpoint_id: Checkpoint ID

        Returns:
            Loaded checkpoint data or None if the file does not exist
        """
        if not self.output_dir:
            return None

        # Create checkpoint file path
        checkpoint_file = os.path.join(
            self.output_dir, f"checkpoint_{checkpoint_id}.json"
        )

        # Check if file exists
        if not os.path.exists(checkpoint_file):
            logger.warning(
                f"Checkpoint file does not exist: {checkpoint_file}",
                extra={"checkpoint_id": checkpoint_id},
            )
            return None

        # Load checkpoint from file
        with open(checkpoint_file, "r") as f:
            checkpoint = json.load(f)

        # Store checkpoint in memory
        self.checkpoints[checkpoint_id] = checkpoint

        logger.debug(
            f"Loaded checkpoint from file: {checkpoint_file}",
            extra={
                "checkpoint_id": checkpoint_id,
                "checkpoint_file": checkpoint_file,
            }
        )

        return checkpoint

    def get_checkpoint(self, checkpoint_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a checkpoint.

        Args:
            checkpoint_id: Checkpoint ID

        Returns:
            Checkpoint data or None if the checkpoint does not exist
        """
        if checkpoint_id in self.checkpoints:
            return self.checkpoints[checkpoint_id]

        # Try to load checkpoint from file
        return self._load_checkpoint_from_file(checkpoint_id)

    def list_checkpoints(self) -> List[str]:
        """
        List all checkpoint IDs.

        Returns:
            List of checkpoint IDs
        """
        # Get checkpoints from memory
        checkpoint_ids = set(self.checkpoints.keys())

        # Get checkpoints from files
        if self.output_dir and os.path.exists(self.output_dir):
            for filename in os.listdir(self.output_dir):
                if filename.startswith("checkpoint_") and filename.endswith(".json"):
                    checkpoint_id = filename[len("checkpoint_"):-len(".json")]
                    checkpoint_ids.add(checkpoint_id)

        return list(checkpoint_ids)

    def delete_checkpoint(self, checkpoint_id: str) -> bool:
        """
        Delete a checkpoint.

        Args:
            checkpoint_id: Checkpoint ID

        Returns:
            True if the checkpoint was deleted, False otherwise
        """
        deleted = False

        # Delete from memory
        if checkpoint_id in self.checkpoints:
            del self.checkpoints[checkpoint_id]
            deleted = True

        # Delete from file
        if self.output_dir:
            checkpoint_file = os.path.join(
                self.output_dir, f"checkpoint_{checkpoint_id}.json"
            )
            if os.path.exists(checkpoint_file):
                os.remove(checkpoint_file)
                deleted = True

        if deleted:
            logger.info(
                f"Deleted checkpoint: {checkpoint_id}",
                extra={"checkpoint_id": checkpoint_id},
            )

        return deleted