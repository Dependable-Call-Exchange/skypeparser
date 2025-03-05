"""
User Management Module for Skype Parser API

This module provides user management functionality for the Skype Parser API,
including user authentication, registration, and API key management.
"""

import os
import json
import logging
import secrets
import hashlib
import time
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class UserManager:
    """
    User management class for the Skype Parser API.

    This class provides methods for user authentication, registration, and API key management.
    User data is stored in a JSON file.
    """

    def __init__(self, user_file: Optional[str] = None):
        """
        Initialize the user manager.

        Args:
            user_file: Path to the user data file
        """
        self.user_file = user_file or os.environ.get('USER_FILE', 'users.json')
        self.users = self._load_users()

    def _load_users(self) -> Dict[str, Dict[str, Any]]:
        """
        Load users from the user data file.

        Returns:
            dict: Dictionary of users
        """
        if os.path.exists(self.user_file):
            try:
                with open(self.user_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.error(f"Error loading user data: {e}")

        # Return empty dict if file doesn't exist or there's an error
        return {}

    def _save_users(self) -> None:
        """Save users to the user data file."""
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(os.path.abspath(self.user_file)), exist_ok=True)

            with open(self.user_file, 'w') as f:
                json.dump(self.users, f, indent=2)
        except Exception as e:
            logger.error(f"Error saving user data: {e}")

    def _hash_password(self, password: str, salt: Optional[str] = None) -> Tuple[str, str]:
        """
        Hash a password with a salt.

        Args:
            password: Password to hash
            salt: Salt to use (if None, a new salt will be generated)

        Returns:
            tuple: (hashed_password, salt)
        """
        if salt is None:
            salt = secrets.token_hex(16)

        # Hash the password with the salt
        hashed = hashlib.pbkdf2_hmac(
            'sha256',
            password.encode('utf-8'),
            salt.encode('utf-8'),
            100000
        ).hex()

        return hashed, salt

    def register_user(self, username: str, password: str, email: str, display_name: str) -> bool:
        """
        Register a new user.

        Args:
            username: Username
            password: Password
            email: Email address
            display_name: Display name

        Returns:
            bool: True if registration was successful, False otherwise
        """
        # Check if username already exists
        if username in self.users:
            logger.warning(f"Username {username} already exists")
            return False

        # Hash the password
        hashed_password, salt = self._hash_password(password)

        # Generate API key
        api_key = self._generate_api_key()

        # Create user
        self.users[username] = {
            'username': username,
            'password': hashed_password,
            'salt': salt,
            'email': email,
            'display_name': display_name,
            'api_key': api_key,
            'created_at': time.time(),
            'last_login': None
        }

        # Save users
        self._save_users()

        logger.info(f"User {username} registered successfully")
        return True

    def authenticate_user(self, username: str, password: str) -> bool:
        """
        Authenticate a user.

        Args:
            username: Username
            password: Password

        Returns:
            bool: True if authentication was successful, False otherwise
        """
        # Check if username exists
        if username not in self.users:
            logger.warning(f"Username {username} not found")
            return False

        # Get user
        user = self.users[username]

        # Hash the password with the user's salt
        hashed_password, _ = self._hash_password(password, user['salt'])

        # Check if passwords match
        if hashed_password != user['password']:
            logger.warning(f"Invalid password for user {username}")
            return False

        # Update last login
        user['last_login'] = time.time()
        self._save_users()

        logger.info(f"User {username} authenticated successfully")
        return True

    def get_user(self, username: str) -> Optional[Dict[str, Any]]:
        """
        Get a user by username.

        Args:
            username: Username

        Returns:
            dict: User data, or None if user not found
        """
        return self.users.get(username)

    def get_user_by_api_key(self, api_key: str) -> Optional[Dict[str, Any]]:
        """
        Get a user by API key.

        Args:
            api_key: API key

        Returns:
            dict: User data, or None if user not found
        """
        for username, user in self.users.items():
            if user.get('api_key') == api_key:
                return user

        return None

    def update_user(self, username: str, **kwargs) -> bool:
        """
        Update a user.

        Args:
            username: Username
            **kwargs: User data to update

        Returns:
            bool: True if update was successful, False otherwise
        """
        # Check if username exists
        if username not in self.users:
            logger.warning(f"Username {username} not found")
            return False

        # Get user
        user = self.users[username]

        # Update user data
        for key, value in kwargs.items():
            if key == 'password':
                # Hash the password
                hashed_password, salt = self._hash_password(value)
                user['password'] = hashed_password
                user['salt'] = salt
            elif key in ['username', 'api_key', 'created_at', 'last_login']:
                # Skip these fields
                continue
            else:
                user[key] = value

        # Save users
        self._save_users()

        logger.info(f"User {username} updated successfully")
        return True

    def delete_user(self, username: str) -> bool:
        """
        Delete a user.

        Args:
            username: Username

        Returns:
            bool: True if deletion was successful, False otherwise
        """
        # Check if username exists
        if username not in self.users:
            logger.warning(f"Username {username} not found")
            return False

        # Delete user
        del self.users[username]

        # Save users
        self._save_users()

        logger.info(f"User {username} deleted successfully")
        return True

    def _generate_api_key(self) -> str:
        """
        Generate a new API key.

        Returns:
            str: API key
        """
        return secrets.token_hex(32)

    def regenerate_api_key(self, username: str) -> Optional[str]:
        """
        Regenerate API key for a user.

        Args:
            username: Username

        Returns:
            str: New API key, or None if user not found
        """
        # Check if username exists
        if username not in self.users:
            logger.warning(f"Username {username} not found")
            return None

        # Generate new API key
        api_key = self._generate_api_key()

        # Update user
        self.users[username]['api_key'] = api_key

        # Save users
        self._save_users()

        logger.info(f"API key regenerated for user {username}")
        return api_key

    def get_all_users(self) -> List[Dict[str, Any]]:
        """
        Get all users.

        Returns:
            list: List of users
        """
        # Return a list of users without sensitive information
        return [
            {
                'username': user['username'],
                'email': user['email'],
                'display_name': user['display_name'],
                'created_at': user['created_at'],
                'last_login': user['last_login']
            }
            for user in self.users.values()
        ]


# Create a global user manager instance
_user_manager = None


def get_user_manager(user_file: Optional[str] = None) -> UserManager:
    """
    Get the global user manager instance.

    Args:
        user_file: Path to the user data file

    Returns:
        UserManager: User manager instance
    """
    global _user_manager

    if _user_manager is None:
        _user_manager = UserManager(user_file)

    return _user_manager