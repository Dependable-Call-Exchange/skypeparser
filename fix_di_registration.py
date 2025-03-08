#!/usr/bin/env python3
"""
Script to register missing dependencies in the dependency injection system.

This script registers the FileHandlerProtocol and other missing dependencies
to fix the test failures.
"""

import os
import sys
from pathlib import Path

# Add the parent directory to the path so we can import from src
sys.path.insert(0, str(Path(__file__).resolve().parent))

from src.utils.di import ServiceProvider, get_service_provider
from src.utils.interfaces import FileHandlerProtocol, MessageHandlerFactoryProtocol
from src.utils.file_handler import FileHandler
from src.utils.message_type_handlers import SkypeMessageHandlerFactory

def register_dependencies():
    """Register missing dependencies in the dependency injection system."""
    print("Getting service provider...")
    provider = get_service_provider()

    print("Registering FileHandlerProtocol...")
    provider.register_singleton(FileHandlerProtocol, FileHandler())

    print("Registering MessageHandlerFactoryProtocol...")
    provider.register_singleton(MessageHandlerFactoryProtocol, SkypeMessageHandlerFactory())

    print("Dependencies registered successfully.")

if __name__ == "__main__":
    register_dependencies()