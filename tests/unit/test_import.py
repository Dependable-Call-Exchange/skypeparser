#!/usr/bin/env python3
"""
Simple test script to verify that the ETL pipeline can be imported correctly.
This helps identify any import issues before running the full test suite.
"""

import os
import sys

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

try:
    print("Attempting to import SkypeETLPipeline...")
    from src.db.etl_pipeline import SkypeETLPipeline
    print("Successfully imported SkypeETLPipeline")

    # Try to create an instance
    pipeline = SkypeETLPipeline()
    print("Successfully created SkypeETLPipeline instance")

except ImportError as e:
    print(f"Import Error: {e}")
    print(f"Error module: {getattr(e, 'name', 'unknown')}")

    # Try to identify the source of the import error
    if hasattr(e, 'name') and e.name:
        module_name = e.name
        print(f"Checking if module {module_name} exists...")
        try:
            import importlib
            importlib.import_module(module_name)
            print(f"Module {module_name} exists but there might be an issue with a specific import within it")
        except ImportError:
            print(f"Module {module_name} does not exist")

except Exception as e:
    print(f"Other Error: {e}")
    print(f"Error type: {type(e).__name__}")