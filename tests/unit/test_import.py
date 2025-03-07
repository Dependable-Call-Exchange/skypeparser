#!/usr/bin/env python3
"""
Simple test script to verify that the ETL pipeline can be imported correctly.
This helps identify any import issues before running the full test suite.
"""

import os
import sys
import pytest

# Add the project root to the path
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

def test_etl_pipeline_import():
    """Test that the ETLPipeline class can be imported and instantiated."""
    try:
        from src.db.etl import ETLPipeline

        # Try to create an instance
        pipeline = ETLPipeline(db_config={}, use_di=False)

        # If we get here, the import and instantiation were successful
        assert pipeline is not None
        assert hasattr(pipeline, 'run_pipeline')

    except ImportError as e:
        pytest.fail(f"Failed to import ETLPipeline: {e}")
    except Exception as e:
        pytest.fail(f"Failed to instantiate ETLPipeline: {e}")

if __name__ == "__main__":
    # This allows the script to be run directly for quick testing
    try:
        print("Attempting to import ETLPipeline...")
        from src.db.etl import ETLPipeline
        print("Successfully imported ETLPipeline")

        # Try to create an instance
        pipeline = ETLPipeline(db_config={}, use_di=False)
        print("Successfully created ETLPipeline instance")

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