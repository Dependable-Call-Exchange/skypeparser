#!/usr/bin/env python3
"""
Script to fix issues in the TestableETLPipeline class and related files.

This script addresses the following issues:
1. Correcting import paths
2. Renaming service classes
3. Fixing the validation override in TestableETLPipeline
4. Adjusting the DI registration
"""

import os
import sys
import ast
import difflib
import argparse
import shutil
from typing import List, Dict, Any, Optional, Tuple, Set, Union

try:
    import libcst as cst
    from libcst.metadata import MetadataWrapper
    LIBCST_AVAILABLE = True
except ImportError:
    print("Warning: libcst is not installed. Using fallback methods.")
    print("For better results, install libcst: pip install libcst")
    LIBCST_AVAILABLE = False

# Define the root directory of the project
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# Import mappings to fix
IMPORT_REPLACEMENTS = {
    "from src.db.etl import ETLPipeline": "from src.db.etl.pipeline_manager import ETLPipeline",
    "from src.utils.message_type_handlers import MessageHandlerFactory": "from src.utils.message_type_handlers import SkypeMessageHandlerFactory",
}

# Class name replacements
CLASS_REPLACEMENTS = {
    "MessageHandlerFactory": "SkypeMessageHandlerFactory",
}

# File patterns for targeted fixes
TESTABLE_ETL_PATTERN = "testable_etl_pipeline.py"
TEST_EXAMPLE_PATTERN = "refactored_test_example.py"

def find_python_files(root_dir: str) -> List[str]:
    """
    Find all Python files in the given directory recursively.

    Args:
        root_dir: The root directory to search in

    Returns:
        List of file paths to Python files
    """
    python_files = []
    for dirpath, _, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith('.py'):
                python_files.append(os.path.join(dirpath, filename))
    return python_files

def validate_python_syntax(file_path: str, content: str) -> bool:
    """
    Validate that the content is valid Python syntax.

    Args:
        file_path: Path to the file being validated
        content: Python code content to validate

    Returns:
        True if the syntax is valid, False otherwise
    """
    try:
        ast.parse(content)
        return True
    except SyntaxError as e:
        print(f"ERROR: Syntax error in {file_path} after modification:")
        print(f"  Line {e.lineno}, column {e.offset}: {e.text}")
        print(f"  {e}")
        return False

def backup_file(file_path: str) -> None:
    """
    Create a backup of a file.

    Args:
        file_path: Path to the file to backup
    """
    backup_path = f"{file_path}.bak"
    print(f"Creating backup of {file_path} to {backup_path}")
    shutil.copy2(file_path, backup_path)

def show_diff(original: str, modified: str, file_path: str) -> None:
    """
    Show the diff between original and modified content.

    Args:
        original: Original content
        modified: Modified content
        file_path: Path to the file being modified
    """
    print(f"\nDiff for {file_path}:")
    diff = difflib.unified_diff(
        original.splitlines(),
        modified.splitlines(),
        fromfile=f"{file_path} (original)",
        tofile=f"{file_path} (modified)",
        lineterm=""
    )
    diff_text = "\n".join(diff)
    if diff_text:
        print(diff_text)
    else:
        print("No changes.")

# LibCST-based transformers
if LIBCST_AVAILABLE:
    class ImportTransformer(cst.CSTTransformer):
        """Transform ImportFrom nodes to fix import statements."""

        def leave_ImportFrom(self, original_node: cst.ImportFrom, updated_node: cst.ImportFrom) -> cst.ImportFrom:
            """
            Replace incorrect imports with the correct ones.

            Args:
                original_node: Original ImportFrom node
                updated_node: Updated ImportFrom node

            Returns:
                Modified ImportFrom node
            """
            # Handle src.db.etl → src.db.etl.pipeline_manager for ETLPipeline
            if (original_node.module and
                isinstance(original_node.module, cst.Name) and
                original_node.module.value == "src.db.etl" and
                any(name.name.value == "ETLPipeline" for name in original_node.names)):

                return cst.ImportFrom(
                    module=cst.Name(value="src.db.etl.pipeline_manager"),
                    names=[
                        name for name in original_node.names
                        if name.name.value == "ETLPipeline"
                    ]
                )

            # Handle MessageHandlerFactory → SkypeMessageHandlerFactory
            if (original_node.module and
                isinstance(original_node.module, cst.Attribute) and
                original_node.module.attr.value == "message_type_handlers" and
                any(name.name.value == "MessageHandlerFactory" for name in original_node.names)):

                new_names = []
                for name in original_node.names:
                    if name.name.value == "MessageHandlerFactory":
                        new_names.append(cst.ImportAlias(
                            name=cst.Name(value="SkypeMessageHandlerFactory"),
                            asname=name.asname
                        ))
                    else:
                        new_names.append(name)

                return cst.ImportFrom(
                    module=original_node.module,
                    names=new_names
                )

            return updated_node

    class ClassNameTransformer(cst.CSTTransformer):
        """Transform class name references."""

        def leave_Name(self, original_node: cst.Name, updated_node: cst.Name) -> cst.Name:
            """
            Replace incorrect class names with the correct ones.

            Args:
                original_node: Original Name node
                updated_node: Updated Name node

            Returns:
                Modified Name node
            """
            if original_node.value in CLASS_REPLACEMENTS:
                return cst.Name(value=CLASS_REPLACEMENTS[original_node.value])
            return updated_node

def fix_imports_with_cst(file_path: str, dry_run: bool = False) -> bool:
    """
    Fix imports using LibCST for AST-based transformations.

    Args:
        file_path: Path to the file to fix
        dry_run: Whether to only show changes without modifying files

    Returns:
        True if changes were made or would be made, False otherwise
    """
    print(f"Fixing imports in {file_path} using LibCST")

    # Read the file
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Parse the code
    module = MetadataWrapper(cst.parse_module(content))

    # Apply transformers
    modified_module = module.visit(ImportTransformer())
    modified_module = modified_module.visit(ClassNameTransformer())

    # Get the modified code
    modified_content = modified_module.code

    # Check if changes were made
    if content == modified_content:
        print(f"No import changes needed in {file_path}")
        return False

    # Show diff
    show_diff(content, modified_content, file_path)

    # If it's a dry run, don't modify the file
    if dry_run:
        print(f"Dry run: Not modifying {file_path}")
        return True

    # Validate syntax
    if not validate_python_syntax(file_path, modified_content):
        print(f"Error: Modified content has syntax errors, not updating {file_path}")
        return False

    # Backup file
    backup_file(file_path)

    # Write the file
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(modified_content)

    print(f"Updated imports in {file_path}")
    return True

def fix_imports_with_regex(file_path: str, dry_run: bool = False) -> bool:
    """
    Fix imports using regex (fallback when LibCST is not available).

    Args:
        file_path: Path to the file to fix
        dry_run: Whether to only show changes without modifying files

    Returns:
        True if changes were made or would be made, False otherwise
    """
    import re
    print(f"Fixing imports in {file_path} using regex (fallback method)")

    # Read the file
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    original_content = content
    replacements_made = 0

    # Replace imports
    for old_import, new_import in IMPORT_REPLACEMENTS.items():
        content, count = re.subn(re.escape(old_import), new_import, content)
        replacements_made += count
        if count > 0:
            print(f"Replaced '{old_import}' with '{new_import}' ({count} times)")

    # Replace class names
    for old_class, new_class in CLASS_REPLACEMENTS.items():
        # Use regex to replace only class names, not parts of other words
        content, count = re.subn(r'\b' + re.escape(old_class) + r'\b', new_class, content)
        replacements_made += count
        if count > 0:
            print(f"Replaced '{old_class}' with '{new_class}' ({count} times)")

    # If no replacements were made, we're done
    if replacements_made == 0:
        print(f"No changes needed in {file_path}")
        return False

    # Show diff
    show_diff(original_content, content, file_path)

    # If it's a dry run, don't modify the file
    if dry_run:
        print(f"Dry run: Not modifying {file_path}")
        return True

    # Validate syntax
    if not validate_python_syntax(file_path, content):
        print(f"Error: Modified content has syntax errors, not updating {file_path}")
        return False

    # Backup file
    backup_file(file_path)

    # Write the file
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

    print(f"Updated {replacements_made} imports/class references in {file_path}")
    return True

def fix_validation_override(file_path: str, dry_run: bool = False) -> bool:
    """
    Fix the validation override in TestableETLPipeline.

    This function uses a surgical approach to modify only the specific
    validation override method without touching other parts of the file.

    Args:
        file_path: Path to the file to fix
        dry_run: Whether to only show changes without modifying files

    Returns:
        True if changes were made or would be made, False otherwise
    """
    import re
    print(f"Fixing validation override in {file_path}")

    # Read the file
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Check if the file contains a run_pipeline method
    if "def run_pipeline" not in content:
        print(f"Warning: No run_pipeline method found in {file_path}")
        return False

    # Find the run_pipeline method and make targeted changes
    original_content = content

    # Define the improved run_pipeline implementation using patch
    improved_impl = """    def run_pipeline(
        self,
        file_path: Optional[str] = None,
        file_obj: Optional[BinaryIO] = None,
        is_tar: bool = False,
        user_display_name: Optional[str] = None,
        resume_from_checkpoint: bool = False,
        checkpoint_id: Optional[str] = None
    ) -> Dict[str, Any]:
        \"\"\"
        Run the ETL pipeline with the injected dependencies.

        Args:
            file_path: Path to the Skype export file
            file_obj: File object (alternative to file_path)
            is_tar: Whether the file is a tar file
            user_display_name: Display name of the user
            resume_from_checkpoint: Whether to resume from a checkpoint
            checkpoint_id: ID of the checkpoint to resume from

        Returns:
            Dictionary with the results of the pipeline run
        \"\"\"
        # Use a patch context manager to override os.path.exists and os.path.isfile
        with patch('os.path.exists', return_value=True), patch('os.path.isfile', return_value=True):
            return self.pipeline.run_pipeline(
                file_path=file_path,
                file_obj=file_obj,
                user_display_name=user_display_name,
                resume_from_checkpoint=resume_from_checkpoint
            )"""

    # Use a regex pattern that captures the entire run_pipeline method
    pattern = r'(\s+)def run_pipeline\s*\([^)]*\)(\s*->.*?)?\s*:.*?(?=\s+def|\s*$)'

    # Find if there's a match
    match = re.search(pattern, content, re.DOTALL)
    if not match:
        print(f"Warning: Could not find run_pipeline method in {file_path}")
        return False

    # Replace the entire run_pipeline method
    modified_content = re.sub(pattern, improved_impl, content, flags=re.DOTALL)

    # Check if changes were made
    if content == modified_content:
        print(f"No validation override changes needed in {file_path}")
        return False

    # Show diff
    show_diff(content, modified_content, file_path)

    # If it's a dry run, don't modify the file
    if dry_run:
        print(f"Dry run: Not modifying {file_path}")
        return True

    # Validate syntax
    if not validate_python_syntax(file_path, modified_content):
        print(f"Error: Modified content has syntax errors, not updating {file_path}")
        return False

    # Backup file
    backup_file(file_path)

    # Write the file
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(modified_content)

    print(f"Updated validation override in {file_path}")
    return True

def fix_di_registration(file_path: str, dry_run: bool = False) -> bool:
    """
    Fix the DI registration in TestableETLPipeline.

    Args:
        file_path: Path to the file to fix
        dry_run: Whether to only show changes without modifying files

    Returns:
        True if changes were made or would be made, False otherwise
    """
    import re
    print(f"Fixing DI registration in {file_path}")

    # Read the file
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Check if the file contains an __init__ method
    if "def __init__" not in content:
        print(f"Warning: No __init__ method found in {file_path}")
        return False

    original_content = content

    # Define the pattern to match the DI registration
    pattern = r'if\s+self\.use_di\s*:.*?self\.pipeline\s*=\s*ETLPipeline\s*\([^)]*\)'

    # Define the replacement
    replacement = """if self.use_di:
            # Use the service registry functions
            provider = get_service_provider()
            register_core_services(provider=provider)
            register_database_connection(db_config=db_config, provider=provider)

            # Register the context we created
            provider.register_singleton(ETLContext, self.context)

            # Register our injected dependencies if provided
            if content_extractor:
                provider.register_singleton(ContentExtractorProtocol, content_extractor)
            if structured_data_extractor:
                provider.register_singleton(StructuredDataExtractorProtocol, structured_data_extractor)
            if message_handler_factory:
                provider.register_singleton(MessageHandlerFactoryProtocol, message_handler_factory)
            if db_connection:
                provider.register_singleton(DatabaseConnectionProtocol, db_connection)

            # Register ETL services with our context
            register_etl_services(db_config=db_config, provider=provider)

            self.pipeline = ETLPipeline(db_config=db_config, context=self.context, use_di=True)"""

    # Find if there's a match
    match = re.search(pattern, content, re.DOTALL)
    if not match:
        print(f"Warning: Could not find DI registration block in {file_path}")
        return False

    # Replace the DI registration
    modified_content = re.sub(pattern, replacement, content, flags=re.DOTALL)

    # Check if changes were made
    if content == modified_content:
        print(f"No DI registration changes needed in {file_path}")
        return False

    # Show diff
    show_diff(content, modified_content, file_path)

    # If it's a dry run, don't modify the file
    if dry_run:
        print(f"Dry run: Not modifying {file_path}")
        return True

    # Validate syntax
    if not validate_python_syntax(file_path, modified_content):
        print(f"Error: Modified content has syntax errors, not updating {file_path}")
        return False

    # Backup file
    backup_file(file_path)

    # Write the file
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(modified_content)

    print(f"Updated DI registration in {file_path}")
    return True

def fix_test_implementation(file_path: str, dry_run: bool = False) -> bool:
    """
    Fix the test implementation with better mocks and setup.

    Args:
        file_path: Path to the file to fix
        dry_run: Whether to only show changes without modifying files

    Returns:
        True if changes were made or would be made, False otherwise
    """
    import re
    print(f"Fixing test implementation in {file_path}")

    # Read the file
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Check if the file contains a test method
    if "test_refactored_with_dependency_injection" not in content:
        print(f"Warning: No test_refactored_with_dependency_injection method found in {file_path}")
        return False

    original_content = content

    # Define the pattern to match the pipeline creation
    pattern = r'pipeline\s*=\s*TestableETLPipeline\s*\(.*?db_connection\s*=\s*mock_db\.conn\s*\)'

    # Define the replacement
    replacement = """# Create mock content extractor
        mock_content_extractor = MagicMock()
        mock_content_extractor.extract_mentions.return_value = []

        # Create mock structured data extractor
        mock_structured_data_extractor = MagicMock()
        mock_structured_data_extractor.extract_structured_data.return_value = {}

        # Create mock message handler factory
        mock_message_handler_factory = MagicMock()
        mock_message_handler_factory.get_handler.return_value = MagicMock()

        # Create testable pipeline with all dependencies injected
        pipeline = TestableETLPipeline(
            # Provide a valid db_config to pass validation
            db_config=self.test_db_config,
            use_di=False,  # Explicitly set to False
            # File operations
            read_file_func=mock_env['read_file'],
            # Validation functions
            validate_file_exists_func=mock_env['validate_file_exists'],
            validate_json_file_func=mock_env['validate_json_file'],
            validate_user_display_name_func=mock_validate_user_display_name,
            # Database connection
            db_connection=mock_db.conn,
            # Additional dependencies
            content_extractor=mock_content_extractor,
            structured_data_extractor=mock_structured_data_extractor,
            message_handler_factory=mock_message_handler_factory
        )"""

    # Find if there's a match
    match = re.search(pattern, content, re.DOTALL)
    if not match:
        print(f"Warning: Could not find pipeline creation in {file_path}")
        return False

    # Replace the pipeline creation
    modified_content = re.sub(pattern, replacement, content, flags=re.DOTALL)

    # Check if changes were made
    if content == modified_content:
        print(f"No test implementation changes needed in {file_path}")
        return False

    # Show diff
    show_diff(content, modified_content, file_path)

    # If it's a dry run, don't modify the file
    if dry_run:
        print(f"Dry run: Not modifying {file_path}")
        return True

    # Validate syntax
    if not validate_python_syntax(file_path, modified_content):
        print(f"Error: Modified content has syntax errors, not updating {file_path}")
        return False

    # Backup file
    backup_file(file_path)

    # Write the file
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(modified_content)

    print(f"Updated test implementation in {file_path}")
    return True

def add_test_file_setup(file_path: str, dry_run: bool = False) -> bool:
    """
    Add test file creation and cleanup to test file.

    Args:
        file_path: Path to the file to fix
        dry_run: Whether to only show changes without modifying files

    Returns:
        True if changes were made or would be made, False otherwise
    """
    import re
    print(f"Adding test file setup to {file_path}")

    # Read the file
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Check if the file already has setUp/tearDown
    has_setup = "def setUp" in content
    has_teardown = "def tearDown" in content

    # If both setup and teardown are present, no changes needed
    if has_setup and has_teardown and "json.dump" in content:
        print(f"No test file setup changes needed in {file_path}")
        return False

    original_content = content
    modified_content = content

    # If setup is present but doesn't create test file
    if has_setup and "json.dump" not in content:
        # Define the pattern to match the setUp method
        setup_pattern = r'def setUp\s*\(\s*self\s*\)\s*:.*?(def|$)'

        # Define the replacement
        setup_replacement = """def setUp(self):
        \"\"\"
        Set up test fixtures before each test method.
        \"\"\"
        # Setup for both original and refactored tests
        self.test_file_path = 'test.json'
        self.test_user_display_name = 'Test User'
        self.test_data = BASIC_SKYPE_DATA

        # Add a minimal db_config for tests
        self.test_db_config = {
            'host': 'localhost',
            'port': 5432,
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password'
        }

        # Create a test file
        with open(self.test_file_path, 'w') as f:
            json.dump(self.test_data, f)

        """

        # Replace the setUp method
        modified_content = re.sub(setup_pattern, setup_replacement + r'\g<1>', modified_content, flags=re.DOTALL)

    # If tearDown is not present, add it
    if not has_teardown:
        # Look for the end of the setUp method to insert tearDown
        teardown_method = """    def tearDown(self):
        \"\"\"
        Clean up after each test method.
        \"\"\"
        # Remove the test file
        if os.path.exists(self.test_file_path):
            os.remove(self.test_file_path)

    """

        # Find where to insert tearDown
        if has_setup:
            modified_content = re.sub(
                r'(def setUp.*?\n)(\s+)(def|\s*$)',
                r'\1\2' + teardown_method + r'\3',
                modified_content,
                flags=re.DOTALL
            )
        else:
            # If no setUp either, add both methods after class definition
            setup_teardown = """    def setUp(self):
        \"\"\"
        Set up test fixtures before each test method.
        \"\"\"
        # Setup for both original and refactored tests
        self.test_file_path = 'test.json'
        self.test_user_display_name = 'Test User'
        self.test_data = BASIC_SKYPE_DATA

        # Add a minimal db_config for tests
        self.test_db_config = {
            'host': 'localhost',
            'port': 5432,
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_password'
        }

        # Create a test file
        with open(self.test_file_path, 'w') as f:
            json.dump(self.test_data, f)

    def tearDown(self):
        \"\"\"
        Clean up after each test method.
        \"\"\"
        # Remove the test file
        if os.path.exists(self.test_file_path):
            os.remove(self.test_file_path)

    """
            modified_content = re.sub(
                r'(class TestRefactoringExample.*?:.*?\n)(\s+)(def|\s*$)',
                r'\1\2' + setup_teardown + r'\3',
                modified_content,
                flags=re.DOTALL
            )

    # Check if changes were made
    if content == modified_content:
        print(f"No test file setup changes needed in {file_path}")
        return False

    # Show diff
    show_diff(content, modified_content, file_path)

    # If it's a dry run, don't modify the file
    if dry_run:
        print(f"Dry run: Not modifying {file_path}")
        return True

    # Validate syntax
    if not validate_python_syntax(file_path, modified_content):
        print(f"Error: Modified content has syntax errors, not updating {file_path}")
        return False

    # Backup file
    backup_file(file_path)

    # Write the file
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(modified_content)

    print(f"Updated test file setup in {file_path}")
    return True

def process_file(file_path: str, dry_run: bool = False) -> bool:
    """
    Process a file, applying the appropriate fixes based on the file name.

    Args:
        file_path: Path to the file to process
        dry_run: Whether to only show changes without modifying files

    Returns:
        True if any changes were made or would be made, False otherwise
    """
    changes_made = False

    # Fix imports in all files
    if LIBCST_AVAILABLE:
        import_changes = fix_imports_with_cst(file_path, dry_run)
    else:
        import_changes = fix_imports_with_regex(file_path, dry_run)

    changes_made = changes_made or import_changes

    # Apply specific fixes based on file name
    basename = os.path.basename(file_path)

    if TESTABLE_ETL_PATTERN in basename:
        # Fix TestableETLPipeline
        validation_changes = fix_validation_override(file_path, dry_run)
        di_changes = fix_di_registration(file_path, dry_run)
        changes_made = changes_made or validation_changes or di_changes

    if TEST_EXAMPLE_PATTERN in basename:
        # Fix test implementation
        test_changes = fix_test_implementation(file_path, dry_run)
        setup_changes = add_test_file_setup(file_path, dry_run)
        changes_made = changes_made or test_changes or setup_changes

    return changes_made

def main():
    """Main function."""
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="Fix issues in the TestableETLPipeline class and related files.")
    parser.add_argument("--dry-run", action="store_true", help="Show changes without modifying files")
    parser.add_argument("--dir", default=None, help="Directory to search for files (default: src and tests directories)")
    parser.add_argument("--file", default=None, help="Specific file to fix")
    parser.add_argument("--verbose", action="store_true", help="Show verbose output")
    args = parser.parse_args()

    print("Starting ETL pipeline fixes...")

    # Find files to process
    files_to_process = []

    if args.file:
        # Process a specific file
        if os.path.exists(args.file):
            files_to_process.append(args.file)
        else:
            print(f"File not found: {args.file}")
            return 1
    elif args.dir:
        # Process all Python files in a directory
        if os.path.exists(args.dir):
            files_to_process.extend(find_python_files(args.dir))
        else:
            print(f"Directory not found: {args.dir}")
            return 1
    else:
        # Process src and tests directories by default
        src_dir = os.path.join(ROOT_DIR, "src")
        tests_dir = os.path.join(ROOT_DIR, "tests")

        if os.path.exists(src_dir):
            files_to_process.extend(find_python_files(src_dir))
        else:
            print(f"Directory not found: {src_dir}")

        if os.path.exists(tests_dir):
            files_to_process.extend(find_python_files(tests_dir))
        else:
            print(f"Directory not found: {tests_dir}")

    if not files_to_process:
        print("No files to process.")
        return 1

    print(f"Found {len(files_to_process)} Python files to process.")

    # Process files
    changes_made = False
    for file_path in files_to_process:
        if args.verbose:
            print(f"Processing {file_path}...")

        file_changes = process_file(file_path, args.dry_run)
        changes_made = changes_made or file_changes

    if args.dry_run:
        print("Dry run completed. No files were modified.")
    else:
        print("ETL pipeline fixes completed!")

    if not changes_made:
        print("No changes were needed in any files.")

    return 0

if __name__ == "__main__":
    sys.exit(main())