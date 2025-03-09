#!/bin/bash
# Script to run the entire reorganization process

set -e  # Exit on error

# Create backup directory if it doesn't exist
mkdir -p backup

# Display header
echo "====================================================="
echo "  Skype Parser Project Reorganization"
echo "====================================================="
echo

# Step 1: Create backup
echo "Step 1: Creating backup..."
timestamp=$(date +%Y%m%d_%H%M%S)
backup_dir="backup/project_backup_${timestamp}"
mkdir -p "$backup_dir"
cp -r src "$backup_dir/"
cp -r scripts "$backup_dir/"
echo "Backup created at $backup_dir"
echo

# Step 2: Implement reorganization
echo "Step 2: Implementing reorganization..."
if [ ! -f "reorganization_mapping.md" ]; then
    echo "Error: reorganization_mapping.md not found"
    exit 1
fi

echo "- Adding deprecation notices to duplicated files"
echo "- Updating imports in canonical files"
python scripts/implement_reorganization.py
echo

# Step 3: Update __init__.py files
echo "Step 3: Updating __init__.py files..."
echo "- Exporting symbols from canonical modules"
echo "- Adding re-exports in deprecated modules"
python scripts/update_init_files.py
echo

# Step 4: Test reorganization
echo "Step 4: Testing reorganization..."
echo "- Testing canonical imports"
echo "- Testing deprecated imports"
echo "- Testing all modules"
python scripts/test_reorganization.py
echo

# Check if test results file exists
if [ -f "reorganization_test_results.md" ]; then
    echo "Test results written to reorganization_test_results.md"

    # Count successes and failures
    canonical_success=$(grep -c "✅" reorganization_test_results.md)
    failures=$(grep -c "❌" reorganization_test_results.md)

    echo "Successes: $canonical_success"
    echo "Failures: $failures"

    if [ $failures -gt 0 ]; then
        echo "Warning: Some tests failed. Review reorganization_test_results.md"
        echo
    fi
fi

# Step 5: Finalize
echo "Step 5: Finalizing reorganization..."
echo "- The reorganization has been applied"
echo "- You can review the changes and test the application functionality"
echo "- If you encounter issues, you can restore from the backup at $backup_dir"
echo

# Complete
echo "====================================================="
echo "  Reorganization Complete"
echo "====================================================="
echo
echo "Next steps:"
echo "1. Run your application tests to verify functionality"
echo "2. Commit the changes if everything works as expected"
echo "3. Consider removing duplicated files after sufficient testing"