#!/bin/bash
# Script to push logging changes to GitHub and merge them back to main

set -e  # Exit on error

# Check if we're in the right directory
if [ ! -d "src/utils" ] || [ ! -d "scripts" ]; then
    echo "Error: This script must be run from the root of the SkypeParser repository"
    exit 1
fi

# Function to create and push a branch
create_and_push_branch() {
    local branch_name=$1
    local commit_message=$2
    local files=("${@:3}")

    echo "Creating branch: $branch_name"
    git checkout -b "$branch_name"

    # Add files
    for file in "${files[@]}"; do
        if [ -f "$file" ]; then
            git add "$file"
        else
            echo "Warning: File not found: $file"
        fi
    done

    # Commit and push
    git commit -m "$commit_message"
    git push -u origin "$branch_name"

    # Return to main
    git checkout main
}

# Make sure we're on main and up to date
echo "Updating main branch..."
git checkout main
git pull origin main

# Create and push the logging infrastructure branch
echo "Creating and pushing logging infrastructure branch..."
create_and_push_branch \
    "feature/logging-infrastructure" \
    "Add centralized logging infrastructure" \
    "src/utils/new_structured_logging.py" \
    "src/utils/logging_config.py" \
    "src/utils/logging_compat.py" \
    "tests/utils/test_logging.py" \
    "tests/unit/test_structured_logging.py" \
    "scripts/initialize_logging.py" \
    "scripts/test_logging.py" \
    "docs/implementation/CENTRALIZED_LOGGING.md" \
    "README_LOGGING.md"

# Create and push the ETL logging branch
echo "Creating and pushing ETL logging branch..."
create_and_push_branch \
    "feature/etl-logging" \
    "Update ETL components to use centralized logging" \
    "scripts/update_loader_logging.py"

# Instructions for merging
echo ""
echo "Branches have been created and pushed to GitHub."
echo ""
echo "To merge these changes back to main, follow these steps:"
echo ""
echo "1. Create a pull request for the 'feature/logging-infrastructure' branch"
echo "   - Go to: https://github.com/yourusername/SkypeParser/pull/new/feature/logging-infrastructure"
echo "   - Base: main, Compare: feature/logging-infrastructure"
echo "   - Review and merge the pull request"
echo ""
echo "2. Create a pull request for the 'feature/etl-logging' branch"
echo "   - Go to: https://github.com/yourusername/SkypeParser/pull/new/feature/etl-logging"
echo "   - Base: main, Compare: feature/etl-logging"
echo "   - Review and merge the pull request"
echo ""
echo "3. After merging, run the following commands to update your local repository:"
echo "   git checkout main"
echo "   git pull origin main"
echo ""
echo "4. To apply the loader logging changes, run:"
echo "   python scripts/update_loader_logging.py"
echo ""
echo "5. To initialize the logging system, run:"
echo "   python scripts/initialize_logging.py"
echo ""
echo "6. To run the logging tests, run:"
echo "   python scripts/test_logging.py"