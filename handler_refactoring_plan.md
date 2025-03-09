# Project Reorganization Plan

## Overview

This document outlines a comprehensive plan for reorganizing the components in the SkypeParser project. The current structure has handlers spread across multiple directories (`src/db/handlers` and `src/utils`), and the `utils` directory has become a catch-all for various types of functionality.

The goal is to implement a flatter, domain-specific organization approach, where components are grouped by their functional domain at the top level of the src directory rather than being deeply nested.

## Objectives

1. Create a flatter, more navigable directory structure
2. Improve code organization with clear domain boundaries
3. Reduce confusion for developers and AI tools
4. Preserve all existing functionality
5. Simplify import statements

## Proposed Directory Structure

The new structure will organize code by domain at the top level, making it easier to navigate:

```
src/
├── db/                 # Database-specific code
├── data_handlers/      # All data insertion handlers (moved from db/handlers)
├── messages/           # Message-related functionality
├── attachments/        # Attachment-related functionality
├── conversations/      # Conversation-related functionality
├── files/              # File handling and extraction
├── validation/         # All validators
├── monitoring/         # Progress tracking, memory monitoring, etc.
├── logging/            # Logging and error handling
└── utils/              # Only truly generic utilities
```

## Files to Reorganize

### From db/handlers/ to data_handlers/
- All database handlers (archive_handler.py, message_handler.py, etc.)
- handler_registry.py
- base_handler.py

### From utils/ to domain-specific directories
- message_processor.py → messages/
- message_type_handlers.py → messages/
- message_type_extractor.py → messages/
- attachment_handler.py → attachments/
- conversation_processor.py → conversations/
- file_handler.py → files/
- file_utils.py → files/
- tar_extractor.py → files/
- ... and so on for other utility files

## Implementation Plan

### Phase 1: Preparation

1. **Create a feature branch**
   ```bash
   git checkout -b refactor/reorganize-project
   ```

2. **Backup the current structure**
   ```bash
   cp -r src src_backup
   ```

3. **Convert relative imports to absolute imports**
   Run the `convert_imports.py` script to standardize all imports before restructuring.
   (Script available in the scripts directory)

### Phase 2: Analysis

1. **Run the analysis script**
   Use the `analyze_directory.py` script to categorize files by functionality and identify dependencies.
   (Script available in the scripts directory)

2. **Review analysis results**
   - Analyze the dependency graph
   - Finalize the reorganization mapping
   - Identify potential issues

### Phase 3: Implementation

1. **Create new directory structure**
   ```bash
   mkdir -p src/{data_handlers,messages,attachments,conversations,files,validation,monitoring,logging}
   ```

2. **Move files in stages**
   - First move data handlers (least dependencies)
   - Then move domain-specific components
   - Update imports after each stage
   - Run tests between stages

3. **Create appropriate `__init__.py` files**
   For each new directory, create appropriate exports.

### Phase 4: Verification

1. **Run unit tests**
2. **Verify import statements**
3. **Run integration tests**
4. **Manual testing of key functionality**

### Phase 5: Documentation & Finalization

1. **Update documentation**
2. **Create PR for review**
3. **Merge changes to main branch**

## Scripts

Three Python scripts will be used for this refactoring:

1. **convert_imports.py** - Converts relative imports to absolute imports throughout the codebase
2. **analyze_directory.py** - Analyzes the directory structure and dependencies
3. **reorganize_project.py** - Performs the actual file movements and import updates

Detailed documentation for each script is available in the scripts directory.

## Testing Strategy

1. **Unit Tests**: Run all existing unit tests to verify basic functionality
2. **Import Tests**: Create temporary scripts that import from the new locations
3. **End-to-End Tests**: Run any existing E2E tests to verify system integrity
4. **Manual Verification**: Check a sample of files to ensure imports were correctly updated

## Rollback Plan

If issues are encountered:

1. Restore from the backup directory created at the beginning
2. Or use git to revert the changes:
   ```bash
   git reset --hard HEAD
   git clean -fd
   ```

## Timeline

1. Analysis and preparation: 1 day
2. Implementation: 2-3 days
3. Testing and verification: 1 day
4. Documentation and PR: 1 day

## Conclusion

This refactoring will significantly improve the organization of the codebase, making it more maintainable and easier to understand. The flatter structure will be more intuitive for developers and AI tools alike, while the domain-based organization will create clearer boundaries between different types of functionality.