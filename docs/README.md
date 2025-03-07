# Skype Parser Documentation

This directory contains documentation for the Skype Parser project.

## Documentation Structure

### User Guide
- [Skype Data Analysis](user_guide/skype_data_analysis.md) - Information about Skype data structure and analysis
- [Configuration](user_guide/CONFIGURATION.md) - Configuration options and settings
- [PostgreSQL Setup](user_guide/README_POSTGRES.md) - PostgreSQL database setup instructions
- [Supabase Integration](user_guide/SUPABASE_INTEGRATION.md) - Supabase integration guide
- [Checkpoint Resumption](user_guide/CHECKPOINT_RESUMPTION.md) - How to resume processing from checkpoints
- [Non-Interactive Mode](user_guide/NON_INTERACTIVE.md) - Running in non-interactive mode
- [Content Extraction](user_guide/content_extraction.md) - Details on content extraction features
- [Message Types](user_guide/message_types.md) - Information about supported message types

### Implementation
- [Path to MVP](implementation/path_to_mvp.md) - Implementation plan for the Minimum Viable Product
- [Refactoring Summary](implementation/REFACTORING_SUMMARY.md) - Summary of refactoring efforts
- [Improvements](implementation/IMPROVEMENTS.md) - Planned improvements for the project
- [Improvement Plan](implementation/skype_parser_improvement_plan.md) - Detailed improvement plan
- [ETL Migration Decisions](implementation/ETL_MIGRATION_DECISIONS.md) - Decisions made during ETL migration
- [ETL Migration Plan](implementation/ETL_MIGRATION_PLAN.md) - Plan for ETL migration
- [ETL Validation](implementation/ETL_VALIDATION.md) - Validation approach for ETL processes
- [Migration](implementation/MIGRATION.md) - General migration guidelines
- [Refactoring](implementation/refactoring.md) - Refactoring guidelines and approaches

### Development
- [Dependency Handling](development/DEPENDENCY_HANDLING.md) - Information about dependency injection and handling
- [Error Handling](development/ERROR_HANDLING.md) - Guidelines for error handling in the project
- [Consolidation Summary](development/CONSOLIDATION_SUMMARY.md) - Summary of code consolidation efforts
- [API Documentation](development/API.md) - API documentation and specifications
- [API Usage](development/API_USAGE.md) - Examples of API usage
- [Factory Implementation](development/FACTORY_IMPLEMENTATION.md) - Details on factory pattern implementation
- [Modularization Strategy](development/Modularization_Strategy.md) - Strategy for code modularization
- [Test Factories](development/TEST_FACTORIES.md) - Information about test factory implementations
- [Input Validation](development/INPUT_VALIDATION.md) - Guidelines for input validation
- [Performance](development/performance.md) - Performance considerations and optimizations
- [Test Fixes](development/test_fixes.md) - Information about test fixes and improvements
- [Test Infrastructure](development/test_infrastructure.md) - Details on test infrastructure
- [Contributing](development/CONTRIBUTING.md) - Guidelines for contributing to the project

## Project Summary
- [Summary](SUMMARY.md) - Overall project summary

## Main README
The main [README.md](../README.md) in the root directory provides an overview of the project, setup instructions, and basic usage information.

# Skype History Parser

`skype-parser` is a simple script to create pretty text files from your Skype chat history. This tool can either take in the `.tar` file or the `.json` file given to you by Skype and give you back your entire chat history in `.txt` format (beautifully formatted, too!).

## How do I just get this thing to work?

Download `parser.py` and invoke it like so:

- If you have a `.tar` file:

```bash
python3 parser.py -t your_skype_username_export.tar
```

where `your_skype_username_export.tar` is the `.tar` file your recieved from Skype upon requesting a conversation export.

- If you have a `.json` file:

```bash
python3 parser.py messages.json
```

where `messages.json` is the extracted `.json` file you that contains your conversation history.

If you are not sure how you can get your export from Skype, read the [this section](#how-do-i-export-my-skype-chat-history).

## Detailed Description

skype-parser is a simple python script that makes preserving chat history from Skype easier.

Skype tends be a niche choice for text chatting, but for those of you who you use it and would like to keep a chat log in pliin-text form, this is *the* tool to get the job done.

Skype's own parser is quite frankly, terrible. It produces an ugly HTML that is difficult to navigate and is riddled with unparsed XML.

This tool will take the tar/JSON file given to you by Skype, and creates .txt files containing every chat with every user.

Basic usage:

```bash
skype-parser [-h] [-c] [-t] filename

positional arguments:
  filename      The path/name to the Skype json/tar file you want to parse

optional arguments:
  -h, --help    show this help message and exit
  -t, --tar     Use this flag to feed in a .tar file (at your own risk)
  -c, --choose  Use this flag to choose which convos you'd like to parse
```

If you invoke the script **without** the `t` or `--tar` argument, `filename` must be the skype `.json` file.

If you invoke the script **with** the `-t` or `--tar` argument, `filename` must be the `.tar` file that you get from skype.

If you invoke the script with `-c` or `--choose`, it will let you choose between the conversations you'd like to export.

## Requirements

- python version 3.5 and above

- beautifulsoup4 (optional, but recommended)

## How do I even export my skype chat history?

Follow the instructions [here](https://support.skype.com/en/faq/FA34894/how-do-i-export-my-skype-files-and-chat-history).

Keep in mind that this tool parses your conversations, not your files; so be careful what you export.

Once you have downloaded your exported conversations (which is usually in `.tar` format), you can either to untar the downloaded file and use this tool to parse the resulting `.json` file, or you can take the `.tar` file itself and feed it into the script. Either way *should* work.

## TO DO

- Figure out whether we're being a fed a `.json` file or a tarball.
