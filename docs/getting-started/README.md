# Getting Started with SkypeParser

Welcome to the SkypeParser Getting Started Guide. This guide will help you get up and running with SkypeParser quickly.

## Table of Contents

- [Installation Guide](installation.md) - Installation instructions
- [Basic Usage Guide](basic-usage.md) - Basic usage examples
- [Configuration Guide](configuration.md) - Configuration options

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/skype-parser.git
cd skype-parser

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

See the [Installation Guide](installation.md) for more details.

### Basic Usage

```bash
# Parse a Skype export file
python -m src.parser.skype_parser path/to/skype_export.tar -t -o output_dir -u "Your Name"
```

See the [Basic Usage Guide](basic-usage.md) for more details.

### Configuration

```bash
# Copy the example configuration file
cp config/config.json.example config/config.json

# Edit the configuration file with your database credentials
# ...
```

See the [Configuration Guide](configuration.md) for more details.

## Next Steps

After getting started with SkypeParser, you can:

- Explore the [User Guide](../user-guide/README.md) for more detailed information
- Check out the [Developer Guide](../developer-guide/README.md) if you want to contribute to the project
- Read the [Implementation Details](../implementation/README.md) to understand the internal workings of the system