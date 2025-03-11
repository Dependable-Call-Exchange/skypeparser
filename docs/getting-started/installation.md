# Installation Guide

This guide will help you install and set up the SkypeParser project on your system.

## Prerequisites

Before installing SkypeParser, ensure you have the following prerequisites:

- **Python 3.8+**: The project requires Python 3.8 or higher
- **PostgreSQL**: A PostgreSQL database for storing parsed data (optional, but recommended)
- **Git**: For cloning the repository

## Installation Steps

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/skype-parser.git
cd skype-parser
```

### 2. Create a Virtual Environment

It's recommended to use a virtual environment to avoid conflicts with other Python packages:

```bash
# Create a virtual environment
python -m venv venv

# Activate the virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

### 3. Install Dependencies

Install the required Python packages:

```bash
pip install -r requirements.txt
```

For development, you may want to install additional dependencies:

```bash
pip install -r requirements-dev.txt
```

### 4. Configure the Database

If you plan to use the database features, you'll need to configure the database connection:

1. Copy the example configuration file:
   ```bash
   cp config/config.json.example config/config.json
   ```

2. Edit the configuration file with your database credentials:
   ```json
   {
     "database": {
       "host": "localhost",
       "port": 5432,
       "dbname": "skype_parser",
       "user": "your_username",
       "password": "your_password"
     }
   }
   ```

3. Create the database (if it doesn't exist):
   ```bash
   createdb skype_parser
   ```

### 5. Verify Installation

To verify that the installation was successful, run the following command:

```bash
python -m src.parser.skype_parser --help
```

You should see the help message for the SkypeParser command-line interface.

## Alternative Installation Methods

### Using pip

You can also install SkypeParser using pip:

```bash
pip install skype-parser
```

### Using Docker

If you prefer to use Docker, you can build and run the SkypeParser container:

```bash
# Build the Docker image
docker build -t skype-parser .

# Run the container
docker run -it --rm skype-parser --help
```

## Troubleshooting

### Common Issues

1. **Missing Dependencies**:
   If you encounter errors about missing dependencies, try reinstalling the requirements:
   ```bash
   pip install --force-reinstall -r requirements.txt
   ```

2. **Database Connection Issues**:
   If you have trouble connecting to the database, check that:
   - PostgreSQL is running
   - Your database credentials are correct
   - The database exists
   - Your firewall allows connections to the database

3. **Python Version Issues**:
   If you get errors about unsupported Python syntax, check your Python version:
   ```bash
   python --version
   ```
   Make sure it's 3.8 or higher.

### Getting Help

If you encounter issues that aren't covered here, please:

1. Check the [documentation](../README.md) for more information
2. Look for similar issues in the project's issue tracker
3. Ask for help in the project's discussion forum

## Next Steps

Now that you have installed SkypeParser, you can:

- Read the [Basic Usage Guide](basic-usage.md) to learn how to use the tool
- Explore the [Configuration Guide](configuration.md) to customize the tool for your needs
- Check out the [User Guide](../user-guide/README.md) for more detailed information