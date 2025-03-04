# Contributing to Skype Parser

We love your input! We want to make contributing to Skype Parser as easy and transparent as possible, whether it's:

- Reporting a bug
- Discussing the current state of the code
- Submitting a fix
- Proposing new features
- Becoming a maintainer

## Development Process

We use GitHub to host code, to track issues and feature requests, as well as accept pull requests.

1. Fork the repo and create your branch from `main`.
2. If you've added code that should be tested, add tests.
3. If you've changed APIs, update the documentation.
4. Ensure the test suite passes.
5. Make sure your code lints.
6. Issue that pull request!

## Development Setup

1. Clone your fork of the repo
```bash
git clone https://github.com/YOUR-USERNAME/skype-parser.git
```

2. Create a virtual environment and activate it
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install development dependencies
```bash
pip install -r requirements-dev.txt
```

4. Install pre-commit hooks
```bash
pre-commit install
```

## Code Style

- We use `black` for Python code formatting
- We use `isort` for import sorting
- We use `flake8` for code linting
- We use `mypy` for type checking

Run all style checks with:
```bash
black .
isort .
flake8
mypy src tests
```

## Testing

Run the test suite with:
```bash
pytest
```

## Pull Request Process

1. Update the README.md with details of changes to the interface, if applicable.
2. Update the docs/ with any necessary changes.
3. The PR will be merged once you have the sign-off of at least one other developer.

## Any contributions you make will be under the MIT Software License

In short, when you submit code changes, your submissions are understood to be under the same [MIT License](http://choosealicense.com/licenses/mit/) that covers the project. Feel free to contact the maintainers if that's a concern.

## Report bugs using GitHub's [issue tracker](https://github.com/yourusername/skype-parser/issues)

We use GitHub issues to track public bugs. Report a bug by [opening a new issue](https://github.com/yourusername/skype-parser/issues/new); it's that easy!

## License

By contributing, you agree that your contributions will be licensed under its MIT License.