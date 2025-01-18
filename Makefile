.PHONY: install test lint format clean docs

install:
pip install -r requirements.txt
pip install -e .
pre-commit install

test:
pytest tests/ --cov=src --cov-report=term-missing

lint:
flake8 src tests
mypy src

format:
black src tests
isort src tests

clean:
rm -rf .pytest_cache .coverage .mypy_cache htmlcov
find . -type d -name "__pycache__" -exec rm -r {} +
find . -type f -name "*.pyc" -delete

docs:
mkdocs build
mkdocs serve

