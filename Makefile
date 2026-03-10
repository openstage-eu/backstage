.PHONY: install install-dev install-docs test docs docs-serve clean run run-sample run-dry

# Setup
install:
	uv sync

install-dev:
	uv sync --extra dev --extra parsing

install-docs:
	uv sync --extra docs

# Testing
test:
	uv run pytest

# Documentation
docs:
	uv run mkdocs build --strict

docs-serve:
	uv run mkdocs serve

# Pipeline
run:
	uv run python -m flows.run --case eu

run-sample:
	uv run python -m flows.run --case eu --sample

run-dry:
	uv run python -m flows.run --case eu --sample --dry-run

# Cleanup
clean:
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type d -name .pytest_cache -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf site/ build/ dist/
