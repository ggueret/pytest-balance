.PHONY: install lint typecheck test format check all clean

install:
	uv sync

lint:
	uv run ruff check src tests

typecheck:
	uv run mypy src

test:
	uv run pytest

format:
	uv run ruff format src tests
	uv run ruff check --fix src tests

check:
	uv run ruff format --check src tests
	uv run ruff check src tests
	uv run mypy src

all: lint typecheck test

clean:
	rm -rf .cache .coverage htmlcov dist build *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
