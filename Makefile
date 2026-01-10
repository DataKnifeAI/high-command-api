.PHONY: help install dev test lint format clean docker-build docker-login docker-push docker-pull docker-run run check check-all commit-changes release venv

# Force use of bash shell (required for make to work properly with line continuations)
SHELL := /bin/bash
.SHELLFLAGS := -o pipefail -c

# Use venv Python if available, otherwise fall back to system python
PYTHON := $(shell [ -d venv ] && echo venv/bin/python3 || echo python3)
PIP := $(shell [ -d venv ] && echo venv/bin/pip || echo pip)
PYTEST := $(shell [ -d venv ] && echo venv/bin/pytest || echo pytest)
RUFF := $(shell [ -d venv ] && echo venv/bin/ruff || echo ruff)
BLACK := $(shell [ -d venv ] && echo venv/bin/black || echo black)
MYPY := $(shell [ -d venv ] && echo venv/bin/mypy || echo mypy)

APP_NAME := high-command-api
PORT := 5000

help:
	@echo "High Command API - Helldivers 2 FastAPI Server"
	@echo ""
	@echo "Available targets:"
	@echo "  venv           Create virtual environment"
	@echo "  install        Install dependencies"
	@echo "  dev            Install development dependencies and run with reload"
	@echo "  run            Run the API server"
	@echo "  test           Run tests with coverage"
	@echo "  test-fast      Run tests without coverage"
	@echo "  lint           Run linters (ruff, mypy)"
	@echo "  format         Format code with black and ruff"
	@echo "  clean          Remove build artifacts and cache files"
	@echo "  docker-build   Build Docker image"
	@echo "  docker-login   Login to Harbor registry (requires HARBOR_USERNAME and HARBOR_PASSWORD)"
	@echo "  docker-push    Build and push image to Harbor registry"
	@echo "  docker-pull    Pull image from Harbor registry"
	@echo "  docker-run     Run Docker container"
	@echo "  check          Run linters and tests"
	@echo "  check-all      Format, lint, and test (quality gate)"
	@echo "  commit-changes Git add, commit, and status"
	@echo "  release        Format, lint, test, and prepare release"
	@echo "  help           Show this help message"

venv:
	python3 -m venv venv
	venv/bin/pip install --upgrade pip setuptools wheel

install: venv
	$(PIP) install -e .

dev: venv
	$(PIP) install -e ".[dev]"
	$(PYTHON) -m uvicorn src.app:app --reload --port $(PORT)

run: venv
	$(PYTHON) -m uvicorn src.app:app --host 0.0.0.0 --port $(PORT)

test:
	$(PYTEST)

test-fast:
	$(PYTEST) --no-cov -q

lint:
	$(RUFF) check src tests
	$(MYPY) src --ignore-missing-imports

format:
	$(BLACK) src tests
	$(RUFF) check --fix src tests

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	rm -rf build dist .pytest_cache .mypy_cache htmlcov .ruff_cache .coverage

HARBOR_REGISTRY := harbor.dataknife.net
HARBOR_IMAGE := $(HARBOR_REGISTRY)/library/$(APP_NAME)
IMAGE_TAG ?= latest

docker-login:
	@echo "Logging into Harbor registry..."
	@if [ -z "$$HARBOR_USERNAME" ] || [ -z "$$HARBOR_PASSWORD" ]; then \
		echo "Error: HARBOR_USERNAME and HARBOR_PASSWORD must be set"; \
		exit 1; \
	fi
	@echo "$$HARBOR_PASSWORD" | docker login $(HARBOR_REGISTRY) \
		-u "$$HARBOR_USERNAME" \
		--password-stdin

docker-build:
	docker build -t $(APP_NAME):latest .
	docker tag $(APP_NAME):latest $(HARBOR_IMAGE):$(IMAGE_TAG)

# Ensure we are logged into Harbor before building/tagging and pushing the image.
docker-push: docker-login docker-build
	docker push $(HARBOR_IMAGE):$(IMAGE_TAG)

docker-pull:
	docker pull $(HARBOR_IMAGE):$(IMAGE_TAG)
	docker tag $(HARBOR_IMAGE):$(IMAGE_TAG) $(APP_NAME):latest

docker-run: docker-build
	docker run -it --rm -p $(PORT):5000 $(APP_NAME):latest

check: lint test

check-all: format lint test

commit-changes:
	git add .
	@read -p "Enter commit message: " msg; git commit -m "$$msg" || true
	git status

release: format lint test
	@echo "✓ All checks passed. Ready for release."

info:
	@echo "High Command API - Project Information"
	@echo ""
	@echo "Python Version: 3.14.0 (see .python-version)"
	@echo "Framework: FastAPI 0.104.1"
	@echo "Server: Uvicorn 0.24.0"
	@echo "Database: SQLite3"
	@echo "Scheduler: APScheduler 3.10.4"
	@echo ""
	@echo "Quick Start:"
	@echo "  make install       - Install dependencies"
	@echo "  make dev           - Run development server"
	@echo "  make test          - Run tests"
	@echo "  make lint          - Run linters"
	@echo "  make format        - Format code"
	@echo ""
