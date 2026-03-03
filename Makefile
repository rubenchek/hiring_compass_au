.PHONY: install bootstrap lint lint-fix test

install	:
	uv pip install -e '.[dev]'

bootstrap:
	python -m scripts.bootstrap_workspace

lint:
	ruff check src tests

lint-fix:
	ruff check src tests --fix

format-check:
	ruff format --check src tests

format:
	ruff format src tests

test:
	pytest -q

mail-promote:
	python3 -m hiring_compass_au.data.pipelines.job_alerts