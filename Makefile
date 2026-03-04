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

job-alerts-dev:
	docker compose run --rm job-alerts-dev

notify:
	docker compose -f docker-compose.yml run --rm notify-dev