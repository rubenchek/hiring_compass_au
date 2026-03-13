.PHONY: install bootstrap lint lint-fix test job-alerts-dev notify smoke-test

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
	docker compose run --rm job-alerts

notify:
	docker compose run --rm notify

smoke-test:
	scripts/run_prod_smoke_test.sh
