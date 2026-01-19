bootstrap:
	python -m scripts.bootstrap_workspace

lint:
	ruff check src

lint-fix:
	ruff check src --fix
