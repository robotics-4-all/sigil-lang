COMPOSE  := docker compose
SERVICE  := app
PYTEST   := pytest tests/ -v

.PHONY: build rebuild up down restart install test test-unit test-integration \
        shell logs status check generate validate-examples clean ci help

build:
	$(COMPOSE) build

rebuild:
	$(COMPOSE) build --no-cache

up:
	$(COMPOSE) up -d

down:
	$(COMPOSE) down

restart: down up

install: up
	$(COMPOSE) exec $(SERVICE) pip install -e .

test: up
	$(COMPOSE) exec $(SERVICE) $(PYTEST)

test-unit: up
	$(COMPOSE) exec $(SERVICE) $(PYTEST) --ignore=tests/test_integration.py

test-integration: up
	$(COMPOSE) exec $(SERVICE) $(PYTEST) tests/test_integration.py

shell: up
	$(COMPOSE) exec $(SERVICE) bash

logs:
	$(COMPOSE) logs -f

status:
	$(COMPOSE) ps

check: up
	$(COMPOSE) exec $(SERVICE) sigil check $(FILE)

generate: up
	$(COMPOSE) exec $(SERVICE) sigil generate $(FILE) -o $(or $(OUT),generated/)

validate-examples:
	@python -c "import sys,os;from sigil.language import parse_string;from sigil.semantics import validate;fail=0;files=sorted(f for f in os.listdir('examples') if f.endswith('.sigil'));exec('for f in files:\n path=os.path.join(\"examples\",f)\n try:\n  m=parse_string(open(path).read());errs=[e for e in validate(m) if e.severity==\"error\"]\n  if errs:\n   print(f\"  FAIL {path}\");[print(f\"       {e}\") for e in errs];fail=1\n  else:print(f\"  OK   {path}\")\n except Exception as exc:print(f\"  FAIL {path}\");print(f\"       {exc}\");fail=1');sys.exit(1 if fail else 0)" && echo "" && echo "All examples valid." || (echo "" && echo "Some examples failed validation." && false)

ci:
	ruff check sigil/ tests/
	ruff format --check sigil/ tests/
	mypy sigil
	pytest tests/ -v --ignore=tests/test_integration.py
	$(MAKE) validate-examples

clean: down
	$(COMPOSE) down -v --rmi local
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf .pytest_cache .ruff_cache sigil.egg-info generated/

help:
	@echo "sigil Docker targets"
	@echo ""
	@echo "  build             Build Docker image"
	@echo "  rebuild           Build Docker image (no cache)"
	@echo "  up                Start all services (app + brokers)"
	@echo "  down              Stop all services"
	@echo "  restart           Restart all services"
	@echo "  install           pip install -e . inside container"
	@echo ""
	@echo "  test              Run full test suite"
	@echo "  test-unit         Run unit tests only"
	@echo "  test-integration  Run integration tests only"
	@echo ""
	@echo "  shell             Open bash in app container"
	@echo "  logs              Tail service logs"
	@echo "  status            Show container status"
	@echo ""
	@echo "  check FILE=X        Validate a .sigil file"
	@echo "  generate FILE=X    Generate code (OUT=dir, default: generated/)"
	@echo "  validate-examples  Validate all examples/*.sigil files"
	@echo "  ci                Run full CI locally (lint, test, validate)"
	@echo "  clean              Remove containers, images, caches"
