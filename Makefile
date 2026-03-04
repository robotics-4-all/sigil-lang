COMPOSE  := docker compose
SERVICE  := app
PYTEST   := pytest tests/ -v

.PHONY: build rebuild up down restart install test test-unit test-integration \
        shell logs status check generate clean help

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
	@echo "  check FILE=X      Validate a .sigil file"
	@echo "  generate FILE=X   Generate code (OUT=dir, default: generated/)"
	@echo "  clean             Remove containers, images, caches"
