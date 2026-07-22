.PHONY: help setup test up up-layer2 up-layer3 down status backfill extract full-backfill ui db-status kafka-topics clean

# Default target
.DEFAULT_GOAL := help

# Environment variables
PYTHONPATH := libs/chip_connectors:libs/chip_extractors:connectors/ajk_idsrs:connectors/pitb_dss:connectors/dhis_punjab_weekly:connectors/nih_idsr:extractors/ajk_idsrs_disease_tables:extractors/pitb_dss_disease_tables:extractors/dhis_punjab_disease_tables:extractors/nih_idsr_disease_tables:.
DOCKER_COMPOSE := docker compose -f infra/docker/docker-compose.full.yaml
DOCKER_COMPOSE_L2 := docker compose -f infra/docker/docker-compose.layer2.yaml
DOCKER_COMPOSE_L3 := docker compose -f infra/docker/docker-compose.layer3.yaml

## ----------------------------------------------------------------------
## CHIP Platform — Developer Commands
## ----------------------------------------------------------------------

help: ## Show available Makefile targets
	@echo "Climate-Health Intelligence Platform (CHIP) Commands"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

setup: ## Install project dependencies via uv
	uv sync

test: ## Run full test suite across SDKs, connectors, extractors & integration tests
	uv run pytest

up: ## Start full stack (Postgres + MinIO + Kafka)
	$(DOCKER_COMPOSE) up -d

up-layer2: ## Start Layer 2 stack only (Postgres + MinIO)
	$(DOCKER_COMPOSE_L2) up -d

up-layer3: ## Start Layer 3 stack only (Kafka)
	$(DOCKER_COMPOSE_L3) up -d

down: ## Stop all Docker containers
	$(DOCKER_COMPOSE) down

status: ## Check status of Docker containers
	$(DOCKER_COMPOSE) ps

backfill: ## Materialize Layer 2 raw connector assets via Dagster CLI
	PYTHONPATH=$(PYTHONPATH) uv run dagster asset materialize --select "raw_*" -f pipelines/extraction/definitions.py

extract: ## Materialize Layer 3 document extractor assets via Dagster CLI
	PYTHONPATH=$(PYTHONPATH) uv run dagster asset materialize --select "extracted_*" -f pipelines/extraction/definitions.py

full-backfill: ## Materialize both Layer 2 connectors and Layer 3 extractors end-to-end
	PYTHONPATH=$(PYTHONPATH) uv run dagster asset materialize --select "*" -f pipelines/extraction/definitions.py

ui: ## Launch Dagster Web UI on http://localhost:3000
	PYTHONPATH=$(PYTHONPATH) uv run dagster dev -f pipelines/extraction/definitions.py

db-status: ## Query Postgres for raw_documents, extractor_status, and connector_runs counts
	@docker exec -i chip-postgres psql -U chip -d chip -c "SELECT source, count(*) as archived_docs FROM ingestion.raw_documents GROUP BY source;"
	@docker exec -i chip-postgres psql -U chip -d chip -c "SELECT extractor_name, status, count(*) FROM ingestion.extractor_status GROUP BY extractor_name, status;"
	@docker exec -i chip-postgres psql -U chip -d chip -c "SELECT run_id, source, discovered, archived, skipped_identity, errors, duration_ms FROM ingestion.connector_runs ORDER BY run_id DESC LIMIT 10;"

kafka-topics: ## List Kafka topics and message counts
	@docker exec -i chip-kafka kafka-topics --bootstrap-server localhost:9092 --list

clean: ## Remove temporary python cache and pytest files
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	find . -type d -name "*.egg-info" -exec rm -rf {} +
