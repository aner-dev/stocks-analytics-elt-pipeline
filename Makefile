# Makefile for High-Performance Stock Analytics Pipeline
# Targets for Artix Linux / Podman environment

# --- Variables ---
DOCKER_COMPOSE = astro dev
DBT_DIR = dags/dbt/elt_pipeline_stocks

.PHONY: help up down restart dbt-run dbt-test dbt-debug evidence-logs shell check-socket

# --- Validation ---
# Checks if Podman socket is active to avoid cryptic Astro errors
check-socket:
	@ls $(subst unix://,,$(DOCKER_HOST)) > /dev/null 2>&1 || (echo "Error: Podman socket not found at $(DOCKER_HOST). Run your user service or check zshrc."; exit 1)

help:
	@echo "Usage: make [target]"
	@echo ""
	@echo "Infrastructure:"
	@echo "  up             Start the entire stack (Airflow, Postgres, RustFS, Evidence)"
	@echo "  down           Stop all services"
	@echo "  restart        Restart the containers"
	@echo ""
	@echo "Data Engineering (dbt):"
	@echo "  dbt-run        Run Gold layer models"
	@echo "  dbt-test       Execute dbt_expectations and core tests"
	@echo "  dbt-debug      Check dbt database connectivity"
	@echo ""
	@echo "Monitoring:"
	@echo "  evidence-logs  Follow Evidence.dev container logs"
	@echo "  shell          Enter the Airflow worker container shell"

# --- Infrastructure ---
up: check-socket
	$(DOCKER_COMPOSE) start

down:
	$(DOCKER_COMPOSE) stop

restart: check-socket
	$(DOCKER_COMPOSE) restart

# --- dbt / Transformation Layer ---
dbt-run:
	$(DOCKER_COMPOSE) bash -c "cd $(DBT_DIR) && dbt run"

dbt-test:
	$(DOCKER_COMPOSE) bash -c "cd $(DBT_DIR) && dbt test"

dbt-debug:
	$(DOCKER_COMPOSE) bash -c "cd $(DBT_DIR) && dbt debug"

# --- Monitoring & Debugging ---
evidence-logs:
	podman logs -f stocks_evidence

shell:
	$(DOCKER_COMPOSE) bash
