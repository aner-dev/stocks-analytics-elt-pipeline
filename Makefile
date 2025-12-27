# --- Variables ---
DOCKER_COMPOSE = astro dev
DBT_DIR = dags/dbt/elt_pipeline_stocks
DBT_VENV = /usr/local/airflow/dbt_venv/bin/dbt
SCHEDULER_CONTAINER = dbt-on-astro_f72251-scheduler-1

.PHONY: up down restart dbt-run dbt-test dbt-debug dashboard audit-mem

# --- Infrastructure ---
up:
	$(DOCKER_COMPOSE) start

down:
	$(DOCKER_COMPOSE) stop

restart:
	$(DOCKER_COMPOSE) restart

dbt-run:
	podman exec -it $(SCHEDULER_CONTAINER) $(DBT_VENV) run --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

dbt-test:
	podman exec -it $(SCHEDULER_CONTAINER) $(DBT_VENV) test --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

dbt-debug:
	podman exec -it $(SCHEDULER_CONTAINER) $(DBT_VENV) debug --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

# --- Analytics & Performance ---
# Para correr Streamlit y el audit desde tu local usando el entorno de uv
dashboard:
	uv run streamlit run ui/dashboard.py

audit-mem:
	export DB_URL="postgresql://postgres:postgres@127.0.0.1:5433/stocks_dwh" && uv run python src/scripts/audit_mem.py

# --- Cleanup ---
clean:
	rm -rf __pycache__ .pytest_cache
	find . -name "*.bak" -type f -delete
