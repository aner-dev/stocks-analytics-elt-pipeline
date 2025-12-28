# --- Variables ---
# Astro CLI uses docker-compose internally. 
# We look for the scheduler dynamically so it works on any PC.
SCHEDULER_CONTAINER = $(shell podman ps --filter "name=scheduler" --format "{{.Names}}" | head -n 1)
DBT_DIR = dags/dbt/elt_pipeline_stocks
DBT_VENV = /usr/local/airflow/dbt_venv/bin/dbt

.PHONY: up down restart dbt-run dbt-test dbt-debug dashboard audit-mem install help

# --- Help (Always pro to include this) ---
help:
	@echo "🚀 Stock Pipeline Commands:"
	@echo "  make up           - Start the Astro (Airflow) stack"
	@echo "  make dbt-run      - Execute dbt models inside the scheduler"
	@echo "  make dashboard    - Launch Streamlit UI"
	@echo "  make audit-mem    - Run memory efficiency benchmarking"

# --- Infrastructure ---
up:
	astro dev start

down:
	astro dev stop

# --- DBT (Dynamic Container Execution) ---
# We use the automatically detected container
dbt-run:
	podman exec -it $(SCHEDULER_CONTAINER) $(DBT_VENV) run --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

dbt-test:
	podman exec -it $(SCHEDULER_CONTAINER) $(DBT_VENV) test --project-dir $(DBT_DIR) --profiles-dir $(DBT_DIR)

# --- Analytics & Performance ---
dashboard:
	@echo "📈 Opening Streamlit Dashboard..."
	uv run streamlit run ui/dashboard.py

audit-mem:
	@echo "🧠 Running Memory Audit..."
	export DB_URL="postgresql://postgres:postgres@127.0.0.1:5433/stocks_dwh" && uv run python src/scripts/audit_mem.py

# --- Initial Setup ---
install:
	uv sync
	astro dev start
