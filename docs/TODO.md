# uv script dependency drift

## 🎯 Dependency Duplication Rationale (Astro CLI Context)

In your current setup, duplicating dependencies between **`pyproject.toml`** and **`requirements.txt`** is **necessary** and **correct** to ensure your pipeline runs within the Airflow environment.

### 1. The Conflict: Astro CLI vs. Modern Standards

- **`pyproject.toml` (Modern Standard):** This defines dependencies for your Python **project** (local development, testing, tools like UV).
- **`requirements.txt` (Historical Standard):** The **Astro CLI/Airflow Docker build process** *only* reads this file to install packages into the Airflow container. It ignores the modern `pyproject.toml` dependencies section.

**Crucial Point:** If your project dependencies (like `duckdb`, `polars`, `structlog`) are not explicitly listed in **`requirements.txt`**, the Airflow container will start **without them**, leading to immediate runtime failures when your DAGs execute.

### 2. Beyond Duplication: Necessary Additions

The `requirements.txt` file also serves a secondary purpose by holding necessary **Airflow Providers** (e.g., `apache-airflow-providers-amazon`). These providers are essential for Airflow to recognize AWS Hooks and Operators and often aren't core dependencies of the pipeline project itself.

### 💡 Long-Term Strategy (Addressing Dependency Drift)

Duplicating dependencies creates **Dependency Drift** (a form of technical debt). The ideal solution is to maintain a **Single Source of Truth** in `pyproject.toml` and **automate the generation** of `requirements.txt`.

- **Future Action:** Configure a build script (or use a dedicated `uv` command) to automatically sync or generate `requirements.txt` from your `pyproject.toml` file just before the Astro CLI initiates the Docker image build. This ensures consistency and eliminates manual duplication.

# data engineering interview approaches

- **product sense**
- test me about questions on product sense and business needs
- figure out hot to do a SELF JOIN versus using a LAG or LEAD statement
  - self join preffer for hirers
- system design
  - database design, schemas,
- mencionar los 'puntos de dolor' en la interview
- resaltar los problemas con los que me encontre y las soluciones que tuve que usar
  - example:
  - "Usé [Framework] para la transformación, pero cuando el volumen de datos se incrementó, tuve que optimizar el proceso escalando el cluster y cambiando el método de join para reducir el shuffling (Componente 4 y 2)."

# transformation module

- [ ] Test transformation with other symbols (AAPL, MSFT, TSLA)

# refactoring

- [x] minio_client.py -> airflow s3Hook

# logging

- [x] setup structlog in logging.py

# compose

- [ ] fix compose.yml / init-databases.sh:
  - init-databases.sh not executing before airflow-init services initiate
  - sudo docker compose logs postgres
- [ ] create airflow connection (metadata db conn)

# testing

- [ ] unit test to hooks

# nvim-treesitter

- config queries for bash embedded in YAML files (syntax highlighting)
