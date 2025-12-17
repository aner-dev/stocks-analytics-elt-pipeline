# Dockerfile (astro-cli)

- change in Dockerfile:
  '&& pip install --no-cache-dir -e /usr/local/airflow' to '&& pip install --no-cache-dir -e .'
- reasoning:
  - "The use of the period (.) in pip install -e . is the canonical instruction that gives pip the packaging context, resolving the ModuleNotFoundError

## abstract

- The period '.' tells pip to look for Python metadata (pyproject.toml or setup.py) in the Current Working Directory (WORKDIR, which in Astro is /usr/local/airflow).
- By finding the metadata, pip can register the module with its logical name (elt_pipeline_hn) in the environment, allowing my DAG imports to work correctly, unlike the absolute path which only tries to install a directory without a package name."

# streaming vs incremental loading

i think that use incremental loading will be the correct approach instead of streaming
i was wrong; here why:

**Streaming** solves the immediate **RAM** problem during extraction.
**Incremental Loading** is a separate, downstream task that optimizes **compute cost and processing time** after the data is safely loaded into the S3 Bronze Layer.

- physical implementation: requests.stream

## 🆚 Streaming vs. Incremental Loading: Design Rationale

This principle clarifies the essential difference between optimizing **data transport efficiency** and optimizing **data content processing**.

### 1. 🌊 Streaming (Transfer Efficiency)

- **Primary Focus:** **I/O Transfer** and **RAM**.
- **Definition:** A technique that moves data from source to destination (S3) in **small chunks** (`requests.stream`), rather than loading the entire file into memory at once.
- **Technical Goal:** **Avoid Out-of-Memory (OOM)** errors caused by large files (like your 4.6 GB dataset) overloading the worker's RAM.
- **Application to Your Pipeline:** The current implementation using `requests.stream` is the **correct and necessary solution** for **stability** in the **Extract (E)** stage.

### 2. 📈 Incremental Loading (Content Logic)

- **Primary Focus:** **Content** and **Processing Time/Cost**.
- **Definition:** A logical strategy used to **filter and process only the new or modified records** since the previous pipeline run.
- **Technical Goal:** **Reduce the total volume of data** that the compute engine (DuckDB) must handle, saving time and CPU costs.
- **Application to Your Pipeline:** This logic should be implemented in the **Transformation (T) Layer** (e.g., using DuckDB). The strategy is: **load the full snapshot via Streaming to S3, then filter for new records in the Silver layer.**

______________________________________________________________________

**Conclusion:**

# 9th october

## change the scope of the project

- starting the documentation of the project
- added mkdocs for funcionality improves

### deprecated

- created 'deprecated' dir, rename & mv there extract.py of static dataset
- created new 'extract-alpha-vantage.py'

### branches

- working out in main, change branch for better git workflow and standard practices

## API format

- always choose **json** format for raw data if is available
  - later would be transformed into csv or parquet

## ? in HTTP connections

- ? is the *syntax sugar* that points the init of *query parameters*

## .env files

- NEVER use "" or '' in environment variables on .venv config files to simple values (URLs, API keys, numbers)
- cause: error parsing

## docker containers

- once the docker run command is executed, i must be secure of the flags and config that i choose
- i want to change the user and password env variables? recreate the container, repeat the docker run command

# database

- try queries with NUMERIC and then replace them with DOUBLE PRECISION
- to test performance trade-offs and efficiency

# data modeling in database schema design

- starting the design of the schema for the database
- Adding a primary key will automatically create a unique B-tree index on the column or group of columns listed in the primary key, and will force the column(s) to be marked NOT NULL.

## DDL

- DDL: memory/resources demanding, not very frequent, sensible/fragile
  - highly consequences if its fail; is *change the architecture design* of the *data modeling*
- create dir database/ddl and separate schema design on atomic .sql files
- grain: stock-week

### constraints

- UNIQUE constraint create a INDEX automatically
- so is useless and dumb create a INDEX after using a UNIQUE in a attribute of a dimension table

### possibly dimensions to add to the fact table

- dim_exchange exchange_id (bolsa de valores donde cotiza)
- dim_currency currency_id (moneda de cotizacion)

### foreign keys

- maintain the *referential integrity* between two related tables
- why NOT use FOREIGN KEY expression
  - the REFERENCES implementation that have FOREIGN KEY, would require look over the entire database for coincidences; a nice way to shoot myself in the foot
- instead, incorporate the CONCEPTUAL approach of *foreign keys*, adding *indexes*

## indexes

- load raw data on minIO bucket without indexes
- UNIQUE restriction:
  - usar COPY sentence (fastest way to load)
    - la mejor práctica es cargar la data en una tabla staging sin la restricción de unicidad
    - y luego mover la data a la tabla final dim_stock donde la restricción UNIQUE está activa.

# docker

- healtcheck in compose.yml
- pg_isready for databases checkhealth
  - psql utility created specifically for this purpose
    - test: ["CMD-SHELL", "pg_isready" "-U" "airflow"]
    - instead of 'curl -f'

# airflow

- airflow connections
  - experience: almost create a airflow-connection for *alpha vantage API*; that its already handled for extract.py
  - context of use:
    - use of native operators/hooks of airflow
    - centralized management of secrets
    - reutilize credentials between DAGs

# postgreSQL

- pg_roles = system view that list all the users/roles in psql
- rolname = column with the name of the user/role
- pg_database = system view that list all the databases of the cluster
- datname = column with the name of the database

# dataframes

- polars-pypsark-pandas-duckb
- the main way to transform data previous upload it to the database
-

# python

- ISO 8601 is the international standard for represent dates/hours
- rule-of-thumb: use ISO 8601 format for representing dates/hours (.isoformat() method)
  - avoid ambiguity
- context of use:
  - pipelines metadata
  - execution logs
  - file names
  - timestamps in databases
  - APIs & serialization

# garbage files and project dir organization

- rm -rf logs: unnecessary dir, logs should be handled by airflow/docker
-

# ML (machine learning), OOP & FP

- FP for the logic:
- OOP for the structure:

# uv

- uv add vs uv pip install / high-level vs low-level
- uv add
  - pyproject.toml as source of truth
  - one cmd pipeline: add pkg dependency to pyproject.toml -> uv lock -> uv sync
- uv pip install
  - install pkg directly in the venv
  - doesn't do the 'one cmd pipeline'

# dbt (data build tool)

- dbt context-tree:
  hn_dbt/
  ├── dbt_project.yml (Configuración global)
  ├── profiles.yml (Configuración de conexión - está en ~/.dbt)
  ├── models/
  │ ├── staging/ ⬅️ 1. Limpieza de datos crudos (Fuentes)
  │ │ └── stg_hn_items.sql
  │ ├── intermediate/ ⬅️ 2. Lógica compleja/join de staging (Opcional)
  │ │ └── int_hn_users_enriched.sql
  │ └── marts/ ⬅️ 3. Modelos de Negocio/Analíticas (Consumo final)
  │ ├── marts_hn_daily_stats.sql
  │ └── marts_hn_top_stories.sql
  └── sources.yml ⬅️ Define las tablas cargadas por Python.
