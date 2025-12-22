FROM astrocrpublic.azurecr.io/runtime:3.1-7

USER astro

# 1. Crear el venv e instalar dbt con pip optimizado
RUN python -m venv dbt_venv && \
  dbt_venv/bin/pip install --no-cache-dir --upgrade pip && \
  dbt_venv/bin/pip install --no-cache-dir dbt-postgres dbt-core

# 2. Instalar dependencias de Airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copiar el proyecto
COPY --chown=astro:astro . .

# 4. PASO CLAVE: Ejecutar dbt deps durante el build
# Si esto falla, el log de la terminal te dirá EXACTAMENTE qué falta
WORKDIR /usr/local/airflow/dags/dbt/elt_pipeline_stocks
RUN /usr/local/airflow/dbt_venv/bin/dbt deps

# 5. Instalación editable
WORKDIR /usr/local/airflow
RUN pip install -e .
