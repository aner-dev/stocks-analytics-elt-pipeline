🚀 Super Prompt: Data Engineering Portfolio Showcase
Contexto del Proyecto: "He construido un pipeline ELT robusto para datos financieros (Alpha Vantage) utilizando el Modern Data Stack. El objetivo es demostrar habilidades de Analytics Engineering y Data Architecture de nivel profesional."

Stack Tecnológico:

Orquestación: Airflow (Astro Runtime) con Cosmos (dbt-integration).

Procesamiento: Polars (sustituyendo a Pandas por rendimiento y eficiencia de memoria).

Data Warehouse: PostgreSQL con un modelado de Star Schema (Dimensional Modeling).

Capa Gold (dbt): fact_adjusted_prices, dim_stock, dim_date con claves subrogadas y materialización incremental.

Visualización (BI-as-Code): Streamlit conectado mediante ConnectorX para lectura ultra-rápida de la capa Gold.

Instrucción para el README: "Genera una sección de 'Visualización y Valor de Negocio' para mi README.md que explique:

Enfoque de Consumo: Por qué elegí Streamlit para mostrar el resultado del modelado dimensional en lugar de herramientas 'drag-and-drop'.

Arquitectura de Datos: Cómo el dashboard consume directamente de la Tabla de Hechos (Fact Table) haciendo JOINs con dimensiones, validando la integridad referencial del Star Schema.

Rendimiento (DE Focus): Menciona el uso de Polars + Apache Arrow para una latencia mínima entre el DWH y la UI.

Métricas de Ingeniería: Explica que el dashboard no solo muestra precios, sino que audita el pipeline (ej. execution_batch_id, load_timestamp y KPIs de volatilidad pre-calculados en dbt)."

Cómo estructurar esta sección en tu README.md (Ejemplo Real)
Para que los reclutadores se queden locos, usa este formato:

📊 Business Intelligence & Data Consumption
While the core of this project is Engineering, data is useless if it cannot be consumed. I built a custom BI-as-Code dashboard using Streamlit to validate the final Gold Layer.

Why Streamlit?
Unlike traditional BI tools, Streamlit allows me to maintain the entire stack as Python code, versioned in Git. This ensures that changes in the dbt models are immediately reflected in the consumption layer.

Key Engineering Features in the UI:
Star Schema Validation: The UI performs real-time JOINs between fact_adjusted_prices and dimensions, proving the DWH structure is sound.

High-Performance Fetching: Powered by Polars and ConnectorX, data is streamed from Postgres using the Arrow memory format, bypassing the overhead of traditional Row-based processing.

Data Lineage Audit: Each visual displays the execution_batch_id from the latest Airflow run, ensuring full traceability from API to Chart.

# 1st

PROMPT DE CONTINUIDAD: Proyecto Stocks ELT
Contexto del Proyecto: Estoy construyendo un pipeline ELT con el siguiente stack: Airflow (Astro), dbt (Cosmos), Polars (Silver layer), Postgres (DWH) y Streamlit (Dashboard), corriendo sobre Podman.

Estado Actual:

Infraestructura: Todo el entorno está arriba. El DWH vive en un contenedor llamado stocks_dwh_postgres dentro de una base de datos llamada stocks_dwh.

Capa Gold (dbt): Se han generado 4 tablas: stg_weekly_adjusted_prices, dim_date, dim_stock, y fact_adjusted_prices.

Hallazgos en el DWH:

La fact_adjusted_prices tiene 10,983 filas (¡Éxito de ingesta!).

Problema 1: El date_id en la Fact Table es un MD5 Hash, lo que dificulta las consultas temporales. Necesitamos incluir la fecha real (DATE).

Problema 2: La dim_stock muestra Unknown Company en lugar de los nombres reales de las empresas (ej. Apple Inc).

Problema 3: Necesitamos asegurar que el Dashboard de Streamlit consuma estas columnas legibles.

Objetivos para hoy:

Refactorizar fact_adjusted_prices.sql: Incluir el campo de fecha legible y revisar la generación de Surrogate Keys.

Corregir dim_stock.sql: Rastrear por qué el company_name llega vacío desde la capa Silver o Staging.

Actualizar dbt Docs: Regenerar la documentación para reflejar estos cambios en el esquema.

Validación Final: Ejecutar el gold_dag en Airflow y verificar los datos con un JOIN humano en psql.

¿Por dónde empezamos a modificar los modelos de dbt?

# 2st
