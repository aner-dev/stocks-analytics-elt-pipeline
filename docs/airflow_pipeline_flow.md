# 🚀 Conceptual Flow of the Alpha Vantage ELT Pipeline

This document describes the sequence of critical tasks in the `alpha_vantage_weekly_elt.py` DAG, ensuring the separation of responsibilities between infrastructure (Airflow/PostgreSQL) and business modeling (dbt).

## Fail-Fast Principle (DDL Setup)

The DDL task (`setup_database_tables`) **must** execute successfully **before** any extraction or processing task begins. This ensures that the target system (**Silver Layer**) is ready, preventing the waste of resources if there is an infrastructure failure.

## Flow Diagram (Mermaid)

```mermaid
graph TD
    subgraph Setup and Mapping
        A[task_get_db_connection_url] --> B(task_setup_database_tables);
        A --> C(task_generate_symbols_list);
    end
    
    subgraph Mapped Flow (Per Symbol)
        B --> D;
        C --> D;
        D[task_extract_raw_data] --> E(task_validate_raw_data);
        E --> F(task_load_to_bronze);
        F --> G[task_transform_load_to_silver];
    end
    
    G --> H[dbt_run_models_gold];
    style H fill:#f9f,stroke:#333,stroke-width:2px;
```
