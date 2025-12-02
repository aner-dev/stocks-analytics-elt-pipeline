# development setup

### recommended tools

- 'yq' - YAML files manipulation (docker-compose.yml, config.yml files)
- 'jq' - JSON parsing structured logging output and debugging

### logging setup

-

## 🛠️ Project Setup and Dependencies

### Python Package Installation

To ensure that Airflow and all scripts can correctly locate the project modules (e.g., `extract.py`, `minio_client.py`), the project must be installed as an editable Python package. This facilitates the use of clean **absolute imports**.

1. **Install the Project:** From the root directory of the repository, execute the following command:
   ```bash
   uv pip install -e .
   ```
   The `-e` flag ensures the installation is **editable**, meaning changes to the source files are immediately reflected without needing reinstallation.

### Standardized Import Convention

All internal imports across the project (in DAGs, `src/`, and tests) must use the **absolute import** convention, starting with the package name (`elt_stocks_pipeline`).

- **Incorrect (Relative):** `from ..load.minio_client import ...`
- **Correct (Absolute):** `from elt_stocks_pipeline.src.load.minio_client import get_minio_client`

#### rationale / justification

This measure is implemented to significantly improve code **maintainability**, **portability**, and **readability**.

- aligning the project with **PEP 8** (Python Enhancement Proposal 8) recommendations for clear and unambiguous module imports.

By establishing the project as an installed package, its eliminated the need for unreliable path manipulation (`sys.path`) in Airflow DAGs

- ensuring robust module resolution across all execution environments.
