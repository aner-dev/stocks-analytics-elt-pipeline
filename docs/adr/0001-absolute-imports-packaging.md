# [ADR-001] Implementing Robust Absolute Imports (Python Packaging)

**Date:** 2025-11-19
**Status:** Completed
**Related Files:** `pyproject.toml`, `__init__.py` files, all Python modules (e.g., `src/extract/extract.py`)

**Decision:**
project structure has been converted into a **Python package** and installed in **editable mode**.
Goals:
To standardize imports, ensure testability, and provide a stable execution environment for Airflow/Docker

**Goal:**

1. Eliminate reliance on the execution location (Working Directory) to resolve imports.
1. Enable robust absolute imports using the package name (`from elt.config.logging_config import get_logger`).
1. Support cleaner unit testing (pytest) of modules within `src/`.

**Implementation Steps:**

1. **Package Definition:** Updated `pyproject.toml` to include `[build-system]` and `[tool.setuptools.packages.find]` to define the project directory (`elt`) as the source package.
1. **Modularity:** Added empty `__init__.py` files to the package root (`elt/`) and all code subdirectories (`config/`, `src/`, `src/extract/`, `src/load/`, etc.).
1. **Installation:** The project is now installed in the virtual environment using `uv pip install -e .`.
1. **Code Changes:** All previously ambiguous or relative imports in `src/` (e.g., `from config...`) have been refactored to use the absolute package path (e.g., `from elt.config...`).

**Result:**
The `FIXME` regarding imports in `src/extract/extract.py` is resolved. Imports are now independent of `$PYTHONPATH` manipulation or the CWD.
