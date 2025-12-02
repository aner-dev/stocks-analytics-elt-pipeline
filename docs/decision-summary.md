# ELT Project as a Python Package

- **Goal:** Eliminate dependency on the execution location (working directory) and allow any file to import any other file consistently, as if everything were in a central library.
- **Problem:** Resolving the root module dependency relative to the `$PYTHONPATH`.
- **Solution:** Install the project as a Python package in **editable mode** (`uv pip install -e .`).
- **Outcome:** This avoids the need to manually manipulate the `$PYTHONPATH` based on the *current working directory*, enabling **absolute imports** regardless of where the code is executed.
