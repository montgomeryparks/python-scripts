# ArcGIS Pro Modular Toolbox Workflow

## Project Architecture
This repository uses a custom Python AST bundler to compile independent Python scripts into standalone ArcGIS Pro toolboxes (`.pyt`).
- **`src/toolboxes/`**: Contains the independent tool scripts (`.py`). Each folder here represents a final `.pyt` file. Tools must be written as standard ArcGIS `object` classes with `getParameterInfo` and `execute` methods.
- **`src/utils/`**: Contains shared utility modules (e.g., `auth.py`, `data_conversion.py`, `constants.py`). These are dynamically injected into the compiled `.pyt` files via the AST bundler only when explicitly imported by a tool. 
- **`helpers/`**: Contains standalone data engineering and infrastructure scripts (e.g., Azure Synapse ingestion) that are NEVER compiled into the `.pyt` toolboxes.
- **`dist/`**: The output directory for compiled toolboxes. Never write source code directly to this directory.

## Coding Standards & Syntax Preferences
- **Python Formatting**: Write clean, modern Python 3.x. Use `ruff` for linting and formatting. Avoid using `black` or `autopep8` as they may conflict with `ruff` rules.
- **API Authentication**: Treat API `bearer` parameters strictly as static client keys, not as dynamic session tokens.
- **Data Schemas**: When referencing time-series or project sequence data, ensure the column name syntax is exactly `'Timeline'`.

## Geoprocessing Best Practices
- Always clean up `arcpy.da.InsertCursor`, `UpdateCursor`, and `SearchCursor` objects using `with` blocks or explicit `del` statements to prevent ArcGIS schema locking errors.
- Prefer pushing data to Pandas/DuckDB DataFrames in-memory for heavy transformations rather than executing row-by-row updates via `arcpy`.