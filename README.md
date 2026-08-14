# ArcGIS Pro Modular Toolboxes

This repository houses custom Python toolboxes (`.pyt`) for ArcGIS Pro and standalone Python scripts.

To solve the historical problem of version-controlling massive `.pyt` files, this repository uses a modular "bundler" approach. Tools are written as independent Python scripts (`.py`), and an automated build process stitches them together into standard, standalone ArcGIS Pro Toolboxes ready for deployment.

---

## 📥 For Users: Downloading and Installing Toolboxes

**Do not download source code files directly to use the tools.**

The `.pyt` files are generated automatically every time the code is updated. To obtain the latest, ready-to-use toolboxes:

1. Navigate to the **Releases** page of the `python-scripts` repository.
2. Under the **Assets** header of the Latest Release, click the compiled `.pyt` files (e.g., `Analytics-Tools.pyt`) to download them.
3. Save the downloaded files to the designated SharePoint library under the `GIS\Toolboxes` folder.
4. In ArcGIS Pro, open the **Catalog** pane, right-click **Toolboxes**, and select **Add Toolbox**.
5. Navigate to the locally synced SharePoint `GIS\Toolboxes` directory and select the `.pyt` file to add it to the project.
6. For bug reports, feature requests, or additional assistance configuring the Python environment, contact Carter Hughes.

---

## 🏗️ For Developers: Repository Architecture

This repository operates similarly to a web bundler for the ArcGIS toolboxes, while also serving as a version-controlled home for standalone data scripts. Modular, easily testable Python code is written in the `src/` directory, and the build script compiles it into the `dist/` directory (which is ignored by Git).

    /
    ├── src/
    │   ├── toolboxes/             # Each subfolder here becomes a single .pyt file
    │   │   └── Analytics-Tools/   # Becomes Analytics-Tools.pyt
    │   │       ├── tool_import.py # An independent tool class
    │   │       └── tool_export.py # Another independent tool class
    │   └── utils/                 # Shared helper logic (math, parsing, API calls)
    │       └── data_conversion.py # Dynamically injected only into .pyt files that import it
    ├── helpers/                   # Standalone scripts (Not compiled into toolboxes)
    │   └── FeatureService_Azure_Helper.py 
    ├── tests/                     # PyTest integration tests using a dummy geodatabase
    ├── .vscode/                   # Pre-configured VS Code tasks for Conda and building
    ├── build_toolboxes.py         # The Python AST bundler script
    ├── requirements.txt           # Development dependencies (pytest, ruff, etc.)
    └── .gitignore                 # Keeps compiled artifacts (dist/) out of version control

### How the Build Process Works

The build script functions as a smart, module-level tree-shaker. When `build_toolboxes.py` runs, it:

1. Scans `src/toolboxes/` and groups tools by their parent folder.
2. Analyzes the Abstract Syntax Tree (AST) of each tool script to build a dependency graph.
3. Extracts and deduplicates standard Python third-party imports at the top of the file.
4. Strips out local development imports (e.g., `from src.utils import auth`).
5. Recursively resolves and injects only the specific `src/utils/` modules required by the tools in that toolbox.
6. Generates the `Toolbox(object)` wrapper class dynamically.
7. Outputs a pristine, single-file `.pyt` into the `dist/` folder.

---

## 🛠️ The Helpers Directory

The `helpers/` directory contains standalone, version-controlled Python scripts that are **not** bundled into the `.pyt` toolboxes. These are typically administrative, deployment, or data engineering scripts used for infrastructure management.

**Key Scripts:**

* `FeatureService_Azure_Helper.py`: A utility designed to bridge ArcGIS Feature Services with Azure Synapse environments. By authenticating via the ArcGIS API for Python (`arcgis.gis.GIS`), it inspects live feature layers and dynamically generates the necessary configuration parameters and T-SQL.
  * **Pipeline Configuration:** Generates Azure Function stage parameters for ingesting feature data into parquet formats.
  * **Data Lake Architecture:** Programmatically writes the `CREATE EXTERNAL TABLE` T-SQL syntax for the Bronze layer and the corresponding stored procedures (`USP_S_GIS_...`) for the Silver layer of the logical data warehouse, automatically mapping Esri field types to SQL Server data types.

---

## 💻 Local Development & VS Code Tasks

Because `arcpy` requires the proprietary Esri Conda environment, this repository uses pre-configured VS Code tasks to automate environment cloning and dependency management without requiring manual terminal commands.

### 1. Initial Setup (First Time Only)

To clone the default ArcGIS Pro environment for safe development and testing:

* Open the VS Code Command Palette (`Ctrl+Shift+P` on Windows).
* Select **Tasks: Run Task**.
* Choose **`1. Setup: Clone ArcGIS Pro Env`**.
* *Note: This creates an environment named `da_env`. Ensure the VS Code Python Interpreter is set to this environment.*

### 2. Sync Dependencies

To install formatting and testing tools (`pytest`, `ruff`) into the Conda environment:

* Run task: **`2. Sync: Install Requirements`**.

### 3. Build the Toolboxes

To compile `.py` files into `.pyt` files locally for testing in ArcGIS Pro:

* Press **`Ctrl+Shift+B`** (or Run Task -> **`3. Build: Compile PYT`**).
* Compiled `.pyt` artifacts will be located in the newly created `dist/` folder.

### 4. Run Tests

To execute the PyTest integration suite:

* Run task: **`4. Test: Run PyTest`**.

---

## ☁️ Automated Cloud Builds (Continuous Integration)

This repository uses a **GitHub Action** to automate releases.

Whenever code is pushed or merged into the `main` branch, a GitHub server automatically runs the `build_toolboxes.py` script. The resulting `.pyt` files are immediately packaged and attached to the repository's **Releases** page.
