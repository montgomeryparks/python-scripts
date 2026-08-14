# ArcGIS Pro Modular Toolboxes

This repository houses custom Python toolboxes (`.pyt`) for ArcGIS Pro and standalone python scripts.

To solve the historical problem of version-controlling massive `.pyt` files, this repository uses a modular "bundler" approach. Tools are written as independent Python scripts (`.py`), and an automated build process stitches them together into standard, standalone ArcGIS Pro Toolboxes ready for deployment.

---

## 📥 For Users: How to Download the Toolboxes

**Do not download the source code files in this repository to use the tools.**

The `.pyt` files are generated automatically every time the code is updated. To get the latest, ready-to-use toolboxes:

1. Navigate to the homepage of this GitHub repository.
2. Look at the right-hand sidebar for the **Releases** section.
3. Click on the **Latest Release** (e.g., "Latest Toolbox Build").
4. Under the **Assets** header, click the compiled `.pyt` files (e.g., `Analytics-Tools.pyt`) to download them.
5. Save these files to your local machine or network drive, and connect to them in ArcGIS Pro just like any normal toolbox.

---

## 🏗️ For Developers: Repository Architecture

This repository operates similarly to a web bundler for the ArcGIS toolboxes, while also serving as a version-controlled home for standalone data scripts. We write modular, easily testable Python code in the `src/` directory, and the build script compiles it into the `dist/` directory (which is ignored by Git).

```text
/
├── src/
│   ├── toolboxes/             # Each subfolder here becomes a single .pyt file
│   │   └── Analytics-Tools/   # Becomes Analytics-Tools.pyt
│   │       ├── tool_import.py # An independent tool class
│   │       └── tool_export.py # Another independent tool class
│   └── utils/                 # Shared helper logic (math, parsing, etc.)
│       └── helpers.py         # Injected into the top of EVERY compiled .pyt
├── helpers/                   # Standalone scripts (Not compiled into toolboxes)
│   └── FeatureService_Azure_Helper.py 
├── tests/                     # PyTest integration tests using a dummy geodatabase
├── .vscode/                   # Pre-configured VS Code tasks for Conda and building
├── build_toolboxes.py         # The Python AST bundler script
├── requirements.txt           # Development dependencies (pytest, ruff, etc.)
└── .gitignore                 # Keeps compiled artifacts (dist/) out of version control
```

### How the Build Process Works

When `build_toolboxes.py` runs, it:

1. Gathers all shared code from `src/utils/`.
2. Scans `src/toolboxes/` and groups tools by their parent folder.
3. Extracts and deduplicates standard Python imports at the top of the file.
4. Strips out local development imports (e.g., `from src.utils import helpers`).
5. Generates the `Toolbox(object)` wrapper class dynamically.
6. Outputs a pristine, single-file `.pyt` into the `dist/` folder.

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

To clone the default ArcGIS Pro environment so you can safely develop and test:

* Open the VS Code Command Palette (`Ctrl+Shift+P` on Windows).
* Select **Tasks: Run Task**.
* Choose **`1. Setup: Clone ArcGIS Pro Env`**.
* *Note: This creates an environment named `da_env`. Ensure your VS Code Python Interpreter is set to this environment.*

### 2. Sync Dependencies

To install formatting and testing tools (`pytest`, `ruff`) into your Conda environment:

* Run task: **`2. Sync: Install Requirements`**.

### 3. Build the Toolboxes

To compile your `.py` files into `.pyt` files locally for testing in ArcGIS Pro:

* Press **`Ctrl+Shift+B`** (or Run Task -> **`3. Build: Compile PYT`**).
* Check the newly created `dist/` folder for your toolboxes.

### 4. Run Tests

To execute the PyTest integration suite:

* Run task: **`4. Test: Run PyTest`**.

---

## ☁️ Automated Cloud Builds (Continuous Integration)

This repository uses a **GitHub Action** to automate releases.

Whenever code is pushed or merged into the `main` branch, a GitHub server automatically runs the `build_toolboxes.py` script. The resulting `.pyt` files are immediately packaged and attached to the repository's **Releases** page.

Because of this, you never need to manually commit files from your local `dist/` folder.
