# Batch Value Replace (batch_value_replace)

The `batch_value_replace` tool is a modular ArcGIS Python utility designed to scan and update specified target attributes across feature services and standalone tables in an ArcGIS Portal/Online organization using an `OLD` -> `NEW` crosswalk CSV.

---

## Workflow & Operational Modes

The tool operates in two distinct phases: **Scan Mode** and **Update Mode**.

### 1. Scan Mode (`--mode scan`)

- **Discovery**: Searches the portal for Feature Services (with optional owner, item ID, and tag exclusions such as `--exclude-tags "Azure"`). Automatically excludes hosted feature layer views and view layers to prevent duplication.
- **Inspection**: Iterates through layers and tables, checking if requested target fields exist. Evaluates edit permissions (`EDITABLE`, `ADMIN_OVERRIDE`, `READ_ONLY`) based on service capabilities and administrator privileges.
- **Reporting**: Outputs an Excel workbook containing multiple worksheets:
  - `Run Summary`: Execution metadata and statistics.
  - `Layer Summary`: Summary of scanned layers and matching record counts.
  - `Feature Updates`: Detailed list of individual feature records matching old crosswalk values, including their current and corrected values.
  - `Broken Resources`: List of unreachable or failing services/layers.

### 2. Review & Curation

- Review the generated Excel report.
- **Excluding Records/Layers**: To prevent specific features or layers from being updated during the update phase, simply **delete those rows** from the `Feature Updates` worksheet in the Excel workbook. The tool reads only the remaining rows in `Feature Updates` when executing updates.

### 3. Update Mode (`--mode update`)

- Requires the reviewed Excel workbook (`--update-report` or `Reviewed Update Workbook`).
- Re-connects to the portal, verifies feature existence and current values, and applies approved updates in batches.
- **Edit Permissions**: Only truly read-only layers (`READ_ONLY` / `ACCESS_DENIED`) are skipped. Layers marked `EDITABLE` or `ADMIN_OVERRIDE` are successfully updated.

---

## Parameter Reference

| Parameter Name (CLI) | ArcGIS Pro Toolbox Display Name | Type | Description |
| ---------------------- | --------------------------------- | ------ | ------------- |
| `--mode` | Operation Mode | String (`scan`, `update`) | Operation mode: scan or update. |
| `--csv` | Value Crosswalk | File (`.csv`) | CSV file containing `OLD` and `NEW` value columns. |
| `--output` | Output Excel Workbook | File (`.xlsx`) | Destination Excel workbook report path. |
| `--connection-method` | ArcGIS Connection Method | String (`Saved Profile`, `Explicit Portal`) | Method used to connect to ArcGIS Portal. |
| `--profile` | ArcGIS Profile | String | Saved ArcGIS Pro/API profile name (default: `work`). |
| `--portal` | Portal URL | String | Explicit ArcGIS Portal URL. |
| `--username` | Username | String | Username for explicit portal authentication. |
| `--fields` | Target Fields | String | Comma-separated target fields to scan/replace (default: `PARK_NAME,LOCATION_NAME`). |
| `--owner` | Item Owner | String | Optional portal owner filter. |
| `--item-id` | Feature Service Item ID | String | Optional specific Feature Service Item ID. |
| `--exclude-tags` | Exclude Tags | String | Optional comma-separated tags to exclude (e.g., `Azure`). |
| `--candidate-layer-limit` | Candidate Layer Limit | Long | Optional limit on qualifying candidate layers. |
| `--discovery-item-limit` | Discovery Item Limit | Long | Maximum number of Feature Service items to inspect. |
| `--discovery-page-size` | Discovery Page Size | Long | Items retrieved per portal search page (default: `50`). |
| `--query-batch-size` | Query Batch Size | Long | Query batch size (default: `100`). |
| `--edit-batch-size` | Edit Batch Size | Long | Edit batch size (default: `100`). |
| `--update-report` | Reviewed Update Workbook | File (`.xlsx`) | Reviewed Excel report required for Update mode. |
| `--log-file` | Log File | File (`.log`) | Optional execution log file path. |

---

## Usage Examples

### CLI Usage

**Scan Mode (with Azure tag exclusion):**

```powershell
$env:PYTHONPATH="."; & "C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" -m src.utils.batch_value_replace.cli `
    --profile work `
    --csv "Changed Park Names as of 20260813.csv" `
    --mode scan `
    --fields "PARK_NAME,LOCATION_NAME" `
    --exclude-tags "Azure" `
    --output "C:\temp\park_name_scan_report.xlsx"
```

**Update Mode:**

```powershell
$env:PYTHONPATH="."; & "C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe" -m src.utils.batch_value_replace.cli `
    --profile work `
    --csv "Changed Park Names as of 20260813.csv" `
    --mode update `
    --update-report "C:\temp\park_name_scan_report.xlsx" `
    --output "C:\temp\park_name_update_results.xlsx"
```

### ArcGIS Pro Toolbox Usage

1. Open ArcGIS Pro and load the compiled toolbox (`dist\GIS-Tools.pyt`).
2. Open the **Batch Value Replace** tool.
3. Configure **Operation Mode** (`Scan` or `Update`), select your **Value Crosswalk** CSV and **Output Excel Workbook**.
4. Set connection parameters, target fields, and any desired tag exclusions (`Exclude Tags`).
5. Run the tool. For updates, supply the reviewed workbook in the **Reviewed Update Workbook** parameter.
