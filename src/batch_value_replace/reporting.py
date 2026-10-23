import pandas as pd
from pathlib import Path
from .models import ScanResult, UpdateResult, BatchValueReplaceConfig

def write_scan_workbook(result: ScanResult, config: BatchValueReplaceConfig):
    output_path = config.output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df_features = pd.DataFrame(result.feature_updates)
    if df_features.empty:
        df_features = pd.DataFrame(columns=[
            "Portal URL", "Connection Method", "Profile Name", "Item Title", "Item ID",
            "Item Owner", "Service URL", "Resource Type", "Layer/Table Name", "Layer/Table ID",
            "Layer/Table URL", "Candidate Sequence", "ObjectID", "GlobalID", "Target Field Name",
            "Current Value", "Corrected Value", "Match Status",
            "Eligible for Update", "Update Selected", "Update Result", "Update Message"
        ])

    df_summary = pd.DataFrame(result.layer_summary)
    if df_summary.empty:
        df_summary = pd.DataFrame(columns=[
            "Portal URL", "Item Title", "Item ID", "Item Owner", "Resource Type",
            "Layer/Table Name", "Layer/Table ID", "Layer/Table URL", "Candidate Sequence",
            "Target Fields", "Editing Supported", "Active User Can Edit",
            "Edit Permission Status", "Edit Permission Reason", "Total Records",
            "Matching Records", "Eligible Update Records",
            "Ambiguous Matches", "Query Status", "Query Message"
        ])

    df_broken = pd.DataFrame(result.broken_resources)
    if df_broken.empty:
        df_broken = pd.DataFrame(columns=[
            "Portal URL", "Item Title", "Item ID", "Item Owner", "Service URL",
            "Resource Type", "Layer/Table Name", "Layer/Table ID", "Discovery Sequence",
            "HTTP/Error Code", "Status", "Error Type", "Error Message"
        ])

    df_info = pd.DataFrame([result.run_info])

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_features.to_excel(writer, sheet_name="Feature Updates", index=False)
        df_summary.to_excel(writer, sheet_name="Layer Summary", index=False)
        df_broken.to_excel(writer, sheet_name="Broken Resources", index=False)
        df_info.to_excel(writer, sheet_name="Run Information", index=False)

def read_approved_updates(workbook_path: Path) -> pd.DataFrame:
    if not workbook_path.exists():
        raise FileNotFoundError(f"Reviewed update workbook not found: {workbook_path}")
    df = pd.read_excel(workbook_path, sheet_name="Feature Updates")
    return df

def write_update_workbook(result: UpdateResult, config: BatchValueReplaceConfig):
    output_path = config.output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df_features = pd.DataFrame(result.feature_updates)
    if df_features.empty:
        df_features = pd.DataFrame(columns=[
            "Portal URL", "Connection Method", "Profile Name", "Item Title", "Item ID",
            "Item Owner", "Service URL", "Resource Type", "Layer/Table Name", "Layer/Table ID",
            "Layer/Table URL", "Candidate Sequence", "ObjectID", "GlobalID", "Target Field Name",
            "Current Value", "Corrected Value", "Match Status",
            "Eligible for Update", "Update Selected", "Update Result", "Update Message"
        ])

    df_summary = pd.DataFrame(result.layer_summary)
    if df_summary.empty:
        df_summary = pd.DataFrame(columns=[
            "Portal URL", "Item Title", "Item ID", "Item Owner", "Resource Type",
            "Layer/Table Name", "Layer/Table ID", "Layer/Table URL", "Candidate Sequence",
            "Target Fields", "Editing Supported", "Active User Can Edit",
            "Edit Permission Status", "Edit Permission Reason", "Total Records",
            "Matching Records", "Eligible Update Records",
            "Ambiguous Matches", "Query Status", "Query Message"
        ])

    df_broken = pd.DataFrame(result.broken_resources)
    if df_broken.empty:
        df_broken = pd.DataFrame(columns=[
            "Portal URL", "Item Title", "Item ID", "Item Owner", "Service URL",
            "Resource Type", "Layer/Table Name", "Layer/Table ID", "Discovery Sequence",
            "HTTP/Error Code", "Status", "Error Type", "Error Message"
        ])

    df_info = pd.DataFrame([result.run_info])

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_features.to_excel(writer, sheet_name="Feature Updates", index=False)
        df_summary.to_excel(writer, sheet_name="Layer Summary", index=False)
        df_broken.to_excel(writer, sheet_name="Broken Resources", index=False)
        df_info.to_excel(writer, sheet_name="Run Information", index=False)
