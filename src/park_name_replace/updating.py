import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from .models import ParkNameReplaceConfig, UpdateResult
from .crosswalk import load_crosswalk, build_name_mapping
from .reporting import read_approved_updates, write_update_workbook
from .connection import connect_gis
from .discovery import find_candidate_items, get_layers_and_tables, find_field

logger = logging.getLogger("park_name_replace.updating")

def run_update(config: ParkNameReplaceConfig, message_handler=None) -> UpdateResult:
    def log_info(msg):
        logger.info(msg)
        if message_handler and hasattr(message_handler, "info"):
            message_handler.info(msg)

    def log_warn(msg):
        logger.warning(msg)
        if message_handler and hasattr(message_handler, "warning"):
            message_handler.warning(msg)

    def log_err(msg):
        logger.error(msg)
        if message_handler and hasattr(message_handler, "error"):
            message_handler.error(msg)

    run_started = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_info("Starting Park Name Replace Update workflow...")

    if not config.update_report or not config.update_report.exists():
        raise FileNotFoundError(f"Reviewed update report workbook not found: {config.update_report}")

    df_approved = read_approved_updates(config.update_report)
    log_info(f"Loaded approved updates workbook with {len(df_approved)} total records.")

    if "Update Selected" in df_approved.columns:
        selected_rows = df_approved[df_approved["Update Selected"].astype(str).str.strip().str.lower() == "yes"]
    else:
        selected_rows = pd.DataFrame()

    log_info(f"Found {len(selected_rows)} records explicitly selected for update.")

    df_cw = load_crosswalk(config.csv_path)
    mapping, ambiguous_map = build_name_mapping(df_cw)

    gis = connect_gis(config)
    portal_url = gis.url.rstrip("/")
    authenticated_user = gis.properties.user.username if hasattr(gis.properties, "user") and gis.properties.user else "Anonymous"

    updates_by_resource = {}
    for _, row in selected_rows.iterrows():
        item_id = str(row.get("Item ID", ""))
        layer_id = row.get("Layer/Table ID", 0)
        key = (item_id, layer_id)
        if key not in updates_by_resource:
            updates_by_resource[key] = []
        updates_by_resource[key].append(row)

    feature_updates_out = []
    layer_summary_out = []

    items = find_candidate_items(gis, config)
    item_map = {item.id: item for item in items}
    for item in items:
        item_title = getattr(item, "title", "Untitled")
        item_id = getattr(item, "id", "")
        item_owner = getattr(item, "owner", "")
        service_url = getattr(item, "url", "")

        resources = get_layers_and_tables(item)
        for res, res_type, layer_id in resources:
            res_url = f"{service_url}/{layer_id}" if service_url else ""
            has_park_name, actual_name_field = find_field(res, config.park_name_field)
            if not has_park_name:
                continue

            has_park_code, actual_code_field = find_field(res, config.park_code_field if config.park_code_field else "PARK_CODE")
            layer_name = getattr(res, "properties", {}).get("name", "Layer")

            editing_supported = False
            try:
                caps = getattr(res, "properties", {}).get("capabilities", "")
                if "Update" in caps or "Editing" in caps:
                    editing_supported = True
            except Exception:
                editing_supported = True

            key = (item_id, layer_id)
            res_updates = updates_by_resource.get(key, [])
            selected_in_layer = len(res_updates)

            layer_successful = 0
            layer_failed = 0
            layer_skipped = 0

            if res_updates and editing_supported:
                batch_size = config.edit_batch_size
                for i in range(0, len(res_updates), batch_size):
                    batch = res_updates[i:i+batch_size]
                    features_to_edit = []
                    for row in batch:
                        oid = row.get("ObjectID")
                        scanned_prev_name = row.get("Current PARK_NAME")
                        expected_corrected = row.get("Corrected PARK_NAME")

                        try:
                            q_res = res.query(where=f"OBJECTID = {oid}", out_fields="*", return_geometry=False)
                            if not q_res.features:
                                layer_skipped += 1
                                feature_updates_out.append({
                                    **row,
                                    "Update Result": "FEATURE_NOT_FOUND",
                                    "Update Message": f"Feature with OBJECTID {oid} no longer exists.",
                                })
                            else:
                                feat = q_res.features[0]
                                current_val = str(feat.attributes.get(actual_name_field, "")).strip()

                                if current_val != scanned_prev_name:
                                    layer_skipped += 1
                                    feature_updates_out.append({
                                        **row,
                                        "Update Result": "SOURCE_VALUE_CHANGED",
                                        "Update Message": f"Source value changed from '{scanned_prev_name}' to '{current_val}'.",
                                    })
                                elif current_val not in mapping:
                                    layer_skipped += 1
                                    feature_updates_out.append({
                                        **row,
                                        "Update Result": "SKIPPED_NOT_ELIGIBLE",
                                        "Update Message": f"Value '{current_val}' is no longer in crosswalk mapping.",
                                    })
                                else:
                                    target_info = mapping[current_val]
                                    if target_info["new_name"] != expected_corrected:
                                        layer_skipped += 1
                                        feature_updates_out.append({
                                            **row,
                                            "Update Result": "SOURCE_VALUE_CHANGED",
                                            "Update Message": "Crosswalk mapping changed for this value.",
                                        })
                                    else:
                                        feat.attributes[actual_name_field] = expected_corrected
                                        features_to_edit.append((feat, row))
                        except Exception as val_err:
                            layer_failed += 1
                            feature_updates_out.append({
                                **row,
                                "Update Result": "UPDATE_FAILED",
                                "Update Message": f"Revalidation error: {val_err}",
                            })

                    if features_to_edit:
                        try:
                            edits_list = [item[0] for item in features_to_edit]
                            rows_list = [item[1] for item in features_to_edit]
                            edit_results = res.edit_features(updates=edits_list)
                            for idx_f, f_res in enumerate(edit_results.get("updateResults", [])):
                                feat_row = rows_list[idx_f]
                                if f_res.get("success"):
                                    layer_successful += 1
                                    feature_updates_out.append({
                                        **feat_row,
                                        "Update Result": "UPDATED",
                                        "Update Message": "Successfully updated park name",
                                    })
                                else:
                                    layer_failed += 1
                                    err_desc = f_res.get("error", {}).get("description", "Unknown edit error")
                                    feature_updates_out.append({
                                        **feat_row,
                                        "Update Result": "UPDATE_FAILED",
                                        "Update Message": err_desc,
                                    })
                        except Exception as edit_err:
                            log_err(f"Edit batch failed for {item_title} / {layer_name}: {edit_err}")
                            for _, feat_row in features_to_edit:
                                layer_failed += 1
                                feature_updates_out.append({
                                    **feat_row,
                                    "Update Result": "UPDATE_FAILED",
                                    "Update Message": str(edit_err),
                                })

            elif res_updates and not editing_supported:
                for row in res_updates:
                    layer_skipped += 1
                    feature_updates_out.append({
                        **row,
                        "Update Result": "LAYER_NOT_EDITABLE",
                        "Update Message": "Layer does not support updates or editing capabilities.",
                    })

            layer_summary_out.append({
                "Portal URL": portal_url,
                "Item Title": item_title,
                "Item ID": item_id,
                "Item Owner": item_owner,
                "Resource Type": res_type,
                "Layer/Table Name": layer_name,
                "Layer/Table ID": layer_id,
                "Layer/Table URL": res_url,
                "Candidate Sequence": "",
                "PARK_NAME Field": actual_name_field,
                "PARK_CODE Field Present": "Yes" if has_park_code else "No",
                "Editing Supported": "Yes" if editing_supported else "No",
                "Total Records": len(res_updates),
                "Matching Records": len(res_updates),
                "Eligible Update Records": selected_in_layer,
                "PARK_CODE Mismatches": 0,
                "Ambiguous Matches": 0,
                "Query Status": "SUCCESS",
                "Query Message": f"Selected: {selected_in_layer}, Updated: {layer_successful}, Failed: {layer_failed}, Skipped: {layer_skipped}",
            })

    run_completed = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    run_info = {
        "Operation Mode": "Update",
        "Interface": "CLI / ArcPy Tool",
        "Connection Method": "Explicit Portal" if config.portal_url else "Saved Profile",
        "Profile Name": config.profile or "",
        "Portal URL": portal_url,
        "Authenticated User": authenticated_user,
        "Crosswalk Path": str(config.csv_path),
        "Output Workbook": str(config.output_path),
        "Candidate Layer Limit": config.candidate_layer_limit if config.candidate_layer_limit is not None else "None",
        "Qualifying Candidates Discovered": len(item_map),
        "Candidates Selected": len(selected_rows),
        "Candidate Limit Applied": "No",
        "Candidates Excluded by Limit": 0,
        "Query Batch Size": config.query_batch_size,
        "Edit Batch Size": config.edit_batch_size,
        "Run Started": run_started,
        "Run Completed": run_completed,
        "Run Status": "SUCCESS",
    }

    result = UpdateResult(
        feature_updates=feature_updates_out,
        layer_summary=layer_summary_out,
        run_info=run_info,
        output_path=config.output_path,
    )

    write_update_workbook(result, config)
    log_info(f"Update complete. Output written to {config.output_path}")
    return result

