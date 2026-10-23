import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from .models import BatchValueReplaceConfig, UpdateResult
from .crosswalk import load_crosswalk, build_name_mapping
from .reporting import read_approved_updates, write_update_workbook
from .connection import connect_gis
from .discovery import get_layers_and_tables, find_field, classify_error

logger = logging.getLogger("batch_value_replace.updating")

def run_update(config: BatchValueReplaceConfig, message_handler=None) -> UpdateResult:
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
    log_info("Starting Batch Value Replace Update workflow...")

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
    log_info(f"Authenticated as user: {authenticated_user}")

    updates_by_resource = {}
    item_ids_to_fetch = set()
    for _, row in selected_rows.iterrows():
        item_id = str(row.get("Item ID", ""))
        layer_id = row.get("Layer/Table ID", 0)
        if item_id:
            item_ids_to_fetch.add(item_id)
            key = (item_id, layer_id)
            if key not in updates_by_resource:
                updates_by_resource[key] = []
            updates_by_resource[key].append(row)

    feature_updates_out = []
    layer_summary_out = []
    broken_resources = []

    successful_updates_count = 0
    failed_updates_count = 0
    skipped_updates_count = 0
    for item_id in item_ids_to_fetch:
        items_inspected += 1
        try:
            item = gis.content.get(item_id)
            if not item:
                raise ValueError(f"Item ID {item_id} not found on portal.")
        except Exception as exc:
            status, err_type, code, clean_msg = classify_error(exc)
            log_warn(f"Item ID '{item_id}' unavailable: HTTP {code} ({status}) - {clean_msg}")
            broken_resources.append({
                "Portal URL": portal_url,
                "Item Title": item_id,
                "Item ID": item_id,
                "Item Owner": "",
                "Service URL": "",
                "Resource Type": "Feature Service",
                "Layer/Table Name": "",
                "Layer/Table ID": "",
                "Discovery Sequence": items_inspected,
                "HTTP/Error Code": code,
                "Status": status,
                "Error Type": err_type,
                "Error Message": clean_msg,
            })
            continue

        item_title = getattr(item, "title", "Untitled")
        item_owner = getattr(item, "owner", "")
        service_url = getattr(item, "url", "")

        try:
            resources = get_layers_and_tables(item)
        except Exception as exc:
            status, err_type, code, clean_msg = classify_error(exc)
            broken_resources.append({
                "Portal URL": portal_url,
                "Item Title": item_title,
                "Item ID": item_id,
                "Item Owner": item_owner,
                "Service URL": service_url,
                "Resource Type": "Feature Service",
                "Layer/Table Name": "",
                "Layer/Table ID": "",
                "Discovery Sequence": items_inspected,
                "HTTP/Error Code": code,
                "Status": status,
                "Error Type": err_type,
                "Error Message": clean_msg,
            })
            continue

        resource_map = {(res_type, str(layer_id)): (res, res_type, layer_id) for res, res_type, layer_id in resources}

        for (target_item_id, target_layer_id), res_updates in updates_by_resource.items():
            if target_item_id != item_id:
                continue

            matched_res_tuple = None
            for (rtype, rlid), rtuple in resource_map.items():
                if str(rlid) == str(target_layer_id):
                    matched_res_tuple = rtuple
                    break

            if not matched_res_tuple:
                for row in res_updates:
                    skipped_updates_count += 1
                    feature_updates_out.append({
                        **row,
                        "Update Result": "LAYER_NOT_FOUND",
                        "Update Message": f"Layer/Table ID {target_layer_id} not found in item {item_id}.",
                    })
                continue

            res, res_type, layer_id = matched_res_tuple
            res_url = f"{service_url}/{layer_id}" if service_url else ""
            layer_name = getattr(res, "properties", {}).get("name", "Layer")

            editing_supported = False
            try:
                caps = getattr(res, "properties", {}).get("capabilities", "")
                if "Update" in caps or "Editing" in caps:
                    editing_supported = True
            except Exception:
                editing_supported = True

            layer_successful = 0
            layer_failed = 0
            layer_skipped = 0

            if editing_supported:
                batch_size = config.edit_batch_size
                for i in range(0, len(res_updates), batch_size):
                    batch = res_updates[i:i+batch_size]
                    features_to_edit = []
                    for row in batch:
                        oid = row.get("ObjectID")
                        target_field = row.get("Target Field Name")
                        scanned_prev_val = row.get("Current Value")
                        expected_corrected = row.get("Corrected Value")

                        try:
                            q_res = res.query(where=f"OBJECTID = {oid}", out_fields="*", return_geometry=False)
                            if not q_res.features:
                                layer_skipped += 1
                                skipped_updates_count += 1
                                feature_updates_out.append({
                                    **row,
                                    "Update Result": "FEATURE_NOT_FOUND",
                                    "Update Message": f"Feature with OBJECTID {oid} no longer exists.",
                                })
                            else:
                                feat = q_res.features[0]
                                current_val = str(feat.attributes.get(target_field, "")).strip()

                                if current_val != scanned_prev_val:
                                    layer_skipped += 1
                                    skipped_updates_count += 1
                                    feature_updates_out.append({
                                        **row,
                                        "Update Result": "SOURCE_VALUE_CHANGED",
                                        "Update Message": f"Source value changed from '{scanned_prev_val}' to '{current_val}'.",
                                    })
                                elif current_val not in mapping:
                                    layer_skipped += 1
                                    skipped_updates_count += 1
                                    feature_updates_out.append({
                                        **row,
                                        "Update Result": "SKIPPED_NOT_ELIGIBLE",
                                        "Update Message": f"Value '{current_val}' is no longer in crosswalk mapping.",
                                    })
                                else:
                                    corrected_target = mapping[current_val]
                                    if corrected_target != expected_corrected:
                                        layer_skipped += 1
                                        skipped_updates_count += 1
                                        feature_updates_out.append({
                                            **row,
                                            "Update Result": "SOURCE_VALUE_CHANGED",
                                            "Update Message": "Crosswalk mapping changed for this value.",
                                        })
                                    else:
                                        feat.attributes[target_field] = expected_corrected
                                        features_to_edit.append((feat, row))
                        except Exception as val_err:
                            layer_failed += 1
                            failed_updates_count += 1
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
                                    successful_updates_count += 1
                                    feature_updates_out.append({
                                        **feat_row,
                                        "Update Result": "UPDATED",
                                        "Update Message": "Successfully updated value",
                                    })
                                else:
                                    layer_failed += 1
                                    failed_updates_count += 1
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
                                failed_updates_count += 1
                                feature_updates_out.append({
                                    **feat_row,
                                    "Update Result": "UPDATE_FAILED",
                                    "Update Message": str(edit_err),
                                })
            else:
                for row in res_updates:
                    layer_skipped += 1
                    skipped_updates_count += 1
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
                "Target Fields": config.fields,
                "Editing Supported": "Yes" if editing_supported else "No",
                "Active User Can Edit": "Yes" if editing_supported else "No",
                "Edit Permission Status": "EDITABLE" if editing_supported else "READ_ONLY",
                "Edit Permission Reason": "Layer supports editing." if editing_supported else "Layer is read-only.",
                "Total Records": len(res_updates),
                "Matching Records": len(res_updates),
                "Eligible Update Records": len(res_updates),
                "Ambiguous Matches": 0,
                "Query Status": "SUCCESS",
                "Query Message": f"Updated: {layer_successful}, Failed: {layer_failed}, Skipped: {layer_skipped}",
            })

    run_completed = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    run_info = {
        "Operation Mode": "Update",
        "Interface": "CLI / ArcPy Tool",
        "Connection Method": "Explicit Portal" if config.portal_url else "Saved Profile",
        "Profile Name": config.profile or "",
        "Portal URL": portal_url,
        "Authenticated Username": authenticated_user,
        "Crosswalk Path": str(config.csv_path),
        "Output Workbook": str(config.output_path),
        "Feature Service Items Inspected": items_inspected,
        "Candidate Layers Found": len(updates_by_resource),
        "Candidate Layer Limit": "None",
        "Candidate Limit Reached": "No",
        "Discovery Item Limit": "None",
        "Discovery Item Limit Reached": "No",
        "Discovery Page Size": config.discovery_page_size,
        "Broken Resources Found": len(broken_resources),
        "Discovery Complete": "Yes",
        "Discovery Stop Reason": "UPDATE_REVIEWED_WORKBOOK",
        "Query Batch Size": config.query_batch_size,
        "Edit Batch Size": config.edit_batch_size,
        "Run Started": run_started,
        "Run Completed": run_completed,
        "Run Status": "SUCCESS",
    }

    result = UpdateResult(
        feature_updates=feature_updates_out,
        layer_summary=layer_summary_out,
        broken_resources=broken_resources,
        run_info=run_info,
        output_path=config.output_path,
    )

    write_update_workbook(result, config)
    log_info(f"Update complete. Output written to {config.output_path}")
    return result

