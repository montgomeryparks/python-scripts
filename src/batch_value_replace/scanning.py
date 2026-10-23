import logging
import pandas as pd
import concurrent.futures
from datetime import datetime
from pathlib import Path
from .models import BatchValueReplaceConfig, ScanResult
from .crosswalk import load_crosswalk, build_name_mapping
from .discovery import discover_candidates_and_broken, classify_error
from .reporting import write_scan_workbook

logger = logging.getLogger("batch_value_replace.scanning")

def run_scan(config: BatchValueReplaceConfig, message_handler=None) -> ScanResult:
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
    log_info("Starting Batch Value Replace Scan workflow...")

    df_cw = load_crosswalk(config.csv_path)
    mapping, ambiguous_map = build_name_mapping(df_cw)

    from .connection import connect_gis
    gis = connect_gis(config)
    portal_url = gis.url.rstrip("/")
    authenticated_user = gis.properties.user.username if hasattr(gis.properties, "user") and gis.properties.user else "Anonymous"

    candidates, non_candidates, broken_resources, items_inspected, discovery_complete, stop_reason = discover_candidates_and_broken(
        gis, config, message_handler
    )
    log_info(f"Discovery complete. Items inspected: {items_inspected}, Candidates found: {len(candidates)}, Broken: {len(broken_resources)}")

    feature_updates_rows = []
    layer_summary_rows = []

    def process_single_candidate(seq, cand):
        title, rtype, lname = cand["item_title"], cand["resource_type"], cand["layer_name"]
        res, target_fields, can_edit = cand["resource"], cand["target_fields"], cand["active_user_can_edit"]
        total_records, matching_records, eligible_records, ambiguous_matches = 0, 0, 0, 0
        query_status, query_message = "SUCCESS", "Completed successfully"
        local_updates = []

        try:
            total_count_res = res.query(where="1=1", return_count_only=True)
            if isinstance(total_count_res, int):
                total_records = total_count_res
            elif isinstance(total_count_res, dict) and "count" in total_count_res:
                total_records = total_count_res["count"]
        except Exception:
            total_records = 0

        try:
            old_vals = list(mapping.keys())
            if old_vals:
                for target_field in target_fields:
                    for i in range(0, len(old_vals), config.query_batch_size):
                        batch_names = old_vals[i:i+config.query_batch_size]
                        where_clause = f"{target_field} IN ('" + "', '".join([n.replace("'", "''") for n in batch_names]) + "')"
                        try:
                            features = res.query(where=where_clause, out_fields="*", return_geometry=False).features
                            for feat in features:
                                oid = feat.attributes.get("OBJECTID") or feat.attributes.get("objectid")
                                current_val = str(feat.attributes.get(target_field, "")).strip()

                                if current_val in mapping:
                                    matching_records += 1
                                    corrected_val = mapping[current_val]
                                    match_status, eligible, update_reason = "CONFIRMED", True, ""

                                    if current_val in ambiguous_map:
                                        match_status, eligible, ambiguous_matches = "AMBIGUOUS_CROSSWALK", False, ambiguous_matches + 1
                                    if eligible and can_edit != "Yes":
                                        eligible, update_reason = False, "USER_CANNOT_EDIT"
                                    if eligible:
                                        eligible_records += 1

                                    local_updates.append({
                                        "Portal URL": portal_url, "Connection Method": config.profile if not config.portal_url else "Explicit Portal",
                                        "Profile Name": config.profile or "", "Item Title": title, "Item ID": cand["item_id"],
                                        "Item Owner": cand["item_owner"], "Service URL": cand["service_url"], "Resource Type": rtype,
                                        "Layer/Table Name": lname, "Layer/Table ID": cand["layer_id"], "Layer/Table URL": cand["layer_url"],
                                        "Candidate Sequence": seq, "ObjectID": oid, "GlobalID": "", "Target Field Name": target_field,
                                        "Current Value": current_val, "Corrected Value": corrected_val, "Match Status": match_status,
                                        "Eligible for Update": "Yes" if eligible else "No", "Update Selected": "No", "Update Result": "", "Update Message": update_reason,
                                    })
                        except Exception as q_exc:
                            _, _, code, clean_msg = classify_error(q_exc)
                            log_warn(f"Query batch failed for {title} / {lname} [{target_field}]: HTTP {code} - {clean_msg}")
                            query_status, query_message = "WARNING", clean_msg
        except Exception as e:
            _, _, code, clean_msg = classify_error(e)
            log_err(f"Error querying candidate {title} / {lname}: HTTP {code} - {clean_msg}")
            query_status, query_message = "ERROR", clean_msg

        summary_row = {
            "Portal URL": portal_url,
            "Item Title": title,
            "Item ID": cand["item_id"],
            "Item Owner": cand["item_owner"],
            "Resource Type": rtype,
            "Layer/Table Name": lname,
            "Layer/Table ID": cand["layer_id"],
            "Layer/Table URL": cand["layer_url"],
            "Candidate Sequence": seq,
            "Target Fields": ", ".join(target_fields),
            "Editing Supported": cand["editing_supported"],
            "Active User Can Edit": cand["active_user_can_edit"],
            "Edit Permission Status": cand["edit_permission_status"],
            "Edit Permission Reason": cand["edit_permission_reason"],
            "Total Records": total_records,
            "Matching Records": matching_records,
            "Eligible Update Records": eligible_records,
            "Ambiguous Matches": ambiguous_matches,
            "Query Status": query_status,
            "Query Message": query_message,
        }
        return local_updates, summary_row

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(process_single_candidate, seq, cand) for seq, cand in enumerate(candidates, start=1)]
        for future in concurrent.futures.as_completed(futures):
            try:
                updates, summary = future.result()
                feature_updates_rows.extend(updates)
                layer_summary_rows.append(summary)
            except Exception as e:
                logger.debug(f"Error in candidate scanning worker: {e}")

    for nc in non_candidates:
        layer_summary_rows.append({
            "Portal URL": portal_url,
            "Item Title": nc["item_title"],
            "Item ID": nc["item_id"],
            "Item Owner": nc["item_owner"],
            "Resource Type": nc["resource_type"],
            "Layer/Table Name": nc["layer_name"],
            "Layer/Table ID": nc["layer_id"],
            "Layer/Table URL": nc["layer_url"],
            "Candidate Sequence": "",
            "Target Fields": "",
            "Editing Supported": nc["editing_supported"],
            "Active User Can Edit": nc["active_user_can_edit"],
            "Edit Permission Status": nc["edit_permission_status"],
            "Edit Permission Reason": nc["edit_permission_reason"],
            "Total Records": -1,
            "Matching Records": 0,
            "Eligible Update Records": 0,
            "Ambiguous Matches": 0,
            "Query Status": "NO_TARGET_FIELDS",
            "Query Message": "Target fields not found in resource",
        })

    run_completed = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    candidate_limit_reached = "Yes" if config.candidate_layer_limit is not None and len(candidates) >= config.candidate_layer_limit else "No"
    discovery_item_limit_reached = "Yes" if config.discovery_item_limit is not None and items_inspected >= config.discovery_item_limit else "No"

    run_info = {
        "Operation Mode": "Scan",
        "Interface": "CLI / ArcPy Tool",
        "Connection Method": "Explicit Portal" if config.portal_url else "Saved Profile",
        "Profile Name": config.profile or "",
        "Portal URL": portal_url,
        "Authenticated Username": authenticated_user,
        "Crosswalk Path": str(config.csv_path),
        "Output Workbook": str(config.output_path),
        "Feature Service Items Inspected": items_inspected,
        "Candidate Layers Found": len(candidates),
        "Candidate Layer Limit": config.candidate_layer_limit if config.candidate_layer_limit is not None else "None",
        "Candidate Limit Reached": candidate_limit_reached,
        "Discovery Item Limit": config.discovery_item_limit if config.discovery_item_limit is not None else "None",
        "Discovery Item Limit Reached": discovery_item_limit_reached,
        "Discovery Page Size": config.discovery_page_size,
        "Broken Resources Found": len(broken_resources),
        "Discovery Complete": discovery_complete,
        "Discovery Stop Reason": stop_reason,
        "Query Batch Size": config.query_batch_size,
        "Edit Batch Size": config.edit_batch_size,
        "Run Started": run_started,
        "Run Completed": run_completed,
        "Run Status": "SUCCESS",
    }

    result = ScanResult(
        feature_updates=feature_updates_rows,
        layer_summary=layer_summary_rows,
        broken_resources=broken_resources,
        run_info=run_info,
        output_path=config.output_path,
    )

    write_scan_workbook(result, config)
    log_info(f"Scan complete. Output written to {config.output_path}")
    return result
