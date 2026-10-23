import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple
from .models import ParkNameReplaceConfig, ScanResult
from .crosswalk import load_crosswalk, build_name_mapping
from .discovery import find_candidate_items, get_layers_and_tables, find_field, sort_candidates, apply_candidate_limit
from .reporting import write_scan_workbook

logger = logging.getLogger("park_name_replace.scanning")

def run_scan(config: ParkNameReplaceConfig, message_handler=None) -> ScanResult:
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
    log_info("Starting Park Name Replace Scan workflow...")

    df_cw = load_crosswalk(config.csv_path)
    mapping, ambiguous_map = build_name_mapping(df_cw)
    log_info(f"Loaded crosswalk with {len(df_cw)} rows. Unique previous names: {len(mapping)}. Ambiguous mappings: {len(ambiguous_map)}")

    from .connection import connect_gis
    gis = connect_gis(config)
    portal_url = gis.url.rstrip("/")
    authenticated_user = gis.properties.user.username if hasattr(gis.properties, "user") and gis.properties.user else "Anonymous"

    items = find_candidate_items(gis, config)
    log_info(f"Discovered {len(items)} Feature Service items.")

    discovered_candidates = []
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

            editing_supported = False
            try:
                caps = getattr(res, "properties", {}).get("capabilities", "")
                if "Update" in caps or "Editing" in caps:
                    editing_supported = True
            except Exception:
                editing_supported = True

            discovered_candidates.append({
                "portal_url": portal_url,
                "item_title": item_title,
                "item_id": item_id,
                "item_owner": item_owner,
                "service_url": service_url,
                "resource_type": res_type,
                "layer_name": getattr(res, "properties", {}).get("name", "Layer"),
                "layer_id": layer_id,
                "layer_url": res_url,
                "resource": res,
                "park_name_field": actual_name_field,
                "park_code_field_present": has_park_code,
                "actual_code_field": actual_code_field,
                "editing_supported": editing_supported,
            })

    sorted_candidates = sort_candidates(discovered_candidates)
    log_info(f"Qualifying candidates containing '{config.park_name_field}': {len(sorted_candidates)}")

    selected_candidates, excluded_candidates = apply_candidate_limit(sorted_candidates, config.candidate_layer_limit)
    log_info(f"Selected candidates for scan: {len(selected_candidates)} (Excluded by limit: {len(excluded_candidates)})")

    feature_updates_rows = []
    layer_summary_rows = []

    total_candidates = len(selected_candidates)
    for idx, cand in enumerate(selected_candidates, start=1):
        seq = idx
        title = cand["item_title"]
        rtype = cand["resource_type"]
        lname = cand["layer_name"]
        log_info(f"Scanning {seq} of {total_candidates}: {title} / {lname} ({rtype})")

        res = cand["resource"]
        name_field = cand["park_name_field"]
        code_field = cand["actual_code_field"]
        has_code = cand["park_code_field_present"]

        total_records = 0
        matching_records = 0
        eligible_records = 0
        code_mismatches = 0
        ambiguous_matches = 0
        query_status = "SUCCESS"
        query_message = "Completed successfully"

        try:
            prev_names = list(mapping.keys())
            if prev_names:
                batch_size = config.query_batch_size
                for i in range(0, len(prev_names), batch_size):
                    batch_names = prev_names[i:i+batch_size]
                    escaped_names = [n.replace("'", "''") for n in batch_names]
                    where_clause = f"{name_field} IN ('" + "', '".join(escaped_names) + "')"

                    fields_to_query = ["OBJECTID", name_field]
                    if has_code and code_field:
                        fields_to_query.append(code_field)
                    if hasattr(res, "properties") and "globalIdField" in res.properties and res.properties["globalIdField"]:
                        gid_field = res.properties["globalIdField"]
                        if gid_field not in fields_to_query:
                            fields_to_query.append(gid_field)

                    features = res.query(where=where_clause, out_fields=",".join(fields_to_query), return_geometry=False).features
                    for feat in features:
                        attrs = feat.attributes
                        oid = attrs.get("OBJECTID") or attrs.get("objectid")
                        current_name = str(attrs.get(name_field, "")).strip()
                        current_code = str(attrs.get(code_field, "")).strip() if has_code and code_field else ""

                        if current_name in mapping:
                            matching_records += 1
                            target_info = mapping[current_name]
                            corrected_name = target_info["new_name"]
                            expected_code = target_info["park_code"]

                            match_status = "CONFIRMED"
                            eligible = True

                            if current_name in ambiguous_map:
                                match_status = "AMBIGUOUS_CROSSWALK"
                                eligible = False
                                ambiguous_matches += 1
                            elif has_code and code_field:
                                if not current_code or current_code.lower() == "none" or current_code == "":
                                    match_status = "PARK_CODE_MISSING"
                                    eligible = False
                                elif current_code != expected_code:
                                    match_status = "PARK_CODE_MISMATCH"
                                    eligible = False
                                    code_mismatches += 1

                            if eligible:
                                eligible_records += 1

                            feature_updates_rows.append({
                                "Portal URL": portal_url,
                                "Connection Method": config.profile if not config.portal_url else "Explicit Portal",
                                "Profile Name": config.profile or "",
                                "Item Title": title,
                                "Item ID": cand["item_id"],
                                "Item Owner": cand["item_owner"],
                                "Service URL": cand["service_url"],
                                "Resource Type": rtype,
                                "Layer/Table Name": lname,
                                "Layer/Table ID": cand["layer_id"],
                                "Layer/Table URL": cand["layer_url"],
                                "Candidate Sequence": seq,
                                "ObjectID": oid,
                                "GlobalID": "",
                                "Feature PARK_CODE": current_code,
                                "Crosswalk PARK_CODE": expected_code,
                                "Current PARK_NAME": current_name,
                                "Corrected PARK_NAME": corrected_name,
                                "Match Status": match_status,
                                "Eligible for Update": "Yes" if eligible else "No",
                                "Update Selected": "No",
                                "Update Result": "",
                                "Update Message": "",
                            })
        except Exception as e:
            log_err(f"Error querying candidate {title} / {lname}: {e}")
            query_status = "ERROR"
            query_message = str(e)
        layer_summary_rows.append({
            "Portal URL": portal_url,
            "Item Title": title,
            "Item ID": cand["item_id"],
            "Item Owner": cand["item_owner"],
            "Resource Type": rtype,
            "Layer/Table Name": lname,
            "Layer/Table ID": cand["layer_id"],
            "Layer/Table URL": cand["layer_url"],
            "Candidate Sequence": seq,
            "PARK_NAME Field": name_field,
            "PARK_CODE Field Present": "Yes" if has_code else "No",
            "Editing Supported": "Yes" if cand["editing_supported"] else "No",
            "Total Records": total_records,
            "Matching Records": matching_records,
            "Eligible Update Records": eligible_records,
            "PARK_CODE Mismatches": code_mismatches,
            "Ambiguous Matches": ambiguous_matches,
            "Query Status": query_status,
            "Query Message": query_message,
        })

    for exc in excluded_candidates:
        layer_summary_rows.append({
            "Portal URL": portal_url,
            "Item Title": exc["item_title"],
            "Item ID": exc["item_id"],
            "Item Owner": exc["item_owner"],
            "Resource Type": exc["resource_type"],
            "Layer/Table Name": exc["layer_name"],
            "Layer/Table ID": exc["layer_id"],
            "Layer/Table URL": exc["layer_url"],
            "Candidate Sequence": "",
            "PARK_NAME Field": exc["park_name_field"],
            "PARK_CODE Field Present": "Yes" if exc["park_code_field_present"] else "No",
            "Editing Supported": "Yes" if exc["editing_supported"] else "No",
            "Total Records": 0,
            "Matching Records": 0,
            "Eligible Update Records": 0,
            "PARK_CODE Mismatches": 0,
            "Ambiguous Matches": 0,
            "Query Status": "EXCLUDED_BY_LIMIT",
            "Query Message": "Excluded by candidate layer limit",
        })

    run_completed = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    run_info = {
        "Operation Mode": "Scan",
        "Interface": "CLI / ArcPy Tool",
        "Connection Method": "Explicit Portal" if config.portal_url else "Saved Profile",
        "Profile Name": config.profile or "",
        "Portal URL": portal_url,
        "Authenticated User": authenticated_user,
        "Crosswalk Path": str(config.csv_path),
        "Output Workbook": str(config.output_path),
        "Candidate Layer Limit": config.candidate_layer_limit if config.candidate_layer_limit is not None else "None",
        "Qualifying Candidates Discovered": len(sorted_candidates),
        "Candidates Selected": len(selected_candidates),
        "Candidate Limit Applied": "Yes" if config.candidate_layer_limit is not None else "No",
        "Candidates Excluded by Limit": len(excluded_candidates),
        "Query Batch Size": config.query_batch_size,
        "Edit Batch Size": config.edit_batch_size,
        "Run Started": run_started,
        "Run Completed": run_completed,
        "Run Status": "SUCCESS",
    }

    result = ScanResult(
        feature_updates=feature_updates_rows,
        layer_summary=layer_summary_rows,
        run_info=run_info,
        output_path=config.output_path,
    )

    write_scan_workbook(result, config)
    log_info(f"Scan complete. Output written to {config.output_path}")
    return result

