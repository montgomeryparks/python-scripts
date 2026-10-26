import concurrent.futures
import logging
from typing import Any

import requests
import urllib3
from arcgis.gis import GIS

from .models import BatchValueReplaceConfig

logger = logging.getLogger("batch_value_replace.discovery")


def classify_error(exc: Exception) -> tuple[str, str, int, str]:
    msg = str(exc)
    msg_lower = msg.lower()
    code = 0

    import re

    code_match = re.search(r"\b(400|401|403|404|499|500|502|503|504)\b", msg)
    if code_match:
        code = int(code_match.group(1))

    if (
        isinstance(
            exc,
            (
                requests.exceptions.ConnectTimeout,
                requests.exceptions.ConnectionError,
                urllib3.exceptions.MaxRetryError,
                TimeoutError,
            ),
        )
        or "timeout" in msg_lower
        or "timed out" in msg_lower
        or "connection" in msg_lower
    ):
        status = "CONNECTION_FAILED"
        err_type = "Connection Error"
        code = code or 503
    elif code == 400 or "invalid url" in msg_lower:
        status = "BROKEN_URL"
        err_type = "Client Error"
        code = code or 400
    elif code == 404 or "not found" in msg_lower or "does not exist" in msg_lower:
        status = "NOT_FOUND"
        err_type = "Not Found"
        code = code or 404
    elif (
        code == 500 or "not started" in msg_lower or "service not started" in msg_lower
    ):
        status = "SERVICE_NOT_STARTED"
        err_type = "Server Error"
        code = code or 500
    elif (
        code == 499
        or "token required" in msg_lower
        or "authentication" in msg_lower
        or "credentials" in msg_lower
    ):
        status = "AUTHENTICATION_REQUIRED"
        err_type = "Auth Error"
        code = code or 499
    elif (
        code == 403
        or "permission denied" in msg_lower
        or "forbidden" in msg_lower
        or "unauthorized" in msg_lower
    ):
        status = "ACCESS_DENIED"
        err_type = "Access Denied"
        code = code or 403
    elif "dns" in msg_lower or "resolve" in msg_lower:
        status = "NAME_RESOLUTION_FAILED"
        err_type = "Network Error"
        code = code or 502
    else:
        status = "SERVICE_ERROR" if code >= 500 else "UNKNOWN_ERROR"
        err_type = "General Error"
        code = code or 500

    clean_msg = msg.split("\n")[0][:150] if msg else "Unknown error"
    return status, err_type, code, clean_msg


def is_user_admin(gis: GIS) -> bool:
    try:
        user = getattr(gis, "users", None) and gis.users.me
        if user:
            role = getattr(user, "role", "") or ""
            role_lower = role.lower()
            if "admin" in role_lower or role_lower in ["org_admin", "administrator"]:
                return True
        props = getattr(gis, "properties", {}) or {}
        uprops = props.get("user", {}) or {}
        urole = str(uprops.get("role", "")).lower()
        if "admin" in urole:
            return True
    except Exception:
        pass
    return False


def is_feature_layer_view(item) -> bool:
    try:
        tks = getattr(item, "typeKeywords", []) or []
        tks_lower = [str(tk).lower() for tk in tks]
        if (
            "view service" in tks_lower
            or "service view" in tks_lower
            or "view" in tks_lower
        ):
            return True
        props = getattr(item, "properties", {}) or {}
        if props.get("isView") is True or props.get("is_view") is True:
            return True
    except Exception:
        pass
    return False


def determine_edit_permission(
    gis: GIS, item: Any, resource: Any
) -> tuple[str, str, str]:
    admin_mode = is_user_admin(gis)
    try:
        caps = getattr(resource, "properties", {}).get("capabilities", "")
        if isinstance(caps, list):
            caps = ",".join(caps)
        caps_lower = str(caps).lower()

        has_standard_edit = (
            "update" in caps_lower or "editing" in caps_lower or "create" in caps_lower
        )
        is_editable_item = getattr(item, "editable", True)

        if has_standard_edit and is_editable_item:
            return "Yes", "EDITABLE", "Resource is configured for editing."

        adv_caps = getattr(resource, "properties", {}).get("advancedEditingCapabilities", {}) or {}
        supports_async_apply = adv_caps.get("supportsAsyncApplyEdits") is True
        supports_apply = getattr(resource, "properties", {}).get("supportsApplyEdits") is True or adv_caps.get("supportsApplyEdits") is True or "applyedits" in caps_lower

        supports_any_edits = has_standard_edit or supports_async_apply or supports_apply

        if supports_any_edits or not is_editable_item:
            if admin_mode:
                return (
                    "Yes",
                    "ADMIN_OVERRIDE",
                    "Administrator privileges override layer read-only configuration via supported apply edits.",
                )
            return (
                "No",
                "ACCESS_DENIED",
                "Layer requires administrator privileges for edit override.",
            )

        if admin_mode and (supports_async_apply or supports_apply):
            return (
                "Yes",
                "ADMIN_OVERRIDE",
                "Administrator privileges override layer read-only configuration via supported apply edits.",
            )

        return (
            "No",
            "READ_ONLY",
            "Layer capabilities do not include update/editing and do not support apply edits.",
        )
    except Exception as e:
        if admin_mode:
            return (
                "Yes",
                "ADMIN_OVERRIDE",
                f"Administrator override despite permission check error: {str(e)[:30]}",
            )
        logger.debug(f"Could not determine edit permission: {e}")
        return "Unknown", "UNKNOWN", f"Could not determine permissions: {str(e)[:50]}"


def iter_feature_services(gis: GIS, config: BatchValueReplaceConfig):
    query_parts = []
    if config.owner:
        query_parts.append(f"owner:{config.owner}")
    if config.item_id:
        query_parts.append(f"id:{config.item_id}")

    query_str = " AND ".join(query_parts) if query_parts else ""
    max_items = (
        config.discovery_item_limit if config.discovery_item_limit is not None else 1000
    )

    try:
        logger.info(
            f"Searching portal with query='{query_str}', item_type='Feature Service', max_items={max_items}, sort_field='title', sort_order='asc'"
        )
        items = gis.content.search(
            query=query_str,
            item_type="Feature Service",
            max_items=max_items,
            sort_field="title",
            sort_order="asc",
        )
        if items:
            for item in items:
                yield item
    except Exception as e:
        logger.warning(f"Error searching portal items: {e}")


def get_layers_and_tables(item) -> list[tuple[Any, str, int]]:
    resources = []
    try:
        layers = getattr(item, "layers", []) or []
        for l in layers:
            lid = getattr(l, "properties", {}).get("id", 0)
            resources.append((l, "Feature Layer", lid))
    except Exception as e:
        logger.debug(f"Could not get layers: {e}")

    try:
        tables = getattr(item, "tables", []) or []
        for t in tables:
            tid = getattr(t, "properties", {}).get("id", 0)
            resources.append((t, "Table", tid))
    except Exception as e:
        logger.debug(f"Could not get tables: {e}")

    return resources


def find_field(resource, requested_name: str) -> tuple[bool, str]:
    try:
        fields = getattr(resource, "properties", {}).get("fields", [])
        for f in fields:
            fname = f.get("name", "")
            if fname.lower() == requested_name.lower():
                return True, fname
    except Exception as e:
        logger.debug(f"Error inspecting fields: {e}")
    return False, requested_name


def discover_candidates_and_broken(
    gis: GIS, config: BatchValueReplaceConfig = None, message_handler=None
):
    candidates = []
    non_candidates = []
    broken_resources = []
    stop_reason = "PORTAL_EXHAUSTED"

    portal_url = gis.url.rstrip("/")
    target_fields = [f.strip() for f in config.fields.split(",") if f.strip()]

    def log_warn(msg):
        logger.warning(msg)
        if message_handler and hasattr(message_handler, "warning"):
            message_handler.warning(msg)

    items = list(iter_feature_services(gis, config))
    if (
        config.discovery_item_limit is not None
        and len(items) > config.discovery_item_limit
    ):
        items = items[: config.discovery_item_limit]
        stop_reason = "DISCOVERY_ITEM_LIMIT_REACHED"

    items_inspected = len(items)

    def process_item(seq, item):
        item_title = getattr(item, "title", "Untitled")
        item_id = getattr(item, "id", "")
        item_owner = getattr(item, "owner", "")
        service_url = getattr(item, "url", "")
        item_cands = []
        item_non_cands = []
        item_broken = []

        if is_feature_layer_view(item):
            return item_cands, item_non_cands, item_broken

        if config and config.exclude_tags:
            ex_tags = [t.strip().lower() for t in config.exclude_tags.split(",") if t.strip()]
            item_tags = [str(t).lower() for t in getattr(item, "tags", []) or []]
            if any(et in item_tags for et in ex_tags):
                return item_cands, item_non_cands, item_broken

        try:
            resources = get_layers_and_tables(item)
        except Exception as exc:
            status, err_type, code, clean_msg = classify_error(exc)
            log_warn(
                f"Item '{item_title}' ({item_id}) unavailable: HTTP {code} ({status}) - {clean_msg}"
            )
            item_broken.append(
                {
                    "Portal URL": portal_url,
                    "Item Title": item_title,
                    "Item ID": item_id,
                    "Item Owner": item_owner,
                    "Service URL": service_url,
                    "Resource Type": "Feature Service",
                    "Layer/Table Name": "",
                    "Layer/Table ID": "",
                    "Discovery Sequence": seq,
                    "HTTP/Error Code": code,
                    "Status": status,
                    "Error Type": err_type,
                    "Error Message": clean_msg,
                }
            )
            return item_cands, item_non_cands, item_broken

        for res, res_type, layer_id in resources:
            res_props = getattr(res, "properties", {}) or {}
            if res_props.get("isView") is True or res_props.get("is_view") is True:
                continue

            res_url = f"{service_url}/{layer_id}" if service_url else ""
            layer_name = "Layer"
            try:
                layer_name = getattr(res, "properties", {}).get("name", "Layer")

                actual_matched_fields = []
                for tf in target_fields:
                    found, actual_f = find_field(res, tf)
                    if found:
                        actual_matched_fields.append(actual_f)

                can_edit, perm_status, perm_reason = determine_edit_permission(
                    gis, item, res
                )
                editing_supported = (
                    "Yes"
                    if "Yes" in can_edit
                    or "update"
                    in str(
                        getattr(res, "properties", {}).get("capabilities", "")
                    ).lower()
                    else "No"
                )

                resource_info = {
                    "portal_url": portal_url,
                    "item_title": item_title,
                    "item_id": item_id,
                    "item_owner": item_owner,
                    "service_url": service_url,
                    "resource_type": res_type,
                    "layer_name": layer_name,
                    "layer_id": layer_id,
                    "layer_url": res_url,
                    "resource": res,
                    "target_fields": actual_matched_fields,
                    "editing_supported": editing_supported,
                    "active_user_can_edit": can_edit,
                    "edit_permission_status": perm_status,
                    "edit_permission_reason": perm_reason,
                }

                if actual_matched_fields:
                    item_cands.append(resource_info)
                else:
                    item_non_cands.append(resource_info)

            except Exception as res_exc:
                status, err_type, code, clean_msg = classify_error(res_exc)
                log_warn(
                    f"Resource '{item_title} / {layer_name}' error: HTTP {code} ({status}) - {clean_msg}"
                )
                item_broken.append(
                    {
                        "Portal URL": portal_url,
                        "Item Title": item_title,
                        "Item ID": item_id,
                        "Item Owner": item_owner,
                        "Service URL": service_url,
                        "Resource Type": res_type,
                        "Layer/Table Name": layer_name,
                        "Layer/Table ID": layer_id,
                        "Discovery Sequence": seq,
                        "HTTP/Error Code": code,
                        "Status": status,
                        "Error Type": err_type,
                        "Error Message": clean_msg,
                    }
                )

        return item_cands, item_non_cands, item_broken

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [
            executor.submit(process_item, idx, item)
            for idx, item in enumerate(items, start=1)
        ]
        for future in concurrent.futures.as_completed(futures):
            try:
                cands, non_cands, broken = future.result()
                for c in cands:
                    if (
                        config.candidate_layer_limit is not None
                        and len(candidates) >= config.candidate_layer_limit
                    ):
                        stop_reason = "CANDIDATE_LAYER_LIMIT_REACHED"
                        non_candidates.append(c)
                    else:
                        candidates.append(c)
                non_candidates.extend(non_cands)
                broken_resources.extend(broken)
            except Exception as e:
                logger.debug(f"Error in item discovery worker: {e}")

    discovery_complete = "Yes" if stop_reason == "PORTAL_EXHAUSTED" else "No"
    return (
        candidates,
        non_candidates,
        broken_resources,
        items_inspected,
        discovery_complete,
        stop_reason,
    )
