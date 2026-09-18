import re

import arcpy


def get_referenced_fields(query_str, layer_fields):
    if "*" in query_str:
        return [f.name for f in layer_fields if f.type not in ("Raster", "Blob")]

    referenced = []
    for f in layer_fields:
        pattern = r"\b" + re.escape(f.name) + r"\b"
        if re.search(pattern, query_str, re.IGNORECASE):
            referenced.append(f.name)

    return referenced if referenced else [f.name for f in layer_fields[:1]]


def extract_where_clauses(sql_query, layer_field_map):
    pushdown_clauses = {tbl: [] for tbl in layer_field_map}
    where_match = re.search(
        r"\bWHERE\b(.*?)(?=\bGROUP\b|\bORDER\b|\bHAVING\b|\bLIMIT\b|$)",
        sql_query,
        re.IGNORECASE | re.DOTALL,
    )

    if not where_match:
        return {tbl: "" for tbl in layer_field_map}

    raw_where = where_match.group(1).strip()
    conditions = re.split(r"\s+\bAND\b\s+", raw_where, flags=re.IGNORECASE)

    for cond in conditions:
        cond_clean = cond.strip()
        if re.search(r"\b(ST_\w+|regexp_matches)\b", cond_clean, re.IGNORECASE):
            continue

        matched_tables = set()
        for tbl_name, fields in layer_field_map.items():
            prefix_pattern = rf"\b{re.escape(tbl_name)}\."
            if re.search(prefix_pattern, cond_clean, re.IGNORECASE):
                matched_tables.add(tbl_name)
                continue

            for f in fields:
                field_pattern = rf"\b{re.escape(f.name)}\b"
                if re.search(field_pattern, cond_clean, re.IGNORECASE):
                    matched_tables.add(tbl_name)
                    break

        if len(matched_tables) == 1:
            target_tbl = list(matched_tables)[0]
            cursor_cond = re.sub(
                rf"\b{re.escape(target_tbl)}\.", "", cond_clean, flags=re.IGNORECASE
            )
            pushdown_clauses[target_tbl].append(f"({cursor_cond})")

    return {
        tbl: " AND ".join(clauses) if clauses else ""
        for tbl, clauses in pushdown_clauses.items()
    }


def sanitize_where_clause(target_layer, where_clause):
    if not where_clause:
        return ""
    sanitized = where_clause
    for f in arcpy.ListFields(target_layer):
        delimited_name = arcpy.AddFieldDelimiters(target_layer, f.name)
        pattern = rf"(?<![\"\'\w])\b{re.escape(f.name)}\b(?![\"\'\w])"
        sanitized = re.sub(pattern, delimited_name, sanitized)
    return sanitized
