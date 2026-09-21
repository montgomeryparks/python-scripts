import logging
import re

import arcpy

logger = logging.getLogger("champion_tools")


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
    logger.info("--- Starting Spatial Predicate Pushdown Analysis ---")
    pushdown_clauses = {tbl: [] for tbl in layer_field_map}

    ci_layer_map = {k.lower(): k for k in layer_field_map.keys()}
    logger.info(f"Available Map Layers: {list(ci_layer_map.keys())}")

    alias_map = {}
    pattern = r"\b(?:FROM|JOIN)\s+([a-zA-Z0-9_]+)(?:\s+AS\s+|\s+)?([a-zA-Z0-9_]+)?"
    for match in re.finditer(pattern, sql_query, re.IGNORECASE):
        tbl = match.group(1)
        alias = match.group(2)

        actual_tbl = ci_layer_map.get(tbl.lower())
        if actual_tbl:
            alias_map[actual_tbl.lower()] = actual_tbl
            if alias and alias.upper() not in (
                "ON",
                "WHERE",
                "LEFT",
                "RIGHT",
                "INNER",
                "OUTER",
                "FULL",
                "JOIN",
                "GROUP",
                "ORDER",
                "HAVING",
                "LIMIT",
            ):
                alias_map[alias.lower()] = actual_tbl

    logger.info(f"SQL Alias Map Resolved: {alias_map}")

    where_match = re.search(
        r"\bWHERE\b(.*?)(?=\bGROUP\b|\bORDER\b|\bHAVING\b|\bLIMIT\b|$)",
        sql_query,
        re.IGNORECASE | re.DOTALL,
    )

    if not where_match:
        logger.info("No WHERE clause detected in SQL.")
        return pushdown_clauses

    raw_where = where_match.group(1).strip()
    logger.info(f"Extracted WHERE Block: {raw_where}")

    conditions = re.split(r"\s+\bAND\b\s+", raw_where, flags=re.IGNORECASE)

    for cond in conditions:
        cond_clean = cond.strip()
        cond_no_strings = re.sub(r"'[^']*'", "''", cond_clean)

        if re.search(r"\b(ST_\w+|regexp_matches)\b", cond_no_strings, re.IGNORECASE):
            logger.info(
                f"Skipping condition (contains spatial/DuckDB function): {cond_clean}"
            )
            continue

        matched_tables = set()

        prefix_matches = re.findall(
            r"\b([a-zA-Z0-9_]+)\.[a-zA-Z0-9_]+\b", cond_no_strings
        )
        logger.info(
            f"Evaluating Condition: '{cond_clean}' | Extracted Prefixes: {prefix_matches}"
        )

        for prefix in prefix_matches:
            prefix_lower = prefix.lower()
            if prefix_lower in alias_map:
                matched_tables.add(alias_map[prefix_lower])

        if not matched_tables:
            cond_stripped = re.sub(r"\b[a-zA-Z0-9_]+\.", "", cond_no_strings)
            for tbl_name, fields in layer_field_map.items():
                for f in fields:
                    if re.search(
                        rf"\b{re.escape(f.name)}\b", cond_stripped, re.IGNORECASE
                    ):
                        matched_tables.add(tbl_name)
                        break

        if len(matched_tables) == 1:
            target_tbl = list(matched_tables)[0]
            cursor_cond = re.sub(r"\b[a-zA-Z_][a-zA-Z0-9_]*\.", "", cond_clean)
            pushdown_clauses[target_tbl].append(f"({cursor_cond})")
            logger.info(f"SUCCESS: Pushed down '{cursor_cond}' to table '{target_tbl}'")
        else:
            logger.info(
                f"FAILED: Condition mapped to {len(matched_tables)} tables -> {matched_tables}"
            )

    final_clauses = {
        tbl: " AND ".join(clauses) if clauses else ""
        for tbl, clauses in pushdown_clauses.items()
    }
    logger.info(f"Final Pushdown Filters: {final_clauses}")
    logger.info("--------------------------------------------------")
    return final_clauses


def sanitize_where_clause(target_layer, where_clause):
    if not where_clause:
        return ""
    sanitized = where_clause
    for f in arcpy.ListFields(target_layer):
        delimited_name = arcpy.AddFieldDelimiters(target_layer, f.name)
        pattern = rf"(?<![\"\'\w])\b{re.escape(f.name)}\b(?![\"\'\w])"
        sanitized = re.sub(pattern, delimited_name, sanitized)
    return sanitized
