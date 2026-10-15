import json
import logging
import math
import re
from typing import Any, Literal

import arcpy
import duckdb
import pandas as pd
import requests
import urllib3
from shapely.geometry import shape

from src.utils.auth import get_portal_token

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = logging.getLogger("biomon_tools")


def featureservice_to_df(
    url: str,
    out_fields: list[str] | str = "*",
    where: str = "1=1",
    out_sr: int = 2248,
    token: str = "",
) -> pd.DataFrame:
    base_url = url.rstrip("/")
    query_url = f"{base_url}/query"
    base_params: dict[str, Any] = {"f": "json"}
    if token:
        base_params["token"] = token

    access_resp = requests.get(base_url, params=base_params, verify=False)
    access_resp.raise_for_status()
    access_data = access_resp.json()
    if "error" in access_data:
        err = access_data["error"]
        if err.get("code") == 499:
            if not token:
                token = get_portal_token()
            if token:
                base_params["token"] = token
                arcpy.AddMessage(
                    "Service is private; token will be included in requests."
                )
    elif not token:
        arcpy.AddMessage("Service is public; no token needed.")

    meta_resp = requests.get(base_url, params=base_params, verify=False)
    meta_resp.raise_for_status()
    meta_data = meta_resp.json()
    if "error" in meta_data:
        raise ValueError(
            f"ArcGIS Error getting metadata for {base_url}: {meta_data['error'].get('message', meta_data['error'])}"
        )
    max_record_count = meta_data.get("maxRecordCount", 1000)

    count_params = {**base_params, "where": where, "returnCountOnly": "true"}
    count_resp = requests.get(query_url, params=count_params, verify=False)
    count_resp.raise_for_status()
    count_data = count_resp.json()
    if "error" in count_data:
        raise ValueError(
            f"ArcGIS Error getting count for {base_url}: {count_data['error'].get('message', count_data['error'])}"
        )
    total_records = count_data.get("count", 0)

    arcpy.AddMessage(
        f"Total records to fetch: {total_records} (Page size: {max_record_count})"
    )

    all_features = []
    offset = 0

    while offset < total_records:
        fetch_params = {
            "where": where,
            "outFields": (
                ",".join(out_fields) if isinstance(out_fields, list) else out_fields
            ),
            "outSR": out_sr,
            "f": "geojson",
            "resultOffset": offset,
            "resultRecordCount": max_record_count,
        }
        if token:
            fetch_params["token"] = token

        response = requests.get(query_url, params=fetch_params, verify=False)
        response.raise_for_status()
        data = response.json()

        if "error" in data:
            err_msg = data["error"].get("message") or str(data["error"])
            raise ValueError(f"ArcGIS Error querying {query_url}: {err_msg}")

        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)
        offset += max_record_count

    if not all_features:
        return pd.DataFrame()

    rows = []
    for feat in all_features:
        props = feat.get("properties", {})
        geom_dict = feat.get("geometry", None)
        if geom_dict:
            try:
                props["GEOMWKB"] = shape(geom_dict).wkb
            except ValueError as err:
                raise ValueError(f"Geometry failed to read: {geom_dict}") from err
        else:
            props["GEOMWKB"] = None
        rows.append(props)

    return pd.DataFrame(rows)


def df_to_esri_features(
    df: pd.DataFrame,
    has_geometry: bool = True,
    is_update: bool = False,
    id_field: str = "GlobalID",
    wkid: int = 2248,
) -> list[dict[str, Any]]:
    """Converts a DataFrame into Esri JSON feature dictionaries for applyEdits."""
    drop_cols = {"GEOM", "geom", "GEOMWKB", "row_num", "x", "y"}
    features = []

    for _, row in df.iterrows():
        attributes = {}
        for col, val in row.items():
            if not is_update and col.lower() in {id_field.lower(), "globalid"}:
                continue
            if col in drop_cols:
                continue

            if (
                pd.isna(val)
                or val is None
                or (isinstance(val, float) and math.isnan(val))
            ):
                attributes[col] = None
            elif isinstance(val, pd.Timestamp) or type(val).__name__ in (
                "date",
                "datetime",
                "Timestamp",
            ):
                try:
                    ts = pd.Timestamp(val)
                    attributes[col] = int(ts.timestamp() * 1000)
                except (ValueError, TypeError, OverflowError):
                    attributes[col] = str(val)
            else:
                if hasattr(val, "item"):
                    val = val.item()
                attributes[col] = val

        feat: dict[str, Any] = {"attributes": attributes}

        if has_geometry and not is_update and "x" in row and "y" in row:
            x_val, y_val = row["x"], row["y"]
            if not pd.isna(x_val) and not pd.isna(y_val):
                feat["geometry"] = {
                    "x": float(x_val) if not hasattr(x_val, "item") else x_val.item(),
                    "y": float(y_val) if not hasattr(y_val, "item") else y_val.item(),
                    "spatialReference": {"wkid": wkid},
                }

        features.append(feat)

    return features


def run_apply_edits(
    url: str,
    layer_id: int,
    features: list[dict[str, Any]],
    token: str = "",
    is_update: bool = False,
    chunk_size: int = 500,
) -> list[dict[str, Any]]:
    """Pushes feature additions or updates to an ArcGIS Feature Service via applyEdits."""
    if not features:
        return []
    edits_url = f"{url.rstrip('/')}/{layer_id}/applyEdits"
    all_results = []

    for i in range(0, len(features), chunk_size):
        chunk = features[i : i + chunk_size]
        data = {
            "f": "json",
            "rollbackOnFailure": "true",
            "useGlobalIds": "true" if is_update else "false",
        }
        if token:
            data["token"] = token
        if is_update:
            data["updates"] = json.dumps(chunk)
            action_name = "Update"
        else:
            data["adds"] = json.dumps(chunk)
            action_name = "Add"

        resp = requests.post(edits_url, data=data, verify=False)
        resp.raise_for_status()
        res_json = resp.json()

        if "error" in res_json:
            raise RuntimeError(
                f"ArcGIS applyEdits Error (Layer {layer_id}): {res_json['error']}"
            )

        results_key = "updateResults" if is_update else "addResults"
        results = res_json.get(results_key, [])
        failed = sum(1 for r in results if not r.get("success", False))

        if failed > 0:
            errs = [r.get("error") for r in results if not r.get("success", False)]
            raise RuntimeError(
                f"applyEdits {action_name} failed for {failed}/{len(chunk)} features in Layer {layer_id}: {errs}"
            )

        logger.info(
            f"Layer {layer_id} batch {i // chunk_size + 1}: Successfully {action_name.lower()}ed {len(chunk)} features."
        )
        all_results.extend(results)

    return all_results


def _norm(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = df[c].replace("", pd.NA)
    return df


def featureclass_to_df(
    in_table,
    fields=None,
    out_sr=2248,
    oid_field_out="OBJECTID",
    where_clause=None,
    spatial_filter=None,
    spatial_rel="INTERSECTS",
):
    desc = arcpy.Describe(in_table)
    is_spatial = hasattr(desc, "shapeType") and desc.shapeType != ""

    if not fields:
        fields = [
            f.name
            for f in arcpy.ListFields(in_table)
            if f.type not in ("Geometry", "Raster", "Blob")
        ]

    cursor_fields = list(fields)

    if (
        oid_field_out
        and oid_field_out not in cursor_fields
        and hasattr(desc, "OIDFieldName")
    ):
        cursor_fields.insert(0, desc.OIDFieldName)

    if is_spatial and "SHAPE@WKB" not in cursor_fields:
        cursor_fields.append("SHAPE@WKB")

    kwargs = {}
    if where_clause:
        kwargs["where_clause"] = where_clause
    if out_sr and is_spatial:
        kwargs["spatial_reference"] = (
            arcpy.SpatialReference(out_sr) if isinstance(out_sr, int) else out_sr
        )
    if spatial_filter and is_spatial:
        kwargs["spatial_filter"] = spatial_filter
        kwargs["spatial_relationship"] = spatial_rel

    records = []
    with arcpy.da.SearchCursor(in_table, cursor_fields, **kwargs) as cursor:
        for row in cursor:
            records.append(row)

    df = pd.DataFrame(records, columns=cursor_fields)

    if is_spatial and "SHAPE@WKB" in df.columns:
        df["GEOMWKB"] = df["SHAPE@WKB"].apply(lambda x: bytes(x) if x else None)
        df.drop(columns=["SHAPE@WKB"], inplace=True)

    return df


def map_pandas_dtype_to_arcpy(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "LONG"
    elif pd.api.types.is_float_dtype(dtype):
        return "DOUBLE"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATE"
    return "TEXT"


def duckdb_to_memory(
    con: duckdb.DuckDBPyConnection,
    source_query_or_table: str,
    raw_out_name: str,
    spatial_ref: Any = 2248,
    active_map: Any = None,
    oid_mapping: dict[str, bool] | None = None,
    exclude_cols: set[str] | None = None,
) -> str | None:
    """
    Executes a DuckDB query or reads a DuckDB table/view, auto-detects native DuckDB GEOMETRY
    columns and types, and writes the result to an ArcGIS Pro 'memory' Feature Class or Table.
    Returns the 'memory\\<out_name>' path, or None if 0 rows are returned.
    """
    oid_mapping = oid_mapping or {}
    exclude_lower = {c.lower() for c in (exclude_cols or set())}

    out_name = re.sub(r"[^a-zA-Z0-9_]", "_", raw_out_name.strip())
    if re.match(r"^[0-9]", out_name):
        out_name = f"tbl_{out_name}"

    if out_name != raw_out_name:
        logger.info(
            f"Output Dataset Name '{raw_out_name}' was scrubbed to '{out_name}' to meet workspace naming rules."
        )

    is_select = bool(
        re.match(r"^\s*(SELECT|WITH)\b", source_query_or_table, re.IGNORECASE)
    )
    temp_tbl = "_duckdb_to_mem_tmp"
    if is_select:
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE {temp_tbl} AS {source_query_or_table}"
        )
    else:
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE {temp_tbl} AS SELECT * FROM {source_query_or_table}"
        )

    col_info = con.execute(f"PRAGMA table_info('{temp_tbl}')").fetchall()

    select_cols: list[str] = []
    spatial_cols: list[str] = []

    for col in col_info:
        col_name = str(col[1])
        col_type = str(col[2])
        if col_name.lower() in exclude_lower and col_type != "GEOMETRY":
            continue
        if col_type == "GEOMETRY":
            spatial_cols.append(col_name)
            select_cols.append(f'ST_AsWKB("{col_name}") AS "{col_name}"')
        else:
            select_cols.append(f'"{col_name}"')

    if len(spatial_cols) > 1:
        raise ValueError(
            f"Multiple geometry columns returned ({', '.join(spatial_cols)}). ArcGIS only supports one geometry field per dataset."
        )

    if not select_cols:
        return None

    final_query = f"SELECT {', '.join(select_cols)} FROM {temp_tbl}"
    res_df = con.execute(final_query).df()

    if res_df.empty:
        return None

    out_path = f"memory\\{out_name}"

    if active_map is not None:
        for map_lyr in active_map.listLayers():
            if map_lyr.name == out_name:
                active_map.removeLayer(map_lyr)
        for map_tbl in active_map.listTables():
            if map_tbl.name == out_name:
                active_map.removeTable(map_tbl)

    if arcpy.Exists(out_path):
        arcpy.management.Delete(out_path)

    res_df.columns = res_df.columns.str.strip()
    shape_col = spatial_cols[0].strip() if spatial_cols else None

    rename_map: dict[str, str] = {}
    for col in res_df.columns:
        base_col = col.split(".")[-1]
        base_col_lower = base_col.lower()

        if (
            base_col_lower in {"objectid", "fid", "oid", "globalid"}
            or base_col_lower in oid_mapping
        ) and col != shape_col:
            rename_map[col] = f"{base_col.upper()}_RESULT"
            logger.info(
                f"Renaming intrinsic field '{col}' to '{rename_map[col]}' to avoid Geodatabase conflict."
            )
        elif "." in col and col != shape_col:
            rename_map[col] = col.replace(".", "_")

    if rename_map:
        res_df = res_df.rename(columns=rename_map)

    duckdb_geom_map: dict[
        str, Literal["POINT", "MULTIPOINT", "POLYGON", "POLYLINE"]
    ] = {
        "POINT": "POINT",
        "LINESTRING": "POLYLINE",
        "POLYGON": "POLYGON",
        "MULTIPOINT": "MULTIPOINT",
        "MULTILINESTRING": "POLYLINE",
        "MULTIPOLYGON": "POLYGON",
        "GEOMETRYCOLLECTION": "POLYGON",
    }

    is_spatial_output = False
    out_geom: Literal["POINT", "MULTIPOINT", "POLYGON", "POLYLINE"] = "POLYGON"
    sr_obj = (
        arcpy.SpatialReference(spatial_ref)
        if isinstance(spatial_ref, int)
        else spatial_ref
    )

    if shape_col and shape_col in res_df.columns:
        try:
            geom_query = f'SELECT ST_GeometryType("{shape_col}") FROM {temp_tbl} WHERE "{shape_col}" IS NOT NULL LIMIT 1'
            geom_res = con.execute(geom_query).fetchone()
            if geom_res and geom_res[0]:
                out_geom = duckdb_geom_map.get(str(geom_res[0]), "POLYGON")
        except duckdb.Error:
            logger.warning(
                "Unable to determine output geometry type; defaulting to POLYGON.",
                exc_info=True,
            )

        arcpy.management.CreateFeatureclass(
            "memory", out_name, out_geom, spatial_reference=sr_obj
        )
        is_spatial_output = True
    else:
        arcpy.management.CreateTable("memory", out_name)
        if shape_col and shape_col in res_df.columns:
            res_df.drop(columns=[shape_col], inplace=True)

    attribute_cols = [c for c in res_df.columns if c != shape_col]

    for col in attribute_cols:
        original_col = next((k for k, v in rename_map.items() if v == col), col)
        original_base_lower = original_col.split(".")[-1].lower()

        if original_base_lower in oid_mapping:
            field_type = "BIGINTEGER" if oid_mapping[original_base_lower] else "LONG"
        else:
            field_type = map_pandas_dtype_to_arcpy(res_df[col].dtype)

        arcpy.management.AddField(
            out_path,
            col,
            field_type,
            field_length=500 if field_type == "TEXT" else None,
        )

    def _clean_val(val: Any) -> Any:
        if pd.isna(val) or val is None:
            return None
        if isinstance(val, pd.Timestamp):
            return val.to_pydatetime()
        if hasattr(val, "item"):
            return val.item()
        return val

    if is_spatial_output:
        assert shape_col is not None
        insert_fields = attribute_cols + ["SHAPE@"]
        with arcpy.da.InsertCursor(out_path, insert_fields) as inserter:
            for _, row in res_df.iterrows():
                wkb_val = row[shape_col]
                geom = (
                    arcpy.FromWKB(bytes(wkb_val))
                    if pd.notnull(wkb_val) and wkb_val is not None
                    else None
                )
                attrs = [_clean_val(row[c]) for c in attribute_cols]
                inserter.insertRow(attrs + [geom])
        logger.info(
            f"Created Spatial Layer: {out_path} ({len(res_df)} rows, Auto-detected: {out_geom})"
        )
    else:
        if attribute_cols:
            with arcpy.da.InsertCursor(out_path, attribute_cols) as inserter:
                for _, row in res_df.iterrows():
                    attrs = [_clean_val(row[c]) for c in attribute_cols]
                    inserter.insertRow(attrs)
        logger.info(f"Created Table: {out_path} ({len(res_df)} rows)")

    return out_path
