import arcpy
import pandas as pd
import requests
from shapely.geometry import shape


def featureservice_to_df(
    url: str,
    out_fields: list[str] | str = "*",
    where: str = "1=1",
    out_sr: int = 2248,
    token: str = "",
) -> pd.DataFrame:
    base_url = url.rstrip("/")
    query_url = f"{base_url}/query"
    base_params = {"f": "json"}

    requests.packages.urllib3.disable_warnings()
    access_resp = requests.get(base_url, params=base_params, verify=False)
    access_resp.raise_for_status()
    if "error" in access_resp.json() and access_resp.json()["error"].get("code") == 499:
        base_params["token"] = token
        arcpy.AddMessage("Service is private; token will be included in requests.")
    else:
        arcpy.AddMessage("Service is public; no token needed.")
        token = ""

    meta_resp = requests.get(base_url, params=base_params, verify=False)
    meta_resp.raise_for_status()
    max_record_count = meta_resp.json().get("maxRecordCount", 1000)

    count_params = {**base_params, "where": where, "returnCountOnly": "true"}
    count_resp = requests.get(query_url, params=count_params, verify=False)
    count_resp.raise_for_status()
    total_records = count_resp.json().get("count", 0)

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
            raise ValueError(f"ArcGIS Error: {data['error'].get('message')}")

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
            except ValueError:
                raise ValueError(f"Geometry failed to read: {geom_dict}")
        else:
            props["GEOMWKB"] = None
        rows.append(props)

    return pd.DataFrame(rows)


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
