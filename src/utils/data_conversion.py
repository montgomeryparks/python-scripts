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

    access_resp = requests.get(base_url, params=base_params)
    access_resp.raise_for_status()
    if "error" in access_resp.json() and access_resp.json()["error"].get("code") == 499:
        base_params["token"] = token
        arcpy.AddMessage("Service is private; token will be included in requests.")
    else:
        arcpy.AddMessage("Service is public; no token needed.")
        token = ""

    # 1) maxRecordCount
    meta_resp = requests.get(base_url, params=base_params)
    meta_resp.raise_for_status()
    max_record_count = meta_resp.json().get("maxRecordCount", 1000)

    # 2) total count
    count_params = {**base_params, "where": where, "returnCountOnly": "true"}
    count_resp = requests.get(query_url, params=count_params)
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

        response = requests.get(query_url, params=fetch_params)
        response.raise_for_status()
        data = response.json()

        if "error" in data:
            raise Exception(f"ArcGIS Error: {data['error'].get('message')}")

        features = data.get("features", [])
        if not features:
            break

        all_features.extend(features)
        offset += max_record_count

    if not all_features:
        return pd.DataFrame()

    # Convert GeoJSON to DataFrame with WKB binary
    rows = []
    for feat in all_features:
        props = feat.get("properties", {})
        geom_dict = feat.get("geometry", None)
        if geom_dict:
            try:
                props["GEOMWKB"] = shape(geom_dict).wkb
            except Exception:
                props["GEOMWKB"] = None
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
    in_layer, out_sr=2248, oid_field_out="OBJECTID", where_clause=None
):
    """
    Returns a standard Pandas DataFrame containing a stable OID and geometry WKB byte arrays.
    """
    sr = arcpy.SpatialReference(out_sr)
    desc = arcpy.Describe(in_layer)
    oid_name = desc.OIDFieldName

    oids = []
    geoms = []

    valid_where = where_clause if where_clause else None

    with arcpy.da.SearchCursor(
        in_layer,
        [oid_name, "SHAPE@WKB"],
        where_clause=valid_where,
        spatial_reference=sr,
    ) as cursor:
        for row in cursor:
            oids.append(row[0])
            geoms.append(bytes(row[1]) if row[1] else None)

    return pd.DataFrame({oid_field_out: oids, "GEOMWKB": geoms})
