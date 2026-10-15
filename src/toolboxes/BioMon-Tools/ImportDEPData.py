# %%
import json
import math

import arcpy
import duckdb
import pandas as pd
import requests
import urllib3
from shapely.geometry import shape

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
    if token:
        base_params["token"] = token

    access_resp = requests.get(base_url, params=base_params, verify=False)
    access_resp.raise_for_status()
    access_data = access_resp.json()
    if "error" in access_data:
        err = access_data["error"]
        if err.get("code") == 499 and not token:
            try:
                token = arcpy.GetSigninToken().get("token", "")
                if token:
                    base_params["token"] = token
            except Exception:
                pass

    meta_resp = requests.get(base_url, params=base_params, verify=False)
    meta_resp.raise_for_status()
    meta_data = meta_resp.json()
    if "error" in meta_data:
        raise Exception(
            f"ArcGIS Error getting metadata for {base_url}: {meta_data['error'].get('message', meta_data['error'])}"
        )
    max_record_count = meta_data.get("maxRecordCount", 1000)

    count_params = {**base_params, "where": where, "returnCountOnly": "true"}
    count_resp = requests.get(query_url, params=count_params, verify=False)
    count_resp.raise_for_status()
    count_data = count_resp.json()
    if "error" in count_data:
        raise Exception(
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
            "outFields": ",".join(out_fields)
            if isinstance(out_fields, list)
            else out_fields,
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
            raise Exception(f"ArcGIS Error querying {query_url}: {err_msg}")

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
            props["GEOMWKB"] = shape(geom_dict).wkb if geom_dict else None
        rows.append(props)

    return pd.DataFrame(rows)


def get_portal_token():
    try:
        t = arcpy.GetSigninToken()
        if isinstance(t, dict):
            return t.get("token", "")
    except Exception:
        pass
    return ""


def df_to_esri_features(
    df, has_geometry=True, is_update=False, id_field="GlobalID", wkid=2248
):
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

            # Rely on pd.Timestamp and string class names to avoid import conflicts
            elif isinstance(val, pd.Timestamp) or type(val).__name__ in (
                "date",
                "datetime",
                "Timestamp",
            ):
                try:
                    # pd.Timestamp universally handles dates, datetimes, and pandas timestamps
                    ts = pd.Timestamp(val)
                    attributes[col] = int(ts.timestamp() * 1000)
                except Exception:
                    attributes[col] = str(val)
            else:
                if hasattr(val, "item"):
                    val = val.item()
                attributes[col] = val

        feat = {"attributes": attributes}

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


def run_apply_edits(url, layer_id, features, token="", is_update=False, chunk_size=500):
    if not features:
        return []
    edits_url = f"{url.rstrip('/')}/{layer_id}/applyEdits"
    all_results = []
    for i in range(0, len(features), chunk_size):
        chunk = features[i : i + chunk_size]
        # useGlobalIds must be true for updates, false for inserts
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
            raise Exception(
                f"ArcGIS applyEdits Error (Layer {layer_id}): {res_json['error']}"
            )
        results_key = "updateResults" if is_update else "addResults"
        results = res_json.get(results_key, [])
        failed = sum(1 for r in results if not r.get("success", False))
        if failed > 0:
            errs = [r.get("error") for r in results if not r.get("success", False)]
            raise Exception(
                f"applyEdits {action_name} failed for {failed}/{len(chunk)} features in Layer {layer_id}: {errs}"
            )
        print(
            f"Layer {layer_id} batch {i // chunk_size + 1}: Successfully {action_name.lower()}ed {len(chunk)} features."
        )
        all_results.extend(results)
    return all_results


con = duckdb.connect()
con.sql("INSTALL spatial; LOAD spatial;")


# %%
# Load existing data to insert/update visits
token = get_portal_token()
df_stations = featureservice_to_df(
    "https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/Biological_Monitoring/FeatureServer/0",
    out_fields=["STATION"],
    token=token,
)

df_visits = featureservice_to_df(
    "https://services1.arcgis.com/HbzrdBZjOwNHp70P/ArcGIS/rest/services/BioMon_EDIT/FeatureServer/0",
    out_fields=["GlobalID", "STATION", "VISIT_TYPE", "VISIT_DATE", "DATA_NOTES"],
    token=token,
)

sql = """
CREATE OR REPLACE TABLE stations AS
SELECT
    * EXCLUDE (GEOMWKB)
    ,ST_GeomFromWKB(GEOMWKB) AS geom
FROM df_stations;

CREATE OR REPLACE TABLE visits_existing AS
SELECT
    * EXCLUDE (GEOMWKB, VISIT_DATE)
    ,CAST(epoch_ms(VISIT_DATE::BIGINT)::DATE AS TIMESTAMP) + INTERVAL 12 HOUR AS VISIT_DATE
    ,ST_GeomFromWKB(GEOMWKB) AS geom
FROM df_visits;
"""

con.sql(sql)

# %%
# target_file = r"C:\Users\carter.hughes\OneDrive - The Maryland-National Capital Park and Planning Commission\MCP - Data Analytics WHQ\Projects\PPSD\Resource Analysis\Biological Monitoring\MCDEP\Data Exchange\20240805_Gauza_SCSC204A.xlsx"
"""
PhysChem
MON_TYPE field present

Rhab
MON_TYPE field absent
"""

# target_file = r"C:\Users\carter.hughes\OneDrive - The Maryland-National Capital Park and Planning Commission\MCP - Data Analytics WHQ\Projects\PPSD\Resource Analysis\Biological Monitoring\MCDEP\Data Exchange\20260429_Gauza_Crystal Rock.xlsx"
"""
PhysChem
MON_TYPE field absent

Rhab
MON_TYPE field absent
"""

# target_file = r"C:\Users\carter.hughes\OneDrive - The Maryland-National Capital Park and Planning Commission\MCP - Data Analytics WHQ\Projects\PPSD\Resource Analysis\Biological Monitoring\MCDEP\Data Exchange\20260724_Gauza_Paint Branch.xlsx"
"""
PhysChem
MON_TYPE field absent

Rhab
MON_TYPE field absent
"""

target_file = r"C:\Users\carter.hughes\OneDrive - The Maryland-National Capital Park and Planning Commission\MCP - Data Analytics WHQ\Projects\PPSD\Resource Analysis\Biological Monitoring\MCDEP\Data Exchange\20260915_Gauza_Little Falls.xlsx"
"""
PhysChem
MON_TYPE field present

Rhab
MON_TYPE field present
"""


sql = f'''
CREATE OR REPLACE TABLE raw_benthics_counts AS
SELECT
    TRIM(STATION::VARCHAR) AS STATION
    ,(CAST('1899-12-30' AS DATE) + CAST(FLOOR(CAST("DATE" AS DOUBLE)) AS INTEGER))::DATE AS VISIT_DATE
    ,REPLACE(LOWER(TAXA::VARCHAR), 'none', 'no benthics observed') AS BC_TAXA
    
    ,NULLIF("#INDVS", 'NULL')::INT AS INDIVIDUALS
    ,NULLIF(trays, 'NULL')::INT AS TRAYS
    ,NULLIF("#GRIDS", 'NULL')::INT AS GRIDS
    
FROM read_xlsx("{target_file}", sheet="Benthics", all_varchar = true);

CREATE OR REPLACE TABLE raw_fish_counts AS
SELECT
    TRIM(STATION::VARCHAR) AS STATION
    ,(CAST('1899-12-30' AS DATE) + CAST(FLOOR(CAST(SAMPLE_DATE AS DOUBLE)) AS INTEGER))::DATE AS VISIT_DATE
    ,REPLACE(LOWER(SPECIES::VARCHAR), 'none', 'no fish observed') AS FC_SPECIES
    
    ,PASS1::INT AS PASS_1
    ,PASS2::INT AS PASS_2
    ,PASS_1 + PASS_2 AS TOTAL_COUNT
    ,ANOMALIES::INT AS TOTAL_ANOMALIES
    ,CASE
        WHEN ANOMALIES_TYPE::VARCHAR = 'NULL' THEN NULL
        ELSE UPPER(ANOMALIES_TYPE::VARCHAR)
    END AS ANOMALIES
    
    ,ROW_NUMBER() OVER () AS row_num
    
FROM read_xlsx("{target_file}", sheet="Fish", all_varchar = true);

CREATE OR REPLACE TABLE raw_phys_chem AS
SELECT
    TRIM(STATION::VARCHAR) AS STATION
    ,(CAST('1899-12-30' AS DATE) + CAST(FLOOR(CAST(SAMPLE_DATE AS DOUBLE)) AS INTEGER))::DATE AS VISIT_DATE
    ,TRIM(MON_TYPE::VARCHAR) AS VISIT_TYPE
    
    ,(NULLIF(AIRTEMP, 'NULL')::INTEGER * 9 / 5 + 32) AS PC_AIRTEMP_F
    ,NULLIF(AIRTEMP, 'NULL')::INTEGER AS PC_AIRTEMP_C
    ,NULLIF(H2OTEMP_C, 'NULL')::FLOAT AS PC_WATERTEMP_C
    ,NULLIF(PH, 'NULL')::FLOAT AS PC_PH
    ,NULLIF(DISOXY, 'NULL')::FLOAT AS PC_DO_CONC
    ,NULLIF("%_SAT", 'NULL')::FLOAT AS PC_DO_SAT
    ,NULLIF(CONDUCT, 'NULL')::FLOAT AS PC_COND
    ,NULLIF(Turbidity, 'NULL')::FLOAT AS PC_TURB
    ,CASE 
        WHEN NULLIF(TRIM(MonitoringTime), 'NULL') IS NULL THEN NULL
        ELSE STRFTIME(
            STRPTIME(
                CASE 
                    WHEN TRY_CAST(NULLIF(TRIM(MonitoringTime), 'NULL') AS DOUBLE) IS NOT NULL 
                         AND TRY_CAST(NULLIF(TRIM(MonitoringTime), 'NULL') AS DOUBLE) >= 0 
                         AND TRY_CAST(NULLIF(TRIM(MonitoringTime), 'NULL') AS DOUBLE) < 1
                    THEN STRFTIME(
                        TIMESTAMP '2000-01-01 00:00:00' + INTERVAL (ROUND(CAST(NULLIF(TRIM(MonitoringTime), 'NULL') AS DOUBLE) * 86400)) SECOND, 
                        '%H%M'
                    )
                    ELSE LPAD(REPLACE(NULLIF(TRIM(MonitoringTime), 'NULL'), ':', ''), 4, '0')
                END, 
                '%H%M'
            ), 
            '%H%M'
        )
    END AS PC_TIME
    
    ,NULLIF(COMMENTS, 'NULL')::VARCHAR AS PC_NOTES
    
FROM read_xlsx("{target_file}", sheet="PhysChem", all_varchar = true);

CREATE OR REPLACE TABLE raw_rhab AS
SELECT
    TRIM(STATION::VARCHAR) AS STATION
    ,(CAST('1899-12-30' AS DATE) + CAST(FLOOR(CAST(SAMPLE_DATE AS DOUBLE)) AS INTEGER))::DATE AS VISIT_DATE
    ,TRIM(MON_TYPE::VARCHAR) AS VISIT_TYPE

    ,NULLIF(INSTR_COVER, 'NULL')::INTEGER AS RHAB_INSTREAMCOVER
    ,NULLIF(EPI_SUBSTR, 'NULL')::INTEGER AS RHAB_EPISUBSTRATE
    ,NULLIF(EMBEDD, 'NULL')::INTEGER AS RHAB_EMBEDDEDNESS
    ,NULLIF(CH_ALTER, 'NULL')::INTEGER AS RHAB_CHANNELALT
    ,NULLIF(SED_DEPOSIT, 'NULL')::INTEGER AS RHAB_SEDDEPOSIT
    ,NULLIF(RIFFLE_FREQ, 'NULL')::INTEGER AS RHAB_RIFFLEFREQ
    ,NULLIF(CHAN_FLOW, 'NULL')::INTEGER AS RHAB_CHANNELFLOWSTATUS
    ,NULLIF(LB_BANKVEG, 'NULL')::INTEGER AS RHAB_VEG_LB
    ,NULLIF(RB_BANKVEG, 'NULL')::INTEGER AS RHAB_VEG_RB
    ,NULLIF(LB_BANKSTAB, 'NULL')::INTEGER AS RHAB_STABILITY_LB
    ,NULLIF(RB_BANKSTAB, 'NULL')::INTEGER AS RHAB_STABILITY_RB
    ,NULLIF(LB_BUFFER, 'NULL')::INTEGER AS RHAB_BUFFER_LB
    ,NULLIF(RB_BUFFER, 'NULL')::INTEGER AS RHAB_BUFFER_RB

    ,NULLIF(COMMENTS, 'NULL')::VARCHAR AS RHAB_NOTES

FROM read_xlsx("{target_file}", sheet="Rhab", all_varchar = true);
SELECT * 
FROM raw_phys_chem
WHERE STATION IN ('LFLF102', 'LFLF301A')
ORDER BY STATION, VISIT_DATE;
'''

con.sql(sql)

# %%
sql = """
CREATE OR REPLACE TABLE visits AS
SELECT
    -- Shared fields
    COALESCE(pc.STATION, rhab.STATION) AS STATION
    ,CAST(COALESCE(pc.VISIT_DATE, rhab.VISIT_DATE) AS TIMESTAMP) + INTERVAL 12 HOUR AS VISIT_DATE
    ,CASE
        WHEN COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE) = 'BIF' AND COALESCE(pc.STATION, rhab.STATION) NOT IN (SELECT STATION FROM raw_fish_counts) AND rhab.VISIT_TYPE IS NULL THEN 'OTH'
        WHEN COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE) = 'BIF' AND COALESCE(pc.STATION, rhab.STATION) NOT IN (SELECT STATION FROM raw_fish_counts) AND rhab.VISIT_TYPE IS NOT NULL THEN 'HAB'
        ELSE COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE)
    END AS VISIT_TYPE
    
    -- Calculated fields
    ,CASE
        WHEN COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE) = 'BIF' THEN 'DEP'
        ELSE NULL
    END AS FISH_LEADER1
    ,CASE
        WHEN COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE) = 'BIF' THEN 'DEP'
        ELSE NULL
    END AS FISH_LEADER2
    ,CASE
        WHEN rhab.VISIT_TYPE IS NOT NULL THEN 'DEP'
        ELSE NULL
    END AS HAB_ASSESSOR
    ,CASE
        WHEN COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE) = 'BIB' THEN 'DEP'
        ELSE NULL
    END AS BEN_SAMPLER
    ,CASE
        WHEN COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE) = 'BIB' THEN 'DEP'
        ELSE NULL
    END AS BEN_SUBSAMPLER
    ,CASE
        WHEN COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE) = 'BIF' THEN 'DEP'
        ELSE NULL
    END AS FISH_TAXONOMIST1
    ,CASE
        WHEN COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE) = 'BIF' THEN 'DEP'
        ELSE NULL
    END AS FISH_TAXONOMIST2
    ,'DEP' AS DATA_ENTERER
    ,CURRENT_DATE AS DATA_ENTER_DATE
    
    ,CASE 
        WHEN TRIM(SPLIT_PART(COALESCE(ve.DATA_NOTES, ''), 'Bulk upload as of', 1)) = '' THEN ''
        ELSE TRIM(SPLIT_PART(COALESCE(ve.DATA_NOTES, ''), 'Bulk upload as of', 1)) || ' '
    END || 'Bulk upload as of ' || CURRENT_DATE || '.' || CASE
        WHEN pc.STATION IS NOT NULL AND rhab.STATION IS NOT NULL THEN ' PhysChem and Rhab data imported.'
        WHEN pc.STATION IS NOT NULL AND rhab.STATION IS NULL THEN ' PhysChem data imported.'
        WHEN pc.STATION IS NULL AND rhab.STATION IS NOT NULL THEN ' Rhab data imported.'
        ELSE ''
    END AS DATA_NOTES

    -- Phys Chem
    ,pc.PC_AIRTEMP_F
    ,pc.PC_AIRTEMP_C
    ,pc.PC_WATERTEMP_C
    ,pc.PC_PH
    ,pc.PC_DO_CONC
    ,pc.PC_DO_SAT
    ,pc.PC_COND
    ,pc.PC_TURB
    ,pc.PC_NOTES

    -- RHAB
    ,rhab.RHAB_INSTREAMCOVER
    ,rhab.RHAB_EPISUBSTRATE
    ,rhab.RHAB_EMBEDDEDNESS
    ,rhab.RHAB_CHANNELALT
    ,rhab.RHAB_SEDDEPOSIT
    ,rhab.RHAB_RIFFLEFREQ
    ,rhab.RHAB_CHANNELFLOWSTATUS
    ,rhab.RHAB_VEG_LB
    ,rhab.RHAB_VEG_RB
    ,rhab.RHAB_STABILITY_LB
    ,rhab.RHAB_STABILITY_RB
    ,rhab.RHAB_BUFFER_LB
    ,rhab.RHAB_BUFFER_RB
    ,rhab.RHAB_NOTES
    
    ,COALESCE(ve.geom, s.geom) AS GEOM
    ,ve.GlobalId

FROM raw_phys_chem pc
FULL OUTER JOIN raw_rhab rhab
    ON pc.STATION = rhab.STATION
    AND pc.VISIT_DATE = rhab.VISIT_DATE
    AND pc.VISIT_TYPE = rhab.VISIT_TYPE
LEFT JOIN visits_existing ve
    ON COALESCE(pc.STATION, rhab.STATION) = ve.STATION
    AND DATE(COALESCE(pc.VISIT_DATE, rhab.VISIT_DATE)) = DATE(ve.VISIT_DATE)
    AND COALESCE(pc.VISIT_TYPE, rhab.VISIT_TYPE) = ve.VISIT_TYPE
LEFT JOIN stations s
    ON COALESCE(pc.STATION, rhab.STATION) = s.STATION
WHERE pc.STATION IS NOT NULL OR rhab.STATION IS NOT NULL
;
SELECT 
    STATION
    ,VISIT_DATE
    ,VISIT_TYPE
    ,GlobalId
    ,DATA_NOTES
FROM visits
ORDER BY STATION, VISIT_DATE;
"""
con.sql(sql)

# %%
# Step 1 & 2: Auth & Introspection

token = get_portal_token()
fs_url = "https://services1.arcgis.com/HbzrdBZjOwNHp70P/arcgis/rest/services/BioMon_EDIT/FeatureServer"


# %%
df_visits_update = con.sql("SELECT * FROM visits WHERE GlobalID IS NOT NULL").df()
df_visits_insert = con.sql(
    "SELECT * EXCLUDE (GlobalID) FROM visits WHERE GlobalID IS NULL"
).df()

# %%
df_visits_update

# %%
df_visits_insert

# %%
sql = """
SELECT
    ve.GlobalId
    ,ve.STATION
    ,ve.VISIT_TYPE
    ,ve.VISIT_DATE AS EXISTING_VISIT_DATE
    ,v.VISIT_DATE AS CORRECT_IMPORT_DATE
    ,ve.DATA_NOTES
FROM visits_existing ve
INNER JOIN visits v
    ON ve.STATION = v.STATION
    AND ve.VISIT_TYPE = v.VISIT_TYPE
WHERE CAST(ve.VISIT_DATE AS DATE) != CAST(v.VISIT_DATE AS DATE)
    AND (
        CAST(ve.VISIT_DATE AS DATE) = CAST(v.VISIT_DATE AS DATE) - INTERVAL 1 DAY
        OR CAST(ve.VISIT_DATE AS DATE) = CAST(v.VISIT_DATE AS DATE) + INTERVAL 1 DAY
    )
ORDER BY ve.STATION, v.VISIT_DATE;
"""

con.sql(sql)

# %%
# Step 5: Two-Phase Ingestion & Relational Re-linking (Visits & Fish Counts)
# 5A: Visits Upsert (Updates & Inserts)
if len(df_visits_update) > 0:
    run_apply_edits(
        fs_url,
        0,
        df_to_esri_features(df_visits_update, False, True, "GlobalID"),
        token,
        True,
    )
if len(df_visits_insert) > 0:
    run_apply_edits(
        fs_url,
        0,
        df_to_esri_features(df_visits_insert, True, False, "GlobalID", 2248),
        token,
        False,
    )


# %%
sql = """
CREATE OR REPLACE TABLE fc_visits AS
WITH fish_agg AS (
    SELECT
        STATION
        ,VISIT_DATE
        ,'BIF' AS VISIT_TYPE
    FROM raw_fish_counts
    GROUP BY STATION, VISIT_DATE
)
SELECT
    fa.STATION
    ,fa.VISIT_DATE
    ,fa.VISIT_TYPE
    
    -- Calculated fields
    ,'DEP' AS FISH_LEADER1
    ,'DEP' AS FISH_LEADER2
    ,'DEP' AS FISH_TAXONOMIST1
    ,'DEP' AS FISH_TAXONOMIST2
    ,'DEP' AS DATA_ENTERER
    ,CURRENT_DATE AS DATA_ENTER_DATE
    ,COALESCE(ve.DATA_NOTES, '') || 'Bulk upload as of ' || CURRENT_DATE AS DATA_NOTES
    
    ,COALESCE(ve.geom, s.geom) AS GEOM
    ,ve.GlobalId
FROM fish_agg fa
LEFT JOIN visits_existing ve
    ON fa.STATION = ve.STATION
    AND DATE(fa.VISIT_DATE) = DATE(ve.VISIT_DATE)
    AND fa.VISIT_TYPE = ve.VISIT_TYPE
LEFT JOIN stations s
    ON fa.STATION = s.STATION
WHERE
    ve.GlobalId IS NULL
    AND hash(fa.STATION, fa.VISIT_DATE, fa.VISIT_TYPE) NOT IN (
        SELECT
            hash(v.STATION, v.VISIT_DATE, v.VISIT_TYPE)
        FROM visits v
    )
;
FROM fc_visits
"""
con.sql(sql)

# %%
df_visits_insert_fc = con.sql("SELECT * FROM fc_visits").df()

if len(df_visits_insert_fc) > 0:
    run_apply_edits(
        fs_url,
        0,
        df_to_esri_features(df_visits_insert_fc, True, False, "GlobalID", 2248),
        token,
        False,
    )

# %%
sql = """
CREATE OR REPLACE TABLE bc_visits AS
WITH benthic_agg AS (
    SELECT
        STATION
        ,VISIT_DATE
        ,'BIB' AS VISIT_TYPE
    FROM raw_benthics_counts
    GROUP BY STATION, VISIT_DATE
)
SELECT
    ba.STATION
    ,ba.VISIT_DATE
    ,ba.VISIT_TYPE
    
    -- Calculated fields
    ,'DEP' AS BEN_SAMPLER
    ,'DEP' AS BEN_SUBSAMPLER
    ,'DEP' AS DATA_ENTERER
    ,CURRENT_DATE AS DATA_ENTER_DATE
    ,COALESCE(ve.DATA_NOTES, '') || 'Bulk upload as of ' || CURRENT_DATE AS DATA_NOTES
    
    ,COALESCE(ve.geom, s.geom) AS GEOM
    ,ve.GlobalId
FROM benthic_agg ba
LEFT JOIN visits_existing ve
    ON ba.STATION = ve.STATION
    AND DATE(ba.VISIT_DATE) = DATE(ve.VISIT_DATE)
    AND ba.VISIT_TYPE = ve.VISIT_TYPE
LEFT JOIN stations s
    ON ba.STATION = s.STATION
WHERE
    ve.GlobalId IS NULL
    AND hash(ba.STATION, ba.VISIT_DATE, ba.VISIT_TYPE) NOT IN (
        SELECT
            hash(v.STATION, v.VISIT_DATE, v.VISIT_TYPE)
        FROM visits v
    )
;
FROM bc_visits
"""
con.sql(sql)

# %%
df_visits_insert_bc = con.sql(
    "SELECT * FROM bc_visits WHERE GlobalID IS NULL AND GEOM IS NOT NULL"
).df()

if len(df_visits_insert_bc) > 0:
    run_apply_edits(
        fs_url,
        0,
        df_to_esri_features(df_visits_insert_bc, True, False, "GlobalID", 2248),
        token,
        False,
    )

# %%
# Load existing data to insert/update fish counts
token = get_portal_token()
df_visits = featureservice_to_df(
    "https://services1.arcgis.com/HbzrdBZjOwNHp70P/ArcGIS/rest/services/BioMon_EDIT/FeatureServer/0",
    out_fields=["GlobalID", "STATION", "VISIT_TYPE", "VISIT_DATE", "DATA_NOTES"],
    token=token,
)

df_fishcounts = featureservice_to_df(
    "https://services1.arcgis.com/HbzrdBZjOwNHp70P/ArcGIS/rest/services/BioMon_EDIT/FeatureServer/2",
    out_fields=["GlobalID", "GUID", "FC_SPECIES"],
    token=token,
)

sql = """
CREATE OR REPLACE TABLE visits_existing AS
SELECT
    * EXCLUDE (GEOMWKB, VISIT_DATE)
    ,CAST(epoch_ms(VISIT_DATE::BIGINT)::DATE AS TIMESTAMP) + INTERVAL 12 HOUR AS VISIT_DATE
    ,ST_GeomFromWKB(GEOMWKB) AS geom
FROM df_visits;
"""

con.sql(sql)

sql = """
CREATE OR REPLACE TABLE fish_counts_existing AS
SELECT
    *
FROM df_fishcounts;
"""

con.sql(sql)

# %%
sql = """
CREATE OR REPLACE TABLE transformed_fish_counts AS
WITH split_anomalies AS (
    SELECT 
        row_num
        ,UNNEST(STRING_SPLIT(REPLACE(UPPER(ANOMALIES), '/', ','), ',')) AS token
        ,TOTAL_ANOMALIES
    FROM raw_fish_counts
    WHERE ANOMALIES IS NOT NULL
),
parse_anomalies AS (
    SELECT 
        row_num
        ,TRIM(token) AS token_clean
        ,REGEXP_EXTRACT(TRIM(token), '^[0-9]+') AS num_part
        ,REGEXP_EXTRACT(TRIM(token), '[A-Z_]+') AS code_part
        ,TOTAL_ANOMALIES
    FROM split_anomalies
    WHERE TRIM(token) != ''
),
agg_anomalies AS (
    SELECT 
        row_num,
        code_part,
        COALESCE(CAST(NULLIF(num_part, '') AS INT), TOTAL_ANOMALIES) AS count_val
    FROM parse_anomalies
    WHERE code_part != ''
),
pivot_anomalies AS (
    SELECT 
        row_num,
        SUM(CASE WHEN code_part = 'BL' THEN count_val ELSE 0 END) AS BL,
        SUM(CASE WHEN code_part = 'RE' THEN count_val ELSE 0 END) AS RE,
        SUM(CASE WHEN code_part = 'LE' THEN count_val ELSE 0 END) AS LE,
        SUM(CASE WHEN code_part = 'GR' THEN count_val ELSE 0 END) AS GR,
        SUM(CASE WHEN code_part = 'UL' THEN count_val ELSE 0 END) AS UL,
        SUM(CASE WHEN code_part = 'DV' THEN count_val ELSE 0 END) AS DV,
        SUM(CASE WHEN code_part = 'DM' THEN count_val ELSE 0 END) AS DM,
        SUM(CASE WHEN code_part = 'SC' THEN count_val ELSE 0 END) AS SC,
        SUM(CASE WHEN code_part = 'RS' THEN count_val ELSE 0 END) AS RS,
        SUM(CASE WHEN code_part = 'DI' THEN count_val ELSE 0 END) AS DI,
        SUM(CASE WHEN code_part = 'HM' THEN count_val ELSE 0 END) AS HM,
        SUM(CASE WHEN code_part = 'BS' THEN count_val ELSE 0 END) AS BS,
        SUM(CASE WHEN code_part = 'AW' THEN count_val ELSE 0 END) AS AW,
        SUM(CASE WHEN code_part = 'EP' THEN count_val ELSE 0 END) AS EP,
        SUM(CASE WHEN code_part = 'IK' THEN count_val ELSE 0 END) AS IK,
        SUM(CASE WHEN code_part = 'FU' THEN count_val ELSE 0 END) AS FU,
        SUM(CASE WHEN code_part = 'FI' THEN count_val ELSE 0 END) AS FI,
        SUM(CASE WHEN code_part = 'CL' THEN count_val ELSE 0 END) AS CL,
        SUM(CASE WHEN code_part = 'FD' THEN count_val ELSE 0 END) AS FD,
        SUM(CASE WHEN code_part = 'HK' THEN count_val ELSE 0 END) AS HK,
        SUM(CASE WHEN code_part = 'CT' THEN count_val ELSE 0 END) AS CT,
        SUM(CASE WHEN code_part = 'AN' THEN count_val ELSE 0 END) AS AN,
        SUM(CASE WHEN code_part = 'EC' THEN count_val ELSE 0 END) AS EC,
        SUM(CASE WHEN code_part = 'EH' THEN count_val ELSE 0 END) AS EH,
        SUM(CASE WHEN code_part = 'PO' THEN count_val ELSE 0 END) AS PO,
        SUM(CASE WHEN code_part = 'OR_' THEN count_val ELSE 0 END) AS OR_,
        SUM(CASE WHEN code_part = 'NO' THEN count_val ELSE 0 END) AS NO,
        SUM(CASE WHEN code_part = 'CA' THEN count_val ELSE 0 END) AS CA,
        SUM(CASE WHEN code_part = 'OT' THEN count_val ELSE 0 END) AS OT
    FROM agg_anomalies
    GROUP BY row_num
)
SELECT 
    r.STATION,
    r.VISIT_DATE,
    r.FC_SPECIES,
    r.PASS_1,
    r.PASS_2,
    r.TOTAL_COUNT,
    r.TOTAL_ANOMALIES,
    r.ANOMALIES,
    COALESCE(p.BL, 0) AS BL,
    COALESCE(p.RE, 0) AS RE,
    COALESCE(p.LE, 0) AS LE,
    COALESCE(p.GR, 0) AS GR,
    COALESCE(p.UL, 0) AS UL,
    COALESCE(p.DV, 0) AS DV,
    COALESCE(p.DM, 0) AS DM,
    COALESCE(p.SC, 0) AS SC,
    COALESCE(p.RS, 0) AS RS,
    COALESCE(p.DI, 0) AS DI,
    COALESCE(p.HM, 0) AS HM,
    COALESCE(p.BS, 0) AS BS,
    COALESCE(p.AW, 0) AS AW,
    COALESCE(p.EP, 0) AS EP,
    COALESCE(p.IK, 0) AS IK,
    COALESCE(p.FU, 0) AS FU,
    COALESCE(p.FI, 0) AS FI,
    COALESCE(p.CL, 0) AS CL,
    COALESCE(p.FD, 0) AS FD,
    COALESCE(p.HK, 0) AS HK,
    COALESCE(p.CT, 0) AS CT,
    COALESCE(p.AN, 0) AS AN,
    COALESCE(p.EC, 0) AS EC,
    COALESCE(p.EH, 0) AS EH,
    COALESCE(p.PO, 0) AS PO,
    COALESCE(p.OR_, 0) AS OR_,
    COALESCE(p.NO, 0) AS NO,
    COALESCE(p.CA, 0) AS CA,
    COALESCE(p.OT, 0) AS OT
    
    , 'Yes' AS IND_ANOMALIES
    ,ve.GlobalId AS GUID
    
    ,fc.GlobalId
    
FROM raw_fish_counts r
LEFT JOIN pivot_anomalies p ON r.row_num = p.row_num
LEFT JOIN visits_existing ve
    ON r.STATION = ve.STATION
    AND DATE(r.VISIT_DATE) = DATE(ve.VISIT_DATE)
LEFT JOIN fish_counts_existing fc
    ON ve.GlobalId = fc.GUID
    AND r.FC_SPECIES = fc.FC_SPECIES
ORDER BY r.row_num;
FROM transformed_fish_counts
"""

con.sql(sql)

# %%
df_fish_upd = con.sql(
    "SELECT * FROM transformed_fish_counts WHERE GlobalID IS NOT NULL"
).df()
df_fish_ins = con.sql(
    "SELECT * EXCLUDE (GlobalID) FROM transformed_fish_counts WHERE GlobalID IS NULL"
).df()

print(
    f"Fish counts to update: {len(df_fish_upd)} | Fish counts to insert: {len(df_fish_ins)}"
)

if len(df_fish_upd) > 0:
    run_apply_edits(
        fs_url,
        2,
        df_to_esri_features(df_fish_upd, False, True, "GlobalID"),
        token,
        True,
    )
if len(df_fish_ins) > 0:
    run_apply_edits(
        fs_url,
        2,
        df_to_esri_features(df_fish_ins, False, False, "GlobalID"),
        token,
        False,
    )
