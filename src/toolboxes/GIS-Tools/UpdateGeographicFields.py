import gc

import arcpy
import duckdb
import pandas as pd

from src.utils.auth import get_portal_token
from src.utils.constants import (
    URL_MGMT_BOUNDARIES,
    URL_PARK_LOCATIONS,
    URL_PARK_UNITS,
    URL_TRAIL_UNITS,
)
from src.utils.data_conversion import _norm, featureclass_to_df, featureservice_to_df


class UpdateGeographicFields(object):
    def __init__(self):
        self.label = "Update Park, Location, and Management Fields"
        self.description = "Updates the specified target text fields for the input layer(s) using an in-memory DuckDB spatial engine."
        self.canRunInBackground = True

    def getParameterInfo(self):
        params = []

        in_layers = arcpy.Parameter(
            displayName="Input Layer(s)",
            name="in_layers",
            datatype=["GPFeatureLayer", "DEFeatureClass"],
            parameterType="Required",
            direction="Input",
            multiValue=True,
        )
        in_layers.description = "Select one or more feature layers to update with park, location, and management attribute values."
        params.append(in_layers)

        # Field update text mapping - Updated descriptions for clarity
        fields_to_update = [
            (
                "target_park_code",
                "Target PARK_CODE Field",
                "PARK_CODE",
                "Provide the target field name to populate with the matching park code. To skip this update, leave this text box completely blank.",
            ),
            (
                "target_park_name",
                "Target PARK_NAME Field",
                "PARK_NAME",
                "Provide the target field name to populate with the matching park name. To skip this update, leave this text box completely blank.",
            ),
            (
                "target_loc_code",
                "Target LOCATION_CODE Field",
                "LOCATION_CODE",
                "Provide the target field name to populate with the matching location code. To skip this update, leave this text box completely blank.",
            ),
            (
                "target_loc_name",
                "Target LOCATION_NAME Field",
                "LOCATION_NAME",
                "Provide the target field name to populate with the matching location name. To skip this update, leave this text box completely blank.",
            ),
            (
                "target_mgmt_area",
                "Target MGMT_AREA Field",
                "MGMT_AREA",
                "Provide the target field name to populate with the matching management area. To skip this update, leave this text box completely blank.",
            ),
            (
                "target_mgmt_region",
                "Target MGMT_REGION Field",
                "MGMT_REGION",
                "Provide the target field name to populate with the matching management region. To skip this update, leave this text box completely blank.",
            ),
            (
                "target_trail_name",
                "Target TRAIL_NAME Field",
                "TRAIL_NAME",
                "Provide the target field name to populate with the matching trail name. To skip this update, leave this text box completely blank.",
            ),
        ]

        for name, display, default_val, desc in fields_to_update:
            param = arcpy.Parameter(
                displayName=display,
                name=name,
                datatype="GPString",
                parameterType="Optional",
                direction="Input",
            )
            param.value = default_val  # Set the string field name as default
            param.description = desc
            params.append(param)

        param_def_query = arcpy.Parameter(
            displayName="Definition Query (Optional)",
            name="def_query",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
        )
        param_def_query.description = "Optionally limit the update to only the features that match this SQL definition query."
        params.append(param_def_query)

        return params

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        if parameters[0].value:
            parameters[0].setWarningMessage(
                "To prevent 'Cannot acquire a lock' errors, ensure the Attribute Table "
                "is closed and pending edits are saved before running."
            )
        return

    def execute(self, parameters, messages):
        in_layers_text = parameters[0].valueAsText
        in_layers = in_layers_text.split(";") if in_layers_text else []

        # Helper to strictly validate and sanitize the input text
        def _clean_val(val):
            if val is None:
                return None
            clean_str = str(val).strip()
            # ArcPy sometimes returns the literal string "None" when a parameter is cleared
            if not clean_str or clean_str.lower() == "none":
                return None
            return clean_str

        # Map internal processing name -> user's specified field name (or None if blank)
        global_update_map = {
            "PARK_CODE": _clean_val(parameters[1].valueAsText),
            "PARK_NAME": _clean_val(parameters[2].valueAsText),
            "LOCATION_CODE": _clean_val(parameters[3].valueAsText),
            "LOCATION_NAME": _clean_val(parameters[4].valueAsText),
            "MGMT_AREA": _clean_val(parameters[5].valueAsText),
            "MGMT_REGION": _clean_val(parameters[6].valueAsText),
            "TRAIL_NAME": _clean_val(parameters[7].valueAsText),
        }

        # Log configuration intent so it is clear in the geoprocessing history
        arcpy.AddMessage("--- Configuration ---")
        for internal_name, target_field in global_update_map.items():
            if target_field:
                arcpy.AddMessage(f"{internal_name}: Will map to field '{target_field}'")
            else:
                arcpy.AddMessage(
                    f"{internal_name}: Blank parameter. Field will NOT be calculated."
                )
        arcpy.AddMessage("---------------------")

        def_query = parameters[8].valueAsText if parameters[8].value else ""

        token = get_portal_token()

        df_parks = pd.DataFrame()
        df_locs = pd.DataFrame()
        df_mgmt = pd.DataFrame()
        df_trails = pd.DataFrame()

        # Check if we need to fetch datasets based on if any target fields are actually provided
        if any(
            [
                global_update_map.get("PARK_CODE"),
                global_update_map.get("PARK_NAME"),
                global_update_map.get("LOCATION_CODE"),
                global_update_map.get("LOCATION_NAME"),
            ]
        ):
            arcpy.AddMessage("Fetch Park Units...")
            df_parks = featureservice_to_df(
                url=URL_PARK_UNITS,
                out_fields=["PARK_CODE", "PARK_NAME"],
                out_sr=2248,
                where="PARK_CODE IS NOT NULL AND OWNER = 'M-NCPPC' AND STATUS = 'Existing'",
                token=token,
            )
            df_parks = _norm(df_parks, ["PARK_CODE", "PARK_NAME"])

        if any(
            [
                global_update_map.get("LOCATION_CODE"),
                global_update_map.get("LOCATION_NAME"),
            ]
        ):
            arcpy.AddMessage("Fetch Park Locations...")
            df_locs = featureservice_to_df(
                url=URL_PARK_LOCATIONS,
                out_fields=["PL_NAME", "PL_CODE"],
                out_sr=2248,
                where="PL_CODE IS NOT NULL AND STATUS = 'Installed'",
                token=token,
            )
            df_locs = _norm(df_locs, ["PL_NAME", "PL_CODE"])

        if any(
            [global_update_map.get("MGMT_AREA"), global_update_map.get("MGMT_REGION")]
        ):
            arcpy.AddMessage("Fetch Management Boundaries...")
            df_mgmt = featureservice_to_df(
                url=URL_MGMT_BOUNDARIES,
                out_fields=["MGMT_AREA", "MGMT_REGION"],
                out_sr=2248,
                where="1=1",
                token=token,
            )
            df_mgmt = _norm(df_mgmt, ["MGMT_AREA", "MGMT_REGION"])

        if any([global_update_map.get("TRAIL_NAME")]):
            arcpy.AddMessage("Fetch Trails...")
            df_trails = featureservice_to_df(
                url=URL_TRAIL_UNITS,
                out_fields=["TRAIL_NAME"],
                out_sr=2248,
                where="TRAIL_NAME IS NOT NULL",
                token=token,
            )
            df_trails = _norm(df_trails, ["TRAIL_NAME"])

        # process each input layer
        for i, in_layer in enumerate(in_layers, start=1):
            in_layer = in_layer.strip("'")  # remove quotes if present
            arcpy.AddMessage(f"--- Processing {i}/{len(in_layers)}: {in_layer} ---")
            self._process_input_layer(
                in_layer,
                df_parks,
                df_locs,
                df_mgmt,
                df_trails,
                global_update_map,
                def_query,
            )

        arcpy.AddMessage("Done.")

    def postExecute(self, parameters):
        return

    def _process_input_layer(
        self,
        in_layer,
        df_parks,
        df_locs,
        df_mgmt,
        df_trails,
        global_update_map,
        def_query,
    ):
        # Extract only the fields where the user actually provided a valid string name
        active_updates = {k: v for k, v in global_update_map.items() if v is not None}

        if not active_updates:
            arcpy.AddMessage("No target fields provided for update. Skipping layer.")
            return

        try:
            oid_field = arcpy.Describe(in_layer).OIDFieldName
            arcpy.AddMessage(f"OBJECTID field for layer is: {oid_field}")

            # Ensure the provided target fields actually exist in this specific layer's schema
            arcpy.AddMessage("Step 1: Check existing fields...")
            existing_fields = {
                f.name.lower(): f.name for f in arcpy.ListFields(in_layer)
            }

            valid_updates = {}
            for src_attr, dest_field in active_updates.items():
                if dest_field.lower() not in existing_fields:
                    arcpy.AddWarning(
                        f"Field '{dest_field}' does not exist in {in_layer}. Skipping this field."
                    )
                else:
                    # Map the internal attr name to the exact casing of the existing field name
                    valid_updates[src_attr] = existing_fields[dest_field.lower()]

            if not valid_updates:
                arcpy.AddWarning(
                    "No valid fields remain to update for this layer. Skipping."
                )
                return

            # Load input layer -> Pandas DataFrame
            arcpy.AddMessage("Step 2: Load input layer...")
            try:
                df_in = featureclass_to_df(
                    in_layer, out_sr=2248, where_clause=def_query
                )
            except Exception as e:
                arcpy.AddWarning(
                    f"Invalid definition query or failed to read layer: {e}. Skipping."
                )
                return

            if "OBJECTID" not in df_in.columns:
                df_in = df_in.rename(columns={oid_field: "OBJECTID"})

            arcpy.AddMessage(f"Input layer subset has {len(df_in)} features.")
            if len(df_in) == 0:
                arcpy.AddMessage("No features match the definition query. Skipping.")
                return

            # Build DuckDB Execution Engine
            arcpy.AddMessage("Step 3: Run DuckDB spatial pipeline...")

            # Connect to DuckDB and load spatial extension
            con = duckdb.connect(database=":memory:")
            con.execute("INSTALL spatial;")
            con.execute("LOAD spatial;")

            # Register the pandas dataframes into the duckdb environment
            con.register("df_in", df_in)

            need_parks = not df_parks.empty and any(
                [
                    valid_updates.get("PARK_CODE"),
                    valid_updates.get("PARK_NAME"),
                    valid_updates.get("LOCATION_CODE"),
                    valid_updates.get("LOCATION_NAME"),
                ]
            )
            need_locs = not df_locs.empty and any(
                [valid_updates.get("LOCATION_CODE"), valid_updates.get("LOCATION_NAME")]
            )
            need_mgmt = not df_mgmt.empty and any(
                [valid_updates.get("MGMT_AREA"), valid_updates.get("MGMT_REGION")]
            )
            need_trails = not df_trails.empty and any([valid_updates.get("TRAIL_NAME")])

            if need_parks:
                con.register("df_parks", df_parks)
            if need_locs:
                con.register("df_locs", df_locs)
            if need_mgmt:
                con.register("df_mgmt", df_mgmt)
            if need_trails:
                con.register("df_trails", df_trails)

            # Construct DuckDB CTE SQL Query
            query_ctes = [
                "base AS (SELECT OBJECTID, ST_GeomFromWKB(GEOMWKB) AS geom FROM df_in WHERE GEOMWKB IS NOT NULL)"
            ]

            if need_parks:
                query_ctes.append("""
                parks AS (SELECT PARK_CODE, PARK_NAME, ST_GeomFromWKB(GEOMWKB) AS geom FROM df_parks WHERE GEOMWKB IS NOT NULL),
                park_intersect AS (
                    SELECT s.OBJECTID, p.PARK_CODE, p.PARK_NAME
                    FROM base s
                    JOIN parks p ON ST_Intersects(s.geom, p.geom)
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.OBJECTID ORDER BY ST_Area(ST_Intersection(s.geom, p.geom)) DESC) = 1
                ),
                park_nearest AS (
                    SELECT s.OBJECTID, p_near.PARK_CODE AS PARK_CODE_near, p_near.PARK_NAME AS PARK_NAME_near, p_near.dist_to_park
                    FROM base s
                    CROSS JOIN LATERAL (
                        SELECT p.PARK_CODE, p.PARK_NAME, ST_Distance(s.geom, p.geom) AS dist_to_park
                        FROM parks p
                        ORDER BY dist_to_park ASC
                        LIMIT 1
                    ) p_near
                )
                """)

            if need_locs:
                query_ctes.append("""
                locs AS (SELECT PL_CODE, PL_NAME, ST_GeomFromWKB(GEOMWKB) AS geom FROM df_locs WHERE GEOMWKB IS NOT NULL),
                loc_intersect AS (
                    SELECT s.OBJECTID, l.PL_CODE, l.PL_NAME
                    FROM base s
                    JOIN locs l ON ST_Intersects(s.geom, l.geom)
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.OBJECTID ORDER BY ST_Area(ST_Intersection(s.geom, l.geom)) DESC) = 1
                )
                """)

            if need_mgmt:
                query_ctes.append("""
                mgmt AS (SELECT MGMT_AREA, MGMT_REGION, ST_GeomFromWKB(GEOMWKB) AS geom FROM df_mgmt WHERE GEOMWKB IS NOT NULL),
                mgmt_intersect AS (
                    SELECT s.OBJECTID, m.MGMT_AREA, m.MGMT_REGION
                    FROM base s
                    JOIN mgmt m ON ST_Intersects(s.geom, m.geom)
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY s.OBJECTID ORDER BY ST_Area(ST_Intersection(s.geom, m.geom)) DESC) = 1
                )
                """)

            if need_trails:
                query_ctes.append("""
                trails AS (SELECT TRAIL_NAME, ST_GeomFromWKB(GEOMWKB) AS geom FROM df_trails WHERE GEOMWKB IS NOT NULL),
                trail_nearest AS (
                    SELECT s.OBJECTID, tn.TRAIL_NAME AS TRAIL_NAME_near, tn.dist_to_trail
                    FROM base s
                    CROSS JOIN LATERAL (
                        SELECT t.TRAIL_NAME, ST_Distance(s.geom, t.geom) AS dist_to_trail
                        FROM trails t
                        ORDER BY dist_to_trail ASC
                        LIMIT 1
                    ) tn
                )
                """)

            cte_string = "WITH " + ",\n".join(query_ctes)

            # Build Select statements following the COALESCE logic
            selects = ["SELECT b.OBJECTID"]

            if valid_updates.get("PARK_CODE"):
                selects.append("""
                ,COALESCE(
                    pi.PARK_CODE
                    ,CASE WHEN pn.dist_to_park <= 100 THEN pn.PARK_CODE_near ELSE NULL END
                ) AS PARK_CODE
                """)
            if valid_updates.get("PARK_NAME"):
                selects.append("""
                ,COALESCE(
                    pi.PARK_NAME
                    ,CASE WHEN pn.dist_to_park <= 100 THEN pn.PARK_NAME_near ELSE NULL END
                ) AS PARK_NAME
                """)
            if valid_updates.get("LOCATION_CODE"):
                selects.append("""
                ,COALESCE(
                    li.PL_CODE
                    ,pi.PARK_CODE
                    ,CASE WHEN pn.dist_to_park <= 100 THEN pn.PARK_CODE_near ELSE NULL END
                ) AS LOCATION_CODE
                """)
            if valid_updates.get("LOCATION_NAME"):
                selects.append("""
                ,COALESCE(
                    li.PL_NAME
                    ,pi.PARK_NAME
                    ,CASE WHEN pn.dist_to_park <= 100 THEN pn.PARK_NAME_near ELSE NULL END
                ) AS LOCATION_NAME
                """)
            if valid_updates.get("MGMT_AREA"):
                selects.append(",mi.MGMT_AREA")
            if valid_updates.get("MGMT_REGION"):
                selects.append(",mi.MGMT_REGION")
            if valid_updates.get("TRAIL_NAME"):
                selects.append("""
                ,CASE WHEN tn.dist_to_trail <= 50 THEN tn.TRAIL_NAME_near ELSE NULL END AS TRAIL_NAME
                """)

            # Join logic
            joins = ["FROM base b"]
            if need_parks:
                joins.append("INNER JOIN park_intersect pi ON b.OBJECTID = pi.OBJECTID")
                joins.append("INNER JOIN park_nearest pn ON b.OBJECTID = pn.OBJECTID")
            if need_locs:
                joins.append("INNER JOIN loc_intersect li ON b.OBJECTID = li.OBJECTID")
            if need_mgmt:
                joins.append("INNER JOIN mgmt_intersect mi ON b.OBJECTID = mi.OBJECTID")
            if need_trails:
                joins.append("INNER JOIN trail_nearest tn ON b.OBJECTID = tn.OBJECTID")

            full_query = (
                cte_string + "\n" + "\n".join(selects) + "\n" + "\n".join(joins)
            )

            # Execute DuckDB query and return to Pandas DataFrame
            df_out = con.execute(full_query).df()
            con.close()

            # Map results to dict for cursor
            update_map = {}
            for index, row in df_out.iterrows():
                oid = row["OBJECTID"]
                update_map[oid] = row.to_dict()

            # UpdateCursor writeback using dynamically mapped schema
            arcpy.AddMessage("Step 4: UpdateCursor writeback...")

            # Use the destination field names for the cursor
            cursor_fields = ["OID@"] + list(valid_updates.values())
            valid_where = def_query if def_query else None

            with arcpy.da.UpdateCursor(
                in_layer, cursor_fields, where_clause=valid_where
            ) as cur:
                for row in cur:
                    oid = row[0]
                    vals = update_map.get(oid)
                    if vals:
                        new_row = [oid]

                        # Read from the DuckDB output (src_attr) and write to Cursor
                        for src_attr, dest_field in valid_updates.items():
                            val = vals.get(src_attr)
                            new_row.append(None if pd.isna(val) else val)

                        cur.updateRow(new_row)

            # Safely destroy cursor references
            try:
                del row
            except NameError:
                pass
            del cur

        finally:
            arcpy.AddMessage("Cleaning up memory and releasing locks...")

            try:
                con.close()
            except Exception:
                pass

            # Delete variables to free up physical RAM
            try:
                del df_in
                del df_out
                del update_map
            except NameError:
                pass

            # Force Python garbage collection
            gc.collect()

            arcpy.AddMessage("Done with this layer.")
