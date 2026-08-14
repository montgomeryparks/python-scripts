import gc

import arcpy
import duckdb
import pandas as pd

from src.utils.auth import get_portal_token
from src.utils.constants import URL_MGMT_BOUNDARIES, URL_PARK_LOCATIONS, URL_PARK_UNITS
from src.utils.data_conversion import _norm, featureclass_to_df, featureservice_to_df


class UpdateGeographicFields(object):
    def __init__(self):
        self.label = "Update Park, Location, and Management Fields"
        self.description = "Updates the specified standard text fields for the input layer(s) using an in-memory DuckDB spatial engine."
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
        params.append(in_layers)

        # Field update toggles
        fields_to_add = [
            ("update_park_code", "Update PARK_CODE", "PARK_CODE"),
            ("update_park_name", "Update PARK_NAME", "PARK_NAME"),
            ("update_loc_code", "Update LOCATION_CODE", "LOCATION_CODE"),
            ("update_loc_name", "Update LOCATION_NAME", "LOCATION_NAME"),
            ("update_mgmt_area", "Update MGMT_AREA", "MGMT_AREA"),
            ("update_mgmt_region", "Update MGMT_REGION", "MGMT_REGION"),
        ]

        for name, display, _ in fields_to_add:
            param = arcpy.Parameter(
                displayName=display,
                name=name,
                datatype="GPBoolean",
                parameterType="Optional",
                direction="Input",
            )
            param.value = True  # Checked by default
            params.append(param)

        param_def_query = arcpy.Parameter(
            displayName="Definition Query (Optional)",
            name="def_query",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
        )
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

        global_update_flags = {
            "PARK_CODE": parameters[1].value,
            "PARK_NAME": parameters[2].value,
            "LOCATION_CODE": parameters[3].value,
            "LOCATION_NAME": parameters[4].value,
            "MGMT_AREA": parameters[5].value,
            "MGMT_REGION": parameters[6].value,
        }

        def_query = parameters[7].valueAsText if parameters[7].value else ""

        token = get_portal_token()

        df_parks = pd.DataFrame()
        df_locs = pd.DataFrame()
        df_mgmt = pd.DataFrame()

        if any(
            [
                global_update_flags["PARK_CODE"],
                global_update_flags["PARK_NAME"],
                global_update_flags["LOCATION_CODE"],
                global_update_flags["LOCATION_NAME"],
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
            [global_update_flags["LOCATION_CODE"], global_update_flags["LOCATION_NAME"]]
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

        if any([global_update_flags["MGMT_AREA"], global_update_flags["MGMT_REGION"]]):
            arcpy.AddMessage("Fetch Management Boundaries...")
            df_mgmt = featureservice_to_df(
                url=URL_MGMT_BOUNDARIES,
                out_fields=["MGMT_AREA", "MGMT_REGION"],
                out_sr=2248,
                where="1=1",
                token=token,
            )
            df_mgmt = _norm(df_mgmt, ["MGMT_AREA", "MGMT_REGION"])

        # process each input layer
        for i, in_layer in enumerate(in_layers, start=1):
            in_layer = in_layer.strip("'")  # remove quotes if present
            arcpy.AddMessage(f"--- Processing {i}/{len(in_layers)}: {in_layer} ---")
            self._process_input_layer(
                in_layer, df_parks, df_locs, df_mgmt, global_update_flags, def_query
            )

        arcpy.AddMessage("Done.")

    def postExecute(self, parameters):
        return

    def _process_input_layer(
        self, in_layer, df_parks, df_locs, df_mgmt, global_update_flags, def_query
    ):
        update_flags = global_update_flags.copy()

        target_fields = [f for f, do_update in update_flags.items() if do_update]
        if not target_fields:
            arcpy.AddMessage("No fields selected for update. Skipping layer.")
            return

        try:
            oid_field = arcpy.Describe(in_layer).OIDFieldName
            arcpy.AddMessage(f"OBJECTID field for layer is: {oid_field}")

            # Ensure fields exist - If missing, dynamically treat as unchecked
            arcpy.AddMessage("Step 1: Check existing fields...")
            existing = {f.name for f in arcpy.ListFields(in_layer)}

            for f in list(target_fields):
                if f not in existing:
                    arcpy.AddWarning(
                        f"Field '{f}' does not exist in {in_layer}. Treating as unchecked."
                    )
                    update_flags[f] = False

            target_fields = [f for f, do_update in update_flags.items() if do_update]
            if not target_fields:
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
                    update_flags.get("PARK_CODE"),
                    update_flags.get("PARK_NAME"),
                    update_flags.get("LOCATION_CODE"),
                    update_flags.get("LOCATION_NAME"),
                ]
            )
            need_locs = not df_locs.empty and any(
                [update_flags.get("LOCATION_CODE"), update_flags.get("LOCATION_NAME")]
            )
            need_mgmt = not df_mgmt.empty and any(
                [update_flags.get("MGMT_AREA"), update_flags.get("MGMT_REGION")]
            )

            if need_parks:
                con.register("df_parks", df_parks)
            if need_locs:
                con.register("df_locs", df_locs)
            if need_mgmt:
                con.register("df_mgmt", df_mgmt)

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

            cte_string = "WITH " + ",\n".join(query_ctes)

            # Build Select statements following the COALESCE logic
            selects = ["SELECT b.OBJECTID"]

            if update_flags.get("PARK_CODE"):
                selects.append("""
                ,COALESCE(
                    pi.PARK_CODE
                    ,CASE WHEN pn.dist_to_park <= 100 THEN pn.PARK_CODE_near ELSE NULL END
                ) AS PARK_CODE
                """)
            if update_flags.get("PARK_NAME"):
                selects.append("""
                ,COALESCE(
                    pi.PARK_NAME
                    ,CASE WHEN pn.dist_to_park <= 100 THEN pn.PARK_NAME_near ELSE NULL END
                ) AS PARK_NAME
                """)
            if update_flags.get("LOCATION_CODE"):
                selects.append("""
                ,COALESCE(
                    li.PL_CODE
                    ,pi.PARK_CODE
                    ,CASE WHEN pn.dist_to_park <= 100 THEN pn.PARK_CODE_near ELSE NULL END
                ) AS LOCATION_CODE
                """)
            if update_flags.get("LOCATION_NAME"):
                selects.append("""
                ,COALESCE(
                    li.PL_NAME
                    ,pi.PARK_NAME
                    ,CASE WHEN pn.dist_to_park <= 100 THEN pn.PARK_NAME_near ELSE NULL END
                ) AS LOCATION_NAME
                """)
            if update_flags.get("MGMT_AREA"):
                selects.append(",mi.MGMT_AREA")
            if update_flags.get("MGMT_REGION"):
                selects.append(",mi.MGMT_REGION")

            # Join logic
            joins = ["FROM base b"]
            if need_parks:
                joins.append("INNER JOIN park_intersect pi ON b.OBJECTID = pi.OBJECTID")
                joins.append("INNER JOIN park_nearest pn ON b.OBJECTID = pn.OBJECTID")
            if need_locs:
                joins.append("INNER JOIN loc_intersect li ON b.OBJECTID = li.OBJECTID")
            if need_mgmt:
                joins.append("INNER JOIN mgmt_intersect mi ON b.OBJECTID = mi.OBJECTID")

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

            # UpdateCursor writeback (using the definition query directly on the cursor)
            arcpy.AddMessage("Step 4: UpdateCursor writeback...")
            cursor_fields = ["OID@"] + target_fields
            valid_where = def_query if def_query else None

            with arcpy.da.UpdateCursor(
                in_layer, cursor_fields, where_clause=valid_where
            ) as cur:
                for row in cur:
                    oid = row[0]
                    vals = update_map.get(oid)
                    if vals:
                        new_row = [oid]
                        for f in target_fields:
                            val = vals.get(f)
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
