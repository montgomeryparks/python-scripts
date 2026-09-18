import re
from typing import Literal

import arcpy
import duckdb
import pandas as pd
from shapely import wkb

from src.utils.data_conversion import (
    _norm,
    featureclass_to_df,
    map_pandas_dtype_to_arcpy,
)
from src.utils.sql import (
    extract_where_clauses,
    get_referenced_fields,
    sanitize_where_clause,
)


class ExecuteDuckDBSQL:
    def __init__(self):
        self.label = "Execute DuckDB Spatial SQL"
        self.description = "Executes DuckDB SQL against active map layers with attribute pushdown and native spatial support."
        self.canRunInBackground = False

    def getParameterInfo(self):
        param_query = arcpy.Parameter(
            displayName="SQL Query",
            name="sql_query",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
        )
        param_query.controlCLSID = "{E5456E51-0C41-4797-9EE4-5269820C6F0E}"

        param_sql_file = arcpy.Parameter(
            displayName="SQL File (.sql)",
            name="sql_file",
            datatype="DEFile",
            parameterType="Optional",
            direction="Input",
        )
        param_sql_file.filter.list = ["sql", "txt"]  # pyright: ignore[reportOptionalMemberAccess]

        param_output_name = arcpy.Parameter(
            displayName="Output Dataset Name",
            name="output_name",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )
        param_output_name.value = "duckdb_result"

        param_derived_out = arcpy.Parameter(
            displayName="Output Dataset",
            name="out_dataset",
            datatype=["GPFeatureLayer", "GPTableView"],
            parameterType="Derived",
            direction="Output",
        )

        return [param_query, param_sql_file, param_output_name, param_derived_out]

    def isLicensed(self):
        return True

    def updateMessages(self, parameters):
        """Provides real-time SQL syntax and schema validation before execution."""
        query_param = parameters[0]
        file_param = parameters[1]

        query_param.clearMessage()
        file_param.clearMessage()

        query_text = query_param.valueAsText
        sql_file = file_param.valueAsText

        if query_text and sql_file:
            err_msg = "Provide either a SQL Query OR a SQL File, not both. Please clear one of the inputs."
            query_param.setErrorMessage(err_msg)
            file_param.setErrorMessage(err_msg)
            return

        query = None
        if sql_file:
            try:
                with open(sql_file, "r", encoding="utf-8") as f:
                    query = f.read()
            except Exception as e:
                file_param.setErrorMessage(f"Could not read file: {e}")
                return
        elif query_text:
            query = query_text

        if query and len(query.strip()) > 5:
            try:
                con = duckdb.connect(database=":memory:")
                con.execute("INSTALL spatial; LOAD spatial;")

                try:
                    aprx = arcpy.mp.ArcGISProject("CURRENT")
                    active_map = aprx.activeMap
                    if active_map:
                        for lyr in active_map.listLayers() + active_map.listTables():
                            if getattr(lyr, "isFeatureLayer", False) or getattr(
                                lyr, "isTable", False
                            ):
                                clean_name = lyr.name.replace(" ", "_")
                                if not re.search(
                                    rf"\b{re.escape(clean_name)}\b",
                                    query,
                                    re.IGNORECASE,
                                ):
                                    continue

                                fields = [
                                    f'"{f.name}" VARCHAR'
                                    for f in arcpy.ListFields(lyr)
                                    if f.type not in ("Geometry", "Raster", "Blob")
                                ]
                                if getattr(lyr, "isFeatureLayer", False):
                                    fields.append("GEOM GEOMETRY")
                                if fields:
                                    con.execute(
                                        f"CREATE TABLE {clean_name} ({', '.join(fields)})"
                                    )
                except Exception:
                    pass

                con.execute(f"PREPARE v1 AS {query}")
            except Exception as e:
                err_msg = str(e)
                clean_msg = (
                    err_msg.split("\n")[0]
                    .replace("Binder Error: ", "")
                    .replace("Parser Error: ", "")
                )
                if (
                    "Parser Error" in err_msg
                    or "syntax error" in err_msg.lower()
                    or "Binder Error" in err_msg
                ):
                    target_param = file_param if sql_file else query_param
                    target_param.setErrorMessage(f"SQL Error: {clean_msg}")

    def execute(self, parameters, messages):
        arcpy.env.overwriteOutput = True

        arcpy.SetProgressor("default", "Initializing tool and validating inputs...")

        query_text = parameters[0].valueAsText
        sql_file = parameters[1].valueAsText
        raw_out_name = parameters[2].valueAsText

        if query_text and sql_file:
            arcpy.AddError("Provide either a SQL Query OR a SQL File, not both.")
            return

        if sql_file:
            with open(sql_file, "r", encoding="utf-8") as f:
                query = f.read()
        elif query_text:
            query = query_text
        else:
            arcpy.AddError(
                "You must provide either a SQL query string or load a .sql file."
            )
            return

        out_name = re.sub(r"[^a-zA-Z0-9_]", "_", raw_out_name.strip())
        if re.match(r"^[0-9]", out_name):
            out_name = f"tbl_{out_name}"

        if out_name != raw_out_name:
            arcpy.AddWarning(
                f"Output Dataset Name '{raw_out_name}' was scrubbed to '{out_name}' to meet workspace naming rules."
            )

        aprx = arcpy.mp.ArcGISProject("CURRENT")
        active_map = aprx.activeMap
        if not active_map:
            arcpy.AddError("No active map found.")
            return

        arcpy.SetProgressorLabel("Initializing DuckDB engine and spatial extensions...")
        con = duckdb.connect(database=":memory:")
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
        except Exception as e:
            arcpy.AddWarning(f"Spatial extension could not be loaded: {e}")

        layer_field_map = {}
        referenced_layers = []

        for lyr in active_map.listLayers() + active_map.listTables():
            if getattr(lyr, "isFeatureLayer", False) or getattr(lyr, "isTable", False):
                clean_name = lyr.name.replace(" ", "_")

                if re.search(rf"\b{re.escape(clean_name)}\b", query, re.IGNORECASE):
                    layer_field_map[clean_name] = [
                        f
                        for f in arcpy.ListFields(lyr)
                        if f.type not in ("Geometry", "Raster", "Blob")
                    ]
                    referenced_layers.append((clean_name, lyr))

        pushdown_filters = extract_where_clauses(query, layer_field_map)

        # Sort layers so those with explicit WHERE clauses load first (driving layers)
        referenced_layers.sort(key=lambda x: 0 if pushdown_filters.get(x[0], "") else 1)

        # Check if the query implies spatial predicate pushdown is beneficial
        build_dynamic_extent = bool(
            re.search(
                r"\bST_(Intersects|Within|Contains|Crosses|Touches|Overlaps|DWithin)\b",
                query,
                re.IGNORECASE,
            )
        )
        cumulative_extent = None

        # Respect explicit arcpy.env.extent bounding box constraints if the user set them
        env_ext = arcpy.env.extent
        if not isinstance(env_ext, str) and env_ext and env_ext.XMin is not None:
            cumulative_extent = arcpy.Polygon(
                arcpy.Array(
                    [
                        arcpy.Point(env_ext.XMin, env_ext.YMin),
                        arcpy.Point(env_ext.XMin, env_ext.YMax),
                        arcpy.Point(env_ext.XMax, env_ext.YMax),
                        arcpy.Point(env_ext.XMax, env_ext.YMin),
                    ]
                ),
                active_map.spatialReference,
            )

        for clean_name, lyr in referenced_layers:
            arcpy.SetProgressorLabel(f"Reading '{clean_name}' into memory...")

            raw_filter = pushdown_filters.get(clean_name, "")
            cursor_where = sanitize_where_clause(lyr, raw_filter)
            layer_fields = layer_field_map[clean_name]
            selected_field_names = get_referenced_fields(query, layer_fields)

            spatial_filter = None
            if cumulative_extent and getattr(lyr, "isFeatureLayer", False):
                spatial_filter = cumulative_extent
                arcpy.AddMessage(
                    f"Pushing down spatial bounding box filter to '{clean_name}'"
                )

            try:
                df = featureclass_to_df(
                    in_table=lyr,
                    fields=selected_field_names,
                    out_sr=active_map.spatialReference.factoryCode,
                    where_clause=cursor_where,
                    spatial_filter=spatial_filter,
                    spatial_rel="INTERSECTS",
                )
                df = _norm(df, df.columns)

                # If this layer acts as a driving layer (has a WHERE clause), aggregate its bounding box
                if (
                    build_dynamic_extent
                    and cursor_where
                    and getattr(lyr, "isFeatureLayer", False)
                    and not df.empty
                    and "GEOMWKB" in df.columns
                ):
                    try:
                        geoms = [wkb.loads(w) for w in df["GEOMWKB"] if w]
                        if geoms:
                            minx = min(g.bounds[0] for g in geoms)
                            miny = min(g.bounds[1] for g in geoms)
                            maxx = max(g.bounds[2] for g in geoms)
                            maxy = max(g.bounds[3] for g in geoms)

                            layer_env = arcpy.Polygon(
                                arcpy.Array(
                                    [
                                        arcpy.Point(minx, miny),
                                        arcpy.Point(minx, maxy),
                                        arcpy.Point(maxx, maxy),
                                        arcpy.Point(maxx, miny),
                                    ]
                                ),
                                active_map.spatialReference,
                            )

                            cumulative_extent = (
                                cumulative_extent.union(layer_env)
                                if cumulative_extent
                                else layer_env
                            )
                    except Exception as e:
                        arcpy.AddWarning(
                            f"Could not calculate extent for {clean_name}: {e}"
                        )

                raw_name = f"_raw_{clean_name}"
                con.register(raw_name, df)

                if "GEOMWKB" in df.columns:
                    view_sql = f"CREATE VIEW {clean_name} AS SELECT * EXCLUDE (GEOMWKB), ST_GeomFromWKB(GEOMWKB) AS GEOM FROM {raw_name}"
                    con.execute(view_sql)
                else:
                    con.execute(f"CREATE VIEW {clean_name} AS SELECT * FROM {raw_name}")

                arcpy.AddMessage(f"Registered {clean_name} ({len(df)} rows)")
            except Exception as e:
                arcpy.AddWarning(
                    f"Failed to register {lyr.name}: {str(e).splitlines()[0]}"
                )

        arcpy.SetProgressorLabel("Executing DuckDB SQL...")
        arcpy.AddMessage("Executing DuckDB SQL...")

        try:
            con.execute(f"CREATE TEMP TABLE _user_result AS {query}")
        except Exception as e:
            clean_msg = (
                str(e)
                .split("\n")[0]
                .replace("Binder Error: ", "")
                .replace("Parser Error: ", "")
            )
            arcpy.AddError(f"SQL Error: {clean_msg}")
            return

        col_info = con.execute("PRAGMA table_info('_user_result')").fetchall()

        select_cols = []
        spatial_cols = []

        for col in col_info:
            col_name = col[1]
            col_type = col[2]
            if col_type == "GEOMETRY":
                select_cols.append(f'ST_AsWKB("{col_name}") AS "{col_name}"')
                spatial_cols.append(col_name)
            else:
                select_cols.append(f'"{col_name}"')

        final_query = f"SELECT {', '.join(select_cols)} FROM _user_result"
        res_df = con.execute(final_query).df()

        if res_df.empty:
            arcpy.AddWarning("Query executed successfully but returned 0 rows.")
            return

        arcpy.SetProgressorLabel("Writing results to map dataset...")
        out_path = f"memory\\{out_name}"

        for map_lyr in active_map.listLayers():
            if map_lyr.name == out_name:
                active_map.removeLayer(map_lyr)
        for map_tbl in active_map.listTables():
            if map_tbl.name == out_name:
                active_map.removeTable(map_tbl)

        arcpy.management.Delete(out_path)

        spatial_ref = active_map.spatialReference
        shape_col = spatial_cols[0] if spatial_cols else None

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

        if shape_col and shape_col in res_df.columns:
            try:
                geom_query = f'SELECT ST_GeometryType("{shape_col}") FROM _user_result WHERE "{shape_col}" IS NOT NULL LIMIT 1'
                geom_res = con.execute(geom_query).fetchone()
                if geom_res and geom_res[0]:
                    out_geom = duckdb_geom_map.get(geom_res[0], "POLYGON")
            except Exception:
                pass

            arcpy.management.CreateFeatureclass(
                "memory", out_name, out_geom, spatial_reference=spatial_ref
            )
            is_spatial_output = True
        else:
            arcpy.management.CreateTable("memory", out_name)
            if shape_col in res_df.columns:
                res_df.drop(columns=[shape_col], inplace=True)

        attribute_cols = [c for c in res_df.columns if c != shape_col]

        for col in attribute_cols:
            field_type = map_pandas_dtype_to_arcpy(res_df[col].dtype)
            arcpy.management.AddField(
                out_path,
                col,
                field_type,
                field_length=500 if field_type == "TEXT" else None,
            )

        if is_spatial_output:
            assert shape_col is not None
            insert_fields = attribute_cols + ["SHAPE@"]
            with arcpy.da.InsertCursor(out_path, insert_fields) as inserter:
                for _, row in res_df.iterrows():
                    geom = (
                        arcpy.FromWKB(row[shape_col])
                        if pd.notnull(row[shape_col])
                        else None
                    )
                    attrs = [
                        row[c] if pd.notnull(row[c]) else None for c in attribute_cols
                    ]
                    inserter.insertRow(attrs + [geom])
            arcpy.AddMessage(
                f"Created Spatial Layer: {out_path} (Auto-detected: {out_geom})"
            )
        else:
            if attribute_cols:
                with arcpy.da.InsertCursor(out_path, attribute_cols) as inserter:
                    for _, row in res_df.iterrows():
                        attrs = [
                            row[c] if pd.notnull(row[c]) else None
                            for c in attribute_cols
                        ]
                        inserter.insertRow(attrs)
            arcpy.AddMessage(f"Created Table: {out_path}")

        parameters[3].value = out_path
        arcpy.ResetProgressor()
