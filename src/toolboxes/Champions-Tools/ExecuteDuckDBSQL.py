import logging
import re

import arcpy
import duckdb
from shapely import wkb

from src.utils.arc_logging import setup_arcgis_logging
from src.utils.data_conversion import (
    _norm,
    duckdb_to_memory,
    featureclass_to_df,
)
from src.utils.sql import (
    extract_where_clauses,
    get_referenced_fields,
    sanitize_where_clause,
)

logger = logging.getLogger("champion_tools")


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
        param_sql_file.filter.list = ["sql", "txt"]

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
            except (OSError, UnicodeError) as e:
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
                except (OSError, RuntimeError, duckdb.Error):
                    pass

                con.execute(f"CREATE TEMP TABLE _val_check AS {query}")

                col_info = con.execute("PRAGMA table_info('_val_check')").fetchall()
                geom_cols = [c[1] for c in col_info if c[2] == "GEOMETRY"]
                if len(geom_cols) > 1:
                    target_param = file_param if sql_file else query_param
                    target_param.setErrorMessage(
                        f"SQL Error: Multiple geometry columns returned ({', '.join(geom_cols)}). ArcGIS only supports one geometry field."
                    )

            except (duckdb.Error, OSError, RuntimeError) as e:
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
                    or "duplicate column name" in err_msg.lower()
                ):
                    target_param = file_param if sql_file else query_param
                    target_param.setErrorMessage(f"SQL Error: {clean_msg}")

    def execute(self, parameters, messages):
        if not logger.handlers and not logging.getLogger().handlers:
            setup_arcgis_logging(logging.DEBUG, "champion_tools")
        else:
            logger.handlers.clear()
            setup_arcgis_logging(logging.DEBUG, "champion_tools")

        logger.info("DuckDB tool execution started.")

        arcpy.env.overwriteOutput = True
        arcpy.SetProgressor("default", "Initializing tool and validating inputs...")

        query_text = parameters[0].valueAsText
        sql_file = parameters[1].valueAsText
        raw_out_name = parameters[2].valueAsText

        if query_text and sql_file:
            logger.error("Provide either a SQL Query OR a SQL File, not both.")
            return

        if sql_file:
            with open(sql_file, "r", encoding="utf-8") as f:
                query = f.read()
        elif query_text:
            query = query_text
        else:
            logger.error(
                "You must provide either a SQL query string or load a .sql file."
            )
            return

        aprx = arcpy.mp.ArcGISProject("CURRENT")
        active_map = aprx.activeMap
        if not active_map:
            logger.error("No active map found.")
            return

        arcpy.SetProgressorLabel("Initializing DuckDB engine and spatial extensions...")
        con = duckdb.connect(database=":memory:")
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
        except duckdb.Error as e:
            logger.warning(f"Spatial extension could not be loaded: {e}")

        layer_field_map = {}
        referenced_layers = []
        oid_mapping = {}

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

                    desc = arcpy.Describe(lyr)
                    if hasattr(desc, "OIDFieldName"):
                        oid_name = desc.OIDFieldName.lower()
                        is_64bit = getattr(desc, "hasOID64", False)
                        oid_mapping[oid_name] = is_64bit

        pushdown_filters = extract_where_clauses(query, layer_field_map)

        referenced_layers.sort(key=lambda x: 0 if pushdown_filters.get(x[0], "") else 1)

        build_dynamic_extent = bool(
            re.search(
                r"\bST_(Intersects|Within|Contains|Crosses|Touches|Overlaps|DWithin)\b",
                query,
                re.IGNORECASE,
            )
        )
        cumulative_extent = None

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

            if cursor_where:
                logger.info(
                    f"Applying native Geodatabase filter to {clean_name}: {cursor_where}"
                )

            layer_fields = layer_field_map[clean_name]
            selected_field_names = get_referenced_fields(query, layer_fields)

            spatial_filter = None
            if cumulative_extent and getattr(lyr, "isFeatureLayer", False):
                spatial_filter = cumulative_extent
                logger.info(
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
                    except (TypeError, ValueError, RuntimeError) as e:
                        logger.warning(
                            f"Could not calculate extent for {clean_name}: {e}"
                        )

                raw_name = f"_raw_{clean_name}"
                con.register(raw_name, df)

                if "GEOMWKB" in df.columns:
                    view_sql = f"CREATE VIEW {clean_name} AS SELECT * EXCLUDE (GEOMWKB), ST_GeomFromWKB(GEOMWKB) AS GEOM FROM {raw_name}"
                    con.execute(view_sql)
                else:
                    con.execute(f"CREATE VIEW {clean_name} AS SELECT * FROM {raw_name}")

                logger.info(f"Registered {clean_name} ({len(df)} rows)")
            except (TypeError, ValueError, RuntimeError, duckdb.Error) as e:
                logger.warning(
                    f"Failed to register {lyr.name}: {str(e).splitlines()[0]}"
                )

        arcpy.SetProgressorLabel("Executing DuckDB SQL and writing to map dataset...")
        logger.info("Executing DuckDB SQL...")

        try:
            out_path = duckdb_to_memory(
                con=con,
                source_query_or_table=query,
                raw_out_name=raw_out_name,
                spatial_ref=active_map.spatialReference,
                active_map=active_map,
                oid_mapping=oid_mapping,
            )
        except (duckdb.Error, ValueError) as e:
            clean_msg = (
                str(e)
                .split("\n")[0]
                .replace("Binder Error: ", "")
                .replace("Parser Error: ", "")
            )
            logger.error(f"SQL Error: {clean_msg}")
            return

        if not out_path:
            logger.warning("Query executed successfully but returned 0 rows.")
            return

        parameters[3].value = out_path
        arcpy.ResetProgressor()
