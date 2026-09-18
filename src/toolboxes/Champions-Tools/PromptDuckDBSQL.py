import logging

import arcpy

from src.utils.arc_logging import setup_arcgis_logging

logger = logging.getLogger(__name__)


class PromptDuckDBSQL:
    def __init__(self):
        self.label = "Prompt DuckDB SQL"
        self.description = (
            "Scans all layers and tables in the active web map and produces a "
            "Markdown report of names, schemas, data types, and distinct text "
            "values. Intended to be pasted into an AI chat (MS365 CoPilot, "
            "Gemini) so the user can request DuckDB SQL queries to test with the "
            "sister 'Execute DuckDB Spatial SQL' tool."
        )
        self.canRunInBackground = False

    def getParameterInfo(self):
        param_output = arcpy.Parameter(
            displayName="Output Markdown File",
            name="output_file",
            datatype="DEFile",
            parameterType="Required",
            direction="Input",
        )
        param_output.filter.list = ["md", "markdown"]  # pyright: ignore[reportOptionalMemberAccess]
        param_output.value = "LayerSchemaReport.md"

        param_max_distinct = arcpy.Parameter(
            displayName="Max Distinct Values per Text Field",
            name="max_distinct",
            datatype="GPLong",
            parameterType="Optional",
            direction="Input",
        )
        param_max_distinct.value = 50

        return [param_output, param_max_distinct]

    def updateMessages(self, parameters):
        output_param = parameters[0]
        max_distinct_param = parameters[1]

        output_param.clearMessage()
        max_distinct_param.clearMessage()

        max_val = max_distinct_param.valueAsText
        if max_val:
            try:
                v = int(max_val)
                if v < 1 or v > 10000:
                    max_distinct_param.setErrorMessage(
                        "Max Distinct Values must be between 1 and 10000."
                    )
            except ValueError:
                max_distinct_param.setErrorMessage(
                    "Max Distinct Values must be a whole number."
                )

    def isLicensed(self):
        return True

    def _get_arcpy_type(self, field):
        """Map arcpy field type to a short human-readable string."""
        t = field.type
        if t == "String":
            return "TEXT"
        if t == "SmallInteger":
            return "SMALLINT"
        if t == "Integer":
            return "INTEGER"
        if t == "Single":
            return "FLOAT"
        if t == "Double":
            return "DOUBLE"
        if t == "Date":
            return "DATE"
        if t == "GUID":
            return "GUID"
        if t == "Blob":
            return "BLOB"
        if t == "Raster":
            return "RASTER"
        if t == "Geometry":
            return "GEOMETRY"
        if t == "OID":
            return "OID"
        return t.upper() if t else "UNKNOWN"

    def _sample_distinct(self, cursor_fields, rows, field_name, max_distinct):
        """Collect up to max_distinct distinct non-null values for a field."""
        idx = cursor_fields.index(field_name)
        seen = set()
        result = []
        for row in rows:
            val = row[idx]
            if val is None:
                continue
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            key = str(val)
            if key not in seen:
                seen.add(key)
                result.append(key)
                if len(result) >= max_distinct:
                    break
        return result

    def execute(self, parameters, messages):
        setup_arcgis_logging(logging.DEBUG)
        logger.info("PromptDuckDBSQL execution started.")

        arcpy.env.overwriteOutput = True

        arcpy.SetProgressor("default", "Scanning active map layers...")

        output_file = parameters[0].valueAsText
        max_distinct = int(parameters[1].valueAsText or "50")

        aprx = arcpy.mp.ArcGISProject("CURRENT")
        active_map = aprx.activeMap
        if not active_map:
            logger.error("No active map found.")
            return

        layers = active_map.listLayers() + active_map.listTables()
        if not layers:
            logger.warning("No layers or tables found in the active map.")
            return

        lines: list[str] = []
        lines.append("# Web Map Layer Schema Report")
        lines.append("")
        lines.append(
            "This report describes every layer/table visible in the active web "
            "map. Copy the entire contents and paste it into an AI chat (MS365 "
            "CoPilot, Gemini, etc.) along with a request such as:\n"
            '"Write a DuckDB SQL query that ..." and then test the generated '
            "SQL with the companion tool **Execute DuckDB Spatial SQL**."
        )
        lines.append("---")
        lines.append("")

        total_rows_all = 0
        total_layers = 0

        for lyr in layers:
            is_fl = getattr(lyr, "isFeatureLayer", False)
            is_tbl = getattr(lyr, "isTable", False)
            if not (is_fl or is_tbl):
                continue

            clean_name = lyr.name.replace(" ", "_")
            total_layers += 1
            arcpy.SetProgressorLabel(f"Scanning '{lyr.name}'...")

            fields = [f for f in arcpy.ListFields(lyr) if f.type != "Geometry"]
            if not fields:
                logger.warning(f"No readable fields on '{lyr.name}'; skipping.")
                continue

            # Row count
            try:
                row_count = int(arcpy.GetCount_management(lyr).getOutput(0))
            except (RuntimeError, OSError):
                row_count = -1
                logger.warning(f"Could not count rows for '{lyr.name}'.")

            total_rows_all += max(row_count, 0)

            lines.append(f"## {clean_name}")
            lines.append("")
            feat_type = "Feature Layer" if is_fl else "Table"
            lines.append(f"- **Type**: {feat_type}")
            lines.append(f"- **Row Count**: {row_count}")
            if is_fl:
                try:
                    sr = lyr.spatialReference
                    lines.append(f"- **Spatial Reference**: {sr.name}")
                except (RuntimeError, OSError):
                    pass
            lines.append("")

            # Schema table
            lines.append("| Field | Type | Nullable | Alias | Length |")
            lines.append("|-------|------|----------|-------|--------|")
            for f in fields:
                alias = (f.aliasName or f.name).replace("|", "\\|")
                lines.append(
                    f"| `{f.name}` | {self._get_arcpy_type(f)} "
                    f"| {'YES' if f.isNullable else 'NO'} "
                    f"| {alias} "
                    f"| {f.length or '-'} |"
                )
            lines.append("")

            # Sample distinct text values for text fields
            text_fields = [f.name for f in fields if f.type == "String"]
            if text_fields:
                lines.append("### Distinct Value Samples")
                lines.append("")
                cursor_fields = text_fields[:]
                try:
                    with arcpy.da.SearchCursor(lyr, cursor_fields) as cur:
                        rows = list(cur)
                    for tf in text_fields:
                        samples = self._sample_distinct(
                            cursor_fields, rows, tf, max_distinct
                        )
                        lines.append(f"**{tf}** ({len(samples)} distinct)")
                        lines.append("")
                        if samples:
                            lines.append("```")
                            for s in samples:
                                safe = s.replace("`", "\\`")
                                lines.append(safe)
                            lines.append("```")
                        else:
                            lines.append("_no non-null values_")
                        lines.append("")
                except (RuntimeError, OSError) as e:
                    logger.warning(f"Could not sample values for '{lyr.name}': {e}")
                    lines.append("_Could not sample values._")
                    lines.append("")

            lines.append("---")
            lines.append("")

        # Summary
        lines.append("## Summary")
        lines.append("")
        lines.append(f"- **Layers/Tables scanned**: {total_layers}")
        lines.append(f"- **Total rows (approx.)**: {total_rows_all}")
        lines.append("")
        lines.append(
            "Use this report with the **Execute DuckDB Spatial SQL** tool by "
            "pasting the schema info into an AI chat and asking for the SQL you "
            "need."
        )
        lines.append("")

        report_md = "\n".join(lines)

        # Ensure directory exists
        out_dir = arcpy.env.scratchFolder
        import os

        os.makedirs(out_dir, exist_ok=True)
        final_path = os.path.join(out_dir, os.path.basename(output_file))
        if not final_path.lower().endswith(".md"):
            final_path += ".md"

        with open(final_path, "w", encoding="utf-8") as f:
            f.write(report_md)

        arcpy.AddMessage(f"Markdown report written to: {final_path}")
        logger.info(f"Report saved to {final_path}")
        arcpy.SetProgressorLabel("Done.")
        arcpy.ResetProgressor()
