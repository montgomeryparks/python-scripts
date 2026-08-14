import csv
import os

import arcpy


class ImportSynapseSchema(object):
    def __init__(self):
        self.label = "Create Feature Class from Synapse CSV"
        self.description = "Creates an ArcGIS Feature Class using a schema exported from Azure Synapse SQL metadata."
        self.canRunInBackground = False

    def getParameterInfo(self):
        # Parameter 0: Input CSV File
        param_csv = arcpy.Parameter(
            displayName="Input Synapse Metadata CSV",
            name="in_csv",
            datatype="DEFile",
            parameterType="Required",
            direction="Input",
        )
        param_csv.filter.list = ["csv"]

        # Parameter 1: Target Geodatabase
        param_gdb = arcpy.Parameter(
            displayName="Target Geodatabase / Workspace",
            name="target_gdb",
            datatype="DEWorkspace",
            parameterType="Required",
            direction="Input",
        )

        # Parameter 2: Output Feature Class Name
        param_name = arcpy.Parameter(
            displayName="Output Feature Class Name",
            name="out_name",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )

        # Parameter 3: Execution Action (Overwrite or Add)
        param_action = arcpy.Parameter(
            displayName="Action",
            name="action",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )
        param_action.filter.list = ["Overwrite", "Add Fields"]
        param_action.value = "Overwrite"

        # Parameter 4: Geometry Type
        param_geom = arcpy.Parameter(
            displayName="Geometry Type",
            name="geometry_type",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )
        param_geom.filter.list = ["POINT", "MULTIPOINT", "POLYLINE", "POLYGON", "Null"]
        param_geom.value = "POINT"

        # Parameter 5: Spatial Reference
        param_sr = arcpy.Parameter(
            displayName="Spatial Reference",
            name="spatial_reference",
            datatype="GPSpatialReference",
            parameterType="Required",
            direction="Input",
        )
        # Defaulting to WKID 2248
        param_sr.value = arcpy.SpatialReference(2248)

        # Parameter 6: Fields to Exclude (Exact Name)
        param_ex_names = arcpy.Parameter(
            displayName="Fields to Exclude (Exact Name)",
            name="exclude_names",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
            multiValue=True,
        )
        param_ex_names.value = ["OBJECTID", "Shape", "GlobalID", "GEOMWKB", "GEOMWKT"]

        # Parameter 7: Field Prefixes to Exclude
        param_ex_prefixes = arcpy.Parameter(
            displayName="Field Prefixes to Exclude",
            name="exclude_prefixes",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
            multiValue=True,
        )
        param_ex_prefixes.value = ["GEOM_", "INGEST_"]

        # Parameter 8: Field Suffixes to Exclude
        param_ex_suffixes = arcpy.Parameter(
            displayName="Field Suffixes to Exclude",
            name="exclude_suffixes",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
            multiValue=True,
        )
        param_ex_suffixes.value = ["_AREAS"]

        return [
            param_csv,
            param_gdb,
            param_name,
            param_action,
            param_geom,
            param_sr,
            param_ex_names,
            param_ex_prefixes,
            param_ex_suffixes,
        ]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        # Retrieve parameter values
        csv_path = parameters[0].valueAsText
        target_gdb = parameters[1].valueAsText
        out_name = parameters[2].valueAsText
        action = parameters[3].valueAsText
        geometry_type = parameters[4].valueAsText
        spatial_ref = parameters[5].value

        exclude_names = (
            parameters[6].valueAsText.split(";") if parameters[6].valueAsText else []
        )
        exclude_prefixes = (
            parameters[7].valueAsText.split(";") if parameters[7].valueAsText else []
        )
        exclude_suffixes = (
            parameters[8].valueAsText.split(";") if parameters[8].valueAsText else []
        )

        exclude_names = [n.strip().lower() for n in exclude_names]
        exclude_prefixes = [p.strip().lower() for p in exclude_prefixes]
        exclude_suffixes = [s.strip().lower() for s in exclude_suffixes]

        target_fc = os.path.join(target_gdb, out_name)

        # 1. Read and Sort CSV to determine total steps for the Progressor
        metadata_rows = []
        with open(csv_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                metadata_rows.append(row)

        try:
            metadata_rows.sort(key=lambda x: int(x["RowOrder"]))
        except KeyError:
            messages.addErrorMessage(
                "CSV does not contain 'RowOrder' column. Ensure your Synapse view uses ROW_NUMBER()."
            )
            return

        # Initialize Progressor: 1 step for FC Creation/Check + len(metadata_rows) for fields
        total_steps = 1 + len(metadata_rows)
        arcpy.SetProgressor(
            "step", "Initializing schema configuration...", 0, total_steps, 1
        )
        current_step = 0

        # 2. Handle Feature Class Creation or Overwrite
        if arcpy.Exists(target_fc):
            if action == "Overwrite":
                arcpy.SetProgressorLabel(
                    f"Overwriting existing Feature Class: {out_name}"
                )
                messages.addMessage(f"Deleting existing Feature Class: {target_fc}")
                arcpy.management.Delete(target_fc)
                arcpy.management.CreateFeatureclass(
                    out_path=target_gdb,
                    out_name=out_name,
                    geometry_type=geometry_type,
                    spatial_reference=spatial_ref,
                )
            else:
                arcpy.SetProgressorLabel(
                    f"Appending to existing Feature Class: {out_name}"
                )
                messages.addMessage(
                    f"Feature Class {out_name} exists. Appending fields."
                )
        else:
            arcpy.SetProgressorLabel(f"Creating Feature Class: {out_name}")
            messages.addMessage(f"Creating Feature Class: {target_fc}")
            arcpy.management.CreateFeatureclass(
                out_path=target_gdb,
                out_name=out_name,
                geometry_type=geometry_type,
                spatial_reference=spatial_ref,
            )

        current_step += 1
        arcpy.SetProgressorPosition(current_step)

        # 3. Iterate and add fields
        for row in metadata_rows:
            current_step += 1
            arcpy.SetProgressorPosition(current_step)

            col_name = row.get("COLUMN_NAME")
            if not col_name:
                continue

            arcpy.SetProgressorLabel(f"Evaluating field: {col_name}")

            col_name_lower = col_name.lower()
            data_type = row.get("DATA_TYPE", "").lower()

            if col_name_lower in exclude_names:
                messages.addMessage(f"Skipping (Exact Match): {col_name}")
                continue
            if any(col_name_lower.startswith(prefix) for prefix in exclude_prefixes):
                messages.addMessage(f"Skipping (Prefix Match): {col_name}")
                continue
            if any(col_name_lower.endswith(suffix) for suffix in exclude_suffixes):
                messages.addMessage(f"Skipping (Suffix Match): {col_name}")
                continue

            max_len = row.get("CHARACTER_MAXIMUM_LENGTH")
            field_length = (
                int(max_len)
                if max_len and max_len.isdigit() and int(max_len) > 0
                else 255
            )

            clean_name = arcpy.ValidateFieldName(col_name, target_gdb)

            if data_type in ["varchar", "nvarchar", "char", "text"]:
                field_type = "TEXT"
            elif data_type in ["int"]:
                field_type = "LONG"
            elif data_type in ["bigint"]:
                field_type = "BIGINTEGER"
            elif data_type in ["smallint", "tinyint"]:
                field_type = "SHORT"
            elif data_type in ["decimal", "numeric", "float", "real"]:
                field_type = "DOUBLE"
            elif data_type in ["datetime", "datetime2"]:
                field_type = "DATE"
            elif data_type in ["date"]:
                field_type = "DATEONLY"
            elif data_type in ["datetimeoffset"]:
                field_type = "TIMESTAMPOFFSET"
            else:
                messages.addWarningMessage(
                    f"Skipping unmapped system/spatial data type: {col_name} ({data_type})"
                )
                continue

            messages.addMessage(f"Adding field: {clean_name} ({field_type})")

            try:
                arcpy.management.AddField(
                    in_table=target_fc,
                    field_name=clean_name,
                    field_type=field_type,
                    field_length=field_length if field_type == "TEXT" else None,
                )
            except Exception as e:
                messages.addErrorMessage(f"Failed to add field {clean_name}: {str(e)}")

        arcpy.SetProgressorLabel("Processing complete.")
        messages.addMessage("Processing complete.")
        return
