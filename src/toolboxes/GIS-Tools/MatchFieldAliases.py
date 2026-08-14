import arcpy


class MatchFieldAliases(object):
    def __init__(self):
        self.label = "Match Field Aliases"
        self.description = (
            "Updates the field aliases of a target layer to match those of a "
            "source layer wherever they share the exact same field name."
        )
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""

        # Parameter 0: Source Layer
        param_source = arcpy.Parameter(
            displayName="Source Layer (Contains the desired aliases)",
            name="in_source",
            datatype=["GPFeatureLayer", "GPTableView", "DEFeatureClass", "DETable"],
            parameterType="Required",
            direction="Input",
        )
        param_source.description = "Choose the source layer or table that contains the field aliases to copy."

        # Parameter 1: Target Layer
        param_target = arcpy.Parameter(
            displayName="Target Layer (Layer to be updated)",
            name="in_target",
            datatype=["GPFeatureLayer", "GPTableView", "DEFeatureClass", "DETable"],
            parameterType="Required",
            direction="Input",
        )
        param_target.description = "Choose the layer or table whose field aliases will be updated to match the source dataset."

        return [param_source, param_target]

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed."""
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""

        source_layer = parameters[0].valueAsText
        target_layer = parameters[1].valueAsText

        # 1. Build a dictionary mapping Source Field Name -> Source Field Alias
        arcpy.AddMessage("Reading fields from the source layer...")
        source_fields = arcpy.ListFields(source_layer)

        # Only grab fields where the alias actually exists and differs from the base name (optional, but good practice)
        alias_mapping = {}
        for f in source_fields:
            if f.aliasName:
                alias_mapping[f.name.lower()] = f.aliasName

        if not alias_mapping:
            arcpy.AddWarning("No valid aliases found in the source layer.")
            return

        # 2. Iterate through Target Fields and update if a match is found
        arcpy.AddMessage("Checking target layer fields for matches...")
        target_fields = arcpy.ListFields(target_layer)

        update_count = 0

        for t_field in target_fields:
            t_name_lower = t_field.name.lower()

            # Check if the target field name exists in our source mapping
            if t_name_lower in alias_mapping:
                new_alias = alias_mapping[t_name_lower]

                # Only attempt to alter if the alias is actually different
                if t_field.aliasName != new_alias:
                    try:
                        # AlterField updates the geodatabase schema
                        arcpy.management.AlterField(
                            in_table=target_layer,
                            field=t_field.name,
                            new_field_alias=new_alias,
                        )
                        arcpy.AddMessage(
                            f"  [SUCCESS] Updated alias for '{t_field.name}' to '{new_alias}'."
                        )
                        update_count += 1

                    except Exception as e:
                        # Usually fails on protected/system fields (e.g., OBJECTID, Shape) or due to schema locks
                        arcpy.AddWarning(
                            f"  [SKIPPED] Could not update '{t_field.name}'. It may be a locked system field. Error: {str(e)}"
                        )

        arcpy.AddMessage(
            f"--- Complete. Successfully updated {update_count} field aliases. ---"
        )
        return
