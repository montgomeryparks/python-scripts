import arcpy


class AddGeographicFields(object):
    def __init__(self):
        self.label = "Add Park, Location, and Management Fields"
        self.description = "Adds the specified standard text fields to the input layer(s) if they do not already exist."
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

        # Field add toggles
        fields_to_add = [
            ("add_park_code", "Add PARK_CODE", "PARK_CODE"),
            ("add_park_name", "Add PARK_NAME", "PARK_NAME"),
            ("add_loc_code", "Add LOCATION_CODE", "LOCATION_CODE"),
            ("add_loc_name", "Add LOCATION_NAME", "LOCATION_NAME"),
            ("add_mgmt_area", "Add MGMT_AREA", "MGMT_AREA"),
            ("add_mgmt_region", "Add MGMT_REGION", "MGMT_REGION"),
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

        return params

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        # Dynamically uncheck boxes if the field already exists in the selected layer(s)
        if parameters[0].value:
            in_layers_text = parameters[0].valueAsText
            in_layers = in_layers_text.split(";") if in_layers_text else []

            field_map = {
                "PARK_CODE": 1,
                "PARK_NAME": 2,
                "LOCATION_CODE": 3,
                "LOCATION_NAME": 4,
                "MGMT_AREA": 5,
                "MGMT_REGION": 6,
            }

            # Determine which fields exist across all selected layers
            fields_in_all = set(field_map.keys())

            for in_layer in in_layers:
                in_layer = in_layer.strip("'")
                try:
                    # Use upper() to normalize case
                    existing = {f.name.upper() for f in arcpy.ListFields(in_layer)}
                    fields_in_all = fields_in_all.intersection(existing)
                except Exception:
                    pass

            # If a field already exists, uncheck its parameter (unless user manually altered it)
            for f_name, p_idx in field_map.items():
                if not parameters[p_idx].altered:
                    parameters[p_idx].value = f_name not in fields_in_all
        return

    def updateMessages(self, parameters):
        if parameters[0].value:
            parameters[0].setWarningMessage(
                "Adding fields requires an exclusive schema lock. Ensure the Attribute Table "
                "is closed before running."
            )
        return

    def execute(self, parameters, messages):
        in_layers_text = parameters[0].valueAsText
        in_layers = in_layers_text.split(";") if in_layers_text else []

        flags = {
            "PARK_CODE": parameters[1].value,
            "PARK_NAME": parameters[2].value,
            "LOCATION_CODE": parameters[3].value,
            "LOCATION_NAME": parameters[4].value,
            "MGMT_AREA": parameters[5].value,
            "MGMT_REGION": parameters[6].value,
        }

        # Defined standard lengths for string fields to keep schema consistent
        field_lengths = {
            "PARK_CODE": 30,
            "PARK_NAME": 80,
            "LOCATION_CODE": 30,
            "LOCATION_NAME": 80,
            "MGMT_AREA": 15,
            "MGMT_REGION": 15,
        }

        field_aliases = {
            "PARK_NAME": "Park Name",
            "PARK_CODE": "Park Code",
            "LOCATION_NAME": "Location Name",
            "LOCATION_CODE": "Location Code",
            "MGMT_AREA": "Managagement Area",
            "MGMT_REGION": "Managagement Region",
        }

        target_fields = [f for f, do_add in flags.items() if do_add]

        if not target_fields:
            arcpy.AddWarning("No fields selected to add.")
            return

        for i, in_layer in enumerate(in_layers, start=1):
            in_layer = in_layer.strip("'")
            arcpy.AddMessage(f"--- Processing {i}/{len(in_layers)}: {in_layer} ---")

            existing_fields = {f.name.upper() for f in arcpy.ListFields(in_layer)}

            for f in target_fields:
                if f.upper() not in existing_fields:
                    arcpy.AddMessage(f"Adding field: {f} (Length: {field_lengths[f]})")
                    arcpy.management.AddField(
                        in_table=in_layer,
                        field_name=f,
                        field_type="TEXT",
                        field_length=field_lengths[f],
                        field_alias=field_aliases[f],
                    )
                else:
                    arcpy.AddMessage(f"Field '{f}' already exists. Skipping.")

        arcpy.AddMessage("Done.")

    def postExecute(self, parameters):
        return


class BatchCalculateField(object):
    def __init__(self):
        self.label = "Batch Calculate Field"
        self.description = "Applies the exact same Calculate Field statement across multiple input feature layers. Skips layers that do not have the target field."
        self.canRunInBackground = True

    def getParameterInfo(self):
        param_layers = arcpy.Parameter(
            displayName="Input Layers or Tables",
            name="in_layers",
            datatype=["GPFeatureLayer", "GPTableView"],
            parameterType="Required",
            direction="Input",
            multiValue=True,
        )

        param_field = arcpy.Parameter(
            displayName="Target Field Name",
            name="field_name",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )

        param_expr = arcpy.Parameter(
            displayName="Expression",
            name="expression",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )

        param_exp_type = arcpy.Parameter(
            displayName="Expression Type",
            name="expression_type",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )
        param_exp_type.filter.type = "ValueList"
        param_exp_type.filter.list = ["PYTHON3", "ARCADE", "SQL"]
        param_exp_type.value = "PYTHON3"

        param_code = arcpy.Parameter(
            displayName="Code Block",
            name="code_block",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
        )

        params = [param_layers, param_field, param_expr, param_exp_type, param_code]
        return params

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        in_layers_raw = parameters[0].valueAsText.split(";")
        field_name = parameters[1].valueAsText
        expression = parameters[2].valueAsText
        expression_type = parameters[3].valueAsText
        code_block = parameters[4].valueAsText if parameters[4].value else ""

        arcpy.SetProgressor(
            "step", "Initializing batch calculation...", 0, len(in_layers_raw), 1
        )

        for layer in in_layers_raw:
            layer_clean = layer.replace("'", "").strip()

            arcpy.AddMessage(f"Evaluating: {layer_clean}")
            arcpy.SetProgressorLabel(
                f"Checking for field '{field_name}' in {layer_clean}..."
            )

            existing_fields = [f.name.lower() for f in arcpy.ListFields(layer_clean)]

            if field_name.lower() not in existing_fields:
                arcpy.AddWarning(
                    f"  -> SKIPPED: The field '{field_name}' does not exist in '{layer_clean}'."
                )
                arcpy.SetProgressorPosition()
                continue

            try:
                arcpy.SetProgressorLabel(
                    f"Calculating field '{field_name}' for {layer_clean}..."
                )

                arcpy.management.CalculateField(
                    in_table=layer_clean,
                    field=field_name,
                    expression=expression,
                    expression_type=expression_type,
                    code_block=code_block,
                )

                arcpy.AddMessage(
                    f"  -> Successfully calculated '{field_name}' in {layer_clean}"
                )

            except arcpy.ExecuteError:
                arcpy.AddError(f"  -> Failed to calculate field for {layer_clean}.")
                arcpy.AddError(arcpy.GetMessages(2))
            except Exception as e:
                arcpy.AddError(
                    f"  -> Failed to calculate field for {layer_clean}. Error: {str(e)}"
                )

            arcpy.SetProgressorPosition()

        arcpy.ResetProgressor()
        arcpy.AddMessage("Batch calculation routine complete.")
        return
