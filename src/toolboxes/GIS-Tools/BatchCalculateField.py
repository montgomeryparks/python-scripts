import arcpy


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
        param_layers.description = "Select one or more feature classes or tables to update with the same calculation."

        param_field = arcpy.Parameter(
            displayName="Target Field Name",
            name="field_name",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )
        param_field.description = "Enter the field name that will receive the calculated value in each selected layer."

        param_expr = arcpy.Parameter(
            displayName="Expression",
            name="expression",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )
        param_expr.description = "Type the value or expression to apply to the target field during the calculation."

        param_exp_type = arcpy.Parameter(
            displayName="Expression Type",
            name="expression_type",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )

        # Satisfy Pylance lint by ensuring the filter object is not None before assignment
        if param_exp_type.filter:
            param_exp_type.filter.type = "ValueList"
            param_exp_type.filter.list = ["PYTHON3", "ARCADE", "SQL"]
            param_exp_type.value = "PYTHON3"
        param_exp_type.description = "Choose the expression language used by the field calculation."

        param_code = arcpy.Parameter(
            displayName="Code Block",
            name="code_block",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
        )
        param_code.description = "Optionally provide helper logic for expressions that require a code block or custom function."

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
