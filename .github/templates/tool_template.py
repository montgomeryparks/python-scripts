import arcpy

# The bundler will dynamically inject these during the build process
# from src.utils.data_conversion import featureclass_to_df
# from src.utils.auth import get_portal_token

class TemplateTool(object):
    def __init__(self):
        self.label = "Tool Name"
        self.description = "Tool Description."
        self.canRunInBackground = True

    def getParameterInfo(self):
        # NOTE: Enforce leading commas for all parameter definitions
        param_example = arcpy.Parameter(
            displayName="Example Parameter"
            ,name="example_param"
            ,datatype="GPString"
            ,parameterType="Required"
            ,direction="Input"
        )
        
        params = [param_example]
        return params

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def execute(self, parameters, messages):
        # Retrieve parameters
        example_val = parameters[0].valueAsText

        # Initialize progressor
        arcpy.SetProgressor("step", "Initializing...", 0, 100, 1)

        try:
            arcpy.AddMessage(f"Executing with {example_val}")
            
        except arcpy.ExecuteError:
            arcpy.AddError(arcpy.GetMessages(2))
        except Exception as e:
            arcpy.AddError(f"Unexpected error: {str(e)}")
        finally:
            arcpy.ResetProgressor()
            
        return