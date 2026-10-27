from pathlib import Path

import arcpy
import urllib3

from src.utils.auth import get_portal_token
from src.utils.batch_value_replace import BatchValueReplaceConfig, run_scan, run_update

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class ArcPyMessageHandler:
    def info(self, message):
        arcpy.AddMessage(message)

    def warning(self, message):
        arcpy.AddWarning(message)

    def error(self, message):
        arcpy.AddError(message)


class BatchValueReplace:
    def __init__(self):
        self.label = "Batch Value Replace"
        self.description = (
            "Scans ArcGIS feature layers and tables for specified old values "
            "and optionally replaces approved values with corrected values across parameterized fields, "
            "authenticating via the active ArcGIS Pro portal session."
        )
        self.canRunInBackground = True

    def getParameterInfo(self):
        params = []

        p_mode = arcpy.Parameter(
            displayName="Operation Mode",
            name="mode",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )
        p_mode.filter.type = "ValueList"
        p_mode.filter.list = ["Scan", "Update"]
        p_mode.value = "Scan"
        params.append(p_mode)

        p_crosswalk = arcpy.Parameter(
            displayName="Value Crosswalk",
            name="crosswalk",
            datatype="DEFile",
            parameterType="Required",
            direction="Input",
        )
        if p_crosswalk.filter:
            p_crosswalk.filter.list = ["csv"]
        params.append(p_crosswalk)

        p_output = arcpy.Parameter(
            displayName="Output Excel Workbook",
            name="output_workbook",
            datatype="DEFile",
            parameterType="Required",
            direction="Output",
        )
        if p_output.filter:
            p_output.filter.list = ["xlsx"]
        params.append(p_output)

        p_fields = arcpy.Parameter(
            displayName="Target Fields",
            name="fields",
            datatype="GPString",
            parameterType="Required",
            direction="Input",
        )
        p_fields.value = "PARK_NAME,LOCATION_NAME"
        params.append(p_fields)

        p_owner = arcpy.Parameter(
            displayName="Item Owner",
            name="owner",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
        )
        params.append(p_owner)

        p_item_id = arcpy.Parameter(
            displayName="Feature Service Item ID",
            name="item_id",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
        )
        params.append(p_item_id)

        p_candidate_layer_limit = arcpy.Parameter(
            displayName="Candidate Layer Limit",
            name="candidate_layer_limit",
            datatype="GPLong",
            parameterType="Optional",
            direction="Input",
        )
        params.append(p_candidate_layer_limit)

        p_discovery_item_limit = arcpy.Parameter(
            displayName="Discovery Item Limit",
            name="discovery_item_limit",
            datatype="GPLong",
            parameterType="Optional",
            direction="Input",
        )
        params.append(p_discovery_item_limit)

        p_discovery_page_size = arcpy.Parameter(
            displayName="Discovery Page Size",
            name="discovery_page_size",
            datatype="GPLong",
            parameterType="Required",
            direction="Input",
        )
        p_discovery_page_size.value = 50
        params.append(p_discovery_page_size)

        p_query_batch_size = arcpy.Parameter(
            displayName="Query Batch Size",
            name="query_batch_size",
            datatype="GPLong",
            parameterType="Required",
            direction="Input",
        )
        p_query_batch_size.value = 100
        params.append(p_query_batch_size)

        p_edit_batch_size = arcpy.Parameter(
            displayName="Edit Batch Size",
            name="edit_batch_size",
            datatype="GPLong",
            parameterType="Optional",
            direction="Input",
        )
        p_edit_batch_size.value = 100
        params.append(p_edit_batch_size)

        p_update_report = arcpy.Parameter(
            displayName="Reviewed Update Workbook",
            name="update_report",
            datatype="DEFile",
            parameterType="Optional",
            direction="Input",
        )
        if p_update_report.filter:
            p_update_report.filter.list = ["xlsx"]
        params.append(p_update_report)

        p_exclude_tags = arcpy.Parameter(
            displayName="Exclude Tags",
            name="exclude_tags",
            datatype="GPString",
            parameterType="Optional",
            direction="Input",
        )
        params.append(p_exclude_tags)

        p_log_file = arcpy.Parameter(
            displayName="Log File",
            name="log_file",
            datatype="DEFile",
            parameterType="Optional",
            direction="Input",
        )
        params.append(p_log_file)

        return params

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        mode = parameters[0].valueAsText
        if mode == "Update":
            parameters[11].enabled = True
            parameters[10].enabled = True
            parameters[10].enabled = True
        else:
            parameters[11].enabled = False

    def updateMessages(self, parameters):
        csv_path = parameters[1].valueAsText
        output_path = parameters[2].valueAsText
        mode = parameters[0].valueAsText
        update_report = parameters[11].valueAsText
        cand_limit = parameters[6].value
        disc_item_limit = parameters[7].value
        disc_page_size = parameters[8].value

        if csv_path:
            if not Path(csv_path).exists():
                parameters[1].setErrorMessage(
                    f"Crosswalk CSV file does not exist: {csv_path}"
                )

        if output_path and not output_path.lower().endswith(".xlsx"):
            parameters[2].setErrorMessage("Output workbook must be an .xlsx file.")

        if mode == "Update" and not update_report:
            parameters[11].setErrorMessage(
                "Reviewed Update Workbook is required for Update mode."
            )
        elif mode == "Update" and update_report and not Path(update_report).exists():
            parameters[11].setErrorMessage(
                f"Reviewed Update Workbook does not exist: {update_report}"
            )

        if cand_limit is not None and cand_limit < 1:
            parameters[6].setErrorMessage(
                "Candidate layer limit must be greater than zero."
            )

        if disc_item_limit is not None and disc_item_limit < 1:
            parameters[7].setErrorMessage(
                "Discovery item limit must be greater than zero."
            )

        if disc_page_size is not None and disc_page_size < 1:
            parameters[8].setErrorMessage(
                "Discovery page size must be greater than zero."
            )

    def execute(self, parameters, messages):
        mode = parameters[0].valueAsText.lower()
        csv_path = Path(parameters[1].valueAsText)
        output_path = Path(parameters[2].valueAsText)
        fields = parameters[3].valueAsText or "PARK_NAME,LOCATION_NAME"
        owner = parameters[4].valueAsText if parameters[4].valueAsText else None
        item_id = parameters[5].valueAsText if parameters[5].valueAsText else None
        candidate_layer_limit = (
            parameters[6].value if parameters[6].value is not None else None
        )
        discovery_item_limit = (
            parameters[7].value if parameters[7].value is not None else None
        )
        discovery_page_size = (
            int(parameters[8].value) if parameters[8].value is not None else 50
        )
        query_batch_size = (
            int(parameters[9].value) if parameters[9].value is not None else 100
        )
        edit_batch_size = (
            int(parameters[10].value) if parameters[10].value is not None else 100
        )
        update_report = (
            Path(parameters[11].valueAsText) if parameters[11].valueAsText else None
        )
        exclude_tags = (
            parameters[12].valueAsText if parameters[12].valueAsText else None
        )
        log_file = (
            Path(parameters[13].valueAsText) if parameters[13].valueAsText else None
        )

        config = BatchValueReplaceConfig(
            mode=mode,
            csv_path=csv_path,
            output_path=output_path,
            fields=fields,
            owner=owner,
            item_id=item_id,
            candidate_layer_limit=candidate_layer_limit,
            discovery_item_limit=discovery_item_limit,
            discovery_page_size=discovery_page_size,
            query_batch_size=query_batch_size,
            edit_batch_size=edit_batch_size,
            update_report=update_report,
            log_file=log_file,
            exclude_tags=exclude_tags,
        )

        handler = ArcPyMessageHandler()

        try:
            if mode == "scan":
                result = run_scan(config, handler)
            else:
                result = run_update(config, handler)

            arcpy.AddMessage(f"Output workbook: {result.output_path}")
        except Exception as e:
            arcpy.AddError(f"Tool execution failed: {e}")
            raise

    def postExecute(self, parameters):
        return
