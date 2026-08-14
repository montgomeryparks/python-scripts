import concurrent.futures
import json
import re

import arcpy
import pandas as pd
from arcgis.gis import GIS


class FindWebMapsUsingServices(object):
    def __init__(self):
        self.label = "Find Web Maps Using Services"
        self.description = "Searches the active portal for Web Maps containing specific Feature or Map Services using concurrent threads, then exports a report to Excel."
        self.canRunInBackground = True

    def getParameterInfo(self):
        param_services = arcpy.Parameter(
            displayName="Target Service URLs or Layers",
            name="in_services",
            datatype=["GPFeatureLayer", "GPLayer", "GPString"],
            parameterType="Required",
            direction="Input",
            multiValue=True,
        )
        param_services.description = (
            "Select one or more Feature Services, Map Services, or layer references to search for across the active portal's Web Maps. "
            "You can provide service URLs directly or choose layers from the current project."
        )

        param_output = arcpy.Parameter(
            displayName="Output Excel File",
            name="out_excel",
            datatype="DEFile",
            parameterType="Required",
            direction="Output",
        )
        param_output.description = "Choose the destination .xlsx file that will contain the list of matching Web Maps and their metadata."
        if param_output.filter:
            param_output.filter.list = ["xlsx"]

        return [param_services, param_output]

    def isLicensed(self):
        return True

    def updateParameters(self, parameters):
        return

    def updateMessages(self, parameters):
        return

    def _analyze_map(self, web_map, target_pattern, portal_url):
        """Worker function executed by each thread to fetch and evaluate a single map."""
        try:
            map_json_dict = web_map.get_data(False)
            if not map_json_dict:
                return None

            map_json_str = json.dumps(map_json_dict)

            if re.search(target_pattern, map_json_str, re.IGNORECASE):
                return {
                    "map_title": web_map.title,
                    "map_id": web_map.id,
                    "map_owner": web_map.owner,
                    "map_url": web_map.homepage,
                    "map_views": web_map.numViews,
                    "item_details_url": f"{portal_url}/home/item.html?id={web_map.id}",
                }
        except Exception as e:
            return {
                "error": f"Could not read data for Web Map '{web_map.title}' ({web_map.id}): {e}"
            }

        return None

    def execute(self, parameters, messages):
        in_services_text = parameters[0].valueAsText
        out_excel = parameters[1].valueAsText

        raw_inputs = in_services_text.split(";")
        target_urls = []

        for item in raw_inputs:
            item = item.strip("'\" ")
            if item.startswith("http"):
                target_urls.append(item)
            else:
                try:
                    desc = arcpy.Describe(item)
                    if hasattr(desc, "url") and desc.url:
                        target_urls.append(desc.url)
                    elif hasattr(desc, "catalogPath") and str(
                        desc.catalogPath
                    ).startswith("http"):
                        target_urls.append(desc.catalogPath)
                    elif hasattr(desc, "path") and str(desc.path).startswith("http"):
                        target_urls.append(desc.path)
                    else:
                        arcpy.AddWarning(
                            f"Could not extract a web service URL from layer '{item}'. Skipping."
                        )
                except Exception as e:
                    arcpy.AddWarning(
                        f"Could not process input '{item}': {e}. Skipping."
                    )

        target_urls = list(set([url for url in target_urls if url]))

        if not target_urls:
            arcpy.AddError("No valid URLs were found or extracted from the inputs.")
            return

        arcpy.SetProgressor("default", "Authenticating with active portal...")

        gis = GIS("pro")
        portal_url = gis.url.rstrip("/")
        arcpy.AddMessage(f"Authenticated to: {portal_url}")

        arcpy.SetProgressorLabel("Searching for Web Maps...")
        web_maps = gis.content.search(query="", item_type="Web Map", max_items=10000)
        total_maps = len(web_maps)

        arcpy.AddMessage(
            f"Found {total_maps} Web Maps. Analyzing contents concurrently..."
        )
        arcpy.SetProgressor("step", "Analyzing Web Maps...", 0, total_maps, 1)

        target_pattern = "|".join([re.escape(url) for url in target_urls])
        map_data = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_map = {
                executor.submit(
                    self._analyze_map, web_map, target_pattern, portal_url
                ): web_map
                for web_map in web_maps
            }

            for future in concurrent.futures.as_completed(future_to_map):
                arcpy.SetProgressorPosition()
                result = future.result()

                if result:
                    if "error" in result:
                        arcpy.AddWarning(result["error"])
                    else:
                        map_data.append(result)

        if not map_data:
            arcpy.AddWarning("No Web Maps found utilizing the provided services.")
            return

        arcpy.SetProgressorLabel("Exporting to Excel...")

        df = pd.DataFrame(map_data)
        df.to_excel(out_excel, index=False)

        arcpy.AddMessage(f"Successfully exported {len(df)} Web Maps to {out_excel}")
        arcpy.ResetProgressor()

        return
