import logging
import arcpy

from arcgis.gis import GIS
from src.utils.auth import get_portal_token

from .models import BatchValueReplaceConfig

logger = logging.getLogger("batch_value_replace.connection")


def connect_gis(config: BatchValueReplaceConfig) -> GIS:
    portal_url = ""
    try:
        portal_url = arcpy.GetActivePortalURL() or ""
    except Exception:
        pass

    token = get_portal_token()

    logger.info(f"Connecting to active portal URL: {portal_url or 'pro'}")
    try:
        if portal_url:
            if token:
                gis = GIS(url=portal_url, token=token)
            else:
                gis = GIS(url=portal_url)
        else:
            gis = GIS("pro")
        logger.info(f"Successfully connected to active portal: {gis.url}")
        return gis
    except Exception as e:
        logger.error(f"Failed to connect to active portal '{portal_url}': {e}")
        raise ConnectionError(
            f"Failed to connect to active portal '{portal_url}': {e}"
        ) from e
