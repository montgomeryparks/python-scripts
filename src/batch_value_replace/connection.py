import logging
from arcgis.gis import GIS
from .models import BatchValueReplaceConfig

logger = logging.getLogger("batch_value_replace.connection")

def connect_gis(config: BatchValueReplaceConfig) -> GIS:
    if config.portal_url:
        logger.info(f"Connecting to explicit portal: {config.portal_url}")
        try:
            if config.username:
                gis = GIS(url=config.portal_url, username=config.username)
            else:
                gis = GIS(url=config.portal_url)
            logger.info(f"Successfully connected to explicit portal: {gis.url}")
            return gis
        except Exception as e:
            logger.error(f"Failed to connect to explicit portal '{config.portal_url}': {e}")
            raise ConnectionError(f"Failed to connect to explicit portal '{config.portal_url}': {e}") from e

    profile_name = config.profile or "work"
    logger.info(f"Connecting using ArcGIS profile: {profile_name}")
    try:
        gis = GIS(profile=profile_name)
        logger.info(f"Successfully connected using profile '{profile_name}' to {gis.url}")
        return gis
    except Exception as e:
        logger.error(f"Failed to connect using ArcGIS profile '{profile_name}': {e}")
        raise ConnectionError(f"Failed to connect using ArcGIS profile '{profile_name}': {e}") from e
