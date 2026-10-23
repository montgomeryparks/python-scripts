import logging

import arcpy


class ArcGISLogHandler(logging.Handler):
    def emit(self, record):
        msg = self.format(record)
        if record.levelno >= logging.ERROR:
            arcpy.AddError(msg)
        elif record.levelno >= logging.WARNING:
            arcpy.AddWarning(msg)
        else:
            arcpy.AddMessage(msg)


def setup_arcgis_logging(
    level: int = logging.DEBUG, logger_name: str = "biomon_tools"
) -> logging.Logger:
    """Configures a named logger to send messages to ArcGIS, avoiding duplicates."""
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    # CRITICAL: Stop messages from bubbling up to the polluted global root logger
    logger.propagate = False

    # Wipe any existing handlers from previous runs in the persistent Pro session
    logger.handlers.clear()

    handler = ArcGISLogHandler()
    formatter = logging.Formatter("%(levelname)s: %(message)s")
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger
