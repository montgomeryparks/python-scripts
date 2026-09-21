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


def setup_arcgis_logging(level=logging.DEBUG):
    """Configures the root logger to send all messages to ArcGIS."""
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Remove existing handlers by class string name to survive ArcGIS module reloads
    for h in root_logger.handlers[:]:
        if type(h).__name__ == "ArcGISLogHandler":
            root_logger.removeHandler(h)

    handler = ArcGISLogHandler()
    formatter = logging.Formatter("%(levelname)s: %(message)s")
    handler.setFormatter(formatter)

    root_logger.addHandler(handler)
