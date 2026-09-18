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
    root_logger = logging.getLogger()  # Empty string gets the Root Logger
    root_logger.setLevel(level)

    # Avoid adding duplicate handlers if the script runs multiple times
    if not any(isinstance(h, ArcGISLogHandler) for h in root_logger.handlers):
        handler = ArcGISLogHandler()
        # Optional: Add a formatter to include timestamps/levels
        formatter = logging.Formatter("%(levelname)s: %(message)s")
        handler.setFormatter(formatter)

        root_logger.addHandler(handler)
