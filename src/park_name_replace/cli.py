import argparse
import sys
from pathlib import Path
from .config import ParkNameReplaceConfig, load_config_from_args
from .validation import validate_configuration
from .logging_config import setup_logging
from .scanning import run_scan
from .updating import run_update

def main():
    parser = argparse.ArgumentParser(description="ArcGIS Park Name Replacement Tool CLI")
    parser.add_argument("--profile", default="work", help="Saved ArcGIS profile (default: work)")
    parser.add_argument("--portal", help="Optional explicit ArcGIS portal URL")
    parser.add_argument("--username", help="Optional username for explicit portal authentication")
    parser.add_argument("--csv", required=True, help="Park-name crosswalk CSV file")
    parser.add_argument("--fields", default="PARK_NAME,LOCATION_NAME", help="Comma-separated target fields to scan/replace (default: PARK_NAME,LOCATION_NAME)")
    parser.add_argument("--mode", default="scan", choices=["scan", "update"], help="Operation mode: scan or update")
    parser.add_argument("--owner", help="Optional portal owner filter")
    parser.add_argument("--item-id", help="Optional Feature Service item ID")
    parser.add_argument("--candidate-layer-limit", type=int, help="Optional positive integer limiting qualifying candidate resources")
    parser.add_argument("--discovery-item-limit", type=int, help="Maximum number of Feature Service items to inspect")
    parser.add_argument("--discovery-page-size", type=int, default=50, help="Number of Feature Service items retrieved per portal search page (default: 50)")
    parser.add_argument("--query-batch-size", type=int, default=100, help="Query batch size (default: 100)")
    parser.add_argument("--edit-batch-size", type=int, default=100, help="Edit batch size (default: 100)")
    parser.add_argument("--output", required=True, help="Output XLSX file path")
    parser.add_argument("--update-report", help="Reviewed scan report used for update mode")
    parser.add_argument("--log-file", help="Optional log file path")

    args = parser.parse_args()

    config = load_config_from_args(args)

    logger = setup_logging(config.log_file)

    errors = validate_configuration(config)
    if errors:
        for err in errors:
            logger.error(f"Configuration Error: {err}")
        sys.exit(1)

    class ConsoleMessageHandler:
        def info(self, msg):
            logger.info(msg)
        def warning(self, msg):
            logger.warning(msg)
        def error(self, msg):
            logger.error(msg)

    handler = ConsoleMessageHandler()

    try:
        if config.mode == "scan":
            result = run_scan(config, handler)
            logger.info(f"Scan successful. Output workbook: {result.output_path}")
        else:
            result = run_update(config, handler)
            logger.info(f"Update successful. Output workbook: {result.output_path}")
    except Exception as e:
        logger.exception(f"Execution failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
