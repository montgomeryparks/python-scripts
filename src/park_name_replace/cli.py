import argparse
import sys
from pathlib import Path
from .config import ParkNameReplaceConfig
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
    parser.add_argument("--field", default="PARK_NAME", help="Park name field name (default: PARK_NAME)")
    parser.add_argument("--park-code-field", default="PARK_CODE", help="Park code field name (default: PARK_CODE)")
    parser.add_argument("--mode", default="scan", choices=["scan", "update"], help="Operation mode: scan or update")
    parser.add_argument("--owner", help="Optional portal owner filter")
    parser.add_argument("--item-id", help="Optional Feature Service item ID")
    parser.add_argument("--candidate-layer-limit", type=int, help="Optional positive integer limiting qualifying candidate resources")
    parser.add_argument("--query-batch-size", type=int, default=100, help="Query batch size (default: 100)")
    parser.add_argument("--edit-batch-size", type=int, default=100, help="Edit batch size (default: 100)")
    parser.add_argument("--output", required=True, help="Output XLSX file path")
    parser.add_argument("--update-report", help="Reviewed scan report used for update mode")
    parser.add_argument("--log-file", help="Optional log file path")

    args = parser.parse_args()

    config = ParkNameReplaceConfig(
        mode=args.mode.lower(),
        csv_path=Path(args.csv),
        output_path=Path(args.output),
        profile=args.profile,
        portal_url=args.portal,
        username=args.username,
        owner=args.owner,
        item_id=args.item_id,
        park_name_field=args.field,
        park_code_field=args.park_code_field,
        candidate_layer_limit=args.candidate_layer_limit,
        query_batch_size=args.query_batch_size,
        edit_batch_size=args.edit_batch_size,
        update_report=Path(args.update_report) if args.update_report else None,
        log_file=Path(args.log_file) if args.log_file else None,
    )

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
