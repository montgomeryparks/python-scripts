from .crosswalk import load_crosswalk, validate_crosswalk
from .models import BatchValueReplaceConfig


def validate_configuration(config: BatchValueReplaceConfig) -> list[str]:
    errors = []

    if not config.csv_path or not config.csv_path.exists():
        errors.append(f"Crosswalk CSV path does not exist: {config.csv_path}")
    else:
        try:
            df = load_crosswalk(config.csv_path)
            cw_errors = validate_crosswalk(df)
            errors.extend(cw_errors)
        except Exception as e:
            errors.append(f"Invalid crosswalk CSV: {e}")

    if not config.output_path or not str(config.output_path).endswith(".xlsx"):
        errors.append("Output path must be a valid .xlsx file path.")

    if config.candidate_layer_limit is not None and config.candidate_layer_limit < 1:
        errors.append("Candidate layer limit must be greater than zero.")

    if config.query_batch_size < 1:
        errors.append("Query batch size must be greater than zero.")

    if config.edit_batch_size < 1:
        errors.append("Edit batch size must be greater than zero.")

    if config.portal_url and not config.profile:
        if not config.portal_url.startswith("http"):
            errors.append("Portal URL must be a valid URL starting with http/https.")

    if config.mode == "update":
        if not config.update_report or not config.update_report.exists():
            errors.append(
                "A valid reviewed update workbook (.xlsx) is required for Update mode."
            )

    return errors
