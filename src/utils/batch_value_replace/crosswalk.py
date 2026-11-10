import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger("batch_value_replace.crosswalk")


def load_crosswalk(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Crosswalk CSV file not found: {csv_path}")

    df = pd.read_csv(csv_path)

    if {"OLD", "NEW"}.issubset(df.columns):
        cols_to_use = ["OLD", "NEW"]
    elif {"PREV_PARK_NAME", "PARK_NAME"}.issubset(df.columns):
        df = df.rename(columns={"PREV_PARK_NAME": "OLD", "PARK_NAME": "NEW"})
        cols_to_use = ["OLD", "NEW"]
    else:
        raise ValueError(
            "Crosswalk CSV must contain 'OLD' and 'NEW' columns (or 'PREV_PARK_NAME' and 'PARK_NAME')."
        )

    for col in cols_to_use:
        df[col] = df[col].astype(str).str.strip()

    return df


def validate_crosswalk(df: pd.DataFrame) -> list[str]:
    errors = []
    if df.empty:
        errors.append("Crosswalk CSV is empty.")
    return errors


def build_name_mapping(df: pd.DataFrame) -> tuple[dict[str, str], dict[str, list[str]]]:
    mapping: dict[str, str] = {}
    ambiguous: dict[str, list[str]] = {}

    temp_map: dict[str, set] = {}

    for _, row in df.iterrows():
        old_val = row["OLD"]
        new_val = row["NEW"]

        if not old_val or old_val.lower() == "nan":
            continue

        if old_val not in temp_map:
            temp_map[old_val] = set()
        temp_map[old_val].add(new_val)

        if old_val not in mapping:
            mapping[old_val] = new_val

    for old_val, new_vals in temp_map.items():
        if len(new_vals) > 1:
            ambiguous[old_val] = list(new_vals)
            mapping.pop(old_val, None)

    return mapping, ambiguous
