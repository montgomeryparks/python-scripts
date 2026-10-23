import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Tuple
import logging

logger = logging.getLogger("park_name_replace.crosswalk")

def load_crosswalk(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Crosswalk CSV file not found: {csv_path}")
    
    df = pd.read_csv(csv_path)
    required_cols = {"PARK_CODE", "PARK_NAME", "PREV_PARK_NAME"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Crosswalk CSV is missing required columns: {missing}")
    
    # Clean string columns
    for col in required_cols:
        df[col] = df[col].astype(str).str.strip()
        
    return df

def validate_crosswalk(df: pd.DataFrame) -> List[str]:
    errors = []
    if df.empty:
        errors.append("Crosswalk CSV is empty.")
    return errors

def build_name_mapping(df: pd.DataFrame) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, List[str]]]:
    """
    Builds the primary mapping around PREV_PARK_NAME:
    {
        previous_name: {
            "new_name": corrected_name,
            "park_code": park_code,
        }
    }
    Also detects and flags ambiguous mappings where PREV_PARK_NAME maps to multiple new_names.
    """
    mapping: Dict[str, Dict[str, Any]] = {}
    ambiguous: Dict[str, List[str]] = {}
    
    temp_map: Dict[str, set] = {}
    
    for _, row in df.iterrows():
        prev_name = row["PREV_PARK_NAME"]
        new_name = row["PARK_NAME"]
        park_code = row["PARK_CODE"]
        
        if not prev_name or prev_name.lower() == "nan":
            continue
            
        if prev_name not in temp_map:
            temp_map[prev_name] = set()
        temp_map[prev_name].add((new_name, park_code))
        
        if prev_name not in mapping:
            mapping[prev_name] = {
                "new_name": new_name,
                "park_code": park_code,
            }
            
    # Check for ambiguity (same prev_name mapped to multiple different new_names)
    for prev_name, targets in temp_map.items():
        new_names = set(t[0] for t in targets)
        if len(new_names) > 1:
            ambiguous[prev_name] = list(new_names)
            # Remove or mark as ambiguous in mapping
            if prev_name in mapping:
                del mapping[prev_name]
                
    return mapping, ambiguous
