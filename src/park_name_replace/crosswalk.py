import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Tuple
import logging

logger = logging.getLogger("park_name_replace.crosswalk")

def load_crosswalk(csv_path: Path) -> pd.DataFrame:
    if not csv_path.exists():
        raise FileNotFoundError(f"Crosswalk CSV file not found: {csv_path}")
    
    df = pd.read_csv(csv_path)
    
    # Support OLD and NEW columns, with fallback to PREV_PARK_NAME and PARK_NAME
    if {"OLD", "NEW"}.issubset(df.columns):
        cols_to_use = ["OLD", "NEW"]
    elif {"PREV_PARK_NAME", "PARK_NAME"}.issubset(df.columns):
        df = df.rename(columns={"PREV_PARK_NAME": "OLD", "PARK_NAME": "NEW"})
        cols_to_use = ["OLD", "NEW"]
    else:
        raise ValueError("Crosswalk CSV must contain 'OLD' and 'NEW' columns (or 'PREV_PARK_NAME' and 'PARK_NAME').")
    
    for col in cols_to_use:
        df[col] = df[col].astype(str).str.strip()
        
    return df

def validate_crosswalk(df: pd.DataFrame) -> List[str]:
    errors = []
    if df.empty:
        errors.append("Crosswalk CSV is empty.")
    return errors

def build_name_mapping(df: pd.DataFrame) -> Tuple[Dict[str, str], Dict[str, List[str]]]:
    """
    Builds the primary mapping around OLD -> NEW:
    {
        old_value: new_value
    }
    Also detects and flags ambiguous mappings where OLD maps to multiple different NEW values.
    """
    mapping: Dict[str, str] = {}
    ambiguous: Dict[str, List[str]] = {}
    
    temp_map: Dict[str, set] = {}
    
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
            
    # Check for ambiguity (same old_val mapped to multiple different new_vals)
    for old_val, new_vals in temp_map.items():
        if len(new_vals) > 1:
            ambiguous[old_val] = list(new_vals)
            if old_val in mapping:
                del mapping[old_val]
                
    return mapping, ambiguous

