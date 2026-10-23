import pytest
from pathlib import Path
import pandas as pd
from src.park_name_replace.crosswalk import load_crosswalk, build_name_mapping
from src.park_name_replace.config import ParkNameReplaceConfig
from src.park_name_replace.validation import validate_configuration
from src.toolboxes.GIS-Tools.ParkNameReplace import ParkNameReplace

def test_crosswalk_loading_and_mapping():
    csv_path = Path("Changed Park Names as of 20260813.csv")
    assert csv_path.exists(), "Crosswalk CSV must exist in root."

    df = load_crosswalk(csv_path)
    assert not df.empty
    assert "PARK_CODE" in df.columns
    assert "PARK_NAME" in df.columns
    assert "PREV_PARK_NAME" in df.columns

    mapping, ambiguous = build_name_mapping(df)
    assert isinstance(mapping, dict)
    assert isinstance(ambiguous, dict)
    # Check a known entry from CSV
    # B23,Glen Mar Park,Glen Mar Neighborhood Park
    assert "Glen Mar Neighborhood Park" in mapping
    assert mapping["Glen Mar Neighborhood Park"]["new_name"] == "Glen Mar Park"
    assert mapping["Glen Mar Neighborhood Park"]["park_code"] == "B23"

def test_configuration_validation(tmp_path):
    csv_path = Path("Changed Park Names as of 20260813.csv")
    out_xlsx = tmp_path / "test_output.xlsx"

    config = ParkNameReplaceConfig(
        mode="scan",
        csv_path=csv_path,
        output_path=out_xlsx,
        profile="work",
    )

    errors = validate_configuration(config)
    assert not errors

    # Invalid output extension
    config_bad = ParkNameReplaceConfig(
        mode="scan",
        csv_path=csv_path,
        output_path=tmp_path / "test_output.txt",
    )
    errors_bad = validate_configuration(config_bad)
    assert any("xlsx" in err.lower() for err in errors_bad)

def test_park_name_replace_tool_structure():
    tool = ParkNameReplace()
    assert tool.label == "Park Name Replace"
    assert tool.canRunInBackground is True

    params = tool.getParameterInfo()
    assert len(params) == 18

    # Verify method presence
    assert hasattr(tool, "isLicensed")
    assert hasattr(tool, "updateParameters")
    assert hasattr(tool, "updateMessages")
    assert hasattr(tool, "execute")
    assert hasattr(tool, "postExecute")
    assert tool.isLicensed() is True

