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

def test_scan_imports_without_update_failure():
    from src.park_name_replace.scanning import run_scan
    assert callable(run_scan)

def test_candidate_limit_stops_discovery_early():
    from unittest.mock import MagicMock
    from src.park_name_replace.discovery import discover_candidates_and_broken

    # Mock GIS and content search returning items that yield more than 20 candidates
    mock_gis = MagicMock()
    mock_item = MagicMock()
    mock_item.title = "Mock Item"
    mock_item.id = "item123"
    mock_item.owner = "testowner"
    mock_item.url = "http://server/arcgis/rest/services/Mock/FeatureServer"
    
    mock_layer = MagicMock()
    mock_layer.properties = {
        "id": 0,
        "name": "MockLayer",
        "fields": [{"name": "PARK_NAME"}],
        "capabilities": "Query,Update"
    }
    mock_item.layers = [mock_layer]
    mock_item.tables = []

    mock_gis.content.search.return_value = [mock_item] * 25

    config = ParkNameReplaceConfig(
        mode="scan",
        csv_path=Path("Changed Park Names as of 20260813.csv"),
        output_path=Path("dummy.xlsx"),
        candidate_layer_limit=5,
        discovery_page_size=10,
    )

    candidates, broken, inspected, complete, stop_reason = discover_candidates_and_broken(mock_gis, config)
    assert len(candidates) == 5
    assert stop_reason == "CANDIDATE_LAYER_LIMIT_REACHED"

def test_park_name_replace_tool_structure():
    tool = ParkNameReplace()
    assert tool.label == "Park Name Replace"
    assert tool.canRunInBackground is True

    params = tool.getParameterInfo()
    assert len(params) == 18

    assert hasattr(tool, "isLicensed")
    assert hasattr(tool, "updateParameters")
    assert hasattr(tool, "updateMessages")
    assert hasattr(tool, "execute")
    assert hasattr(tool, "postExecute")
    assert tool.isLicensed() is True


