import importlib.util
from pathlib import Path

from src.utils.batch_value_replace.config import BatchValueReplaceConfig
from src.utils.batch_value_replace.crosswalk import build_name_mapping, load_crosswalk
from src.utils.batch_value_replace.validation import validate_configuration

# Load BatchValueReplace from file due to hyphenated folder name
toolbox_py = Path("src/toolboxes/GIS-Tools/BatchValueReplace.py")
spec = importlib.util.spec_from_file_location("BatchValueReplace", toolbox_py)
batch_value_replace_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(batch_value_replace_mod)
BatchValueReplace = batch_value_replace_mod.BatchValueReplace


def test_crosswalk_loading_and_mapping():
    csv_path = Path("Changed Park Names as of 20260813.csv")
    assert csv_path.exists(), "Crosswalk CSV must exist in root."

    df = load_crosswalk(csv_path)
    assert not df.empty
    assert "OLD" in df.columns
    assert "NEW" in df.columns

    mapping, ambiguous = build_name_mapping(df)
    assert isinstance(mapping, dict)
    assert isinstance(ambiguous, dict)
    assert "Glen Mar Neighborhood Park" in mapping
    assert mapping["Glen Mar Neighborhood Park"] == "Glen Mar Park"


def test_configuration_validation(tmp_path):
    csv_path = Path("Changed Park Names as of 20260813.csv")
    out_xlsx = tmp_path / "test_output.xlsx"

    config = BatchValueReplaceConfig(
        mode="scan",
        csv_path=csv_path,
        output_path=out_xlsx,
        profile="work",
    )

    errors = validate_configuration(config)
    assert not errors

    config_bad = BatchValueReplaceConfig(
        mode="scan",
        csv_path=csv_path,
        output_path=tmp_path / "test_output.txt",
    )
    errors_bad = validate_configuration(config_bad)
    assert any("xlsx" in err.lower() for err in errors_bad)


def test_scan_imports_without_update_failure():
    from src.utils.batch_value_replace.scanning import run_scan

    assert callable(run_scan)


def test_batch_value_replace_tool_structure():
    tool = BatchValueReplace()
    assert tool.label == "Batch Value Replace"
    assert tool.canRunInBackground is True

    params = tool.getParameterInfo()
    assert len(params) == 18

    assert hasattr(tool, "isLicensed")
    assert hasattr(tool, "updateParameters")
    assert hasattr(tool, "updateMessages")
    assert hasattr(tool, "execute")
    assert hasattr(tool, "postExecute")
    assert tool.isLicensed() is True
