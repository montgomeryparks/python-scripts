from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class BatchValueReplaceConfig:
    mode: str  # "scan" or "update"
    csv_path: Path
    output_path: Path

    profile: str | None = "work"
    portal_url: str | None = None
    username: str | None = None

    owner: str | None = None
    item_id: str | None = None

    fields: str = "PARK_NAME,LOCATION_NAME"

    candidate_layer_limit: int | None = None
    discovery_item_limit: int | None = None
    discovery_page_size: int = 50

    query_batch_size: int = 100
    edit_batch_size: int = 100

    update_report: Path | None = None
    log_file: Path | None = None


# Backwards-compatibility alias
ParkNameReplaceConfig = BatchValueReplaceConfig


@dataclass
class ScanResult:
    feature_updates: list[dict[str, Any]] = field(default_factory=list)
    layer_summary: list[dict[str, Any]] = field(default_factory=list)
    broken_resources: list[dict[str, Any]] = field(default_factory=list)
    run_info: dict[str, Any] = field(default_factory=dict)
    output_path: Path = None


@dataclass
class UpdateResult:
    feature_updates: list[dict[str, Any]] = field(default_factory=list)
    layer_summary: list[dict[str, Any]] = field(default_factory=list)
    broken_resources: list[dict[str, Any]] = field(default_factory=list)
    run_info: dict[str, Any] = field(default_factory=dict)
    output_path: Path = None
