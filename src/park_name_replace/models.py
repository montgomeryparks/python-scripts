from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, List, Dict, Any

@dataclass
class ParkNameReplaceConfig:
    mode: str  # "scan" or "update"
    csv_path: Path
    output_path: Path

    profile: Optional[str] = "work"
    portal_url: Optional[str] = None
    username: Optional[str] = None

    owner: Optional[str] = None
    item_id: Optional[str] = None

    fields: str = "PARK_NAME,LOCATION_NAME"

    candidate_layer_limit: Optional[int] = None
    discovery_item_limit: Optional[int] = None
    discovery_page_size: int = 50

    query_batch_size: int = 100
    edit_batch_size: int = 100

    update_report: Optional[Path] = None
    log_file: Optional[Path] = None


@dataclass
class ScanResult:
    feature_updates: List[Dict[str, Any]] = field(default_factory=list)
    layer_summary: List[Dict[str, Any]] = field(default_factory=list)
    broken_resources: List[Dict[str, Any]] = field(default_factory=list)
    run_info: Dict[str, Any] = field(default_factory=dict)
    output_path: Path = None


@dataclass
class UpdateResult:
    feature_updates: List[Dict[str, Any]] = field(default_factory=list)
    layer_summary: List[Dict[str, Any]] = field(default_factory=list)
    broken_resources: List[Dict[str, Any]] = field(default_factory=list)
    run_info: Dict[str, Any] = field(default_factory=dict)
    output_path: Path = None

