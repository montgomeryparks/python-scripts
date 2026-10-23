import logging
from typing import List, Dict, Any, Tuple
from arcgis.gis import GIS
from arcgis.features import FeatureLayer, Table
from .models import ParkNameReplaceConfig

logger = logging.getLogger("park_name_replace.discovery")

def find_candidate_items(gis: GIS, config: ParkNameReplaceConfig) -> List[Any]:
    query_parts = []
    if config.owner:
        query_parts.append(f"owner:{config.owner}")
    if config.item_id:
        query_parts.append(f"id:{config.item_id}")
    else:
        query_parts.append('type:"Feature Service"')
        
    query_str = " AND ".join(query_parts) if query_parts else 'type:"Feature Service"'
    logger.info(fumbra := f"Searching portal with query: {query_str}")
    
    items = gis.content.search(query=query_str, max_items=2000)
    return items

def get_layers_and_tables(item) -> List[Tuple[Any, str, int]]:
    """
    Extracts feature layers and standalone tables from a Feature Service item.
    Returns tuples of (resource_obj, resource_type, layer_id)
    """
    resources = []
    try:
        layers = getattr(item, "layers", []) or []
        for l in layers:
            lid = getattr(l, "properties", {}).get("id", 0)
            resources.append((l, "Feature Layer", lid))
    except Exception as e:
        logger.debug(f"Could not get layers for item {item.id}: {e}")
        
    try:
        tables = getattr(item, "tables", []) or []
        for t in tables:
            tid = getattr(t, "properties", {}).get("id", 0)
            resources.append((t, "Table", tid))
    except Exception as e:
        logger.debug(f"Could not get tables for item {item.id}: {e}")
        
    return resources

def find_field(resource, requested_name: str) -> Tuple[bool, str]:
    """
    Case-insensitive field lookup. Returns (found, actual_field_name).
    """
    try:
        fields = getattr(resource, "properties", {}).get("fields", [])
        for f in fields:
            fname = f.get("name", "")
            if fname.lower() == requested_name.lower():
                return True, fname
    except Exception as e:
        logger.debug(f"Error inspecting fields for resource: {e}")
    return False, requested_name

def sort_candidates(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Sort candidates deterministically:
    Item Title, Item ID, Resource Type, Layer/Table ID
    """
    return sorted(
        candidates,
        key=lambda c: (
            str(c.get("item_title", "")).lower(),
            str(c.get("item_id", "")),
            str(c.get("resource_type", "")),
            int(c.get("layer_id", 0))
        )
    )

def apply_candidate_limit(candidates: List[Dict[str, Any]], limit: int | None) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Splits candidates into selected and excluded based on limit.
    """
    if limit is None or len(candidates) <= limit:
        return candidates, []
    return candidates[:limit], candidates[limit:]
