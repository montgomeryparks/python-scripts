from pathlib import Path
from typing import Optional
from .models import ParkNameReplaceConfig

def load_config_from_args(args) -> ParkNameReplaceConfig:
    return ParkNameReplaceConfig(
        mode=args.mode.lower(),
        csv_path=Path(args.csv),
        output_path=Path(args.output),
        profile=args.profile,
        portal_url=args.portal,
        username=args.username,
        owner=args.owner,
        item_id=args.item_id,
        park_name_field=args.field,
        park_code_field=args.park_code_field,
        candidate_layer_limit=args.candidate_layer_limit,
        discovery_item_limit=getattr(args, "discovery_item_limit", None),
        discovery_page_size=getattr(args, "discovery_page_size", 50),
        query_batch_size=args.query_batch_size,
        edit_batch_size=args.edit_batch_size,
        update_report=Path(args.update_report) if args.update_report else None,
        log_file=Path(args.log_file) if args.log_file else None,
    )
