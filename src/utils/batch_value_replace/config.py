from pathlib import Path

from .models import BatchValueReplaceConfig


def load_config_from_args(args) -> BatchValueReplaceConfig:
    return BatchValueReplaceConfig(
        mode=args.mode.lower(),
        csv_path=Path(args.csv),
        output_path=Path(args.output),
        profile=args.profile,
        portal_url=args.portal,
        username=args.username,
        owner=args.owner,
        item_id=args.item_id,
        fields=getattr(args, "fields", "PARK_NAME,LOCATION_NAME"),
        candidate_layer_limit=args.candidate_layer_limit,
        discovery_item_limit=getattr(args, "discovery_item_limit", None),
        discovery_page_size=getattr(args, "discovery_page_size", 50),
        query_batch_size=args.query_batch_size,
        edit_batch_size=args.edit_batch_size,
        update_report=Path(args.update_report) if args.update_report else None,
        log_file=Path(args.log_file) if args.log_file else None,
        exclude_tags=getattr(args, "exclude_tags", None),
    )
