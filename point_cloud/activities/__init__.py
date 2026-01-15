"""
Activity implementations for Temporal workflows.

Each activity wraps an existing function or class from the legacy
``point_cloud`` package and exposes it as an async callable suitable for
Temporal's activity execution model.  Activities should be idempotent
whenever possible and return simple JSON‑serialisable results.
"""

from .gateway_activities import las_choice
from .gateway_activities import load_metadata_for_file
from .reproject_activities import reproject_file
from .pipe_activities import resolve_crs_to_pdal_srs
from .export_activities import export_merged_laz
from .cluster_activities import (
    extract_scale_offset,
    split_into_tiles,
    split_ground_offground,
    cluster_tile,
    crop_buffer,
    merge_tiles,
)
from .ingest_activities import (
    create_scan,
    ensure_company,
    ensure_crs,
    ensure_dataset,
    ensure_dataset_version,
    upload_raw_artifact,
    create_ingest_run,
    process_ingest_run,
    get_scan,
    list_raw_artifacts,
)

__all__ = [
    "las_choice",
    "load_metadata_for_file",
    "reproject_file",
    "create_scan",
    "ensure_company",
    "ensure_crs",
    "ensure_dataset",
    'ensure_dataset_version',
    "upload_raw_artifact",
    "create_ingest_run",
    "process_ingest_run",
    "get_scan",
    "list_raw_artifacts",
    "export_merged_laz",
    "extract_scale_offset",
    "split_into_tiles",
    "split_ground_offground",
    "cluster_tile",
    "crop_buffer",
    "merge_tiles",
]
