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
from .insert_activities import insert_file_into_db
from .fetch_activities import fetch_file_from_db
from .records_activities import read_records_table
# from .cesium_activities import convert_to_tileset
from .pipe_activities import resolve_crs_to_pdal_srs
from .export_activities import export_merged_laz
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
    "insert_file_into_db",
    "fetch_file_from_db",
    "read_records_table",
    # "convert_to_tileset",
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
    'export_merged_laz'
]