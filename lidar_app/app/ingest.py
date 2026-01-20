import json
from datetime import datetime, timezone
from pathlib import Path
import sys

# add project root to PYTHONPATH
script_dir = Path(__file__).resolve().parent
project_root = script_dir.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from .models import IngestRun, Scan, Artifact


def _deep_merge(base: dict, overrides: dict) -> dict:
    merged = dict(base)
    for key, value in overrides.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def _detect_point_cloud_format(raw_arts: list[Artifact]) -> str | None:
    cloud = next((a for a in raw_arts if a.kind == "raw.point_cloud"), None)
    if not cloud:
        return None
    name = (cloud.s3_key or "").lower()
    if name.endswith(".copc.laz") or "copc" in name:
        return "copc.laz"
    if name.endswith(".laz"):
        return "laz"
    if name.endswith(".las"):
        return "las"
    return None


def build_ingest_manifest(*, run: IngestRun, scan: Scan, raw_arts: list[Artifact]) -> dict:
    def a_to_dict(a: Artifact) -> dict:
        return {
            "kind": a.kind,
            "bucket": a.s3_bucket,
            "key": a.s3_key,
            "etag": a.etag,
            "size_bytes": a.size_bytes,
            "status": a.status,
            "meta": a.meta or {},
        }

    def _artifact_by_kind(kind: str) -> dict | None:
        art = next((a for a in raw_arts if a.kind == kind), None)
        if not art:
            return None
        return a_to_dict(art)

    scan_meta = scan.meta or {}
    overrides = scan_meta.get("manifest", {}) if isinstance(scan_meta.get("manifest"), dict) else {}

    manifest = {
        "material": {
            "point_cloud_format": _detect_point_cloud_format(raw_arts),
        },
        "coordinate_system": {
            "guarantor": {
                "source": None,
                "metadata": None,
                "reference": None,
                "client": None,
            },
            "crs_id": scan.crs_id,
            "crs_type": None,
            "datum": None,
            "projection": {
                "type": None,
                "zone_width": None,
                "zone_number": None,
                "central_meridian": None,
            },
            "units": None,
            "axis_order": None,
        },
        "z_measurement": None,
        "imu_dimensions": None,
        "geometry_mode": None,
        "control_points": {
            "table": _artifact_by_kind("raw.control_point"),
            "local_system": {
                "x": None,
                "y": None,
                "z": None,
                "z_mode": None,
            },
            "final_system": {
                "x": None,
                "y": None,
                "z": None,
                "z_mode": None,
            },
        },
        "business_logic": {
            "company": scan.company_id,
            "department": scan.owner_department_id,
            "employee": None,
            "tariff": None,
            "processing_version": scan.schema_version,
        },
        "recording_modes": {
            "duplicates": None,
            "coordinate_system": {
                "mode": None,
                "override_epsg": None,
            },
        },
        "ingest": {
            "run_id": int(run.id),
            "company_id": run.company_id,
            "scan_id": run.scan_id,
            "schema_version": run.schema_version,
            "input_fingerprint": run.input_fingerprint,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "scan": {
                "id": scan.id,
                "dataset_id": scan.dataset_id,
                "dataset_version_id": scan.dataset_version_id,
                "crs_id": scan.crs_id,
                "status": scan.status,
                "schema_version": scan.schema_version,
                "meta": scan.meta or {},
            },
            "raw_artifacts": [a_to_dict(a) for a in raw_arts],
        },
    }

    return _deep_merge(manifest, overrides)
