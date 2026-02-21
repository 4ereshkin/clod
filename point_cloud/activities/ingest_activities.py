"""
Activities for ingesting point cloud data using the new database structure.

These activities integrate the lidar_app/app logic for scan management,
artifact storage, and ingest runs with Temporal workflows.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

from temporalio import activity
from lidar_app.app.repo import Repo
from lidar_app.app.s3_store import S3Store, S3Ref, scan_prefix, derived_manifest_key
from env_vars import settings
from infrastructure.orm_models import IngestRun, Scan, Artifact
from lidar_app.app.artifact_service import store_artifact


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


def _linear_unit(unit: str) -> dict:
    if unit in ("m", "meter", "metre", "meters", "metres"):
        return {"type": "LinearUnit", "name": "metre", "conversion_factor": 1}
    if unit in ("km", "kilometer", "kilometre", "kilometers", "kilometres"):
        return {"type": "LinearUnit", "name": "kilometre", "conversion_factor": 1000}
    return {"type": "LinearUnit", "name": unit, "conversion_factor": 1}


def _axis_from_order(axis_order: str | None) -> list[dict]:
    if not axis_order:
        return [
            {"name": "Easting", "abbreviation": "E", "direction": "east"},
            {"name": "Northing", "abbreviation": "N", "direction": "north"},
        ]
    axes = []
    mapping = {
        "x_east": {"name": "Easting", "abbreviation": "E", "direction": "east"},
        "y_north": {"name": "Northing", "abbreviation": "N", "direction": "north"},
        "z_up": {"name": "Ellipsoidal height", "abbreviation": "h", "direction": "up"},
    }
    for entry in axis_order.split(","):
        entry = entry.strip()
        if entry in mapping:
            axes.append(mapping[entry])
    return axes or [
        {"name": "Easting", "abbreviation": "E", "direction": "east"},
        {"name": "Northing", "abbreviation": "N", "direction": "north"},
    ]


def _ellipsoid_from_name(name: str) -> dict:
    normalized = name.strip().upper().replace(" ", "").replace("_", "")
    ellipsoids = {
        "GRS80": {"name": "GRS 1980", "semi_major_axis": 6378137.0, "inverse_flattening": 298.257222101},
        "GRS1980": {"name": "GRS 1980", "semi_major_axis": 6378137.0, "inverse_flattening": 298.257222101},
        "CGCS2000": {"name": "CGCS2000", "semi_major_axis": 6378137.0, "inverse_flattening": 298.257222101},
        "CGCS2000DATUM": {"name": "CGCS2000", "semi_major_axis": 6378137.0, "inverse_flattening": 298.257222101},
        "WGS84": {"name": "WGS 84", "semi_major_axis": 6378137.0, "inverse_flattening": 298.257223563},
        "WGS1984": {"name": "WGS 84", "semi_major_axis": 6378137.0, "inverse_flattening": 298.257223563},
    }
    return ellipsoids.get(normalized, ellipsoids["GRS80"])


def _build_projjson(coordinate_system: dict) -> dict | None:
    projection = coordinate_system.get("projection") or {}
    projection_type = projection.get("type")
    central_meridian = projection.get("central_meridian")
    zone_width = projection.get("zone_width")
    zone_number = projection.get("zone_number")

    if projection_type is None and central_meridian is None and zone_width is None:
        return None

    method = "Transverse Mercator"
    if projection_type not in (None, "GK", "MCK", "tmerc"):
        method = str(projection_type)

    lon_0 = central_meridian
    if lon_0 is None and zone_width is not None and zone_number is not None:
        lon_0 = (zone_width * zone_number) - (zone_width / 2)
    if lon_0 is None:
        return None

    lat_0 = projection.get("lat_0", 0)
    k = projection.get("k", 1)
    x_0 = projection.get("x_0", 500000)
    y_0 = projection.get("y_0", 0)
    datum = coordinate_system.get("datum") or projection.get("ellps") or "GRS80"
    units = coordinate_system.get("units") or "m"
    ellipsoid = _ellipsoid_from_name(datum)

    return {
        "type": "ProjectedCRS",
        "name": coordinate_system.get("name") or f"{datum} / {method}",
        "base_crs": {
            "type": "GeographicCRS",
            "name": datum,
            "datum": {
                "type": "GeodeticReferenceFrame",
                "name": datum,
                "ellipsoid": ellipsoid,
            },
            "coordinate_system": {
                "subtype": "ellipsoidal",
                "axis": [
                    {"name": "Latitude", "abbreviation": "Lat", "direction": "north"},
                    {"name": "Longitude", "abbreviation": "Lon", "direction": "east"},
                ],
                "unit": {"type": "AngularUnit", "name": "degree", "conversion_factor": 0.0174532925199433},
            },
        },
        "conversion": {
            "name": f"{method} conversion",
            "method": {"name": method},
            "parameters": [
                {
                    "name": "Latitude of natural origin",
                    "value": lat_0,
                    "unit": {"type": "AngularUnit", "name": "degree", "conversion_factor": 0.0174532925199433},
                },
                {
                    "name": "Longitude of natural origin",
                    "value": lon_0,
                    "unit": {"type": "AngularUnit", "name": "degree", "conversion_factor": 0.0174532925199433},
                },
                {"name": "Scale factor at natural origin", "value": k, "unit": {"type": "ScaleUnit", "name": "unity", "conversion_factor": 1}},
                {"name": "False easting", "value": x_0, "unit": _linear_unit(units)},
                {"name": "False northing", "value": y_0, "unit": _linear_unit(units)},
            ],
        },
        "coordinate_system": {
            "subtype": "Cartesian",
            "axis": _axis_from_order(coordinate_system.get("axis_order")),
            "unit": _linear_unit(units),
        },
    }


def _apply_control_point_defaults(manifest: dict) -> None:
    control_points = manifest.get("control_points") or {}
    verified = control_points.get("verified_from_control_point") or {}
    verified_cs = verified.get("coordinate_system") or {}

    coordinate_system = manifest.setdefault("coordinate_system", {})
    for key in ("crs_type", "datum", "units", "axis_order"):
        if coordinate_system.get(key) is None and verified_cs.get(key) is not None:
            coordinate_system[key] = verified_cs[key]

    projection = coordinate_system.setdefault("projection", {})
    verified_projection = verified_cs.get("projection") or {}
    for key in ("type", "zone_width", "zone_number", "central_meridian"):
        if projection.get(key) is None and verified_projection.get(key) is not None:
            projection[key] = verified_projection[key]

    if manifest.get("geometry_mode") is None and verified.get("geometry_mode") is not None:
        manifest["geometry_mode"] = verified["geometry_mode"]
    if manifest.get("z_measurement") is None and verified.get("z_measurement") is not None:
        manifest["z_measurement"] = verified["z_measurement"]


def build_ingest_manifest(*, run: IngestRun, scan: Scan, raw_arts: list[Artifact]) -> dict:
    """Build ingest manifest from run, scan and raw artifacts."""
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
            "verified_from_control_point": {
                "who_guarantees": None,
                "xyz_consistent": None,
                "geometry_mode": None,
                "z_measurement": None,
                "gps": {
                    "latlon_format": None,
                    "height_type": None,
                },
                "coordinate_system": {
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

    merged = _deep_merge(manifest, overrides)
    _apply_control_point_defaults(merged)
    if "projjson" not in (merged.get("coordinate_system") or {}):
        merged.setdefault("coordinate_system", {})["projjson"] = _build_projjson(
            merged.get("coordinate_system") or {}
        )

    return merged


@activity.defn
async def create_scan(
    company_id: str,
    dataset_version_id: str,
) -> str:
    """
    Create a new scan in the database.

    Parameters
    ----------
    company_id:
        Company identifier
    dataset_version_id:
        Dataset identifier

    Returns
    -------
    str
        The created scan ID
    """
    def _create():
        repo = Repo()
        return repo.create_scan(company_id=company_id, dataset_version_id=dataset_version_id)

    return await asyncio.to_thread(_create)


@activity.defn
async def ensure_company(
    company_id: str,
    name: Optional[str] = None,
) -> None:
    """
    Ensure a company exists in the database.

    Parameters
    ----------
    company_id:
        Company identifier
    name:
        Company name (optional)
    """
    def _ensure():
        repo = Repo()
        repo.ensure_company(company_id=company_id, name=name)

    await asyncio.to_thread(_ensure)


@activity.defn
async def ensure_crs(
    crs_id: str,
    name: str,
    zone_degree: int,
    epsg: Optional[int] = None,
    units: str = "m",
    axis_order: str = "x_east,y_north,z_up",
) -> None:
    """
    Ensure a CRS exists in the database.

    Parameters
    ----------
    crs_id:
        CRS identifier
    name:
        CRS name
    zone_degree:
        Zone degree
    epsg:
        EPSG code (optional)
    units:
        Units (default: "m")
    axis_order:
        Axis order (default: "x_east,y_north,z_up")
    """
    def _ensure():
        repo = Repo()
        repo.ensure_crs(
            crs_id=crs_id,
            name=name,
            zone_degree=zone_degree,
            epsg=epsg,
            units=units,
            axis_order=axis_order,
        )

    await asyncio.to_thread(_ensure)


@activity.defn
async def ensure_dataset(
    company_id: str,
    crs_id: Optional[str],
    name: str,
) -> str:
    def _ensure():
        repo = Repo()
        return repo.ensure_dataset(
            company_id=company_id,
            crs_id=crs_id,
            name=name,
        )
    return await asyncio.to_thread(_ensure)

@activity.defn
async def ensure_dataset_version(dataset_id: str, bump: bool = False) -> Dict[str, Any]:
    def _ensure():
        repo = Repo()
        dv = repo.bump_dataset_version(dataset_id) if bump else repo.ensure_dataset_version(dataset_id)
        return {
            "id": dv.id,
            "dataset_id": dv.dataset_id,
            "version": dv.version,
        }
    return await asyncio.to_thread(_ensure)

@activity.defn
async def upload_raw_artifact(
    company_id: str,
    dataset_version_id: str,
    scan_id: str,
    artifact_kind: str,
    local_file_path: str,
    filename: Optional[str] = None,
) -> Dict[str, Any]:
    captured_filename = filename

    def _upload():
        s3 = S3Store(
            settings.s3_endpoint,
            settings.s3_access_key,
            settings.s3_secret_key,
            settings.s3_region,
        )
        repo = Repo()

        artifact_filename = captured_filename if captured_filename is not None else Path(local_file_path).name
        return store_artifact(
            repo=repo,
            s3=s3,
            company_id=company_id,
            dataset_version_id=dataset_version_id,
            scan_id=scan_id,
            kind=artifact_kind,
            local_file_path=local_file_path,
            bucket=settings.s3_bucket,
            filename=artifact_filename,
        )

    activity.heartbeat({"status": "uploading", "file": local_file_path})
    return await asyncio.to_thread(_upload)


@activity.defn
async def create_ingest_run(
    company_id: str,
    scan_id: str,
    schema_version: str = "1.1.0",
    force: bool = False,
) -> int:
    """
    Create an ingest run for a scan.

    Parameters
    ----------
    company_id:
        Company identifier
    scan_id:
        Scan identifier
    schema_version:
        Schema version (default: "1.1.0")
    force:
        Force creation even if run already exists (default: False)

    Returns
    -------
    int
        Ingest run ID
    """
    def _create():
        repo = Repo()

        # Verify scan belongs to company
        scan = repo.get_scan(scan_id)
        if scan.company_id != company_id:
            raise RuntimeError(f"Scan {scan_id} does not belong to company {company_id}")

        # Compute fingerprint
        fp = repo.compute_fingerprint(scan_id=scan_id)

        # Check if run already exists
        existing = repo.find_ingest_run(
            company_id=company_id,
            scan_id=scan_id,
            schema_version=schema_version,
            input_fingerprint=fp,
        )
        if existing and not force:
            return int(existing.id)

        # Create new ingest run
        run_id = repo.create_ingest_run(
            company_id=company_id,
            scan_id=scan_id,
            schema_version=schema_version,
            input_fingerprint=fp,
            status="QUEUED",
        )

        return run_id

    return await asyncio.to_thread(_create)


@activity.defn
async def process_ingest_run(
    run_id: int,
) -> Dict[str, Any]:
    """
    Process an ingest run: build manifest and register derived artifacts.

    Parameters
    ----------
    run_id:
        Ingest run ID

    Returns
    -------
    Dict[str, Any]
        Result including manifest key and status
    """
    def _process():
        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        # Get ingest run
        run = repo.get_ingest_run(run_id)
        repo.set_ingest_run_status(run_id=run_id, status="RUNNING")

        try:
            # Get scan and raw artifacts
            scan = repo.get_scan(run.scan_id)
            raw_arts = repo.list_raw_artifacts(run.scan_id)

            # Validate inputs
            if not raw_arts:
                raise RuntimeError(
                    f"No raw artifacts found for scan {run.scan_id}. "
                    f"Make sure artifacts were uploaded successfully."
                )

            cloud = next((a for a in raw_arts if a.kind == "raw.point_cloud"), None)
            if not cloud:
                available_kinds = [a.kind for a in raw_arts]
                raise RuntimeError(
                    f"raw.point_cloud is required but not found. "
                    f"Available artifact kinds: {available_kinds}. "
                    f"Make sure point cloud artifact was uploaded successfully."
                )

            # Build manifest
            manifest = build_ingest_manifest(run=run, scan=scan, raw_arts=raw_arts)
            body = json.dumps(manifest, ensure_ascii=False, indent=2).encode("utf-8")

            # Upload manifest to S3 and register derived artifact
            prefix = scan_prefix(scan.company_id, scan.dataset_version_id, scan.id)
            manifest_key = derived_manifest_key(prefix, run.schema_version)
            existing_manifest = repo.find_derived_artifact(
                scan_id=run.scan_id,
                kind="derived.ingest_manifest",
                schema_version=run.schema_version,
            )
            if existing_manifest and existing_manifest.status == "AVAILABLE":
                repo.set_ingest_run_status(run_id=run_id, status="SUCCEEDED", set_finished_at=True)
                return {
                    "run_id": run_id,
                    "manifest_key": existing_manifest.s3_key,
                    "manifest_bucket": existing_manifest.s3_bucket,
                    "status": "SUCCEEDED",
                }
            if existing_manifest is None:
                repo.register_artifact(
                    company_id=run.company_id,
                    scan_id=run.scan_id,
                    kind="derived.ingest_manifest",
                    bucket=settings.s3_bucket,
                    key=manifest_key,
                    schema_version=run.schema_version,
                    etag=None,
                    size_bytes=None,
                    status="PENDING",
                    meta={"format": "json"},
                )
            etag, size = s3.put_bytes(
                S3Ref(settings.s3_bucket, manifest_key),
                body,
                content_type="application/json",
            )
            repo.upsert_derived_artifact(
                company_id=run.company_id,
                scan_id=run.scan_id,
                kind="derived.ingest_manifest",
                schema_version=run.schema_version,
                s3_bucket=settings.s3_bucket,
                s3_key=manifest_key,
                etag=etag,
                size_bytes=size,
                status="AVAILABLE",
                meta={"format": "json"},
            )

            # Update ingest run status
            repo.set_ingest_run_status(run_id=run_id, status="SUCCEEDED", set_finished_at=True)
        except Exception as exc:
            repo.set_ingest_run_status(
                run_id=run_id,
                status="FAILED",
                error={"message": str(exc), "type": type(exc).__name__},
                set_finished_at=True,
            )
            raise

        return {
            "run_id": run_id,
            "manifest_key": manifest_key,
            "manifest_bucket": settings.s3_bucket,
            "status": "SUCCEEDED",
        }

    activity.heartbeat({"status": "processing", "run_id": run_id})
    return await asyncio.to_thread(_process)


@activity.defn
async def reconcile_pending_ingest_manifests(limit: int = 100) -> Dict[str, Any]:
    def _reconcile() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        pending = repo.list_pending_artifacts(kind="derived.ingest_manifest", limit=limit)
        approved = 0
        failed = 0
        skipped = 0

        for art in pending:
            etag, size = s3.head_object(S3Ref(art.s3_bucket, art.s3_key))
            if etag and size is not None:
                repo.update_artifact_status(
                    artifact_id=int(art.id),
                    status="AVAILABLE",
                    etag=etag,
                    size_bytes=size,
                )
                approved += 1
            else:
                repo.update_artifact_status(
                    artifact_id=int(art.id),
                    status="FAILED",
                )
                failed += 1

        return {
            "pending_checked": len(pending),
            "approved": approved,
            "failed": failed,
            "skipped": skipped,
        }

    activity.heartbeat({"status": "reconciling", "limit": limit})
    return await asyncio.to_thread(_reconcile)


@activity.defn
async def get_scan(
    scan_id: str,
) -> Dict[str, Any]:
    """
    Get scan information from database.

    Parameters
    ----------
    scan_id:
        Scan identifier

    Returns
    -------
    Dict[str, Any]
        Scan information
    """
    def _get():
        repo = Repo()
        scan = repo.get_scan(scan_id)
        return {
            "id": scan.id,
            "company_id": scan.company_id,
            "dataset_id": scan.dataset_id,
            "dataset_version_id": scan.dataset_version_id,
            "crs_id": scan.crs_id,
            "status": scan.status,
            "schema_version": scan.schema_version,
            "meta": scan.meta or {},
        }

    return await asyncio.to_thread(_get)


@activity.defn
async def update_scan_meta(
    scan_id: str,
    meta_update: Dict[str, Any],
) -> None:
    def _update() -> None:
        repo = Repo()
        repo.update_scan_meta(scan_id, meta_update)

    await asyncio.to_thread(_update)


@activity.defn
async def list_raw_artifacts(
    scan_id: str,
) -> List[Dict[str, Any]]:
    """
    List raw artifacts for a scan.

    Parameters
    ----------
    scan_id:
        Scan identifier

    Returns
    -------
    List[Dict[str, Any]]
        List of raw artifacts
    """
    def _list():
        repo = Repo()
        arts = repo.list_raw_artifacts(scan_id)
        return [
            {
                "id": int(a.id),
                "kind": a.kind,
                "bucket": a.s3_bucket,
                "key": a.s3_key,
                "etag": a.etag,
                "size_bytes": a.size_bytes,
                "status": a.status,
                "meta": a.meta or {},
            }
            for a in arts
        ]

    return await asyncio.to_thread(_list)
