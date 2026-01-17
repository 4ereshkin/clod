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
from lidar_app.app.config import settings
from lidar_app.app.models import IngestRun, Scan, Artifact
from lidar_app.app.artifact_service import store_artifact


def build_ingest_manifest(*, run: IngestRun, scan: Scan, raw_arts: list[Artifact]) -> dict:
    """Build ingest manifest from run, scan and raw artifacts."""
    def a_to_dict(a: Artifact) -> dict:
        return {
            'kind': a.kind,
            'bucket': a.s3_bucket,
            'key': a.s3_key,
            'etag': a.etag,
            'size_bytes': a.size_bytes,
            'status': a.status,
            'meta': a.meta or {},
        }

    return {
        'run_id': int(run.id),
        'company_id': run.company_id,
        'scan_id': run.scan_id,
        'schema_version': run.schema_version,
        'input_fingerprint': run.input_fingerprint,
        'created_at': datetime.now(timezone.utc).isoformat(),
        'scan': {
            'id': scan.id,
            'dataset_id': scan.dataset_id,
            'dataset_version_id': scan.dataset_version_id,
            'crs_id': scan.crs_id,
            'status': scan.status,
            'schema_version': scan.schema_version,
            'meta': scan.meta or {},
        },
        'raw_artifacts': [a_to_dict(a) for a in raw_arts],
    }


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
