from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from typing import Any, Dict, List

import pdal
from temporalio import activity

from lidar_app.app.artifact_service import download_artifact, store_artifact
from lidar_app.app.config import settings
from lidar_app.app.repo import Repo
from lidar_app.app.s3_store import S3Store, scan_prefix


def _run_pdal_pipeline(pipeline: dict) -> dict:
    pipe = pdal.Pipeline(json.dumps(pipeline))
    try:
        pipe.execute()
    except Exception as exc:
        raise RuntimeError(f"PDAL pipeline failed: {exc}")
    return pipe.metadata or {}


def _resolve_input_artifact(
    repo: Repo,
    scan_id: str,
    schema_version: str,
    input_kind: str,
) -> Any:
    art = repo.find_derived_artifact(scan_id, input_kind, schema_version)
    if not art:
        raise RuntimeError(f"{input_kind} not found for scan {scan_id}")
    return art


def _resolve_output_subdir(kind: str) -> str:
    mapping = {
        "derived.preprocessed_point_cloud": "preprocessed/point_cloud",
        "derived.registration_point_cloud": "registration/point_cloud",
    }
    if kind not in mapping:
        raise ValueError(f"Unsupported output kind: {kind}")
    return mapping[kind]


@activity.defn
async def list_scans_by_dataset_version(dataset_version_id: str) -> List[str]:
    def _run() -> List[str]:
        repo = Repo()
        scans = repo.list_scans_by_dataset_version(dataset_version_id)
        return [s.id for s in scans]

    return await asyncio.to_thread(_run)


@activity.defn
async def preprocess_point_cloud(
    company_id: str,
    dataset_version_id: str,
    scan_id: str,
    schema_version: str,
    input_kind: str = "derived.reprojected_point_cloud",
    output_kind: str = "derived.preprocessed_point_cloud",
    voxel_size_m: float = 0.10,
    mean_k: int = 20,
    multiplier: float = 2.0,
) -> Dict[str, Any]:
    def _run() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(
            settings.s3_endpoint,
            settings.s3_access_key,
            settings.s3_secret_key,
            settings.s3_region,
        )

        inp_art = _resolve_input_artifact(repo, scan_id, schema_version, input_kind)

        with tempfile.TemporaryDirectory() as td:
            td_path = Path(td)

            local_in = download_artifact(
                s3=s3,
                bucket=inp_art.s3_bucket,
                key=inp_art.s3_key,
                dst_dir=td_path,
            )

            out_name = (
                f"{local_in.stem}__prep__vox{voxel_size_m:.2f}m"
                f"__k{mean_k}__m{multiplier:.2f}.laz"
            ).replace("..", ".")
            local_out = td_path / out_name

            pipeline = {
                "pipeline": [
                    {"type": "readers.las", "filename": str(local_in)},
                    {
                        "type": "filters.outlier",
                        "method": "statistical",
                        "mean_k": int(mean_k),
                        "multiplier": float(multiplier),
                    },
                    {
                        "type": "filters.voxelcenternearestneighbor",
                        "cell": float(voxel_size_m),
                    },
                    {"type": "writers.las", "filename": str(local_out), "compression": "laszip"},
                ]
            }

            metadata = _run_pdal_pipeline(pipeline)

            if not local_out.exists():
                raise RuntimeError(f"PDAL did not produce output file: {local_out}")

            prefix = scan_prefix(company_id, dataset_version_id, scan_id)
            subdir = _resolve_output_subdir(output_kind)
            out_key = f"{prefix}/derived/v{schema_version}/{subdir}/{local_out.name}"
            meta = {
                "engine": "pdal",
                "input_kind": input_kind,
                "output_kind": output_kind,
                "voxel_size_m": float(voxel_size_m),
                "outlier": {
                    "method": "statistical",
                    "mean_k": int(mean_k),
                    "multiplier": float(multiplier),
                },
                "pdal_metadata": metadata,
            }
            result = store_artifact(
                repo=repo,
                s3=s3,
                company_id=company_id,
                scan_id=scan_id,
                kind=output_kind,
                schema_version=schema_version,
                bucket=settings.s3_bucket,
                key=out_key,
                local_file_path=str(local_out),
                status="READY",
                meta=meta,
                upsert=True,
                upload_method="upload_file",
            )

        return {
            "kind": output_kind,
            "s3_bucket": settings.s3_bucket,
            "s3_key": out_key,
            "etag": result["etag"],
            "size_bytes": result["size_bytes"],
        }

    activity.heartbeat({"stage": "preprocess_pdal", "scan_id": scan_id})
    return await asyncio.to_thread(_run)
