from __future__ import annotations

import asyncio
import json
import subprocess
import tempfile
import pdal
from pathlib import Path
from typing import Any, Dict

from temporalio import activity

from lidar_app.app.config import settings
from lidar_app.app.repo import Repo
from lidar_app.app.s3_store import S3Store, scan_prefix
from lidar_app.app.artifact_service import download_artifact, store_artifact


def _run_pdal_pipeline(pipeline: dict) -> None:
    pipe = pdal.Pipeline(json.dumps(pipeline))
    try:
        pipe.execute()
    except Exception as e:
        raise RuntimeError(f"PDAL pipeline failed: {e}")

    pipeline_metadata = pipe.metadata()

    return pipeline_metadata

@activity.defn
async def convert_laz_pcd(
    company_id: str,
    dataset_version_id: str,
    scan_id: str,
    schema_version: str,
):
    def _run() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        scan = repo.get_scan(scan_id=scan_id)

        raw_arts = repo



@activity.defn
async def shift_point_cloud(
        company_id: str,
        dataset_version_id: str,
        scan_id: str,
        schema_version: str):
    def _run() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        shift_art = repo.find_derived_artifact(scan_id=scan_id, kind='derived.reprojected_point_cloud', schema_version=schema_version)
        if not shift_art:
            raise RuntimeError(f"derived.reprojected_point_cloud not found for scan {scan_id}")




@activity.defn
async def preprocess_point_cloud(
    company_id: str,
    dataset_version_id: str,
    scan_id: str,
    schema_version: str,
    voxel_size_m: float = 0.10,
    mean_k: int = 20,
    multiplier: float = 2.0,
) -> Dict[str, Any]:
    def _run() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        inp_art = repo.find_derived_artifact(scan_id, "derived.reprojected_point_cloud", schema_version)
        if not inp_art:
            raise RuntimeError(f"derived.reprojected_point_cloud not found for scan {scan_id}")

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)

            local_in = download_artifact(
                s3=s3,
                bucket=inp_art.s3_bucket,
                key=inp_art.s3_key,
                dst_dir=td,
            )

            out_name = f"{local_in.stem}__pre_pdal__vox{voxel_size_m:.2f}m__k{mean_k}__m{multiplier:.2f}.laz".replace("..", ".")
            local_out = td / out_name

            pipeline = {
                "pipeline": [
                    {
                        "type": "readers.las",
                        "filename": str(local_in)},
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
                    {
                        "type": "writers.las",
                        "filename": str(local_out),
                        "compression": "laszip",
                    },
                ]
            }

            _run_pdal_pipeline(pipeline)

            if not local_out.exists():
                raise RuntimeError(f"PDAL did not produce output file: {local_out}")

            prefix = scan_prefix(company_id, dataset_version_id, scan_id)
            out_key = f"{prefix}/derived/v{schema_version}/preprocessed/point_cloud/{local_out.name}"
            meta = {
                "engine": "pdal",
                "voxel_size_m": float(voxel_size_m),
                "outlier": {"method": "statistical", "mean_k": int(mean_k), "multiplier": float(multiplier)},
            }
            result = store_artifact(
                repo=repo,
                s3=s3,
                company_id=company_id,
                scan_id=scan_id,
                kind="derived.preprocessed_point_cloud",
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
            "kind": "derived.preprocessed_point_cloud",
            "s3_bucket": settings.s3_bucket,
            "s3_key": out_key,
            "etag": result["etag"],
            "size_bytes": result["size_bytes"],
        }

    activity.heartbeat({"stage": "preprocess_pdal", "scan_id": scan_id})
    return await asyncio.to_thread(_run)
