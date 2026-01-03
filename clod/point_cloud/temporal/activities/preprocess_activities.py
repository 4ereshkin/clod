from __future__ import annotations

import asyncio
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict

from temporalio import activity

from lidar_app.app.config import settings
from lidar_app.app.repo import Repo
from lidar_app.app.s3_store import S3Store, S3Ref, scan_prefix


def _run_pdal_pipeline(pipeline: dict) -> None:
    p = subprocess.run(
        ["pdal", "pipeline", "--stdin"],
        input=json.dumps(pipeline),
        text=True,
        capture_output=True,
    )
    if p.returncode != 0:
        raise RuntimeError(
            "pdal pipeline failed\n"
            f"rc: {p.returncode}\n"
            f"stdout:\n{p.stdout}\n"
            f"stderr:\n{p.stderr}\n"
            f"pipeline:\n{json.dumps(pipeline, indent=2)}\n"
        )


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

            local_in = td / Path(inp_art.s3_key).name
            s3.download_file(S3Ref(inp_art.s3_bucket, inp_art.s3_key), str(local_in))

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
            etag, size = s3.upload_file(S3Ref(settings.s3_bucket, out_key), str(local_out))

        repo.upsert_derived_artifact(
            company_id=company_id,
            scan_id=scan_id,
            kind="derived.preprocessed_point_cloud",
            schema_version=schema_version,
            s3_bucket=settings.s3_bucket,
            s3_key=out_key,
            etag=etag,
            size_bytes=size,
            status="READY",
            meta={
                "engine": "pdal",
                "voxel_size_m": float(voxel_size_m),
                "outlier": {"method": "statistical", "mean_k": int(mean_k), "multiplier": float(multiplier)},
            },
        )

        return {
            "kind": "derived.preprocessed_point_cloud",
            "s3_bucket": settings.s3_bucket,
            "s3_key": out_key,
            "etag": etag,
            "size_bytes": size,
        }

    activity.heartbeat({"stage": "preprocess_pdal", "scan_id": scan_id})
    return await asyncio.to_thread(_run)
