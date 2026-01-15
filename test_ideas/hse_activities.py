from __future__ import annotations

import asyncio
import pdal

from pathlib import Path
from temporalio import activity

from lidar_app.app.config import settings
from lidar_app.app.repo import Repo
from lidar_app.app.s3_store import S3Store
from lidar_app.app.artifact_service import download_artifact


@activity.defn
async def download_dataset_version_artifact(
        dataset_version_id: str,
        kind: str,
        schema_version: str,
        dst_dir: str,
):
    def _run():
        repo = Repo()
        art = repo.find_dataset_version_artifact(
            dataset_version_id=dataset_version_id,
            kind=kind,
            schema_version=schema_version,
        )
        if not art:
            raise RuntimeError(
                f"Dataset artifact not found for dataset_version_id={dataset_version_id} "
                f"kind={kind} schema_version={schema_version}"
            )

        s3 = S3Store(
            settings.s3_endpoint,
            settings.s3_access_key,
            settings.s3_secret_key,
            settings.s3_region,
        )

        dst_path = Path(dst_dir)
        dst_path.mkdir(parents=True, exist_ok=True)

        local_path = download_artifact(
            s3=s3,
            bucket=art.s3_bucket,
            key=art.s3_key,
            dst_dir=dst_path,
        )
        return {
            "local_path": str(local_path),
            "bucket": art.s3_bucket,
            "key": art.s3_key,
            "etag": art.etag,
            "size_bytes": art.size_bytes,
        }

    activity.heartbeat(
        {
            "stage": "download_dataset_version_artifact",
            "dataset_version_id": dataset_version_id,
            "kind": kind,
        }
    )
    return await asyncio.to_thread(_run)
