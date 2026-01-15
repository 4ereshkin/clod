from __future__ import annotations

from pathlib import Path

import asyncio

from temporalio import activity

from lidar_app.app.config import settings
from lidar_app.app.repo import Repo
from lidar_app.app.s3_store import S3Store
from lidar_app.app.artifact_service import download_artifact


@activity.defn
async def download_from_s3(
        bucket: str,
        key: str,
        dst_dir: str,
):
    def _run():
        s3 = S3Store(
            settings.s3_endpoint,
            settings.s3_access_key,
            settings.s3_secret_key,
            settings.s3_region,
        )

        dst_path = Path(dst_dir)
        dst_path.mkdir(parents=True, exist_ok=True)

        local_path = download_artifact(s3=s3,
                                       bucket=bucket,
                                       key=key,
                                       dst_dir=dst_path)
        return str(local_path)

    activity.heartbeat({'stage': 'download_from_s3', 'key': key, 'dst_dir': str(dst_dir)})
    return await asyncio.to_thread(_run)

@activity.defn
async def list_raw_artifacts(
        scan_id: str
):
    def _run():
        repo = Repo()
        res = repo.list_raw_artifacts(scan_id=scan_id)
        return res

    activity.heartbeat({'stage': 'list_raw_artifacts', 'scan_id': scan_id})
    return await asyncio.to_thread(_run)


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
