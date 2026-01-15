from __future__ import annotations

import asyncio
import pdal
import json

from typing import Any, Dict

from pathlib import Path
from temporalio import activity
from temporalio.exceptions import ApplicationError

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
            raise ApplicationError(
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

@activity.defn
async def point_cloud_meta(point_cloud_file: str) -> Dict[str, Any]:

    def _run():
        output_path = Path(point_cloud_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        pipeline_json = [
            {
                "type": "readers.las",
             "filename": fr"{point_cloud_file}"
            }
        ]

        try:
            pipeline_spec = json.dumps(pipeline_json)
        except Exception as exc:
            raise ApplicationError(f"Failed to serialize PDAL pipeline:: \n{exc}")

        pipeline = pdal.Pipeline(pipeline_spec)
        try:
            pipeline.execute()
        except Exception as exc:
            raise ApplicationError(f"Failed to execute PDAL pipeline: \n{exc}")

        raw_metadata = pipeline.metadata['metadata']

        try:
            metadata = json.loads(raw_metadata) if isinstance(raw_metadata, str) else raw_metadata
        except Exception as exc:
            raise ApplicationError(f"Failed to decode PDAL metadata: \n{exc}")

        return metadata.get('metadata', metadata)

    activity.heartbeat(
        {'stage': 'point_cloud_meta',
         'file': point_cloud_file})
    return await asyncio.to_thread(_run)