from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Optional, Dict, Any

from botocore.exceptions import ClientError
from temporalio import activity

from lidar_app.app.repo import Repo
from lidar_app.app.s3_store import S3Store
from lidar_app.app.config import settings


@activity.defn
async def register_raw_artifact_from_s3(
    company_id: str,
    dataset_version_id: str,
    scan_id: str,
    artifact_kind: str,
    s3_key: str,
    s3_bucket: Optional[str] = None,
) -> Dict[str, Any]:
    bucket = s3_bucket or settings.s3_bucket

    def _register():
        repo = Repo()
        scan = repo.get_scan(scan_id)
        if scan.company_id != company_id:
            raise RuntimeError(f"Scan {scan_id} does not belong to company {company_id}")
        if dataset_version_id and scan.dataset_version_id != dataset_version_id:
            raise RuntimeError(
                f"Scan {scan_id} does not belong to dataset_version {dataset_version_id}"
            )

        s3 = S3Store(
            settings.s3_endpoint,
            settings.s3_access_key,
            settings.s3_secret_key,
            settings.s3_region,
        )

        try:
            head = s3.client.head_object(Bucket=bucket, Key=s3_key)
        except ClientError as exc:
            raise RuntimeError(f"S3 object not found: s3://{bucket}/{s3_key}") from exc

        etag = head.get("ETag")
        size = head.get("ContentLength")

        repo.register_raw_artifact(
            company_id=company_id,
            scan_id=scan_id,
            kind=artifact_kind,
            bucket=bucket,
            key=s3_key,
            etag=etag.strip("\"") if isinstance(etag, str) else None,
            size_bytes=int(size) if size is not None else None,
            meta={"filename": Path(s3_key).name},
        )

        return {
            "bucket": bucket,
            "key": s3_key,
            "etag": etag.strip("\"") if isinstance(etag, str) else None,
            "size_bytes": int(size) if size is not None else None,
            "kind": artifact_kind,
        }

    activity.heartbeat({"status": "registering", "key": s3_key})
    return await asyncio.to_thread(_register)
