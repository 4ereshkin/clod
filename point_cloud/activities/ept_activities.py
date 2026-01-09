from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Optional, Dict, Any, List
from datetime import datetime, timezone

from temporalio import activity
from temporalio.exceptions import ApplicationError

from lidar_app.app.repo import Repo
from lidar_app.app.s3_store import S3Store, scan_prefix, derived_manifest_key
from lidar_app.app.config import settings
from lidar_app.app.models import IngestRun, Scan, Artifact
from lidar_app.app.artifact_service import store_artifact


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
async def ensure_dataset(
    company_id: str,
    crs_id: str,
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