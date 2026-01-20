from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from typing import Any, Dict, List

import pdal
from temporalio import activity

from lidar_app.app.repo import Repo
from lidar_app.app.config import settings
from lidar_app.app.s3_store import S3Store, S3Ref


def _pose_to_pdal_matrix(pose: dict) -> str:
    """
    PDAL filters.transformation expects a 4x4 matrix serialized as
    'a b c d e f g h i j k l m n o p' (row-major).
    pose: {"R": [[...],[...],[...]], "t":[x,y,z]}
    """
    R = pose.get("R") or [[1,0,0],[0,1,0],[0,0,1]]
    t = pose.get("t") or [0,0,0]
    M = [
        [float(R[0][0]), float(R[0][1]), float(R[0][2]), float(t[0])],
        [float(R[1][0]), float(R[1][1]), float(R[1][2]), float(t[1])],
        [float(R[2][0]), float(R[2][1]), float(R[2][2]), float(t[2])],
        [0.0, 0.0, 0.0, 1.0],
    ]
    flat = [str(x) for row in M for x in row]
    return " ".join(flat)


def _run_pdal_pipeline(pipeline: dict) -> None:
    pipe = pdal.Pipeline(json.dumps(pipeline))
    try:
        pipe.execute()
    except Exception as exc:
        raise RuntimeError(
            "pdal pipeline failed\n"
            f"error: {exc}\n"
            f"pipeline:\n{json.dumps(pipeline, indent=2)}\n"
        ) from exc


def _find_merge_cloud_artifact(repo: Repo, scan_id: str, schema_version: str) -> tuple[str, Any]:
    for kind in (
        "derived.registration_point_cloud",
        "derived.preprocessed_point_cloud",
        "derived.reprojected_point_cloud",
    ):
        art = repo.find_derived_artifact(scan_id, kind, schema_version)
        if art:
            return kind, art
    raise RuntimeError(
        f"No derived cloud found for scan {scan_id} "
        "(registration/preprocessed/reprojected missing)"
    )


@activity.defn
async def export_merged_laz(
    company_id: str,
    dataset_version_id: str,
    schema_version: str,
    out_name: str = "merged.laz",
) -> Dict[str, Any]:
    """
    Downloads derived.registration/preprocessed/reprojected point clouds for all scans,
    applies absolute ScanPose (core.scan_poses) to each, merges into one LAZ,
    uploads to S3. Returns s3 ref.
    """

    def _run() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(
            settings.s3_endpoint,
            settings.s3_access_key,
            settings.s3_secret_key,
            settings.s3_region,
        )

        scans = repo.list_scans_by_dataset_version(dataset_version_id)
        if not scans:
            raise RuntimeError(f"No scans in dataset_version_id={dataset_version_id}")

        poses = repo.list_scan_poses_by_dataset_version(dataset_version_id)

        pose_by_scan = {}
        for p in poses:
            if isinstance(p, dict):
                sid = p.get("scan_id")
                pose = p.get("pose")
            else:
                sid = p.scan_id
                pose = p.pose
            if sid and pose:
                pose_by_scan[sid] = pose

        # sanity: poses must exist for all scans you want to merge
        missing = [s.id for s in scans if s.id not in pose_by_scan]
        if missing:
            raise RuntimeError(f"Missing scan_poses for scans: {missing}")

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)

            stages: List[dict] = []
            local_files: List[Path] = []
            merge_inputs: List[str] = []

            # 1) download each derived cloud locally and add reader + transform stage
            for idx, s in enumerate(scans, start=1):
                _, art = _find_merge_cloud_artifact(repo, s.id, schema_version)

                local = td / Path(art.s3_key).name
                s3.download_file(S3Ref(art.s3_bucket, art.s3_key), str(local))
                local_files.append(local)

                reader_tag = f"scan_reader_{idx}"
                transform_tag = f"scan_transform_{idx}"
                stages.append(
                    {
                        "type": "readers.las",
                        "filename": str(local),
                        "tag": reader_tag,
                    }
                )

                # apply absolute pose
                M = _pose_to_pdal_matrix(pose_by_scan[s.id])
                stages.append(
                    {
                        "type": "filters.transformation",
                        "matrix": M,
                        "inputs": [reader_tag],
                        "tag": transform_tag,
                    }
                )
                merge_inputs.append(transform_tag)

            # 2) merge (if needed) and write
            out_local = td / out_name
            if len(merge_inputs) > 1:
                merge_tag = "merge_all_scans"
                stages.append(
                    {
                        "type": "filters.merge",
                        "inputs": merge_inputs,
                        "tag": merge_tag,
                    }
                )
                writer_inputs = [merge_tag]
            else:
                writer_inputs = merge_inputs

            stages.append(
                {
                    "type": "writers.las",
                    "filename": str(out_local),
                    "compression": "laszip",
                    "inputs": writer_inputs,
                }
            )

            pipeline = {"pipeline": stages}
            _run_pdal_pipeline(pipeline)

            if not out_local.exists():
                raise RuntimeError("Merged LAZ not produced by PDAL")

            # 3) upload to S3
            dvid = dataset_version_id
            cid = company_id

            out_key = (
                f"tenants/{cid}/dataset_versions/{dvid}/"
                f"derived/v{schema_version}/merged/point_cloud/{out_local.name}"
            )

            etag, size = s3.upload_file(S3Ref(settings.s3_bucket, out_key), str(out_local))

            # ВОТ СЮДА
            anchor_scan_id = scans[0].id
            repo.upsert_derived_artifact(
                company_id=company_id,
                scan_id=anchor_scan_id,
                kind="derived.merged_point_cloud",
                schema_version=schema_version,
                s3_bucket=settings.s3_bucket,
                s3_key=out_key,
                etag=etag,
                size_bytes=size,
                status="READY",
                meta={
                    "scope": "dataset_version",
                    "dataset_version_id": dataset_version_id,
                    "scan_ids": [s.id for s in scans],
                },
            )

            return {
                "bucket": settings.s3_bucket,
                "key": out_key,
                "etag": etag,
                "size_bytes": size,
                "scans": [s.id for s in scans],
            }

    activity.heartbeat({"stage": "export_merged_laz", "dataset_version_id": dataset_version_id})
    return await asyncio.to_thread(_run)
