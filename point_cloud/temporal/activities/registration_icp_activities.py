from __future__ import annotations

import asyncio
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from temporalio import activity

from lidar_app.app.repo import Repo
from lidar_app.app.config import settings
from lidar_app.app.s3_store import S3Store, S3Ref

import open3d as o3d


def _pdal_to_ply(inp: Path, out: Path) -> None:
    # PDAL translate <in> <out> (формат по расширению)
    cmd = ["pdal", "translate", str(inp), str(out)]
    p = subprocess.run(cmd, capture_output=True, text=True)
    if p.returncode != 0 or not out.exists():
        raise RuntimeError(
            "pdal translate failed\n"
            f"cmd: {' '.join(cmd)}\n"
            f"rc: {p.returncode}\n"
            f"stdout:\n{p.stdout}\n"
            f"stderr:\n{p.stderr}\n"
        )


def _read_cloud_any(local_path: Path) -> o3d.geometry.PointCloud:
    # если уже ply/pcd/xyz — читаем напрямую; если laz/las — конвертим в ply
    suf = local_path.suffix.lower()
    if suf in [".ply", ".pcd", ".xyz", ".xyzn", ".xyzrgb", ".pts"]:
        pcd = o3d.io.read_point_cloud(str(local_path))
        if pcd.is_empty():
            raise RuntimeError(f"Open3D read empty point cloud: {local_path}")
        return pcd

    if suf in [".laz", ".las"]:
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            ply = td / (local_path.stem + ".ply")
            _pdal_to_ply(local_path, ply)
            pcd = o3d.io.read_point_cloud(str(ply))
            if pcd.is_empty():
                raise RuntimeError(f"Open3D read empty point cloud after PDAL: {local_path}")
            return pcd

    raise ValueError(f"Unsupported point cloud extension: {local_path.suffix}")


def _crop_ball(pcd: o3d.geometry.PointCloud, center_xyz: List[float], radius: float) -> o3d.geometry.PointCloud:
    # Open3D: вырежем сферой через расстояние до центра (быстро, достаточно для MVP)
    import numpy as np
    pts = np.asarray(pcd.points)
    c = np.array(center_xyz, dtype=float).reshape(1, 3)
    d2 = ((pts - c) ** 2).sum(axis=1)
    mask = d2 <= (radius * radius)
    idx = np.where(mask)[0]
    return pcd.select_by_index(idx.tolist())


def _to_R_t(T: List[List[float]]) -> Tuple[List[List[float]], List[float]]:
    R = [T[0][:3], T[1][:3], T[2][:3]]
    t = [T[0][3], T[1][3], T[2][3]]
    return R, t


def _make_T_from_guess(guess: Dict[str, Any]) -> List[List[float]]:
    # guess: {"t":[x,y,z], "R":[[...],[...],[...]]}
    t = guess.get("t") or [0.0, 0.0, 0.0]
    R = guess.get("R") or [[1.0,0.0,0.0],[0.0,1.0,0.0],[0.0,0.0,1.0]]
    T = [
        [R[0][0], R[0][1], R[0][2], float(t[0])],
        [R[1][0], R[1][1], R[1][2], float(t[1])],
        [R[2][0], R[2][1], R[2][2], float(t[2])],
        [0.0, 0.0, 0.0, 1.0],
    ]
    return T


@activity.defn
async def refine_edges_with_icp(
    company_id: str,
    dataset_version_id: str,
    schema_version: str,
    max_edges: int = 50,
    crop_radius_m: float = 30.0,
    voxel_m: float = 0.5,
    max_corr_m: float = 2.0,
    min_points: int = 5000,
    fitness_min: float = 0.15,
) -> Dict[str, Any]:
    """
    1) Берём edges из core.scan_edges
    2) Для каждого ребра делаем ICP между окном вокруг tail(source) и head(target)
    3) Пишем transform_guess обратно в scan_edges (upsert)
    """

    def _run() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        scans = repo.list_scans_by_dataset_version(dataset_version_id)
        scan_ids = [s.id for s in scans]

        edges_db = repo.list_scan_edges(dataset_version_id)
        if not edges_db:
            return {"refined": 0, "skipped": 0, "reason": "no_edges_in_db"}

        # anchors: заранее подгрузим, чтобы не долбить S3 по 100 раз
        anchors: Dict[str, Dict[str, Any]] = {}
        for sid in scan_ids:
            art = repo.find_derived_artifact(sid, "derived.registration_anchors", schema_version)
            if not art:
                continue
            try:
                anchors[sid] = json.loads(s3.get_bytes(S3Ref(art.s3_bucket, art.s3_key)).decode("utf-8"))
            except Exception:
                continue

        # helper: скачать derived cloud
        def _get_derived_cloud_local(scan_id: str, td: Path) -> Path:
            # 1) prefer preprocessed
            art = repo.find_derived_artifact(scan_id, "derived.preprocessed_point_cloud", schema_version)
            # 2) fallback to reprojected
            if not art:
                art = repo.find_derived_artifact(scan_id, "derived.reprojected_point_cloud", schema_version)
            if not art:
                raise RuntimeError(f"No derived cloud found for scan {scan_id} (preprocessed/reprojected missing)")

            local = td / Path(art.s3_key).name
            s3.download_file(S3Ref(art.s3_bucket, art.s3_key), str(local))
            return local

        refined_edges: List[dict] = []
        refined = 0
        skipped = 0

        # ограничим количество, чтобы не умереть
        edges_db = edges_db[:max_edges]

        for e in edges_db:
            src = e.scan_id_from
            tgt = e.scan_id_to

            a_src = anchors.get(src) or {}
            a_tgt = anchors.get(tgt) or {}
            src_tail = a_src.get("tail")
            tgt_head = a_tgt.get("head")

            if not (isinstance(src_tail, list) and len(src_tail) == 3 and isinstance(tgt_head, list) and len(tgt_head) == 3):
                skipped += 1
                continue

            with tempfile.TemporaryDirectory() as td:
                td = Path(td)

                # download both clouds
                src_local = _get_derived_cloud_local(src, td)
                tgt_local = _get_derived_cloud_local(tgt, td)

                # load
                src_pcd = _read_cloud_any(src_local)
                tgt_pcd = _read_cloud_any(tgt_local)

                # crop around endpoints
                src_crop = _crop_ball(src_pcd, src_tail, crop_radius_m)
                tgt_crop = _crop_ball(tgt_pcd, tgt_head, crop_radius_m)

                if len(src_crop.points) < min_points or len(tgt_crop.points) < min_points:
                    skipped += 1
                    continue

                # downsample
                src_ds = src_crop.voxel_down_sample(voxel_m)
                tgt_ds = tgt_crop.voxel_down_sample(voxel_m)

                # normals for point-to-plane
                src_ds.estimate_normals(search_param=o3d.geometry.KDTreeSearchParamHybrid(radius=voxel_m * 4.0, max_nn=30))
                tgt_ds.estimate_normals(search_param=o3d.geometry.KDTreeSearchParamHybrid(radius=voxel_m * 4.0, max_nn=30))

                # initial guess from current edge transform_guess (already tail->head approx)
                init_guess = e.transform_guess or {}
                T0 = _make_T_from_guess(init_guess)

                reg = o3d.pipelines.registration.registration_icp(
                    src_ds,
                    tgt_ds,
                    max_corr_m,
                    T0,
                    o3d.pipelines.registration.TransformationEstimationPointToPlane(),
                    o3d.pipelines.registration.ICPConvergenceCriteria(max_iteration=50),
                )

                if reg.fitness < fitness_min:
                    skipped += 1
                    continue

                T = reg.transformation.tolist()
                R, t = _to_R_t(T)

                # weight: примитивно, но работает
                w = float(reg.fitness) / max(float(reg.inlier_rmse), 1e-6)

                refined_edges.append({
                    "from": src,
                    "to": tgt,
                    "kind": e.kind,
                    "weight": w,
                    "transform_guess": {"R": R, "t": t},
                    "meta": {
                        "icp": {
                            "fitness": float(reg.fitness),
                            "rmse": float(reg.inlier_rmse),
                            "voxel_m": float(voxel_m),
                            "crop_radius_m": float(crop_radius_m),
                            "max_corr_m": float(max_corr_m),
                            "n_src": int(len(src_ds.points)),
                            "n_tgt": int(len(tgt_ds.points)),
                        }
                    },
                })
                refined += 1

        # upsert back into DB
        if refined_edges:
            repo.add_scan_edges(company_id, dataset_version_id, refined_edges)

        return {"refined": refined, "skipped": skipped, "total": len(edges_db)}

    activity.heartbeat({"stage": "icp_refine", "dataset_version_id": dataset_version_id})
    return await asyncio.to_thread(_run)
