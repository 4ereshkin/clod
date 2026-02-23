from __future__ import annotations

import asyncio
import json
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import numpy as np
import open3d as o3d
from temporalio import activity

from legacy_env_vars import settings
from lidar_app.app.repo import Repo
from lidar_app.app.s3_store import S3Ref, S3Store, safe_segment


def _pdal_to_ply(inp: Path, out: Path) -> None:
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
    pts = np.asarray(pcd.points)
    c = np.array(center_xyz, dtype=float).reshape(1, 3)
    d2 = ((pts - c) ** 2).sum(axis=1)
    idx = np.where(d2 <= radius * radius)[0]
    return pcd.select_by_index(idx.tolist())


def _estimate_normals(pcd: o3d.geometry.PointCloud, radius: float, max_nn: int = 30) -> None:
    pcd.estimate_normals(
        search_param=o3d.geometry.KDTreeSearchParamHybrid(radius=radius, max_nn=max_nn)
    )


def _voxel_down(pcd: o3d.geometry.PointCloud, voxel: float) -> o3d.geometry.PointCloud:
    if voxel <= 0:
        return pcd
    return pcd.voxel_down_sample(voxel_size=voxel)


def _compute_fpfh(pcd: o3d.geometry.PointCloud, voxel: float) -> o3d.pipelines.registration.Feature:
    radius_normal = voxel * 2.0
    radius_feature = voxel * 5.0
    _estimate_normals(pcd, radius=radius_normal)
    return o3d.pipelines.registration.compute_fpfh_feature(
        pcd,
        o3d.geometry.KDTreeSearchParamHybrid(radius=radius_feature, max_nn=100),
    )


def _mat4_to_pose(T: np.ndarray) -> Dict[str, Any]:
    R = T[:3, :3].tolist()
    t = T[:3, 3].tolist()
    return {"R": R, "t": t}


def _pose_to_mat(pose: Dict[str, Any]) -> np.ndarray:
    R = np.array(pose.get("R") or np.eye(3), dtype=float)
    t = np.array(pose.get("t") or [0.0, 0.0, 0.0], dtype=float)
    T = np.eye(4, dtype=float)
    T[:3, :3] = R
    T[:3, 3] = t
    return T


def _identity_pose() -> Dict[str, Any]:
    return {"t": [0.0, 0.0, 0.0], "R": [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]]}


def _dsreg_prefix(company_id: str, dataset_version_id: str) -> str:
    cid = safe_segment(company_id)
    dvid = safe_segment(dataset_version_id)
    return f"tenants/{cid}/dataset_versions/{dvid}/registration"


def _get_derived_cloud_local(repo: Repo, s3: S3Store, scan_id: str, schema_version: str, td: Path) -> Path:
    art = repo.find_derived_artifact(scan_id, "derived.registration_point_cloud", schema_version)
    if not art:
        art = repo.find_derived_artifact(scan_id, "derived.preprocessed_point_cloud", schema_version)
    if not art:
        art = repo.find_derived_artifact(scan_id, "derived.reprojected_point_cloud", schema_version)
    if not art:
        raise RuntimeError(
            f"No derived cloud found for scan {scan_id} "
            "(registration/preprocessed/reprojected missing)"
        )
    local = td / Path(art.s3_key).name
    s3.download_file(S3Ref(art.s3_bucket, art.s3_key), str(local))
    return local


def _cascade_icp(
    src: o3d.geometry.PointCloud,
    tgt: o3d.geometry.PointCloud,
    init_T: np.ndarray,
    voxel_sizes: Iterable[float],
    max_corr_multipliers: Iterable[float],
) -> Tuple[np.ndarray, Dict[str, Any]]:
    T = init_T
    metrics: Dict[str, Any] = {"levels": []}
    for voxel, max_corr_mul in zip(voxel_sizes, max_corr_multipliers):
        src_ds = _voxel_down(src, voxel)
        tgt_ds = _voxel_down(tgt, voxel)
        radius = max(voxel * 2.0, 0.1)
        _estimate_normals(src_ds, radius=radius)
        _estimate_normals(tgt_ds, radius=radius)
        max_corr = max(voxel * max_corr_mul, 0.1)
        reg = o3d.pipelines.registration.registration_icp(
            src_ds,
            tgt_ds,
            max_corr,
            T,
            o3d.pipelines.registration.TransformationEstimationPointToPlane(),
            o3d.pipelines.registration.ICPConvergenceCriteria(max_iteration=50),
        )
        T = reg.transformation
        metrics["levels"].append(
            {
                "voxel": float(voxel),
                "max_corr": float(max_corr),
                "fitness": float(reg.fitness),
                "rmse": float(getattr(reg, "inlier_rmse", 0.0)),
                "n_src": int(len(src_ds.points)),
                "n_tgt": int(len(tgt_ds.points)),
            }
        )
    return T, metrics


def _global_registration(
    src: o3d.geometry.PointCloud,
    tgt: o3d.geometry.PointCloud,
    voxel: float,
) -> Tuple[np.ndarray, Dict[str, Any]]:
    src_ds = _voxel_down(src, voxel)
    tgt_ds = _voxel_down(tgt, voxel)
    src_fpfh = _compute_fpfh(src_ds, voxel)
    tgt_fpfh = _compute_fpfh(tgt_ds, voxel)
    distance_threshold = voxel * 1.5
    reg = o3d.pipelines.registration.registration_ransac_based_on_feature_matching(
        src_ds,
        tgt_ds,
        src_fpfh,
        tgt_fpfh,
        True,
        distance_threshold,
        o3d.pipelines.registration.TransformationEstimationPointToPoint(False),
        4,
        [
            o3d.pipelines.registration.CorrespondenceCheckerBasedOnEdgeLength(0.9),
            o3d.pipelines.registration.CorrespondenceCheckerBasedOnDistance(distance_threshold),
        ],
        o3d.pipelines.registration.RANSACConvergenceCriteria(4000000, 500),
    )
    return reg.transformation, {
        "fitness": float(reg.fitness),
        "rmse": float(getattr(reg, "inlier_rmse", 0.0)),
        "voxel": float(voxel),
        "n_src": int(len(src_ds.points)),
        "n_tgt": int(len(tgt_ds.points)),
    }


@dataclass
class ProdRegistrationPairParams:
    source_scan_id: str
    target_scan_id: str
    schema_version: str
    crop_radius_m: float = 40.0
    global_voxel_m: float = 1.0
    cascade_voxels_m: Tuple[float, float, float] = (1.0, 0.3, 0.1)
    cascade_max_corr_multipliers: Tuple[float, float, float] = (3.0, 2.0, 1.5)
    min_fitness: float = 0.2


@activity.defn
async def prod_build_registration_anchors(
    company_id: str,
    dataset_version_id: str,
    scan_id: str,
    schema_version: str,
) -> Dict[str, Any]:
    def _run() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)
        prefix = f"tenants/{safe_segment(company_id)}/dataset_versions/{safe_segment(dataset_version_id)}/scans/{safe_segment(scan_id)}"

        path_art = repo.find_derived_artifact(scan_id, "derived.reprojected_trajectory", schema_version)
        cp_art = repo.find_derived_artifact(scan_id, "derived.reprojected_control_points", schema_version)

        head = tail = None
        if path_art:
            txt = s3.get_bytes(S3Ref(path_art.s3_bucket, path_art.s3_key)).decode("utf-8", errors="replace")
            pts = _parse_xyz_lines(txt)
            if pts:
                head = pts[0][1]
                tail = pts[-1][1]

        cps = []
        if cp_art:
            txt = s3.get_bytes(S3Ref(cp_art.s3_bucket, cp_art.s3_key)).decode("utf-8", errors="replace")
            pts = _parse_xyz_lines(txt)
            cps = [{"xyz": xyz} for _, xyz in pts[:2000]]

        anchors = {
            "scan_id": scan_id,
            "dataset_version_id": dataset_version_id,
            "head": head,
            "tail": tail,
            "control_points": cps,
        }

        key = f"{prefix}/derived/v{schema_version}/registration/anchors.json"
        body = json.dumps(anchors, ensure_ascii=False, indent=2).encode("utf-8")
        etag, size = s3.put_bytes(S3Ref(settings.s3_bucket, key), body, content_type="application/json")
        repo.upsert_derived_artifact(
            company_id=company_id,
            scan_id=scan_id,
            kind="derived.registration_anchors",
            schema_version=schema_version,
            s3_bucket=settings.s3_bucket,
            s3_key=key,
            etag=etag,
            size_bytes=size,
            status="AVAILABLE",
            meta={},
        )
        return {"anchors_key": key}

    def _parse_xyz_lines(txt: str) -> List[Tuple[int, List[float]]]:
        out: List[Tuple[int, List[float]]] = []
        for i, line in enumerate(txt.splitlines()):
            parts = line.strip().split()
            if len(parts) < 3:
                continue
            try:
                x, y, z = float(parts[0]), float(parts[1]), float(parts[2])
            except ValueError:
                continue
            out.append((i, [x, y, z]))
        return out

    return await asyncio.to_thread(_run)


@activity.defn
async def prod_propose_registration_edges(
    company_id: str,
    dataset_version_id: str,
    scan_id: str,
    schema_version: str,
) -> Dict[str, Any]:
    def _run() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        scans = repo.list_scans_by_dataset_version(dataset_version_id)
        scans_other = [s for s in scans if s.id != scan_id]

        my_art = repo.find_derived_artifact(scan_id, "derived.registration_anchors", schema_version)
        if not my_art:
            raise RuntimeError("anchors for current scan not found")
        my = json.loads(s3.get_bytes(S3Ref(my_art.s3_bucket, my_art.s3_key)).decode("utf-8"))

        def dist(a, b) -> float:
            return ((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2 + (a[2] - b[2]) ** 2) ** 0.5

        edges = []
        for s in scans_other:
            art = repo.find_derived_artifact(s.id, "derived.registration_anchors", schema_version)
            if not art:
                continue
            other = json.loads(s3.get_bytes(S3Ref(art.s3_bucket, art.s3_key)).decode("utf-8"))

            if my.get("tail") and other.get("head"):
                d = dist(my["tail"], other["head"])
                if d < 20.0:
                    t = [other["head"][0] - my["tail"][0], other["head"][1] - my["tail"][1], other["head"][2] - my["tail"][2]]
                    edges.append(
                        {
                            "from": scan_id,
                            "to": s.id,
                            "kind": "traj_tail_head",
                            "weight": max(0.1, 20.0 / (d + 1e-6)),
                            "transform_guess": {"t": t, "R": _identity_pose()["R"]},
                            "meta": {"d_tail_head": d},
                        }
                    )

        prefix = f"tenants/{safe_segment(company_id)}/dataset_versions/{safe_segment(dataset_version_id)}/scans/{safe_segment(scan_id)}"
        key = f"{prefix}/derived/v{schema_version}/registration/edges_proposed.json"
        body = json.dumps({"edges": edges}, ensure_ascii=False, indent=2).encode("utf-8")
        etag, size = s3.put_bytes(S3Ref(settings.s3_bucket, key), body, content_type="application/json")
        repo.upsert_derived_artifact(
            company_id=company_id,
            scan_id=scan_id,
            kind="derived.registration_edges",
            schema_version=schema_version,
            s3_bucket=settings.s3_bucket,
            s3_key=key,
            etag=etag,
            size_bytes=size,
            status="AVAILABLE",
            meta={"count": len(edges)},
        )

        if edges:
            repo.add_scan_edges(company_id, dataset_version_id, edges)

        return {"edges_key": key, "count": len(edges)}

    return await asyncio.to_thread(_run)


@activity.defn
async def prod_register_pair(
    company_id: str,
    dataset_version_id: str,
    params: ProdRegistrationPairParams,
) -> Dict[str, Any]:
    def _run() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            src_local = _get_derived_cloud_local(repo, s3, params.source_scan_id, params.schema_version, td)
            tgt_local = _get_derived_cloud_local(repo, s3, params.target_scan_id, params.schema_version, td)

            src_pcd = _read_cloud_any(src_local)
            tgt_pcd = _read_cloud_any(tgt_local)

            anchors = {}
            for sid in (params.source_scan_id, params.target_scan_id):
                art = repo.find_derived_artifact(sid, "derived.registration_anchors", params.schema_version)
                if art:
                    anchors[sid] = json.loads(s3.get_bytes(S3Ref(art.s3_bucket, art.s3_key)).decode("utf-8"))

            src_tail = anchors.get(params.source_scan_id, {}).get("tail")
            tgt_head = anchors.get(params.target_scan_id, {}).get("head")
            if isinstance(src_tail, list) and isinstance(tgt_head, list):
                src_pcd = _crop_ball(src_pcd, src_tail, params.crop_radius_m)
                tgt_pcd = _crop_ball(tgt_pcd, tgt_head, params.crop_radius_m)

            if src_pcd.is_empty() or tgt_pcd.is_empty():
                return {"accepted": False, "reason": "empty_cloud"}

            T_global, global_meta = _global_registration(src_pcd, tgt_pcd, params.global_voxel_m)

            T_icp, icp_meta = _cascade_icp(
                src_pcd,
                tgt_pcd,
                T_global,
                params.cascade_voxels_m,
                params.cascade_max_corr_multipliers,
            )

            fitness = icp_meta["levels"][-1]["fitness"] if icp_meta["levels"] else 0.0
            if fitness < params.min_fitness:
                return {"accepted": False, "reason": "low_fitness", "fitness": fitness}

            edge = {
                "from": params.source_scan_id,
                "to": params.target_scan_id,
                "kind": "global_icp",
                "weight": max(0.1, fitness),
                "transform_guess": _mat4_to_pose(T_icp),
                "meta": {
                    "global": global_meta,
                    "icp": icp_meta,
                },
            }

            repo.add_scan_edges(company_id, dataset_version_id, [edge])
            return {"accepted": True, "edge": edge}

    return await asyncio.to_thread(_run)


@activity.defn
async def prod_collect_registration_graph(
    company_id: str,
    dataset_version_id: str,
    schema_version: str,
) -> Dict[str, Any]:
    def _run() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        scans = repo.list_scans_by_dataset_version(dataset_version_id)
        scan_ids = [s.id for s in scans]

        anchors: Dict[str, dict] = {}
        for sid in scan_ids:
            art = repo.find_derived_artifact(sid, "derived.registration_anchors", schema_version)
            if not art:
                continue
            data = json.loads(s3.get_bytes(S3Ref(art.s3_bucket, art.s3_key)).decode("utf-8"))
            anchors[sid] = data

        edges_db = repo.list_scan_edges(dataset_version_id)
        edges = [
            {
                "from": e.scan_id_from,
                "to": e.scan_id_to,
                "kind": e.kind,
                "weight": float(e.weight),
                "transform_guess": e.transform_guess or {},
                "meta": e.meta or {},
            }
            for e in edges_db
        ]

        return {
            "company_id": company_id,
            "dataset_version_id": dataset_version_id,
            "schema_version": schema_version,
            "scan_ids": scan_ids,
            "anchors": anchors,
            "edges": edges,
        }

    activity.heartbeat({"stage": "prod_collect_graph"})
    return await asyncio.to_thread(_run)


@activity.defn
async def prod_solve_pose_graph(
    company_id: str,
    dataset_version_id: str,
    schema_version: str,
    graph: Dict[str, Any],
) -> Dict[str, Any]:
    def _run() -> Dict[str, Any]:
        scan_ids: List[str] = graph["scan_ids"]
        edges: List[dict] = graph.get("edges", [])

        if not scan_ids:
            return {"poses": {}, "diagnostics": {"scans_total": 0}}

        root = scan_ids[0]
        poses: Dict[str, np.ndarray] = {root: np.eye(4)}
        adj: Dict[str, List[Tuple[str, np.ndarray]]] = {sid: [] for sid in scan_ids}

        for e in edges:
            a = e.get("from")
            b = e.get("to")
            tg = e.get("transform_guess") or {}
            if not (a in adj and b in adj):
                continue
            T = _pose_to_mat(tg)
            adj[a].append((b, T))

        queue = [root]
        while queue:
            cur = queue.pop(0)
            cur_pose = poses[cur]
            for nxt, T in adj.get(cur, []):
                if nxt in poses:
                    continue
                poses[nxt] = cur_pose @ T
                queue.append(nxt)

        for sid in scan_ids:
            poses.setdefault(sid, np.eye(4))

        pg = o3d.pipelines.registration.PoseGraph()
        index_by_scan = {sid: i for i, sid in enumerate(scan_ids)}
        for sid in scan_ids:
            pg.nodes.append(o3d.pipelines.registration.PoseGraphNode(poses[sid]))

        for e in edges:
            a = e.get("from")
            b = e.get("to")
            tg = e.get("transform_guess") or {}
            if a not in index_by_scan or b not in index_by_scan:
                continue
            T = _pose_to_mat(tg)
            w = float(e.get("weight", 1.0))
            information = np.eye(6) * max(w, 1e-3)
            pg.edges.append(
                o3d.pipelines.registration.PoseGraphEdge(
                    index_by_scan[a],
                    index_by_scan[b],
                    T,
                    information,
                    uncertain=False,
                )
            )

        option = o3d.pipelines.registration.GlobalOptimizationOption(
            max_correspondence_distance=2.0,
            edge_prune_threshold=0.25,
            reference_node=0,
        )
        o3d.pipelines.registration.global_optimization(
            pg,
            o3d.pipelines.registration.GlobalOptimizationLevenbergMarquardt(),
            o3d.pipelines.registration.GlobalOptimizationConvergenceCriteria(),
            option,
        )

        out_poses: Dict[str, dict] = {}
        for sid in scan_ids:
            node = pg.nodes[index_by_scan[sid]]
            out_poses[sid] = _mat4_to_pose(node.pose)

        diagnostics = {
            "root": root,
            "poses_count": len(out_poses),
            "edges_used": len(pg.edges),
        }

        return {"poses": out_poses, "diagnostics": diagnostics}

    activity.heartbeat({"stage": "prod_solve_pose_graph"})
    return await asyncio.to_thread(_run)


@activity.defn
async def prod_persist_pose_graph_solution(
    company_id: str,
    dataset_version_id: str,
    schema_version: str,
    solution: Dict[str, Any],
    force: bool,
) -> Dict[str, Any]:
    def _run() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        prefix = _dsreg_prefix(company_id, dataset_version_id)
        sol_key = f"{prefix}/v{schema_version}/pose_graph_solution.json"
        dia_key = f"{prefix}/v{schema_version}/pose_graph_diagnostics.json"

        body = json.dumps(solution["poses"], ensure_ascii=False, indent=2).encode("utf-8")
        etag1, size1 = s3.put_bytes(S3Ref(settings.s3_bucket, sol_key), body, content_type="application/json")

        dia = json.dumps(solution["diagnostics"], ensure_ascii=False, indent=2).encode("utf-8")
        etag2, size2 = s3.put_bytes(S3Ref(settings.s3_bucket, dia_key), dia, content_type="application/json")

        poses_written = 0
        for scan_id, pose in (solution["poses"] or {}).items():
            repo.upsert_scan_pose(company_id, dataset_version_id, scan_id, pose, quality=1.0)
            poses_written += 1

        return {
            "solution_key": sol_key,
            "diagnostics_key": dia_key,
            "poses_written": poses_written,
            "solution_etag": etag1,
            "diagnostics_etag": etag2,
            "solution_size": size1,
            "diagnostics_size": size2,
            "force": force,
        }

    activity.heartbeat({"stage": "prod_persist_pose_graph_solution"})
    return await asyncio.to_thread(_run)
