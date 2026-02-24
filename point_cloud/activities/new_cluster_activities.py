from __future__ import annotations

import asyncio
import json
import time
import os
import numpy as np
from pathlib import Path
from typing import Any, Dict, List, Optional

import pdal
from temporalio import activity
from temporalio.exceptions import ApplicationError

# Check optional dependencies
try:
    import open3d as o3d
except ImportError:
    o3d = None

try:
    from scipy.spatial import cKDTree
    from scipy.sparse import csr_matrix
    from scipy.sparse.csgraph import connected_components
except ImportError:
    cKDTree = None
    csr_matrix = None
    connected_components = None

from lidar_app.app.repo import Repo
from lidar_app.app.s3_store import S3Store, S3Ref, scan_prefix
from lidar_app.app.artifact_service import store_artifact, download_artifact
from env_vars import settings


# -----------------------------
# 1. Graph Helpers
# -----------------------------
def build_adjacency_radius(points_xyz: np.ndarray, radius: float) -> Any:
    if cKDTree is None or csr_matrix is None:
        raise ImportError("scipy is required for graph clustering")

    n = points_xyz.shape[0]
    tree = cKDTree(points_xyz)
    neigh = tree.query_ball_point(points_xyz, r=radius)

    rows, cols, data = [], [], []
    for i, js in enumerate(neigh):
        for j in js:
            if j != i:
                rows.append(i)
                cols.append(j)
                data.append(1)

    A = csr_matrix((data, (rows, cols)), shape=(n, n), dtype=np.uint8)
    return A.maximum(A.transpose())


def prune_small_components(labels: np.ndarray, min_size: int) -> np.ndarray:
    out = labels.copy()
    if out.size == 0: return out
    uniq, counts = np.unique(out[out >= 0], return_counts=True)
    small = set(uniq[counts < min_size].tolist())
    if small:
        out[np.isin(out, list(small))] = -1

    kept = np.unique(out[out >= 0])
    remap = {old: new for new, old in enumerate(kept.tolist())}
    for old, new in remap.items():
        out[out == old] = new
    return out


@activity.defn
async def cluster_scan_custom(
    company_id: str,
    dataset_version_id: str,
    scan_id: str,
    schema_version: str,
    voxel: float = 0.05,
    graph_radius: float = 0.3,
    min_cluster_size: int = 20,
) -> Dict[str, Any]:
    """
    Custom clustering activity:
    1. Downloads reprojected cloud.
    2. Runs PDAL SMRF (Ground Classification).
    3. Runs Graph-based clustering on non-ground points.
    4. Uploads result as derived.clustered_point_cloud.
    """
    if o3d is None or cKDTree is None:
        raise ApplicationError("open3d and scipy are required for this activity")

    def _run() -> Dict[str, Any]:
        repo = Repo()
        s3 = S3Store(settings.s3_endpoint, settings.s3_access_key, settings.s3_secret_key, settings.s3_region)

        # 1. Find input artifact
        input_kind = "derived.reprojected_point_cloud"
        art = repo.find_derived_artifact(scan_id, input_kind, schema_version)
        if not art:
            raise ApplicationError(f"Artifact {input_kind} not found for scan {scan_id}")

        prefix = scan_prefix(company_id, dataset_version_id, scan_id)

        # Prepare local paths
        import tempfile
        with tempfile.TemporaryDirectory() as td:
            local_dir = Path(td)
            local_in = download_artifact(s3, art.s3_bucket, art.s3_key, local_dir)

            filename = local_in.name
            safe_out_path = str(local_dir / f"clustered_{filename}")
            safe_in_path = str(local_in)

            print(f"[{scan_id}] Start processing...")

            # 2. PDAL Pipeline: Read -> Assign(0) -> SMRF
            # Note: We skip Reprojection as input is already reprojected.
            read_json = [
                {
                    "type": "readers.las",
                    "filename": safe_in_path
                },
                {
                    "type": "filters.assign",
                    "assignment": "Classification[:]=0"
                },
                {
                    "type": "filters.smrf"
                }
            ]

            pipe_read = pdal.Pipeline(json.dumps(read_json))
            pipe_read.execute()
            arr = pipe_read.arrays[0]

            # 3. Preparation for Clustering
            is_ground = arr['Classification'] == 2
            trees_mask = ~is_ground

            trees_idx_global = np.where(trees_mask)[0]
            trees_xyz = np.column_stack((arr['X'][trees_mask], arr['Y'][trees_mask], arr['Z'][trees_mask]))

            print(f"  -> Ground: {np.sum(is_ground):,} | Objects: {len(trees_xyz):,}")

            # Downsample (for faster neighbor search)
            if voxel > 0:
                pcd = o3d.geometry.PointCloud()
                pcd.points = o3d.utility.Vector3dVector(trees_xyz)
                pcd_down = pcd.voxel_down_sample(voxel)
                pts_down = np.asarray(pcd_down.points)
            else:
                pts_down = trees_xyz

            # 4. Clustering
            print(f"  -> Clustering (r={graph_radius}, min={min_cluster_size})...")
            A = build_adjacency_radius(pts_down, radius=graph_radius)
            _, labels_down = connected_components(A, directed=False, return_labels=True)
            labels_down = prune_small_components(labels_down, min_size=min_cluster_size)

            k = int(labels_down.max() + 1) if labels_down.size and labels_down.max() >= 0 else 0
            print(f"  -> Found unique objects: {k}")

            # Remap labels to full cloud
            if voxel > 0:
                tree_kd = cKDTree(pts_down)
                _, idxs = tree_kd.query(trees_xyz, k=1)
                full_labels = labels_down[idxs]
            else:
                full_labels = labels_down

            # 5. Write Attributes
            # New Class: 2 for Ground, others based on cluster?
            # User script: "1 = Noise/Unclassified", "2 = Ground".
            # "new_class[valid_global_idx] = (valid_labels % 253) + 3" -> Colors?
            # "new_source[valid_global_idx] = (valid_labels % 65535) + 1" -> ID in PointSourceId

            print("  -> Updating classes and IDs...")
            new_class = np.ones(len(arr), dtype=np.uint8)  # 1 = Unclassified
            new_class[is_ground] = 2  # 2 = Ground

            new_source = np.zeros(len(arr), dtype=np.uint16)
            if 'PointSourceId' in arr.dtype.names:
                new_source = arr['PointSourceId'].copy()

            valid_tree_mask = full_labels != -1
            valid_labels = full_labels[valid_tree_mask]
            valid_global_idx = trees_idx_global[valid_tree_mask]

            # Cyclic classes 3-255
            new_class[valid_global_idx] = (valid_labels % 253) + 3
            # PointSourceId stores the actual Cluster ID
            new_source[valid_global_idx] = (valid_labels % 65535) + 1

            arr['Classification'] = new_class
            if 'PointSourceId' in arr.dtype.names:
                arr['PointSourceId'] = new_source

            # 6. Save Result
            print("  -> Writing final file...")
            # We must use writers.las. The input might be LAZ.
            # We want to preserve scale/offset if possible, or use auto.
            # User script used scale=0.001 and offset=auto.
            write_json = [
                {
                    "type": "writers.las",
                    "filename": safe_out_path,
                    "forward": "all",
                    "scale_x": 0.001,
                    "scale_y": 0.001,
                    "scale_z": 0.001,
                    "offset_x": "auto",
                    "offset_y": "auto",
                    "offset_z": "auto"
                }
            ]

            pipe_write = pdal.Pipeline(json.dumps(write_json), arrays=[arr])
            pipe_write.execute()

            # 7. Upload Artifact
            out_key = f"{prefix}/derived/v{schema_version}/clustered/point_cloud/{filename}"

            res = store_artifact(
                repo=repo,
                s3=s3,
                company_id=company_id,
                scan_id=scan_id,
                kind="derived.clustered_point_cloud",
                schema_version=schema_version,
                bucket=settings.s3_bucket,
                key=out_key,
                local_file_path=safe_out_path,
                status="AVAILABLE",
                meta={
                    "voxel": voxel,
                    "graph_radius": graph_radius,
                    "min_cluster_size": min_cluster_size,
                    "clusters_found": k
                }
            )

            return {
                "scan_id": scan_id,
                "clustered_key": out_key,
                "clusters": k
            }

    activity.heartbeat({"stage": "cluster_custom", "scan_id": scan_id})
    return await asyncio.to_thread(_run)
