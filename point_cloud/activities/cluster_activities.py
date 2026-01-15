from __future__ import annotations

import asyncio
import json
import pdal
import laspy
import numpy as np
import open3d as o3d

from dataclasses import dataclass
import importlib
import importlib.util
from typing import Any, Dict, List, Optional, Tuple

from pathlib import Path
from temporalio import activity
from temporalio.exceptions import ApplicationError

from lidar_app.app.config import settings
from lidar_app.app.repo import Repo
from lidar_app.app.s3_store import S3Store
from lidar_app.app.artifact_service import download_artifact


@dataclass(frozen=True)
class ClusterHeuristicsParams:
    voxel_size: float = 0.1
    nb_neighbors: int = 20
    std_ratio: float = 2.0
    plane_dist_threshold: float = 0.2
    ransac_n: int = 3
    ransac_iters: int = 1000
    normal_radius: float = 0.5
    normal_max_nn: int = 30
    vertical_z_threshold: float = 0.85
    dbscan_eps: float = 0.6
    dbscan_min_points: int = 20
    min_cluster_size: int = 30
    tall_object_height: float = 2.0


COLOR_ROAD = (0.2, 0.2, 0.2)
COLOR_CURB = (1.0, 0.0, 0.0)
COLOR_TALL = (0.0, 1.0, 0.0)
COLOR_LOW = (0.0, 0.6, 1.0)
COLOR_NOISE = (0.5, 0.5, 0.5)

CLASS_UNCLASSIFIED = 1
CLASS_GROUND = 2
CLASS_LOW_VEG = 3
CLASS_BUILDING = 6
CLASS_NOISE = 7
CLASS_KEYPOINT = 8


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

def _ensure_rgb_point_format(las: laspy.LasData) -> int:
    dims = set(las.point_format.dimension_names)
    if {"red", "green", "blue"}.issubset(dims):
        return int(las.header.point_format.id)
    upgrade_map = {0: 2, 1: 3, 4: 5, 6: 7, 9: 10}
    cur = int(las.header.point_format.id)
    return upgrade_map.get(cur, 3)


def _transfer_nn_values(points_full: np.ndarray, points_proc: np.ndarray, values_proc: np.ndarray) -> np.ndarray:
    if points_proc.shape[0] == 0:
        if values_proc.ndim == 2:
            return np.tile(np.array(COLOR_NOISE), (points_full.shape[0], 1)).astype(values_proc.dtype)
        return np.full((points_full.shape[0],), CLASS_UNCLASSIFIED, dtype=values_proc.dtype)

    scipy_spec = importlib.util.find_spec("scipy")
    if scipy_spec is not None:
        scipy_spatial = importlib.import_module("scipy.spatial")
        tree = scipy_spatial.cKDTree(points_proc)
        _, nn = tree.query(points_full, k=1, workers=-1)
        return values_proc[nn]

    pcd_proc = o3d.geometry.PointCloud(o3d.utility.Vector3dVector(points_proc))
    kdtree = o3d.geometry.KDTreeFlann(pcd_proc)

    if values_proc.ndim == 2:
        out = np.empty((points_full.shape[0], values_proc.shape[1]), dtype=values_proc.dtype)
    else:
        out = np.empty((points_full.shape[0],), dtype=values_proc.dtype)

    for i, point in enumerate(points_full):
        _, idx, _ = kdtree.search_knn_vector_3d(point, 1)
        out[i] = values_proc[idx[0]]
    return out


def _build_processing_colors_and_classes(
    points_proc: np.ndarray,
    params: ClusterHeuristicsParams,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    pcd = o3d.geometry.PointCloud(o3d.utility.Vector3dVector(points_proc))
    pcd, _ = pcd.remove_statistical_outlier(
        nb_neighbors=params.nb_neighbors,
        std_ratio=params.std_ratio,
    )

    pts = np.asarray(pcd.points)
    count = pts.shape[0]
    colors = np.tile(np.array(COLOR_NOISE, dtype=np.float32), (count, 1))
    classes = np.full((count,), CLASS_UNCLASSIFIED, dtype=np.uint8)

    if count < 50:
        return pts, colors, classes

    _, inliers = pcd.segment_plane(
        distance_threshold=params.plane_dist_threshold,
        ransac_n=params.ransac_n,
        num_iterations=params.ransac_iters,
    )

    inliers = np.asarray(inliers, dtype=np.int64)
    mask_road = np.zeros(count, dtype=bool)
    mask_road[inliers] = True

    colors[mask_road] = COLOR_ROAD
    classes[mask_road] = CLASS_GROUND

    road_pcd = pcd.select_by_index(inliers)
    objects_pcd = pcd.select_by_index(inliers, invert=True)

    if len(road_pcd.points) > 50:
        road_pcd.estimate_normals(
            search_param=o3d.geometry.KDTreeSearchParamHybrid(
                radius=params.normal_radius,
                max_nn=params.normal_max_nn,
            )
        )
        normals = np.asarray(road_pcd.normals)
        curb_mask_local = np.abs(normals[:, 2]) < params.vertical_z_threshold

        road_indices = inliers
        curb_indices = road_indices[curb_mask_local]
        colors[curb_indices] = COLOR_CURB
        classes[curb_indices] = CLASS_KEYPOINT

    if len(objects_pcd.points) > 0:
        labels = np.array(
            objects_pcd.cluster_dbscan(
                eps=params.dbscan_eps,
                min_points=params.dbscan_min_points,
                print_progress=False,
            )
        )
        obj_global_idx = np.where(~mask_road)[0]
        noise_local = np.where(labels == -1)[0]
        if noise_local.size > 0:
            classes[obj_global_idx[noise_local]] = CLASS_NOISE

        valid = labels[labels >= 0]
        if valid.size > 0:
            max_label = int(valid.max())
            for label in range(max_label + 1):
                local_idx = np.where(labels == label)[0]
                if local_idx.size < params.min_cluster_size:
                    continue
                cluster = objects_pcd.select_by_index(local_idx)
                bbox = cluster.get_axis_aligned_bounding_box()
                height = float(bbox.get_extent()[2])

                global_idx = obj_global_idx[local_idx]
                if height > params.tall_object_height:
                    colors[global_idx] = COLOR_TALL
                    classes[global_idx] = CLASS_BUILDING
                else:
                    colors[global_idx] = COLOR_LOW
                    classes[global_idx] = CLASS_LOW_VEG

    return pts, colors, classes


@activity.defn
async def extract_scale_offset(point_cloud_file: str) -> Dict[str, Any]:

    def _run():
        output_path = Path(point_cloud_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        pipeline_json = [
            {
                "type": "readers.las",
                "filename": fr"{point_cloud_file}",
            }
        ]

        pipeline_spec = json.dumps(pipeline_json)
        pipeline = pdal.Pipeline(pipeline_spec)
        try:
            pipeline.execute()
        except Exception as exc:
            raise ApplicationError(f"Failed to execute PDAL pipeline: \n{exc}")

        raw_metadata = pipeline.metadata["metadata"]
        metadata = json.loads(raw_metadata) if isinstance(raw_metadata, str) else raw_metadata

        las = laspy.read(point_cloud_file)
        return {
            "scale": list(las.header.scales),
            "offset": list(las.header.offsets),
            "metadata": metadata.get("metadata", metadata),
        }

    activity.heartbeat(
        {"stage": "extract_scale_offset", "file": point_cloud_file}
    )
    return await asyncio.to_thread(_run)


@activity.defn
async def split_into_tiles(
    input_file: str,
    output_dir: str,
    tile_size: float,
    buffer: float,
) -> Dict[str, Any]:
    def _run() -> Dict[str, Any]:
        output_root = Path(output_dir)
        output_root.mkdir(parents=True, exist_ok=True)

        pipeline_json = [
            {"type": "readers.las", "filename": input_file},
            {
                "type": "filters.splitter",
                "length": tile_size,
                "buffer": buffer,
            },
            {
                "type": "writers.las",
                "filename": str(output_root / "tile_#.laz"),
            },
        ]

        pipeline_spec = json.dumps(pipeline_json)
        pipeline = pdal.Pipeline(pipeline_spec)
        try:
            pipeline.execute()
        except Exception as exc:
            raise ApplicationError(f"Failed to split tiles: {exc}")

        generated = sorted(output_root.glob("tile_*.laz"))
        if not generated:
            raise ApplicationError("Splitter produced no tiles.")

        tiles: List[str] = []
        for tile_path in generated:
            tile_id = tile_path.stem.replace("tile_", "")
            raw_dir = output_root / f"tile_{tile_id}" / "raw"
            raw_dir.mkdir(parents=True, exist_ok=True)
            target = raw_dir / f"tile_{tile_id}.laz"
            tile_path.replace(target)
            tiles.append(str(target))

        return {"tiles": tiles}

    activity.heartbeat(
        {"stage": "split_into_tiles", "input_file": input_file}
    )
    return await asyncio.to_thread(_run)


@activity.defn
async def split_ground_offground(
    tile_path: str,
    output_dir: str,
    csf_params: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    def _run() -> Dict[str, str]:
        output_root = Path(output_dir)
        output_root.mkdir(parents=True, exist_ok=True)
        tile_id = Path(tile_path).stem.replace("tile_", "")

        ground_unclassified = output_root / "unclassified" / f"ground_tile_{tile_id}.laz"
        ground_classified = output_root / "classified" / f"ground_tile_{tile_id}.laz"
        offground_unclassified = output_root / "unclassified" / f"offground_tile_{tile_id}.laz"
        offground_classified = output_root / "classified" / f"offground_tile_{tile_id}.laz"

        ground_unclassified.parent.mkdir(parents=True, exist_ok=True)
        ground_classified.parent.mkdir(parents=True, exist_ok=True)

        csf_params = csf_params or {}
        pipeline_json = [
            {"type": "readers.las", "filename": tile_path},
            {
                "type": "filters.csf",
                **csf_params,
            },
            {
                "type": "filters.range",
                "limits": "Classification[2:2]",
            },
            {
                "type": "writers.las",
                "filename": str(ground_classified),
            },
        ]

        pipeline_spec = json.dumps(pipeline_json)
        pipeline = pdal.Pipeline(pipeline_spec)
        try:
            pipeline.execute()
        except Exception as exc:
            raise ApplicationError(f"Failed to classify ground: {exc}")

        pipeline_json = [
            {"type": "readers.las", "filename": tile_path},
            {
                "type": "filters.csf",
                **csf_params,
            },
            {
                "type": "filters.range",
                "limits": "Classification![2:2]",
            },
            {
                "type": "writers.las",
                "filename": str(offground_classified),
            },
        ]
        pipeline_spec = json.dumps(pipeline_json)
        pipeline = pdal.Pipeline(pipeline_spec)
        try:
            pipeline.execute()
        except Exception as exc:
            raise ApplicationError(f"Failed to classify off-ground: {exc}")

        ground_unclassified.write_bytes(ground_classified.read_bytes())
        offground_unclassified.write_bytes(offground_classified.read_bytes())

        return {
            "ground_unclassified": str(ground_unclassified),
            "ground_classified": str(ground_classified),
            "offground_unclassified": str(offground_unclassified),
            "offground_classified": str(offground_classified),
        }

    activity.heartbeat(
        {"stage": "split_ground_offground", "tile_path": tile_path}
    )
    return await asyncio.to_thread(_run)


@activity.defn
async def cluster_tile(
    input_file: str,
    output_file: str,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    def _run() -> Dict[str, str]:
        heuristics = ClusterHeuristicsParams(**(params or {}))
        las = laspy.read(input_file)

        points_full = np.vstack((las.x, las.y, las.z)).T.astype(np.float64)
        pcd_full = o3d.geometry.PointCloud(o3d.utility.Vector3dVector(points_full))
        if heuristics.voxel_size and heuristics.voxel_size > 0:
            pcd_proc = pcd_full.voxel_down_sample(voxel_size=heuristics.voxel_size)
        else:
            pcd_proc = pcd_full

        points_proc = np.asarray(pcd_proc.points).astype(np.float64)
        points_proc, colors_proc, class_proc = _build_processing_colors_and_classes(
            points_proc,
            heuristics,
        )

        colors_full = _transfer_nn_values(points_full, points_proc, colors_proc).astype(np.float32)
        class_full = _transfer_nn_values(points_full, points_proc, class_proc).astype(np.uint8)

        new_pf = _ensure_rgb_point_format(las)
        new_header = laspy.LasHeader(point_format=new_pf, version=las.header.version)
        new_header.scales = las.header.scales
        new_header.offsets = las.header.offsets

        new_las = laspy.LasData(new_header)
        src_dims = list(las.point_format.dimension_names)
        dst_dims = set(new_las.point_format.dimension_names)
        for dim in src_dims:
            if dim in dst_dims and dim not in ("red", "green", "blue"):
                new_las[dim] = las[dim]

        new_las.x = las.x
        new_las.y = las.y
        new_las.z = las.z

        rgb16 = np.clip(colors_full * 65535.0, 0, 65535).astype(np.uint16)
        new_las.red = rgb16[:, 0]
        new_las.green = rgb16[:, 1]
        new_las.blue = rgb16[:, 2]

        if "classification" in dst_dims:
            new_las.classification = class_full
        else:
            raise ApplicationError(
                f"Selected point_format={new_pf} has no classification dimension."
            )

        new_las.header.mins = np.min(points_full, axis=0)
        new_las.header.maxs = np.max(points_full, axis=0)

        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        new_las.write(str(output_path))
        return {"classified_file": str(output_path)}

    activity.heartbeat({"stage": "cluster_tile", "input_file": input_file})
    return await asyncio.to_thread(_run)


@activity.defn
async def crop_buffer(
    input_file: str,
    output_file: str,
    buffer: float,
    scale: List[float],
    offset: List[float],
) -> Dict[str, str]:
    def _run() -> Dict[str, str]:
        las = laspy.read(input_file)
        min_x, min_y, min_z = las.header.mins
        max_x, max_y, max_z = las.header.maxs

        crop_bounds = {
            "minx": min_x + buffer,
            "miny": min_y + buffer,
            "minz": min_z,
            "maxx": max_x - buffer,
            "maxy": max_y - buffer,
            "maxz": max_z,
        }

        bounds_str = (
            f"([{crop_bounds['minx']},{crop_bounds['maxx']}],"
            f"[{crop_bounds['miny']},{crop_bounds['maxy']}],"
            f"[{crop_bounds['minz']},{crop_bounds['maxz']}])"
        )

        pipeline_json = [
            {"type": "readers.las", "filename": input_file},
            {"type": "filters.crop", "bounds": bounds_str},
            {
                "type": "writers.las",
                "filename": output_file,
                "scale_x": scale[0],
                "scale_y": scale[1],
                "scale_z": scale[2],
                "offset_x": offset[0],
                "offset_y": offset[1],
                "offset_z": offset[2],
            },
        ]

        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        pipeline_spec = json.dumps(pipeline_json)
        pipeline = pdal.Pipeline(pipeline_spec)
        try:
            pipeline.execute()
        except Exception as exc:
            raise ApplicationError(f"Failed to crop buffer: {exc}")

        return {"cropped_tile": output_file}

    activity.heartbeat({"stage": "crop_buffer", "input_file": input_file})
    return await asyncio.to_thread(_run)


@activity.defn
async def merge_tiles(
    tiles: List[str],
    output_file: str,
    scale: List[float],
    offset: List[float],
) -> Dict[str, str]:
    def _run() -> Dict[str, str]:
        if not tiles:
            raise ApplicationError("No tiles provided for merge.")

        output_path = Path(output_file)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        pipeline_json = [{"type": "readers.las", "filename": tile} for tile in tiles]
        pipeline_json.append(
            {
                "type": "writers.las",
                "filename": output_file,
                "scale_x": scale[0],
                "scale_y": scale[1],
                "scale_z": scale[2],
                "offset_x": offset[0],
                "offset_y": offset[1],
                "offset_z": offset[2],
            }
        )
        pipeline_spec = json.dumps(pipeline_json)
        pipeline = pdal.Pipeline(pipeline_spec)
        try:
            pipeline.execute()
        except Exception as exc:
            raise ApplicationError(f"Failed to merge tiles: {exc}")

        return {"merged_file": output_file}

    activity.heartbeat({"stage": "merge_tiles", "tiles": len(tiles)})
    return await asyncio.to_thread(_run)
