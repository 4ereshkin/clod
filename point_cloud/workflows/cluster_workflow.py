from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional
import os

from temporalio import workflow

VERSION = os.environ["WORKFLOW_VERSION"] = 'MVP-plus'
SCHEMA_VERSION = '1.1.0'
# SCHEMA_VERSION = os.environ['SCHEMA_VERSION']


@dataclass
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


@dataclass
class ClusterPipelineParams:
    dataset_version_id: str
    schema_version: str = SCHEMA_VERSION
    dst_dir: str = "point_cloud/tmp/cluster"
    tile_size: float = 50.0
    splitter_buffer: float = 3.0
    csf_params: Dict[str, Any] = field(default_factory=dict)
    cluster_params: ClusterHeuristicsParams = field(default_factory=ClusterHeuristicsParams)


@workflow.defn(name=f'{VERSION}_cluster')
class ClusterPipeline:
    def __init__(self):
        self._stage = 'Init'

    @workflow.query
    async def progress(self):
        return {
            'stage': self._stage,
        }

    @workflow.run
    async def run(self, params: ClusterPipelineParams):
        self._stage = "Download merged point cloud"
        dataset_dir = Path(params.dst_dir) / params.dataset_version_id
        raw_dir = dataset_dir / "raw"

        merged_cloud = await workflow.execute_activity(
            "download_dataset_version_artifact",
            args=[
                params.dataset_version_id,
                "derived.merged_point_cloud",
                params.schema_version,
                str(raw_dir),
            ],
            start_to_close_timeout=timedelta(minutes=30)
        )

        self._stage = "Extract scale/offset"
        meta = await workflow.execute_activity(
            "extract_scale_offset",
            args=[merged_cloud["local_path"]],
            start_to_close_timeout=timedelta(minutes=30)
        )

        self._stage = "Split into tiles"
        tiles_result = await workflow.execute_activity(
            "split_into_tiles",
            args=[
                merged_cloud["local_path"],
                str(dataset_dir / "tiles"),
                params.tile_size,
                params.splitter_buffer,
            ],
            start_to_close_timeout=timedelta(minutes=30)
        )

        self._stage = "Process tiles"
        tiles: List[str] = tiles_result["tiles"]
        cropped_tiles: List[str] = []
        for tile in tiles:
            tile_id = Path(tile).stem.replace("tile_", "")
            tile_root = dataset_dir / "tiles" / f"tile_{tile_id}"
            ground_dir = tile_root / "ground"

            ground_split = await workflow.execute_activity(
                "split_ground_offground",
                args=[tile, str(ground_dir), params.csf_params],
                start_to_close_timeout=timedelta(minutes=30)
            )

            classified_inputs = [
                ground_split["ground_classified"],
                ground_split["offground_classified"],
            ]
            classified_outputs: List[str] = []
            for classified_input in classified_inputs:
                classified_output = classified_input
                cluster_result = await workflow.execute_activity(
                    "cluster_tile",
                    args=[
                        classified_input,
                        classified_output,
                        params.cluster_params.__dict__,
                    ],
                    start_to_close_timeout=timedelta(minutes=30)
                )
                classified_outputs.append(cluster_result["classified_file"])

            merged_tile = tile_root / "ground" / "classified" / f"merged_tile_{tile_id}.laz"
            await workflow.execute_activity(
                "merge_tiles",
                args=[
                    classified_outputs,
                    str(merged_tile),
                    meta["scale"],
                    meta["offset"],
                ],
                start_to_close_timeout=timedelta(minutes=30)
            )

            crop_output = tile_root / "cropped" / f"tile_{tile_id}.laz"
            crop_result = await workflow.execute_activity(
                "crop_buffer",
                args=[
                    str(merged_tile),
                    str(crop_output),
                    params.splitter_buffer,
                    meta["scale"],
                    meta["offset"],
                ],
                start_to_close_timeout=timedelta(minutes=30)
            )
            cropped_tiles.append(crop_result["cropped_tile"])

        self._stage = "Merge classified tiles"
        derived_dir = dataset_dir / "derived"
        merged_output = derived_dir / f"merged_classified_{params.dataset_version_id}.laz"
        merged_result = await workflow.execute_activity(
            "merge_tiles",
            args=[
                cropped_tiles,
                str(merged_output),
                meta["scale"],
                meta["offset"],
            ],
            start_to_close_timeout=timedelta(minutes=30)
        )

        self._stage = "Done"
        return {
            "merged_file": merged_result["merged_file"],
            "tile_count": len(tiles),
        }
