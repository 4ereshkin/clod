from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List
import os

from temporalio import workflow
from temporalio.common import RetryPolicy

from point_cloud.activities.prod_reg_activities import (
    ProdRegistrationPairParams,
    prod_build_registration_anchors,
    prod_collect_registration_graph,
    prod_persist_pose_graph_solution,
    prod_propose_registration_edges,
    prod_register_pair,
    prod_solve_pose_graph,
)

VERSION = os.environ["WORKFLOW_VERSION"]

@dataclass
class ProdRegistrationWorkflowParams:
    company_id: str
    dataset_version_id: str
    schema_version: str
    max_pairs: int = 50
    crop_radius_m: float = 40.0
    global_voxel_m: float = 1.0
    cascade_voxels_m: tuple[float, float, float] = (1.0, 0.3, 0.1)
    cascade_max_corr_multipliers: tuple[float, float, float] = (3.0, 2.0, 1.5)
    min_fitness: float = 0.2
    force: bool = False


@workflow.defn(name=f"{VERSION}-registration")
class ProdRegistrationWorkflow:
    def __init__(self) -> None:
        self._stage = "init"
        self._edges: list[dict] = []

    @workflow.query
    def progress(self) -> dict:
        return {"stage": self._stage, "edges": self._edges}

    @workflow.run
    async def run(self, params: ProdRegistrationWorkflowParams) -> Dict[str, Any]:
        rp_fast = RetryPolicy(maximum_attempts=3)
        rp_once = RetryPolicy(maximum_attempts=1)

        self._stage = "build_anchors"
        graph = await workflow.execute_activity(
            prod_collect_registration_graph,
            args=[params.company_id, params.dataset_version_id, params.schema_version],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=rp_fast,
        )
        scan_ids = graph["scan_ids"]
        for scan_id in scan_ids:
            await workflow.execute_activity(
                prod_build_registration_anchors,
                args=[params.company_id, params.dataset_version_id, scan_id, params.schema_version],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=rp_fast,
            )

        self._stage = "propose_edges"
        for scan_id in scan_ids:
            await workflow.execute_activity(
                prod_propose_registration_edges,
                args=[params.company_id, params.dataset_version_id, scan_id, params.schema_version],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=rp_fast,
            )

        self._stage = "pairwise_registration"
        graph = await workflow.execute_activity(
            prod_collect_registration_graph,
            args=[params.company_id, params.dataset_version_id, params.schema_version],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=rp_fast,
        )
        edges = graph.get("edges", [])[: params.max_pairs]
        refined_edges: list[dict] = []
        for e in edges:
            pair_params = ProdRegistrationPairParams(
                source_scan_id=e["from"],
                target_scan_id=e["to"],
                schema_version=params.schema_version,
                crop_radius_m=params.crop_radius_m,
                global_voxel_m=params.global_voxel_m,
                cascade_voxels_m=params.cascade_voxels_m,
                cascade_max_corr_multipliers=params.cascade_max_corr_multipliers,
                min_fitness=params.min_fitness,
            )
            result = await workflow.execute_activity(
                prod_register_pair,
                args=[params.company_id, params.dataset_version_id, pair_params],
                start_to_close_timeout=timedelta(minutes=20),
                retry_policy=rp_once,
            )
            if result.get("accepted"):
                refined_edges.append(result["edge"])

        self._edges = refined_edges

        self._stage = "solve_pose_graph"
        graph = await workflow.execute_activity(
            prod_collect_registration_graph,
            args=[params.company_id, params.dataset_version_id, params.schema_version],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=rp_fast,
        )
        solution = await workflow.execute_activity(
            prod_solve_pose_graph,
            args=[params.company_id, params.dataset_version_id, params.schema_version, graph],
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=rp_fast,
        )

        self._stage = "persist_solution"
        persist = await workflow.execute_activity(
            prod_persist_pose_graph_solution,
            args=[
                params.company_id,
                params.dataset_version_id,
                params.schema_version,
                solution,
                params.force,
            ],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=rp_fast,
        )

        self._stage = "done"
        return {
            "pairs_total": len(edges),
            "pairs_accepted": len(refined_edges),
            "solution": persist,
        }
