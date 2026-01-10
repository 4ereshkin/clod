from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy


@dataclass
class RegistrationPrepParams:
    scan_ids: List[str]
    voxel_size: float = 0.5
    apply_centering: bool = True
    estimate_normals: bool = True
    denoise: bool = False


@dataclass
class RegistrationPairParams:
    source_scan_id: str
    target_scan_id: str
    voxel_size: float = 0.5
    ransac_distance_multiplier: float = 1.5
    icp_max_distance_multiplier: float = 1.0
    min_fitness: float = 0.15
    max_inlier_rmse: float = 2.5


@dataclass
class RegistrationWorkflowParams:
    scan_ids: List[str]
    voxel_size: float = 0.5
    apply_centering: bool = True
    estimate_normals: bool = True
    denoise: bool = False
    min_fitness: float = 0.15
    max_inlier_rmse: float = 2.5


@workflow.defn(name="registration-coarse-to-fine")
class RegistrationCoarseToFineWorkflow:
    """
    Example registration workflow inspired by a coarse-to-fine pipeline:
    1) Per-scan preprocessing
    2) Pairwise global registration (FPFH + RANSAC)
    3) Pairwise ICP refinement

    Note: This workflow intentionally omits N-scan pose graph optimization and
    dataset merge to match the requested scope.
    """

    def __init__(self) -> None:
        self._stage = "init"
        self._edges: list[dict] = []

    @workflow.query
    def progress(self) -> dict:
        return {"stage": self._stage, "edges": self._edges}

    @workflow.run
    async def run(self, params: RegistrationWorkflowParams) -> Dict[str, Any]:
        rp_fast = RetryPolicy(maximum_attempts=3)
        rp_once = RetryPolicy(maximum_attempts=1)

        self._stage = "preprocess_scans"
        prep_params = RegistrationPrepParams(
            scan_ids=params.scan_ids,
            voxel_size=params.voxel_size,
            apply_centering=params.apply_centering,
            estimate_normals=params.estimate_normals,
            denoise=params.denoise,
        )
        await workflow.execute_activity(
            "registration_preprocess_scans",
            args=[prep_params],
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=rp_fast,
        )

        self._stage = "candidate_pairs"
        candidate_pairs: List[Dict[str, str]] = await workflow.execute_activity(
            "registration_select_candidate_pairs",
            args=[params.scan_ids],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=rp_fast,
        )

        self._stage = "pairwise_registration"
        edges: list[dict] = []
        for pair in candidate_pairs:
            pair_params = RegistrationPairParams(
                source_scan_id=pair["source"],
                target_scan_id=pair["target"],
                voxel_size=params.voxel_size,
                min_fitness=params.min_fitness,
                max_inlier_rmse=params.max_inlier_rmse,
            )
            result = await workflow.execute_activity(
                "registration_pairwise_coarse_to_fine",
                args=[pair_params],
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=rp_once,
            )
            if result.get("accepted"):
                edges.append(result)

        self._edges = edges
        self._stage = "done"
        return {
            "edges": edges,
            "pairs_total": len(candidate_pairs),
            "pairs_accepted": len(edges),
        }
