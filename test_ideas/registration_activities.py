from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, List

from temporalio import activity

from test_ideas.registration_workflow import (
    RegistrationPairParams,
    RegistrationPrepParams,
)


@activity.defn
async def registration_preprocess_scans(params: RegistrationPrepParams) -> Dict[str, Any]:
    """
    Placeholder for per-scan preprocessing:
    - optional centering
    - voxel downsampling
    - normal estimation
    - optional denoise
    """
    activity.heartbeat({"stage": "preprocess", "scan_count": len(params.scan_ids)})
    return {
        "prepared_scan_ids": list(params.scan_ids),
        "options": asdict(params),
    }


@activity.defn
async def registration_select_candidate_pairs(scan_ids: List[str]) -> List[Dict[str, str]]:
    """
    Minimal candidate overlap graph for pairwise registration.
    Uses a simple consecutive pairing for now.
    """
    activity.heartbeat({"stage": "candidate_pairs", "scan_count": len(scan_ids)})
    if len(scan_ids) < 2:
        return []
    pairs = []
    for idx in range(len(scan_ids) - 1):
        pairs.append({"source": scan_ids[idx], "target": scan_ids[idx + 1]})
    return pairs


@activity.defn
async def registration_pairwise_coarse_to_fine(params: RegistrationPairParams) -> Dict[str, Any]:
    """
    Placeholder for coarse-to-fine registration:
    - global registration via FPFH + RANSAC
    - quality gate via fitness/inlier_rmse
    - ICP refinement (point-to-plane)
    """
    activity.heartbeat(
        {
            "stage": "pairwise",
            "source": params.source_scan_id,
            "target": params.target_scan_id,
        }
    )

    return {
        "source": params.source_scan_id,
        "target": params.target_scan_id,
        "accepted": True,
        "fitness": params.min_fitness,
        "inlier_rmse": min(params.max_inlier_rmse, 0.5),
        "transform": {
            "t": [0.0, 0.0, 0.0],
            "R": [
                [1.0, 0.0, 0.0],
                [0.0, 1.0, 0.0],
                [0.0, 0.0, 1.0],
            ],
        },
        "params": asdict(params),
    }
