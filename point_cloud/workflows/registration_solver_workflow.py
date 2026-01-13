from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Any

import os
from temporalio import workflow
from temporalio.common import RetryPolicy

VERSION = 'MVP'

@dataclass
class RegistrationSolverParams:
    company_id: str
    dataset_version_id: str
    schema_version: str = "1.1.0"
    force: bool = False

@workflow.defn(name=f"{VERSION}-registration-solver")
class RegistrationSolverWorkflow:
    def __init__(self) -> None:
        self._stage = "init"

    @workflow.query
    def progress(self) -> dict:
        return {"stage": self._stage}

    @workflow.run
    async def run(self, params: RegistrationSolverParams) -> Dict[str, Any]:
        rp_fast = RetryPolicy(maximum_attempts=3)
        rp_once = RetryPolicy(maximum_attempts=1)

        self._stage = "collect_graph"
        graph = await workflow.execute_activity(
            "collect_registration_graph",
            args=[params.company_id, params.dataset_version_id, params.schema_version],
            start_to_close_timeout=timedelta(seconds=60),
            retry_policy=rp_fast,
        )

        self._stage = "icp_refine"
        await workflow.execute_activity(
            "refine_edges_with_icp",
            args=[params.company_id, params.dataset_version_id, params.schema_version, ],
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=rp_once,
        )

        self._stage = "solve"
        solved = await workflow.execute_activity(
            "solve_pose_graph",
            args=[params.company_id, params.dataset_version_id, params.schema_version, graph],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=rp_once,
        )

        self._stage = "persist"
        out = await workflow.execute_activity(
            "persist_pose_graph_solution",
            args=[params.company_id, params.dataset_version_id, params.schema_version, solved, params.force],
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=rp_fast,
        )

        self._stage = "export_laz"
        merged = await workflow.execute_activity(
            "export_merged_laz",
            args=[params.company_id, params.dataset_version_id, params.schema_version],
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=rp_once,
        )

        self._stage = "done"
        return out
