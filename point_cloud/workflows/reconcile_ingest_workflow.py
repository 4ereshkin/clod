from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Any

from temporalio import workflow


@dataclass
class ReconcileIngestParams:
    limit: int = 100


@workflow.defn
class ReconcileIngestWorkflow:
    @workflow.run
    async def run(self, params: ReconcileIngestParams) -> Dict[str, Any]:
        result = await workflow.execute_activity(
            "reconcile_pending_ingest_manifests",
            args=[params.limit],
            start_to_close_timeout=timedelta(minutes=5),
        )
        return result
