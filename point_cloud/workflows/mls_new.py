from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import Dict, Any, Optional, List

import yaml
from temporalio import workflow
from temporalio.common import RetryPolicy

from point_cloud.workflows.ingest_workflow import IngestWorkflowParams
from point_cloud.workflows.registration_solver_workflow import RegistrationSolverParams

with open(r'D:\1_prod\point_cloud\config.yaml', 'r') as f:
    VERSION = yaml.safe_load(f.read())['VERSION_INFO']['WORKFLOW_VERSION']

@dataclass
class MlsPipelineParams:
    company_id: str
    dataset_name: str
    bump_version: bool
    dataset_crs_id: str              # CRS датасета (фикс)
    target_srs: str                  # куда репроецируем (например "EPSG:4326")
    schema_version: str = "1.1.0"
    force: bool = False
    artifacts: Optional[List[Dict[str, str]]] = None  # raw.* local paths для ingest

@workflow.defn(name=f"{VERSION}-mls-pipeline")
class MlsPipelineWorkflow:
    def __init__(self) -> None:
        self._stage = "init"
        self._scan_id: Optional[str] = None
        self._dataset_version_id: Optional[str] = None

        self._registration_started: bool = False
        self._registration_workflow_id: Optional[str] = None

    @workflow.query
    def progress(self) -> dict:
        return {"stage": self._stage,
                "scan_id": self._scan_id,
                "dataset_version_id": self._dataset_version_id,
                "registration_started": self._registration_started,
                "registration_workflow_id": self._registration_workflow_id,}

    @workflow.signal
    async def trigger_registration(self, force: bool = False) -> None:
        # сигнал пришёл — помечаем, что регистрацию хотят
        self._registration_started = True

        # если ещё нет dataset_version_id — просто ждём, запуск будет в конце run()
        if self._dataset_version_id is None:
            return

        # если уже запущено — ничего не делаем
        if self._registration_workflow_id is not None:
            return

        await self._start_registration_solver(force=force)

    async def _start_registration_solver(self, force: bool) -> None:
        # Стабильный workflow_id, чтобы повторные сигналы не создавали дубликаты
        wf_id = f"reg-{self._dataset_version_id}"
        self._registration_workflow_id = wf_id

        params = RegistrationSolverParams(
            company_id=self._company_id,  # зададим ниже в run()
            dataset_version_id=self._dataset_version_id,
            schema_version=self._schema_version,
            force=force,
        )

        # Вариант А: стартуем как отдельный child workflow (привязан к родителю)
        await workflow.start_child_workflow(
            f"{VERSION}-registration-solver",
            params,
            id=wf_id,
            task_queue="point-cloud-task-queue",
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

    @workflow.run
    async def run(self, params: MlsPipelineParams) -> Dict[str, Any]:
        # сохраняем для сигналов
        self._company_id = params.company_id
        self._schema_version = params.schema_version

        self._stage = "child: ingest"
        ingest_params = IngestWorkflowParams(
            company_id=params.company_id,
            dataset_name=params.dataset_name,
            bump_version=params.bump_version,
            crs_id=params.dataset_crs_id,
            schema_version=params.schema_version,
            force=params.force,
            artifacts=params.artifacts or [],
        )

        ingest_res = await workflow.execute_child_workflow(
            f"{VERSION}-ingest",
            ingest_params,
            task_queue="point-cloud-task-queue",
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

        self._scan_id = ingest_res["scan_id"]
        self._dataset_version_id = ingest_res["dataset_version_id"]

        self._stage = "resolve srs"
        in_srs = await workflow.execute_activity(
            "resolve_crs_to_pdal_srs",
            args=[params.dataset_crs_id],  # "CGCS2000"
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        self._stage = "reproject"
        await workflow.execute_activity(
            "reproject_scan_to_target_crs",
            args=[
                params.company_id,
                self._dataset_version_id,
                self._scan_id,
                params.schema_version,
                in_srs,  # теперь "EPSG:4490"
                params.target_srs,  # например "EPSG:4326"
            ],
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

        self._stage = "preprocess"
        await workflow.execute_activity(
            'preprocess_point_cloud',
            args = [
                params.company_id,
                self._dataset_version_id,
                self._scan_id,
                params.schema_version,
            ],
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )

        self._stage = "anchors"
        await workflow.execute_activity(
            "build_registration_anchors",
            args=[params.company_id, self._dataset_version_id, self._scan_id, params.schema_version],
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        self._stage = "edges(dataset)"
        await workflow.execute_activity(
            "propose_registration_edges_for_dataset",
            args=[params.company_id, self._dataset_version_id, params.schema_version],
            start_to_close_timeout=timedelta(minutes=15),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )


        # если сигнал уже приходил до того как мы получили dataset_version_id — запускаем solver сейчас
        if self._registration_started and self._registration_workflow_id is None:
            self._stage = "signal: start registration"
            await self._start_registration_solver(force=False)

        self._stage = "done"
        return {
            "scan_id": self._scan_id,
            "dataset_version_id": self._dataset_version_id,
            "registration_workflow_id": self._registration_workflow_id,
        }