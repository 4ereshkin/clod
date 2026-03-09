from __future__ import annotations

import json

from typing import Any
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy


from shared.workflows.catalog import INGEST_V1

with workflow.unsafe.imports_passed_through():
    from point_cloud.workflows.ingest_child_workflows import (
        DownloadWorkflowParams, DownloadRequest,
        ProfilingWorkflowParams,
        ReprojectWorkflowParams,
    )
    from application.common.contracts import StatusEvent, WorkflowStatus, ResultObject, ScenarioResult

TARGET_CRS = "EPSG:4326"

@workflow.defn(name=INGEST_V1.workflow_name)
class IngestWorkflow:
    def __init__(self) -> None:
        self._stage: str = 'Initializing'
        self._errors: dict[str, str] = {}
        self._results: list[dict[str, Any]] = []

    @workflow.query(name=INGEST_V1.query_name)
    def progress(self) -> dict[str, Any]:
        return {
            'stage': self._stage,
            'errors': self._errors,
            'results': self._results
        }

    @workflow.run
    async def run(self, payload: dict[str, Any]):
        # TODO: Подумать над выносом в pydantic settings
        rp_fast = RetryPolicy(maximum_attempts=3)
        rp_long = RetryPolicy(maximum_attempts=2)

        workflow_id = payload.get('workflow_id', workflow.info().workflow_id)
        scenario = payload.get("scenario", "ingest")

        running_event = StatusEvent(
            workflow_id=workflow_id,
            scenario=scenario,
            status=WorkflowStatus.RUNNING,
            details={"message": "Ingest workflow started"},
            timestamp=workflow.now().timestamp()  # <-- Добавь эту строку!
        )

        await workflow.execute_activity(
            "publish_status_activity",
            args=[running_event.model_dump(mode='json')],
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(maximum_attempts=3)
        )

        dataset = payload.get('dataset', {})
        scan_count = len(dataset)

        georeference = 'NO' if scan_count < 5 else 'YES'

        manifest_data = {
            'georeference': georeference,
            'profiling_scan_count': scan_count,
            'reproject': 'YES',
            'target_crs': TARGET_CRS,
            'scans': {}
        }

        base_work_dir = f'/tmp/ingest/{workflow_id}'

        self._stage = 'Preparing downloads'
        download_requests = []
        for scan_id, scan_data in dataset.items():
            # Собираем облака (пока только point_cloud)
            # В будущем тут можно добавить trajectory и control_point
            clouds = scan_data.get("point_cloud", {})
            for part_id, cloud_info in clouds.items():
                s3_key = cloud_info["s3_key"]

                req = DownloadRequest(
                    key=s3_key,
                    dst_dir=f"{base_work_dir}/{scan_id}",
                    part_id=f"{scan_id}_{part_id}"
                )
                download_requests.append(req)

        self._stage = 'Downloading files'
        download_results = await workflow.execute_child_workflow(
            "IngestDownloadWorkflow",
            DownloadWorkflowParams(requests=download_requests),
            id=f"{workflow_id}-download",
            retry_policy=rp_long,
        )

        for scan_id, scan_data in dataset.items():
            self._stage = f"Processing scan {scan_id}"
            scan_manifest = {
                "profiling": {},
                "reprojected": {}
            }

            clouds = scan_data.get("point_cloud", {})
            for part_id, cloud_info in clouds.items():
                local_path = download_results.get(f"{scan_id}_{part_id}")
                if not local_path:
                    continue

                # Исходный CRS (берем из JSON)
                in_crs_dict = cloud_info.get("crs", {})
                in_crs_str = json.dumps(in_crs_dict) if in_crs_dict else "EPSG:4326"

                # Формируем ключи для новых артефактов в S3 (например, рядом с исходным файлом)
                base_s3_dir = cloud_info["s3_key"].rsplit("/", 1)[0]
                cloud_name = cloud_info["s3_key"].rsplit("/", 1)[-1].split(".")[0]

                # 2.1 Профилирование
                self._stage = f"Profiling {scan_id}_{part_id}"
                prof_result = await workflow.execute_child_workflow(
                    "IngestProfilingWorkflow",
                    ProfilingWorkflowParams(
                        local_cloud_path=local_path,
                        meta_s3_key=f"{base_s3_dir}/profiling/{cloud_name}_meta.json",
                        hexbin_s3_key=f"{base_s3_dir}/profiling/{cloud_name}_hexbin.geojson",
                        stats_s3_key=f"{base_s3_dir}/profiling/{cloud_name}_stats.json"
                    ),
                    id=f"{workflow_id}-prof-{scan_id}-{part_id}",
                    retry_policy=rp_long,
                )
                scan_manifest["profiling"][part_id] = prof_result

                # Добавляем результаты профилирования в общий вывод (чтобы UseCase их получил)
                for kind, res in prof_result.items():
                    res_obj = {"kind": f"profiling.{kind}", **res}
                    self._results.append(res_obj)

                # 2.2 Репроекция
                self._stage = f"Reprojecting {scan_id}_{part_id}"
                repr_s3_key = f"{base_s3_dir}/reprojected/{cloud_name}_copc.laz"

                repr_result = await workflow.execute_child_workflow(
                    "IngestReprojectWorkflow",
                    ReprojectWorkflowParams(
                        local_path=local_path,
                        in_srs=in_crs_str,
                        out_srs=TARGET_CRS,
                        output_s3_key=repr_s3_key
                    ),
                    id=f"{workflow_id}-repr-{scan_id}-{part_id}",
                    retry_policy=rp_long,
                )

                scan_manifest["reprojected"][part_id] = repr_result
                # Добавляем репроецированное облако в общий вывод
                self._results.append({"kind": "point_cloud.copc", **repr_result})

            manifest_data["scans"][scan_id] = scan_manifest

            # --- 3. Сохранение манифеста ---
            self._stage = "Uploading manifest"
            # Для простоты можно прямо тут сбросить манифест в файл и вызвать upload_s3_object
            manifest_local = f"{base_work_dir}/ingest_manifest.json"

            # Активность, которую мы уже писали!
            await workflow.execute_activity(
                "save_dict_to_json",
                args=[manifest_data, manifest_local],
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=rp_fast,
            )

            manifest_s3_key = f"manifests/{workflow_id}_manifest.json"
            manifest_upload_result = await workflow.execute_activity(
                "upload_s3_object",
                args=[manifest_local, manifest_s3_key],
                start_to_close_timeout=timedelta(minutes=5),
                retry_policy=rp_fast,
            )

            self._results.append({"kind": "manifest", **manifest_upload_result})

            self._stage = "Completed"

            outputs = []
            for res in self._results:
                outputs.append(ResultObject(
                    kind=res.get("kind", "unknown"),
                    s3_key=res.get("s3_key", ""),
                    etag=res.get("etag", "")
                ))

            completed_event = ScenarioResult(
                workflow_id=workflow_id,
                scenario=scenario,
                status=WorkflowStatus.COMPLETED,
                outputs=outputs,
                details={"manifest": manifest_data},
                timestamp=workflow.now().timestamp()  # <-- И эту строку здесь!
            )

            await workflow.execute_activity(
                "publish_completed_activity",
                args=[completed_event.model_dump(mode='json')],
                start_to_close_timeout=timedelta(seconds=10),
                retry_policy=RetryPolicy(maximum_attempts=3)
            )

            return {
                "outputs": self._results,  # Это попадёт в ScenarioResult.outputs
                "manifest": manifest_data
            }