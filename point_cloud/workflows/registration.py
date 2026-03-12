from __future__ import annotations

import asyncio
from typing import Any
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from shared.workflows.catalog import REGISTRATION_V1

with workflow.unsafe.imports_passed_through():
    from application.common.contracts import StatusEvent, WorkflowStatus, ScenarioResult, ResultObject


@workflow.defn(name=REGISTRATION_V1.workflow_name)
class RegistrationWorkflow:
    def __init__(self) -> None:
        self._stage: str = 'Initializing'
        self._diagnostics: dict[str, Any] = {}
        self._results: list[ResultObject] = []

    @workflow.query(name=REGISTRATION_V1.query_name)
    def progress(self) -> dict[str, Any]:
        return {
            'stage': self._stage,
            'diagnostics': self._diagnostics,
        }

    async def _prepare_scan(self, scan_id: str, scan_data: dict, base_dir: str,
                            voxel_size: float, rp_long: RetryPolicy, rp_fast: RetryPolicy) -> dict:
        cloud_s3 = scan_data["point_cloud"]["s3_key"]
        traj_s3 = scan_data.get("trajectory", {}).get("s3_key") if scan_data.get("trajectory") else None
        scan_dir = f"{base_dir}/{scan_id}"

        local_traj = None
        if traj_s3:
            local_traj = await workflow.execute_activity(
                "download_scan",  # Предполагается, что она есть в активностях
                args=[traj_s3, scan_dir, f"{scan_id}_traj.txt"],
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=rp_fast
            )

        prep_data = await workflow.execute_activity(
            "prepare_scan_for_registration",
            args=[cloud_s3, local_traj, voxel_size, scan_dir],
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=rp_long
        )
        return prep_data

    @workflow.run
    async def run(self, payload: dict[str, Any]) -> dict[str, Any]:
        # TODO: Подумать над выносом в pydantic settings
        rp_fast = RetryPolicy(maximum_attempts=3)
        rp_long = RetryPolicy(maximum_attempts=2)

        workflow_id = payload.get('workflow_id', workflow.info().workflow_id)
        scenario = payload.get("scenario", "registration")
        dataset = payload.get("dataset", {})
        params = payload.get("params", {})

        running_event = StatusEvent(
            workflow_id=workflow_id,
            scenario=scenario,
            status=WorkflowStatus.RUNNING,
            details={'message': 'Registration workflow started', 'scan_count': len(dataset)},
            timestamp=workflow.now().timestamp(),
        )
        await workflow.execute_activity(
            'publish_status_activity',
            args=[running_event.model_dump(mode='json')],
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=rp_fast,
        )

        base_work_dir = f'/tmp/registration/{workflow_id}'
        self._stage = 'Downloading and caching'

        prep_coroutines = []
        scan_ids = list(dataset.keys())

        for scan_id, scan_data in dataset.items():
            prep_coroutines.append(self._prepare_scan(
                scan_id=scan_id, scan_data=scan_data, base_dir=base_work_dir,
                voxel_size=params.get("global_voxel_m", 1.0), rp_long=rp_long, rp_fast=rp_fast
            ))

        prep_results = await asyncio.gather(*prep_coroutines)
        scans_cache = dict(zip(scan_ids, prep_results))
        self._diagnostics['cached_scans'] = len(scans_cache)

        self._stage = 'Proposing edges'
        all_anchors = {scan_id: data['anchors'] for scan_id, data in scans_cache.items()}
        proposed_edges = await workflow.execute_activity(
            'propose_edges',
            args=[all_anchors, params.get('crop_radius_m', 40.0)]
        )
        self._diagnostics['proposed_edges'] = len(proposed_edges)

        self._stage = 'Pairwise registration (ICP)'
        icp_coroutines = []
        for edge in proposed_edges:
            sid_from, sid_to = edge["from"], edge["to"]
            path_from = scans_cache[sid_from]["downsampled_cloud_path"]
            path_to = scans_cache[sid_to]["downsampled_cloud_path"]

            icp_coroutines.append(workflow.execute_activity(
                "register_pair",
                args=[path_from, path_to, edge, params],
                start_to_close_timeout=timedelta(minutes=20),
                retry_policy=rp_long
            ))

        icp_results = await asyncio.gather(*icp_coroutines)
        refined_edges = [res['edge'] for res in icp_results if res.get('accepted')]
        self._diagnostics['accepted_edges'] = len(refined_edges)

        self._stage = 'Solving Pose Graph'
        graph = {
            'scan_ids': scan_ids,
            'edges': refined_edges,
        }

        solution = await workflow.execute_activity(
            'solve_pose_graph',
            args=[graph, params],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=rp_fast
        )

        self._diagnostics['poses'] = solution['poses']

        self._stage = 'Saving Manifest'

        manifest_data = {
            'workflow_id': workflow_id,
            'scenario': scenario,
            'registration': {
                'metrics': solution.get('diagnostics', {}),
                'poses': solution.get('poses', {}),
                'graph_edges': refined_edges
            }
        }

        manifest_local = f'{base_work_dir}/registration_manifest.json'

        await workflow.execute_activity(
            'save_dict_to_json',
            args=[manifest_data, manifest_local],
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=rp_fast
        )

        manifest_s3_key = f'manifests/{workflow_id}_registration_manifest.json'

        manifest_upload_result = await workflow.execute_activity(
            'upload_s3_object',
            args=[manifest_local, manifest_s3_key],
            start_to_close_timeout=timedelta(minutes=5),
            retry_policy=rp_fast
        )

        self._results.append(ResultObject(
            kind='manifest.registration',
            s3_key=manifest_s3_key,
            etag=manifest_upload_result.get('etag', '')
        ))

        self._stage = 'Completed'

        completed_event = ScenarioResult(
            workflow_id=workflow_id,
            scenario=scenario,
            status=WorkflowStatus.COMPLETED,
            outputs=self._results,
            details=self._diagnostics,
            timestamp=workflow.now().timestamp()
        )

        await workflow.execute_activity(
            'publish_status_activity',
            args=[completed_event.model_dump(mode='json')],
            start_to_close_timeout=timedelta(seconds=10),
            retry_policy=rp_fast
        )

        return {
            'outputs': [r.model_dump(mode='json') for r in self._results],
            'diagnostics': self._diagnostics
        }

