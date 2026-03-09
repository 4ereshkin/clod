from __future__ import annotations

import os
import asyncio
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import List, Dict, Any

from temporalio import workflow
from temporalio.common import RetryPolicy


@dataclass
class DownloadRequest:
    key: str
    dst_dir: str
    part_id: str


@dataclass
class DownloadWorkflowParams:
    requests: List[DownloadRequest]


@workflow.defn(name='IngestDownloadWorkflow')
class IngestDownloadWorkflow:
    @workflow.run
    async def run(self, params: DownloadWorkflowParams) -> Dict[str, str]:
        rp_fast = RetryPolicy(maximum_attempts=3)

        coroutines = []
        part_ids = []

        for req in params.requests:
            coro = workflow.execute_activity(
                'download_s3_object',
                args=[req.key, req.dst_dir],
                start_to_close_timeout=timedelta(hours=1),
                retry_policy=rp_fast,
            )
            coroutines.append(coro)
            part_ids.append(req.part_id)

        results = await asyncio.gather(*coroutines)
        output = {}
        for part_id, result in zip(part_ids, results):
            output[part_id] = result

        return output


@dataclass
class ProfilingWorkflowParams:
    local_cloud_path: str
    meta_s3_key: str
    hexbin_s3_key: str
    stats_s3_key: str

@workflow.defn(name='IngestProfilingWorkflow')
class IngestProfilingWorkflow:
    @workflow.run
    async def run(self, params: ProfilingWorkflowParams) -> Dict[str, dict[str, str]]:
        rp_fast = RetryPolicy(maximum_attempts=3)

        # Формируем пути с помощью pathlib и ОБЯЗАТЕЛЬНО конвертируем обратно в str
        cloud_path_obj = Path(params.local_cloud_path)
        base_dir = cloud_path_obj.parent
        base_name = cloud_path_obj.stem

        local_meta = str(base_dir / f"{base_name}_meta.json")
        local_hexbin = str(base_dir / f"{base_name}_hexbin.geojson")
        local_stats = str(base_dir / f"{base_name}_stats.json")

        # Так как cloud_path_obj - это Path, а нам нужна строка,
        # можно передавать params.local_cloud_path (так как это строка)
        cloud_path_str = params.local_cloud_path

        meta_dict = await workflow.execute_activity(
            "point_cloud_meta",
            args=[cloud_path_str, local_hexbin],
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=rp_fast,
        )

        await workflow.execute_activity(
            "save_dict_to_json",
            args=[meta_dict, local_meta],
            start_to_close_timeout=timedelta(minutes=1),
            retry_policy=rp_fast,
        )

        await workflow.execute_activity(
            "compute_point_cloud_stats",
            args=[cloud_path_str, local_stats],
            start_to_close_timeout=timedelta(minutes=30),
            retry_policy=rp_fast,
        )

        upload_tasks = {
            "metadata": workflow.execute_activity(
                "upload_s3_object",
                args=[local_meta, params.meta_s3_key],
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=rp_fast,
            ),
            "hexbin": workflow.execute_activity(
                "upload_s3_object",
                args=[local_hexbin, params.hexbin_s3_key],
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=rp_fast,
            ),
            "statistics": workflow.execute_activity(
                "upload_s3_object",
                args=[local_stats, params.stats_s3_key],
                start_to_close_timeout=timedelta(minutes=10),
                retry_policy=rp_fast,
            )
        }

        keys = list(upload_tasks.keys())
        coroutines = list(upload_tasks.values())

        results = await asyncio.gather(*coroutines)

        output = {}
        for key, result in zip(keys, results):
            output[key] = result

        return output


@dataclass
class ReprojectWorkflowParams:
    local_path: str
    in_srs: str
    out_srs: str
    output_s3_key: str


@workflow.defn(name='IngestReprojectWorkflow')
class IngestReprojectWorkflow:
    @workflow.run
    async def run(self, params: ReprojectWorkflowParams) -> dict[str, str]:
        rp_fast = RetryPolicy(maximum_attempts=3)
        rp_long = RetryPolicy(maximum_attempts=2)

        reprojected_path = await workflow.execute_activity(
            "reproject_to_copc",
            args=[params.local_path, params.in_srs, params.out_srs],
            start_to_close_timeout=timedelta(hours=2),
            retry_policy=rp_long,
        )

        if not reprojected_path:
            raise RuntimeError(f'Reprojection failed for {params.local_path}')

        upload_result = await workflow.execute_activity(
            'upload_s3_object',
            args=[reprojected_path, params.output_s3_key],
            start_to_close_timeout=timedelta(hours=1),
            retry_policy=rp_fast,
        )

        return upload_result