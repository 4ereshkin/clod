"""
Workflow for orchestration of point cloud processing.

This workflow coordinates a sequence of activities to extract metadata
from raw point cloud files, reproject them into a common spatial
reference system, insert the results into a database and optionally
generate 3D tiles.  The workflow is designed to be idempotent and
replayable, making use of Temporal's deterministic execution model and
retriable activities.

The workflow accepts a list of file paths and optional parameters for
coordinate system transformation and database configuration.  It
returns a summary of the operations performed, including the paths of
the reprojected files and whether tile generation succeeded for each.
"""

from __future__ import annotations

import json
import time
import asyncio
from datetime import timedelta
from dataclasses import dataclass
from typing import List, Dict, Any
from temporalio import workflow
from temporalio.common import RetryPolicy


@dataclass
class MlsPipelineParams:
    file_paths: List[str]
    in_srs: str = "EPSG:4490"
    out_srs: str = "EPSG:4326"
    db_config_path: str = "db.json"
    generate_tiles: bool = False


@workflow.defn
class MlsPipelineWorkflow:
    @workflow.run
    async def run(self, params: MlsPipelineParams) -> Dict[str, Any]:

        # Разворачиваем параметры во внутренние переменные
        file_paths = params.file_paths
        in_srs = params.in_srs
        out_srs = params.out_srs
        db_config_path = params.db_config_path
        generate_tiles = params.generate_tiles

        # 1. Метаданные по всем файлам
        meta_result = await workflow.execute_activity(
            "load_metadata_for_files",
            args=[file_paths],
            heartbeat_timeout=timedelta(seconds=15),
            schedule_to_close_timeout=timedelta(seconds=600),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        # 2. Репроекция каждого файла (параллельно)
        reproject_futures = [workflow.execute_activity("reproject_file",
                args=[file_path, in_srs, out_srs],
                heartbeat_timeout=timedelta(minutes=10),
                schedule_to_close_timeout=timedelta(hours=3),
                retry_policy=RetryPolicy(maximum_attempts=1)) for file_path in file_paths]
        reproject_futures = []
        for file_path in file_paths:
            fut = workflow.execute_activity(
                "reproject_file",
                args=[file_path, in_srs, out_srs],
                heartbeat_timeout=timedelta(minutes=10),
                schedule_to_close_timeout=timedelta(hours=3),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
            reproject_futures.append(fut)

        reproject_results = await asyncio.gather(*reproject_futures)
        # отбрасываем неудачные репроекции (если вернули None/False)
        reprojected_files = [res for res in reproject_results if res]

        # 3. Вставка в БД (по одному, чтобы не валить коннект)
        for file_path in reprojected_files:
            await workflow.execute_activity(
                "insert_file_into_db",
                args=[file_path, db_config_path],
                schedule_to_close_timeout=timedelta(hours=3),
                retry_policy=RetryPolicy(maximum_attempts=2),
            )

        # 4. Опционально делаем 3D Tiles
        tiles_results: List[bool] = []  # инициализируем всегда, чтобы не было UnboundLocalError
        if generate_tiles:
            tile_futures = []
            for file_path in reprojected_files:
                fut = workflow.execute_activity(
                    "convert_to_tileset",
                    args=[file_path, "cesium_tiles"],
                    schedule_to_close_timeout=timedelta(seconds=3600),
                    retry_policy=RetryPolicy(maximum_attempts=3),
                )
                tile_futures.append(fut)
            tiles_results = await asyncio.gather(*tile_futures)

        # 5. Возвращаем аккуратный JSON-совместимый результат
        return {
            "metadata": meta_result,
            "reprojected_files": reprojected_files,
            "tiles_generated": tiles_results,
        }
